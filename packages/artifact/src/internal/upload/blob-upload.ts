import {BlobClient, BlockBlobUploadStreamOptions} from '@azure/storage-blob'
import {TransferProgressEvent} from '@azure/core-http-compat'
import {ZipUploadStream} from './zip'
import {
  getUploadChunkSize,
  getConcurrency,
  getUploadChunkTimeout
} from '../shared/config'
import * as core from '@actions/core'
import * as crypto from 'crypto'
import * as stream from 'stream'
import {NetworkError} from '../shared/errors'
import * as fs from 'fs'

function createHashingTransform(hash: crypto.Hash): stream.Transform {
  return new stream.Transform({
    transform(chunk, _encoding, callback) {
      try {
        hash.update(chunk as Buffer)
        callback(null, chunk)
      } catch (error) {
        callback(error as Error)
      }
    }
  })
}

export interface BlobUploadResponse {
  /**
   * The total reported upload size in bytes. Empty if the upload failed
   */
  uploadSize?: number

  /**
   * The SHA256 hash of the uploaded file. Empty if the upload failed
   */
  sha256Hash?: string
}

export async function uploadZipToBlobStorage(
  authenticatedUploadURL: string,
  zipUploadStream: ZipUploadStream
): Promise<BlobUploadResponse> {
  let uploadByteCount = 0
  let lastProgressTime = Date.now()
  const abortController = new AbortController()

  const chunkTimer = async (interval: number): Promise<void> =>
    new Promise((resolve, reject) => {
      const timer = setInterval(() => {
        if (Date.now() - lastProgressTime > interval) {
          reject(new Error('Upload progress stalled.'))
        }
      }, interval)

      abortController.signal.addEventListener('abort', () => {
        clearInterval(timer)
        resolve()
      })
    })

  const maxConcurrency = getConcurrency()
  const bufferSize = getUploadChunkSize()
  const blobClient = new BlobClient(authenticatedUploadURL)
  const blockBlobClient = blobClient.getBlockBlobClient()

  core.debug(
    `Uploading artifact zip to blob storage with maxConcurrency: ${maxConcurrency}, bufferSize: ${bufferSize}`
  )

  const uploadCallback = (progress: TransferProgressEvent): void => {
    core.info(`Uploaded bytes ${progress.loadedBytes}`)
    uploadByteCount = progress.loadedBytes
    lastProgressTime = Date.now()
  }

  const options: BlockBlobUploadStreamOptions = {
    blobHTTPHeaders: {blobContentType: 'application/zip'},
    onProgress: uploadCallback,
    abortSignal: abortController.signal
  }

  const hash = crypto.createHash('sha256')
  const uploadStream = zipUploadStream.pipe(createHashingTransform(hash))

  core.info('Beginning upload of artifact content to blob storage')

  try {
    await Promise.race([
      blockBlobClient.uploadStream(
        uploadStream,
        bufferSize,
        maxConcurrency,
        options
      ),
      chunkTimer(getUploadChunkTimeout())
    ])
  } catch (error) {
    if (NetworkError.isNetworkErrorCode(error?.code)) {
      throw new NetworkError(error?.code)
    }
    throw error
  } finally {
    abortController.abort()
  }

  core.info('Finished uploading artifact content to blob storage!')

  const sha256Hash = hash.digest('hex')
  core.info(`SHA256 digest of uploaded artifact zip is ${sha256Hash}`)

  if (uploadByteCount === 0) {
    core.warning(
      `No data was uploaded to blob storage. Reported upload byte count is 0.`
    )
  }
  return {
    uploadSize: uploadByteCount,
    sha256Hash
  }
}

export async function uploadFileToBlobStorage(
  authenticatedUploadURL: string,
  filePath: string
): Promise<BlobUploadResponse> {
  let uploadByteCount = 0
  let lastProgressTime = Date.now()
  const abortController = new AbortController()

  const chunkTimer = async (interval: number): Promise<void> =>
    new Promise((resolve, reject) => {
      const timer = setInterval(() => {
        if (Date.now() - lastProgressTime > interval) {
          reject(new Error('Upload progress stalled.'))
        }
      }, interval)

      abortController.signal.addEventListener('abort', () => {
        clearInterval(timer)
        resolve()
      })
    })

  const maxConcurrency = getConcurrency()
  const bufferSize = getUploadChunkSize()
  const blobClient = new BlobClient(authenticatedUploadURL)
  const blockBlobClient = blobClient.getBlockBlobClient()

  core.debug(
    `Uploading single file to blob storage with maxConcurrency: ${maxConcurrency}, bufferSize: ${bufferSize}`
  )

  const uploadCallback = (progress: TransferProgressEvent): void => {
    core.info(`Uploaded bytes ${progress.loadedBytes}`)
    uploadByteCount = progress.loadedBytes
    lastProgressTime = Date.now()
  }

  const options: BlockBlobUploadStreamOptions = {
    blobHTTPHeaders: {blobContentType: 'application/octet-stream'},
    onProgress: uploadCallback,
    abortSignal: abortController.signal
  }

  const fileStream = fs.createReadStream(filePath)
  const hash = crypto.createHash('sha256')
  const uploadStream = fileStream.pipe(createHashingTransform(hash))

  core.info('Beginning upload of single file to blob storage')

  try {
    await Promise.race([
      blockBlobClient.uploadStream(
        uploadStream,
        bufferSize,
        maxConcurrency,
        options
      ),
      chunkTimer(getUploadChunkTimeout())
    ])
  } catch (error) {
    if (NetworkError.isNetworkErrorCode(error?.code)) {
      throw new NetworkError(error?.code)
    }
    throw error
  } finally {
    abortController.abort()
  }

  core.info('Finished uploading single file to blob storage!')

  const sha256Hash = hash.digest('hex')
  core.info(`SHA256 digest of uploaded file is ${sha256Hash}`)

  if (uploadByteCount === 0) {
    core.warning(
      `No data was uploaded to blob storage. Reported upload byte count is 0.`
    )
  }
  return {
    uploadSize: uploadByteCount,
    sha256Hash
  }
}
