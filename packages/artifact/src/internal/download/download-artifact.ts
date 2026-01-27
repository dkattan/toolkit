import fs from 'fs/promises'
import { createWriteStream } from 'fs'
import * as crypto from 'crypto'
import * as stream from 'stream'
import * as path from 'path'

import * as github from '@actions/github'
import * as core from '@actions/core'
import * as httpClient from '@actions/http-client'
import unzip from 'unzip-stream'
import {
  DownloadArtifactOptions,
  DownloadArtifactResponse,
  StreamExtractResponse
} from '../shared/interfaces'
import { getUserAgentString } from '../shared/user-agent'
import { getGitHubWorkspaceDir } from '../shared/config'
import { internalArtifactTwirpClient } from '../shared/artifact-twirp-client'
import {
  GetSignedArtifactURLRequest,
  Int64Value,
  ListArtifactsRequest
} from '../../generated'
import { getBackendIdsFromToken } from '../shared/util'
import { ArtifactNotFoundError } from '../shared/errors'

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

function normalizeExpectedHash(expectedHash: string): string {
  if (expectedHash.startsWith('sha256:')) {
    return expectedHash
  }
  return `sha256:${expectedHash}`
}

const scrubQueryParameters = (url: string): string => {
  const parsed = new URL(url)
  parsed.search = ''
  return parsed.toString()
}

async function exists(path: string): Promise<boolean> {
  try {
    await fs.access(path)
    return true
  } catch (error) {
    if (error.code === 'ENOENT') {
      return false
    } else {
      throw error
    }
  }
}

async function streamExtract(
  url: string,
  directory: string
): Promise<StreamExtractResponse> {
  let retryCount = 0
  while (retryCount < 5) {
    try {
      return await streamExtractExternal(url, directory)
    } catch (error) {
      retryCount++
      core.debug(
        `Failed to download artifact after ${retryCount} retries due to ${error.message}. Retrying in 5 seconds...`
      )
      // wait 5 seconds before retrying
      await new Promise(resolve => setTimeout(resolve, 5000))
    }
  }

  throw new Error(`Artifact download failed after ${retryCount} retries.`)
}

async function streamDownload(
  url: string,
  filePath: string
): Promise<StreamExtractResponse> {
  let retryCount = 0
  while (retryCount < 5) {
    try {
      return await streamDownloadExternal(url, filePath)
    } catch (error) {
      retryCount++
      core.debug(
        `Failed to download artifact after ${retryCount} retries due to ${error.message}. Retrying in 5 seconds...`
      )
      // wait 5 seconds before retrying
      await new Promise(resolve => setTimeout(resolve, 5000))
    }
  }

  throw new Error(`Artifact download failed after ${retryCount} retries.`)
}

export async function streamExtractExternal(
  url: string,
  directory: string,
  opts: { timeout: number } = { timeout: 30 * 1000 }
): Promise<StreamExtractResponse> {
  const client = new httpClient.HttpClient(getUserAgentString())
  const response = await client.get(url)
  if (response.message.statusCode !== 200) {
    throw new Error(
      `Unexpected HTTP response from blob storage: ${response.message.statusCode} ${response.message.statusMessage}`
    )
  }

  let sha256Digest: string | undefined = undefined

  return new Promise((resolve, reject) => {
    const timerFn = (): void => {
      const timeoutError = new Error(
        `Blob storage chunk did not respond in ${opts.timeout}ms`
      )
      response.message.destroy(timeoutError)
      reject(timeoutError)
    }
    const timer = setTimeout(timerFn, opts.timeout)

    const hash = crypto.createHash('sha256')
    const hashingStream = createHashingTransform(hash)

    response.message
      .on('data', () => {
        timer.refresh()
      })
      .on('error', (error: Error) => {
        core.debug(
          `response.message: Artifact download failed: ${error.message}`
        )
        clearTimeout(timer)
        reject(error)
      })
      .pipe(hashingStream)
      .pipe(unzip.Extract({ path: directory }))
      .on('close', () => {
        clearTimeout(timer)
        sha256Digest = hash.digest('hex')
        core.info(`SHA256 digest of downloaded artifact is ${sha256Digest}`)
        resolve({ sha256Digest: `sha256:${sha256Digest}` })
      })
      .on('error', (error: Error) => {
        reject(error)
      })
  })
}

export async function streamDownloadExternal(
  url: string,
  filePath: string,
  opts: { timeout: number } = { timeout: 30 * 1000 }
): Promise<StreamExtractResponse> {
  const client = new httpClient.HttpClient(getUserAgentString())
  const response = await client.get(url)
  if (response.message.statusCode !== 200) {
    throw new Error(
      `Unexpected HTTP response from blob storage: ${response.message.statusCode} ${response.message.statusMessage}`
    )
  }

  await fs.mkdir(path.dirname(filePath), { recursive: true })

  return new Promise((resolve, reject) => {
    const timerFn = (): void => {
      const timeoutError = new Error(
        `Blob storage chunk did not respond in ${opts.timeout}ms`
      )
      response.message.destroy(timeoutError)
      reject(timeoutError)
    }
    const timer = setTimeout(timerFn, opts.timeout)

    const hash = crypto.createHash('sha256')
    const hashingStream = createHashingTransform(hash)
    const out = createWriteStream(filePath)

    out.on('error', error => {
      clearTimeout(timer)
      reject(error)
    })

    response.message
      .on('data', () => {
        timer.refresh()
      })
      .on('error', (error: Error) => {
        clearTimeout(timer)
        reject(error)
      })
      .pipe(hashingStream)
      .pipe(out)
      .on('close', () => {
        clearTimeout(timer)
        const sha256Digest = hash.digest('hex')
        core.info(`SHA256 digest of downloaded artifact is ${sha256Digest}`)
        resolve({ sha256Digest: `sha256:${sha256Digest}` })
      })
  })
}

export async function downloadArtifactPublic(
  artifactId: number,
  repositoryOwner: string,
  repositoryName: string,
  token: string,
  options?: DownloadArtifactOptions
): Promise<DownloadArtifactResponse> {
  const downloadPath = await resolveOrCreateDirectory(options?.path)

  const api = github.getOctokit(token)

  let digestMismatch = false

  core.info(
    `Downloading artifact '${artifactId}' from '${repositoryOwner}/${repositoryName}'`
  )

  const { headers, status } = await api.rest.actions.downloadArtifact({
    owner: repositoryOwner,
    repo: repositoryName,
    artifact_id: artifactId,
    archive_format: 'zip',
    request: {
      redirect: 'manual'
    }
  })

  if (status !== 302) {
    throw new Error(`Unable to download artifact. Unexpected status: ${status}`)
  }

  const { location } = headers
  if (!location) {
    throw new Error(`Unable to redirect to artifact download url`)
  }

  core.info(
    `Redirecting to blob download url: ${scrubQueryParameters(location)}`
  )

  const unzipArtifact = options?.unzip !== false
  const resolvedDownloadPath = unzipArtifact
    ? downloadPath
    : path.join(downloadPath, `${artifactId}.zip`)

  try {
    core.info(`Starting download of artifact to: ${resolvedDownloadPath}`)
    const downloadResponse = unzipArtifact
      ? await streamExtract(location, downloadPath)
      : await streamDownload(location, resolvedDownloadPath)

    core.info(`Artifact download completed successfully.`)
    if (options?.expectedHash) {
      const normalizedExpected = normalizeExpectedHash(options.expectedHash)
      if (normalizedExpected !== downloadResponse.sha256Digest) {
        digestMismatch = true
        core.debug(`Computed digest: ${downloadResponse.sha256Digest}`)
        core.debug(`Expected digest: ${normalizedExpected}`)
      }
    }
  } catch (error) {
    throw new Error(
      unzipArtifact
        ? `Unable to download and extract artifact: ${error.message}`
        : `Unable to download artifact: ${error.message}`
    )
  }

  return { downloadPath: resolvedDownloadPath, digestMismatch }
}

export async function downloadArtifactInternal(
  artifactId: number,
  options?: DownloadArtifactOptions
): Promise<DownloadArtifactResponse> {
  const downloadPath = await resolveOrCreateDirectory(options?.path)

  const artifactClient = internalArtifactTwirpClient()

  let digestMismatch = false

  const { workflowRunBackendId, workflowJobRunBackendId } =
    getBackendIdsFromToken()

  const listReq: ListArtifactsRequest = {
    workflowRunBackendId,
    workflowJobRunBackendId,
    idFilter: Int64Value.create({ value: artifactId.toString() })
  }

  const { artifacts } = await artifactClient.ListArtifacts(listReq)

  if (artifacts.length === 0) {
    throw new ArtifactNotFoundError(
      `No artifacts found for ID: ${artifactId}\nAre you trying to download from a different run? Try specifying a github-token with \`actions:read\` scope.`
    )
  }

  if (artifacts.length > 1) {
    core.warning('Multiple artifacts found, defaulting to first.')
  }

  const signedReq: GetSignedArtifactURLRequest = {
    workflowRunBackendId: artifacts[0].workflowRunBackendId,
    workflowJobRunBackendId: artifacts[0].workflowJobRunBackendId,
    name: artifacts[0].name
  }

  const { signedUrl } = await artifactClient.GetSignedArtifactURL(signedReq)

  core.info(
    `Redirecting to blob download url: ${scrubQueryParameters(signedUrl)}`
  )

  const unzipArtifact = options?.unzip !== false
  const resolvedDownloadPath = unzipArtifact
    ? downloadPath
    : path.join(downloadPath, artifacts[0].name)

  try {
    core.info(`Starting download of artifact to: ${resolvedDownloadPath}`)
    const downloadResponse = unzipArtifact
      ? await streamExtract(signedUrl, downloadPath)
      : await streamDownload(signedUrl, resolvedDownloadPath)

    core.info(`Artifact download completed successfully.`)
    if (options?.expectedHash) {
      const normalizedExpected = normalizeExpectedHash(options.expectedHash)
      if (normalizedExpected !== downloadResponse.sha256Digest) {
        digestMismatch = true
        core.debug(`Computed digest: ${downloadResponse.sha256Digest}`)
        core.debug(`Expected digest: ${normalizedExpected}`)
      }
    }
  } catch (error) {
    throw new Error(
      unzipArtifact
        ? `Unable to download and extract artifact: ${error.message}`
        : `Unable to download artifact: ${error.message}`
    )
  }

  return { downloadPath: resolvedDownloadPath, digestMismatch }
}

async function resolveOrCreateDirectory(
  downloadPath = getGitHubWorkspaceDir()
): Promise<string> {
  if (!(await exists(downloadPath))) {
    core.debug(
      `Artifact destination folder does not exist, creating: ${downloadPath}`
    )
    await fs.mkdir(downloadPath, { recursive: true })
  } else {
    core.debug(`Artifact destination folder already exists: ${downloadPath}`)
  }

  return downloadPath
}
