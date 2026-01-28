import fs from 'fs/promises'
import {createWriteStream} from 'fs'
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
import {getUserAgentString} from '../shared/user-agent'
import {getGitHubWorkspaceDir} from '../shared/config'
import {internalArtifactTwirpClient} from '../shared/artifact-twirp-client'
import {
  GetSignedArtifactURLRequest,
  Int64Value,
  ListArtifactsRequest
} from '../../generated'
import {getBackendIdsFromToken} from '../shared/util'
import {ArtifactNotFoundError} from '../shared/errors'

function formatError(error: unknown): string {
  if (error instanceof Error) {
    const name = error.name || 'Error'
    const message = error.message || String(error)
    const stack = error.stack ? `\n${error.stack}` : ''
    return `${name}: ${message}${stack}`
  }

  try {
    return `Non-Error throw: ${JSON.stringify(error)}`
  } catch {
    return `Non-Error throw: ${String(error)}`
  }
}

function createHashingTransform(
  hash: crypto.Hash,
  onChunk?: (chunkBytes: number) => void
): stream.Transform {
  return new stream.Transform({
    transform(chunk, _encoding, callback) {
      try {
        onChunk?.((chunk as Buffer).length)
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
  const maxAttempts = 5
  let lastError: unknown
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await streamExtractExternal(url, directory)
    } catch (error) {
      lastError = error
      core.info(
        `Artifact streamExtract attempt ${attempt}/${maxAttempts} failed for ${scrubQueryParameters(
          url
        )} -> ${directory}: ${formatError(error)}`
      )

      if (attempt < maxAttempts) {
        core.info(`Retrying in 5 seconds...`)
        await new Promise(resolve => setTimeout(resolve, 5000))
      }
    }
  }

  throw new Error(
    `Artifact download failed after ${maxAttempts} attempts. Last error: ${formatError(
      lastError
    )}`
  )
}

async function streamDownload(
  url: string,
  filePath: string
): Promise<StreamExtractResponse> {
  const maxAttempts = 5
  let lastError: unknown
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await streamDownloadExternal(url, filePath)
    } catch (error) {
      lastError = error
      core.info(
        `Artifact streamDownload attempt ${attempt}/${maxAttempts} failed for ${scrubQueryParameters(
          url
        )} -> ${filePath}: ${formatError(error)}`
      )

      if (attempt < maxAttempts) {
        core.info(`Retrying in 5 seconds...`)
        await new Promise(resolve => setTimeout(resolve, 5000))
      }
    }
  }

  throw new Error(
    `Artifact download failed after ${maxAttempts} attempts. Last error: ${formatError(
      lastError
    )}`
  )
}

export async function streamExtractExternal(
  url: string,
  directory: string,
  opts: {timeout: number} = {timeout: 30 * 1000}
): Promise<StreamExtractResponse> {
  const client = new httpClient.HttpClient(getUserAgentString())
  core.info(
    `Downloading artifact zip from blob storage: ${scrubQueryParameters(
      url
    )} -> extract to ${directory} (timeout=${opts.timeout}ms)`
  )

  let response: httpClient.HttpClientResponse
  try {
    response = await client.get(url)
  } catch (error) {
    core.debug(
      `HTTP GET to blob storage failed before response for ${scrubQueryParameters(
        url
      )}: ${formatError(error)}`
    )
    throw error
  }

  core.info(
    `Blob storage response: ${response.message.statusCode} ${response.message.statusMessage} ` +
      `(content-type=${response.message.headers?.['content-type'] ?? 'unknown'}, ` +
      `content-length=${response.message.headers?.['content-length'] ?? 'unknown'})`
  )
  if (response.message.statusCode !== 200) {
    throw new Error(
      `Unexpected HTTP response from blob storage: ${response.message.statusCode} ${response.message.statusMessage}`
    )
  }

  let sha256Digest: string | undefined = undefined

  return new Promise((resolve, reject) => {
    let bytesRead = 0
    const previewLimitBytes = 512
    let preview = Buffer.alloc(0)
    const contentType = response.message.headers?.['content-type'] ?? 'unknown'
    const contentLength =
      response.message.headers?.['content-length'] ?? 'unknown'

    const timerFn = (): void => {
      const timeoutError = new Error(
        `Blob storage chunk did not respond in ${opts.timeout}ms (bytesRead=${bytesRead})`
      )
      core.warning(
        `Timeout while downloading from blob storage: ${scrubQueryParameters(
          url
        )} (bytesRead=${bytesRead}, content-type=${contentType}, content-length=${contentLength})`
      )
      response.message.destroy(timeoutError)
      reject(timeoutError)
    }
    const timer = setTimeout(timerFn, opts.timeout)

    const hash = crypto.createHash('sha256')
    const hashingStream = createHashingTransform(hash)

    const extractor = unzip.Extract({path: directory})

    extractor.on('error', (error: Error) => {
      const previewHex = preview.length ? preview.toString('hex') : '<empty>'
      const previewUtf8 = preview.length
        ? preview
            .toString('utf8')
            .replace(new RegExp('[^\\x20-\\x7E\\r\\n\\t]', 'g'), '.')
        : '<empty>'

      core.warning(
        `unzip.Extract error while extracting to ${directory} (bytesRead=${bytesRead}, content-type=${contentType}, content-length=${contentLength}). ` +
          `First ${preview.length} bytes (hex)=${previewHex} (utf8)=${previewUtf8}. ` +
          `Error: ${formatError(error)}`
      )
      clearTimeout(timer)
      reject(
        new Error(
          `Not a valid zip file (content-type=${contentType}, content-length=${contentLength}, bytesRead=${bytesRead})`
        )
      )
    })

    response.message
      .on('data', (chunk: Buffer) => {
        bytesRead += chunk.length
        if (preview.length < previewLimitBytes) {
          const remaining = previewLimitBytes - preview.length
          preview = Buffer.concat([preview, chunk.subarray(0, remaining)])
        }
        timer.refresh()
      })
      .on('error', (error: Error) => {
        core.debug(
          `response.message error while downloading from blob storage (bytesRead=${bytesRead}): ${formatError(
            error
          )}`
        )
        clearTimeout(timer)
        reject(error)
      })
      .pipe(hashingStream)
      .pipe(extractor)
      .on('close', () => {
        clearTimeout(timer)
        sha256Digest = hash.digest('hex')
        core.info(
          `SHA256 digest of downloaded artifact is ${sha256Digest} (bytesRead=${bytesRead})`
        )
        resolve({sha256Digest: `sha256:${sha256Digest}`})
      })
  })
}

export async function streamDownloadExternal(
  url: string,
  filePath: string,
  opts: {timeout: number} = {timeout: 30 * 1000}
): Promise<StreamExtractResponse> {
  const client = new httpClient.HttpClient(getUserAgentString())
  core.info(
    `Downloading artifact zip from blob storage: ${scrubQueryParameters(
      url
    )} -> ${filePath} (timeout=${opts.timeout}ms)`
  )

  let response: httpClient.HttpClientResponse
  try {
    response = await client.get(url)
  } catch (error) {
    core.debug(
      `HTTP GET to blob storage failed before response for ${scrubQueryParameters(
        url
      )}: ${formatError(error)}`
    )
    throw error
  }

  core.info(
    `Blob storage response: ${response.message.statusCode} ${response.message.statusMessage} ` +
      `(content-type=${response.message.headers?.['content-type'] ?? 'unknown'}, ` +
      `content-length=${response.message.headers?.['content-length'] ?? 'unknown'})`
  )
  if (response.message.statusCode !== 200) {
    throw new Error(
      `Unexpected HTTP response from blob storage: ${response.message.statusCode} ${response.message.statusMessage}`
    )
  }

  await fs.mkdir(path.dirname(filePath), {recursive: true})

  return new Promise((resolve, reject) => {
    let bytesRead = 0
    const contentType = response.message.headers?.['content-type'] ?? 'unknown'
    const contentLength =
      response.message.headers?.['content-length'] ?? 'unknown'
    const timerFn = (): void => {
      const timeoutError = new Error(
        `Blob storage chunk did not respond in ${opts.timeout}ms (bytesRead=${bytesRead})`
      )
      core.warning(
        `Timeout while downloading from blob storage: ${scrubQueryParameters(
          url
        )} (bytesRead=${bytesRead}, content-type=${contentType}, content-length=${contentLength})`
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
      .on('data', (chunk: Buffer) => {
        bytesRead += chunk.length
        timer.refresh()
      })
      .on('error', (error: Error) => {
        core.debug(
          `response.message error while downloading from blob storage (bytesRead=${bytesRead}): ${formatError(
            error
          )}`
        )
        clearTimeout(timer)
        reject(error)
      })
      .pipe(hashingStream)
      .pipe(out)
      .on('close', () => {
        clearTimeout(timer)
        const sha256Digest = hash.digest('hex')
        core.info(
          `SHA256 digest of downloaded artifact is ${sha256Digest} (bytesRead=${bytesRead})`
        )
        resolve({sha256Digest: `sha256:${sha256Digest}`})
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

  const {headers, status} = await api.rest.actions.downloadArtifact({
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

  const {location} = headers
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
        ? `Unable to download and extract artifact: ${formatError(error)}`
        : `Unable to download artifact: ${formatError(error)}`
    )
  }

  return {downloadPath: resolvedDownloadPath, digestMismatch}
}

export async function downloadArtifactInternal(
  artifactId: number,
  options?: DownloadArtifactOptions
): Promise<DownloadArtifactResponse> {
  const downloadPath = await resolveOrCreateDirectory(options?.path)

  const artifactClient = internalArtifactTwirpClient()

  let digestMismatch = false

  const {workflowRunBackendId, workflowJobRunBackendId} =
    getBackendIdsFromToken()

  const listReq: ListArtifactsRequest = {
    workflowRunBackendId,
    workflowJobRunBackendId,
    idFilter: Int64Value.create({value: artifactId.toString()})
  }

  const {artifacts} = await artifactClient.ListArtifacts(listReq)

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

  const {signedUrl} = await artifactClient.GetSignedArtifactURL(signedReq)

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
        ? `Unable to download and extract artifact: ${formatError(error)}`
        : `Unable to download artifact: ${formatError(error)}`
    )
  }

  return {downloadPath: resolvedDownloadPath, digestMismatch}
}

async function resolveOrCreateDirectory(
  downloadPath = getGitHubWorkspaceDir()
): Promise<string> {
  if (!(await exists(downloadPath))) {
    core.debug(
      `Artifact destination folder does not exist, creating: ${downloadPath}`
    )
    await fs.mkdir(downloadPath, {recursive: true})
  } else {
    core.debug(`Artifact destination folder already exists: ${downloadPath}`)
  }

  return downloadPath
}
