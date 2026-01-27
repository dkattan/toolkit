import * as core from '@actions/core'
import {
  UploadArtifactOptions,
  UploadArtifactResponse
} from '../shared/interfaces'
import {getExpiration} from './retention'
import {validateArtifactName} from './path-and-artifact-name-validation'
import {internalArtifactTwirpClient} from '../shared/artifact-twirp-client'
import {
  UploadZipSpecification,
  getUploadZipSpecification,
  validateRootDirectory
} from './upload-zip-specification'
import {getBackendIdsFromToken} from '../shared/util'
import {uploadZipToBlobStorage, uploadFileToBlobStorage} from './blob-upload'
import {createZipUploadStream} from './zip'
import {
  CreateArtifactRequest,
  FinalizeArtifactRequest,
  StringValue
} from '../../generated'
import {FilesNotFoundError, InvalidResponseError} from '../shared/errors'

export async function uploadArtifact(
  name: string,
  files: string[],
  rootDirectory: string,
  options?: UploadArtifactOptions | undefined
): Promise<UploadArtifactResponse> {
  validateArtifactName(name)
  validateRootDirectory(rootDirectory)

  const zipSpecification: UploadZipSpecification[] = getUploadZipSpecification(
    files,
    rootDirectory
  )
  if (zipSpecification.length === 0) {
    throw new FilesNotFoundError(
      zipSpecification.flatMap(s => (s.sourcePath ? [s.sourcePath] : []))
    )
  }

  // get the IDs needed for the artifact creation
  const backendIds = getBackendIdsFromToken()

  // create the artifact client
  const artifactClient = internalArtifactTwirpClient()

  // create the artifact
  const createArtifactReq: CreateArtifactRequest = {
    workflowRunBackendId: backendIds.workflowRunBackendId,
    workflowJobRunBackendId: backendIds.workflowJobRunBackendId,
    name,
    version: 4
  }

  // if there is a retention period, add it to the request
  const expiresAt = getExpiration(options?.retentionDays)
  if (expiresAt) {
    createArtifactReq.expiresAt = expiresAt
  }

  const createArtifactResp =
    await artifactClient.CreateArtifact(createArtifactReq)
  if (!createArtifactResp.ok) {
    throw new InvalidResponseError(
      'CreateArtifact: response from backend was not ok'
    )
  }

  // Upload to blob storage - handle both zip and direct file uploads
  let uploadResult: {uploadSize?: number; sha256Hash?: string} = {}
  if (options?.zip === false) {
    if (files.length !== 1) {
      throw new Error(
        'zip=false is only supported when exactly one file is provided'
      )
    }

    if (zipSpecification.length !== 1 || !zipSpecification[0].sourcePath) {
      throw new Error(
        'zip=false is only supported for a single file (directories are not supported)'
      )
    }

    // Direct file upload when zip=false and exactly one file
    core.info('Uploading single file directly without zipping')
    const filePath = zipSpecification[0].sourcePath
    uploadResult = await uploadFileToBlobStorage(
      createArtifactResp.signedUploadUrl,
      filePath
    )
  } else {
    // Default behavior: create zip and upload
    const zipUploadStream = await createZipUploadStream(
      zipSpecification,
      options?.compressionLevel
    )

    // Upload zip to blob storage
    uploadResult = await uploadZipToBlobStorage(
      createArtifactResp.signedUploadUrl,
      zipUploadStream
    )
  }

  // finalize the artifact
  const finalizeArtifactReq: FinalizeArtifactRequest = {
    workflowRunBackendId: backendIds.workflowRunBackendId,
    workflowJobRunBackendId: backendIds.workflowJobRunBackendId,
    name,
    size: uploadResult.uploadSize ? uploadResult.uploadSize.toString() : '0'
  }

  if (uploadResult.sha256Hash) {
    finalizeArtifactReq.hash = StringValue.create({
      value: `sha256:${uploadResult.sha256Hash}`
    })
  }

  core.info(`Finalizing artifact upload`)

  const finalizeArtifactResp =
    await artifactClient.FinalizeArtifact(finalizeArtifactReq)
  if (!finalizeArtifactResp.ok) {
    throw new InvalidResponseError(
      'FinalizeArtifact: response from backend was not ok'
    )
  }

  const artifactId = BigInt(finalizeArtifactResp.artifactId)
  core.info(
    `Artifact ${name}${options?.zip === false && files.length === 1 ? '' : '.zip'} successfully finalized. Artifact ID ${artifactId}`
  )

  return {
    size: uploadResult.uploadSize,
    digest: uploadResult.sha256Hash,
    id: Number(artifactId)
  }
}
