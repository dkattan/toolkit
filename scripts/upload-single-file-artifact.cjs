/*
 * Upload a single file as a GitHub Actions artifact without zipping.
 *
 * Uses the dkattan/toolkit fork of @actions/artifact, which adds:
 *   uploadArtifact(..., { zip: false })
 */

"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const process = require("node:process");

function fail(message) {
  console.error(message);
  process.exit(1);
}

function formatPartialSecret(value) {
  if (!value) {
    return "<unset>";
  }

  const s = String(value);
  const len = s.length;
  if (len <= 12) {
    return `${s.slice(0, 2)}…${s.slice(-2)} (len=${len})`;
  }

  return `${s.slice(0, 6)}…${s.slice(-6)} (len=${len})`;
}

function maybePrintRuntimeEnvDebug() {
  if (process.env.SINGLE_ARTIFACT_DEBUG_ENV !== "1") {
    return;
  }

  // Intentionally print only a small prefix/suffix; do NOT print the full token.
  console.log(
    `ACTIONS_RUNTIME_TOKEN=${formatPartialSecret(process.env.ACTIONS_RUNTIME_TOKEN)}`,
  );
  console.log(
    `ACTIONS_RUNTIME_URL=${formatPartialSecret(process.env.ACTIONS_RUNTIME_URL)}`,
  );
  console.log(
    `ACTIONS_RESULTS_URL=${formatPartialSecret(process.env.ACTIONS_RESULTS_URL)}`,
  );
}

async function sha256FileHex(filePath) {
  const hash = crypto.createHash("sha256");
  await new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath);
    stream.on("data", chunk => hash.update(chunk));
    stream.on("error", reject);
    stream.on("end", resolve);
  });
  return hash.digest("hex");
}

async function uploadSingleFileArtifact() {
  maybePrintRuntimeEnvDebug();

  const artifactName = process.env.SINGLE_ARTIFACT_NAME;
  const artifactFile = process.env.SINGLE_ARTIFACT_FILE;

  if (!artifactName) {
    throw new Error("Missing env var SINGLE_ARTIFACT_NAME");
  }

  if (!artifactFile) {
    throw new Error("Missing env var SINGLE_ARTIFACT_FILE");
  }

  const artifactFileAbs = path.resolve(artifactFile);
  if (!fs.existsSync(artifactFileAbs)) {
    throw new Error(`File not found: ${artifactFileAbs}`);
  }

  const sha256 = await sha256FileHex(artifactFileAbs);
  console.log(`SHA256(upload)=${sha256}`);

  const retentionDaysRaw = process.env.SINGLE_ARTIFACT_RETENTION_DAYS;
  const retentionDays = retentionDaysRaw ? Number(retentionDaysRaw) : undefined;
  if (
    retentionDaysRaw &&
    (!Number.isFinite(retentionDays) || retentionDays <= 0)
  ) {
    throw new Error(
      `Invalid SINGLE_ARTIFACT_RETENTION_DAYS='${retentionDaysRaw}' (expected a positive number)`,
    );
  }

  const artifactModulePath = path.resolve(
    __dirname,
    "../packages/artifact/lib/artifact.js",
  );

  let DefaultArtifactClient;
  try {
    ({ DefaultArtifactClient } = require(artifactModulePath));
  }
  catch (error) {
    const originalErrorMessage = error && error.message ? error.message : String(error);
    throw new Error(
      [
        `Unable to load dkattan/toolkit @actions/artifact build output at ${artifactModulePath}.`,
        "Ensure the repo is built (npm ci && npm run bootstrap && npm run build).",
        `Original error: ${originalErrorMessage}`,
      ].join(" "),
    );
  }

  if (typeof DefaultArtifactClient !== "function") {
    throw new TypeError(
      `dkattan/toolkit artifact module did not export DefaultArtifactClient (path: ${artifactModulePath})`,
    );
  }

  const rootDirectory = path.dirname(artifactFileAbs);

  // NOTE: zip:false is a fork-only option.
  const options = {
    zip: false,
    ...(retentionDays ? { retentionDays } : {}),
  };

  const client = new DefaultArtifactClient();
  const res = await client.uploadArtifact(
    artifactName,
    [artifactFileAbs],
    rootDirectory,
    options,
  );

  const id = res && typeof res.id !== "undefined" ? String(res.id) : "<unknown>";
  const digest = res && res.digest ? String(res.digest) : "<unknown>";
  const size = res && typeof res.size !== "undefined" ? String(res.size) : "<unknown>";

  console.log(
    `Uploaded single-file artifact '${artifactName}': id=${id} size=${size} digest=${digest}`,
  );

  // Best-effort: print a clickable artifact link.
  const repo = process.env.GITHUB_REPOSITORY;
  const runId = process.env.GITHUB_RUN_ID;
  if (repo && runId && id !== "<unknown>") {
    console.log(
      `Artifact URL: https://github.com/${repo}/actions/runs/${runId}/artifacts/${id}`,
    );
  }
}

module.exports = {
  uploadSingleFileArtifact,
};

if (require.main === module) {
  uploadSingleFileArtifact().catch((error) => {
    fail(error && error.stack ? error.stack : String(error));
  });
}
