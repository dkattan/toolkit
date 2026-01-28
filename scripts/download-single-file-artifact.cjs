/*
 * Download a single GitHub Actions artifact and optionally verify its SHA-256.
 *
 * Uses the dkattan/toolkit fork build output of @actions/artifact.
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

async function downloadSingleFileArtifact() {
  maybePrintRuntimeEnvDebug();

  const artifactName = process.env.SINGLE_ARTIFACT_NAME;
  const downloadDir = process.env.SINGLE_ARTIFACT_DOWNLOAD_DIR;
  const expectedSha256 = process.env.SINGLE_ARTIFACT_EXPECTED_SHA256;

  if (!artifactName) {
    throw new Error("Missing env var SINGLE_ARTIFACT_NAME");
  }
  if (!downloadDir) {
    throw new Error("Missing env var SINGLE_ARTIFACT_DOWNLOAD_DIR");
  }

  const downloadDirAbs = path.resolve(downloadDir);
  fs.mkdirSync(downloadDirAbs, { recursive: true });

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

  const client = new DefaultArtifactClient();

  const getRes = await client.getArtifact(artifactName);
  const artifactId = getRes && getRes.artifact ? getRes.artifact.id : undefined;
  if (!artifactId || !Number.isFinite(artifactId) || artifactId <= 0) {
    throw new Error(`Unable to resolve a numeric artifact id for '${artifactName}'`);
  }

  // Best-effort: print a clickable artifact link.
  const repo = process.env.GITHUB_REPOSITORY;
  const runId = process.env.GITHUB_RUN_ID;
  if (repo && runId) {
    console.log(
      `Artifact URL: https://github.com/${repo}/actions/runs/${runId}/artifacts/${String(artifactId)}`,
    );
  }

  // IMPORTANT:
  // - Upload action uses zip:false (single file is stored as raw bytes).
  // - Therefore download should use unzip:false (the response is not a ZIP archive).
  // - downloadArtifact() may return either a directory (unzipped) or a file path (raw).
  const res = await client.downloadArtifact(artifactId, {
    path: downloadDirAbs,
    unzip: false,
  });
  const downloadPath = res && res.downloadPath ? res.downloadPath : downloadDirAbs;

  let fileToHash;
  if (fs.existsSync(downloadPath) && fs.statSync(downloadPath).isFile()) {
    fileToHash = downloadPath;
  }
  else {
    const entries = fs.readdirSync(downloadPath, { withFileTypes: true });
    const files = entries
      .filter(e => e.isFile())
      .map(e => path.join(downloadPath, e.name));

    if (files.length === 0) {
      throw new Error(
        `Downloaded artifact '${artifactName}' to '${downloadPath}' but found no files.`,
      );
    }

    if (files.length !== 1) {
      console.warn(
        `::warning::Expected a single file in downloaded artifact '${artifactName}', but found ${files.length}. Using the first file for hash verification: ${files[0]}`,
      );
    }

    fileToHash = files[0];
  }
  const actualSha256 = await sha256FileHex(fileToHash);

  console.log(`Downloaded artifact '${artifactName}' to: ${downloadPath}`);
  console.log(`Downloaded file: ${path.basename(fileToHash)} (${fileToHash})`);
  console.log(`SHA256(downloaded)=${actualSha256}`);

  if (expectedSha256) {
    if (actualSha256.toLowerCase() !== String(expectedSha256).trim().toLowerCase()) {
      throw new Error(
        `SHA-256 mismatch for '${artifactName}': expected ${expectedSha256}, got ${actualSha256}`,
      );
    }
    console.log("SHA-256 matches expected.");
  }

  return { downloadPath, file: fileToHash, sha256: actualSha256 };
}

module.exports = {
  downloadSingleFileArtifact,
};

if (require.main === module) {
  downloadSingleFileArtifact().catch((error) => {
    fail(error && error.stack ? error.stack : String(error));
  });
}
