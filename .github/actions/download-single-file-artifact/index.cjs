"use strict";

const path = require("node:path");
const process = require("node:process");

function getInput(name) {
  const key = `INPUT_${name.replace(/ /g, "_").toUpperCase()}`;
  return process.env[key] || "";
}

function toBool(value) {
  return String(value).trim().toLowerCase() === "true";
}

async function main() {
  const artifactName = getInput("name");
  const downloadDir = getInput("download-dir");
  const expectedSha256 = getInput("expected-sha256");
  const debugEnv = getInput("debug-env");

  process.env.SINGLE_ARTIFACT_NAME = artifactName;
  process.env.SINGLE_ARTIFACT_DOWNLOAD_DIR = downloadDir;
  if (expectedSha256) {
    process.env.SINGLE_ARTIFACT_EXPECTED_SHA256 = expectedSha256;
  }
  process.env.SINGLE_ARTIFACT_DEBUG_ENV = toBool(debugEnv) ? "1" : "0";

  const downloaderPath = path.resolve(
    __dirname,
    "../../../scripts/download-single-file-artifact.cjs",
  );
  const { downloadSingleFileArtifact } = require(downloaderPath);

  await downloadSingleFileArtifact();
}

main().catch((err) => {
  const message = err && err.stack ? err.stack : String(err);
  console.error(`::error::${message}`);
  process.exit(1);
});
