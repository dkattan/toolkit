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
  const artifactFile = getInput("file");
  const retentionDays = getInput("retention-days");
  const debugEnv = getInput("debug-env");

  process.env.SINGLE_ARTIFACT_NAME = artifactName;
  process.env.SINGLE_ARTIFACT_FILE = artifactFile;
  if (retentionDays) {
    process.env.SINGLE_ARTIFACT_RETENTION_DAYS = retentionDays;
  }
  process.env.SINGLE_ARTIFACT_DEBUG_ENV = toBool(debugEnv) ? "1" : "0";

  const uploaderPath = path.resolve(
    __dirname,
    "../../../scripts/upload-single-file-artifact.cjs",
  );
  const { uploadSingleFileArtifact } = require(uploaderPath);

  await uploadSingleFileArtifact();
}

main().catch((err) => {
  const message = err && err.stack ? err.stack : String(err);
  console.error(`::error::${message}`);
  process.exit(1);
});
