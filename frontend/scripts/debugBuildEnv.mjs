import { existsSync } from "node:fs";
import { join } from "node:path";

const endpoint = "http://127.0.0.1:7563/ingest/9340dcf5-ce53-4a24-b5b8-0c6e40330a81";
const sessionId = "4a1489";
const runId = process.env.DEBUG_RUN_ID || "pre-fix";

function debugLog(hypothesisId, location, message, data) {
  // #region agent log
  fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Debug-Session-Id": sessionId,
    },
    body: JSON.stringify({
      sessionId,
      runId,
      hypothesisId,
      location,
      message,
      data,
      timestamp: Date.now(),
    }),
  }).catch(() => {});
  // #endregion
}

const nodeModulesBin = join(process.cwd(), "node_modules", ".bin");
const runPPath = join(nodeModulesBin, "run-p");
const runPCmdPath = join(nodeModulesBin, "run-p.cmd");

debugLog("H1", "frontend/scripts/debugBuildEnv.mjs:31", "Frontend prebuild env snapshot", {
  cwd: process.cwd(),
  nodeEnv: process.env.NODE_ENV || null,
  npmConfigProduction: process.env.npm_config_production || null,
  npmConfigOmit: process.env.npm_config_omit || null,
  npmConfigInclude: process.env.npm_config_include || null,
});

debugLog("H2", "frontend/scripts/debugBuildEnv.mjs:39", "run-p binary existence", {
  runPPath,
  runPCmdPath,
  runPExists: existsSync(runPPath),
  runPCmdExists: existsSync(runPCmdPath),
});

debugLog("H3", "frontend/scripts/debugBuildEnv.mjs:46", "npm-run-all2 package existence", {
  npmRunAll2PackageJsonExists: existsSync(
    join(process.cwd(), "node_modules", "npm-run-all2", "package.json")
  ),
});
