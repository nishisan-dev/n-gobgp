const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawnSync } = require('node:child_process');
const cleanup = Boolean(process.env.STATE_state_file);
const state = process.env.STATE_state_file || path.join(
  process.env.RUNNER_TEMP, `n-gobgp-ci-${crypto.randomUUID()}.json`);
if (!cleanup) {
  fs.appendFileSync(process.env.GITHUB_STATE, `state_file=${state}\n`);
  fs.appendFileSync(process.env.GITHUB_ENV, `NGOBGP_CI_STATE=${state}\n`);
}
const result = spawnSync('python3', [path.join(process.env.GITHUB_WORKSPACE,
  'tools/ci-job.py'), cleanup ? 'cleanup' : 'start'], {
  stdio: 'inherit', env: { ...process.env, NGOBGP_CI_STATE: state }
});
if (result.error) console.error(result.error.message);
process.exitCode = result.status === null ? 1 : result.status;
