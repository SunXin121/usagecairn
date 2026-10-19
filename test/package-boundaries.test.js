const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { installLocalTrackerApp, buildNotifyHandler } = require('../src/commands/init');

const root = path.resolve(__dirname, '..');

test('npm collector excludes server runtime dependencies and pricing', async () => {
  const pkg = require('../package.json');
  assert.equal(pkg.bin['token-pulse'], 'bin/token-pulse.js');
  assert.equal(pkg.dependencies.pg, undefined);
  assert.equal(pkg.dependencies['@mongodb-js/zstd'] !== undefined, true);
  assert.equal(await fs.stat(path.join(root, 'server/pricing/index.js')).then((s) => s.isFile()), true);
  await assert.rejects(fs.stat(path.join(root, 'src/lib/pricing/index.js')), { code: 'ENOENT' });
});

test('installed collector and notify hook use the published executable', async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), 'token-pulse-runtime-'));
  const appDir = path.join(home, 'app');
  try {
    await installLocalTrackerApp({ appDir });
    const executable = path.join(appDir, 'bin', 'token-pulse.js');
    assert.match(execFileSync(process.execPath, [executable, '--help'], { encoding: 'utf8' }), /local collector/);
    const notify = buildNotifyHandler({ trackerDir: home, packageName: 'token-pulse' });
    assert.match(notify, /app.*bin.*token-pulse\.js/);
    assert.doesNotMatch(notify, /tracker\.js/);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});
