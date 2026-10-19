const { cmdInit } = require('./commands/init');
const { cmdSync } = require('./commands/sync');
const { cmdStatus } = require('./commands/status');
const { cmdConnect } = require('./commands/connect');
const { cmdUninstall } = require('./commands/uninstall');
const { uploadQueue } = require('./commands/upload');
const { resolveUploadConfig, defaultQueuePath } = require('./lib/upload-config');

const HELP = [
  'UsageCairn · local collector',
  '',
  '  usagecairn connect --api-url <url> --key <key>   Save Vercel upload credentials',
  '  usagecairn connect --show | --clear              Inspect or remove them',
  '  usagecairn init [--yes]                          Install collection hooks',
  '  usagecairn sync                                  Collect usage (uploads when connected)',
  '  usagecairn upload [--replay]                     Upload new usage (--replay: full ledger)',
  '  usagecairn status                                Show hook status',
  '  usagecairn uninstall [--purge]                   Remove hooks and local state',
].join('\n');

async function uploadIfConfigured() {
  const config = resolveUploadConfig({ queuePath: defaultQueuePath() });
  if (config.invalid) {
    console.error('Upload credentials are invalid; run: usagecairn connect --api-url <url> --key <key>');
    process.exitCode = 1;
    return null;
  }
  if (!config.apiUrl || !config.accessKey) return null;
  return uploadQueue();
}

async function run(argv) {
  const [command, ...rest] = argv;
  if (!command || command === '--help' || command === '-h') {
    console.log(HELP);
    return;
  }
  if (command === '-v' || command === '--version') {
    console.log(`v${require('../package.json').version}`);
    return;
  }
  switch (command) {
    case 'init':
      return cmdInit(rest);
    case 'sync': {
      await cmdSync(rest);
      const result = await uploadIfConfigured();
      if (result) console.log(`Uploaded ${result.uploaded} usage snapshots (${result.pending} pending bytes).`);
      return;
    }
    case 'upload': {
      if (rest.length > 1 || (rest.length === 1 && rest[0] !== '--replay')) throw new Error('Usage: usagecairn upload [--replay]');
      const result = await uploadQueue({ replay: rest[0] === '--replay' });
      console.log(`Uploaded ${result.uploaded} usage snapshots (${result.pending} pending bytes).`);
      return;
    }
    case 'status':
      return cmdStatus(rest);
    case 'connect':
      return cmdConnect(rest);
    case 'uninstall':
      return cmdUninstall(rest);
    default:
      throw new Error(`Unknown command: ${command}`);
  }
}
module.exports = { run, uploadIfConfigured, HELP };
