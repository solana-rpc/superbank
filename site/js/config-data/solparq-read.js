// superbank-solparq-read (archive reader) configuration. Checked against the
// clap definitions in crates/superbank-solparq/src/read/config.rs. The writer
// has its own module (solparq.js). tests/site/config.test.mjs enforces that
// every flag and env name in the reader code is listed here.
//
// Plain text only: `code` spans use backticks, no markup.

const CONFIG = 'crates/superbank-solparq/src/read/config.rs';
const S3 = 'when:archive-location-type=s3';
const S3_REQUIRED = 'when `--archive-location-type` is `s3`';
const ARCHIVE_CMDS = ['subcommand:summary', 'subcommand:schema', 'subcommand:scan'];

// Clap field with an explicit env name; the flag is given explicitly.
const opt = (env, flag, rest) => ({ env, flag, ...rest });

export default {
  id: 'solparq-read',
  label: 'superbank-solparq-read',
  summary: 'Lists and inspects superbank-solparq Parquet archives and prints their transaction rows.',
  source: CONFIG,
  readme: 'crates/superbank-solparq/README.md',
  primary: 'flag',
  intro:
    'Run one subcommand: `list`, `summary`, `schema` or `scan`. Options are flags; only the S3 connection options also have env vars, which use the `SOLPARQ_READ_` prefix, unlike the writer (`SOLPARQ_ARCHIVE_S3_*`). Flag wins over env, then the default.',
  groups: [
    {
      id: 'source',
      title: 'Archive source',
      intro: 'Where archives are read from. These options apply to every subcommand.',
      items: [
        {
          flag: '--archive-location-type',
          type: 'local | s3',
          default: 'local',
          text: 'Archive source type.',
        },
        opt('SOLPARQ_READ_ARCHIVE_S3_ENDPOINT', '--archive-s3-endpoint', {
          type: 'url',
          required: S3_REQUIRED,
          text: 'S3-compatible endpoint, for example `https://s3.eu-central-003.backblazeb2.com`.',
          requires: [S3],
        }),
        opt('SOLPARQ_READ_ARCHIVE_S3_BUCKET_NAME', '--archive-s3-bucket-name', {
          type: 'string',
          required: S3_REQUIRED,
          text: 'S3 bucket name.',
          requires: [S3],
        }),
        opt('SOLPARQ_READ_ARCHIVE_S3_BUCKET_PATH', '--archive-s3-bucket-path', {
          type: 'string',
          default: '',
          text: 'S3 key prefix containing the superbank-solparq archives. `--archive` keys are relative to it.',
          requires: [S3],
        }),
        opt('SOLPARQ_READ_ARCHIVE_S3_AUTH_KEY', '--archive-s3-auth-key', {
          type: 'string',
          required: S3_REQUIRED,
          text: 'S3 access key.',
          requires: [S3],
        }),
        opt('SOLPARQ_READ_ARCHIVE_S3_AUTH_SECRET_KEY', '--archive-s3-auth-secret-key', {
          type: 'string',
          required: S3_REQUIRED,
          secret: true,
          text: 'S3 secret access key.',
          requires: [S3],
        }),
        opt('SOLPARQ_READ_ARCHIVE_S3_REGION', '--archive-s3-region', {
          type: 'string',
          default: 'us-east-1',
          text: 'S3 region used for request signing.',
          requires: [S3],
        }),
      ],
    },
    {
      id: 'list',
      title: 'Subcommand list',
      items: [
        {
          flag: '--archive-dir',
          type: 'path',
          required: 'when `--archive-location-type` is `local`',
          text: 'Local archive directory to list.',
          requires: ['subcommand:list', 'when:archive-location-type=local'],
        },
        {
          flag: '--archive-kind',
          type: 'string',
          text: 'Only list archives of this type, such as `hourly`, `epoch` or `custom`.',
          requires: ['subcommand:list'],
        },
      ],
    },
    {
      id: 'archive',
      title: 'Subcommands summary, schema and scan',
      items: [
        {
          flag: '--archive',
          type: 'string',
          required: true,
          text: 'Local archive file path, or S3 object key relative to `--archive-s3-bucket-path`.',
          requires: ARCHIVE_CMDS,
        },
        {
          flag: '--table',
          type: 'transactions | blocks_metadata | entries | gsfa | gsfa_hot | signatures | token_owner_activity',
          default: 'transactions',
          text: 'Table to read when `--archive` points at a DB archive bundle.',
          requires: ARCHIVE_CMDS,
        },
      ],
    },
    {
      id: 'scan',
      title: 'Subcommand scan',
      items: [
        {
          flag: '--slot-range',
          type: 'string (START-END)',
          required: 'unless `--all` is set; exactly one of the two is needed',
          text: 'Inclusive slot range to read. The end must be greater than or equal to the start.',
          requires: ['subcommand:scan'],
          relations: [{ type: 'conflicts', to: 'all' }],
        },
        {
          flag: '--all',
          type: 'bool',
          default: 'false',
          required: 'unless `--slot-range` is set; exactly one of the two is needed',
          text: 'Read all transaction rows in the archive.',
          requires: ['subcommand:scan'],
        },
        {
          flag: '--columns',
          type: 'list',
          text: 'Comma-separated output columns. Defaults to every column.',
          requires: ['subcommand:scan'],
        },
        {
          flag: '--limit',
          type: 'usize',
          text: 'Maximum number of rows to output.',
          requires: ['subcommand:scan'],
        },
        {
          flag: '--format',
          type: 'jsonl | json | csv',
          default: 'jsonl',
          text: 'Transaction output format.',
          requires: ['subcommand:scan'],
        },
      ],
    },
  ],
};
