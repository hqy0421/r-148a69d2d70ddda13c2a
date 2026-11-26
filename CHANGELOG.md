# Changelog

All notable changes to this project will be documented in this file.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Snapshot-consistent multi-key read, end to end:
  - Engine `DB.BatchGet(keys)` reads any number of discrete keys against a
    single logical time point. All keydir lookups share one read lock and
    all value reads happen under one `segmentsMu.RLock`, so a concurrent
    `BatchPut`, segment rotation, or compaction commit can never produce a
    mixed-before/after result. Results are positional; repeated keys occupy
    repeated slots; missing keys and empty-byte values are distinct; any
    single read failure fails the whole batch.
  - Wire opcode `0x08 BATCHGET` (`u32 count | (u32 klen | key)*` request;
    `u32 count | (u8 found | u32 vlen | val)*` single-frame response), with
    the same pre-allocation and frame-size guards as the other batch
    shapes.
  - TCP server dispatch, Go client `Client.BatchGet`, and the
    `little-db batch-get` CLI subcommand (positional keys or NDJSON on
    stdin, `ndjson`/`raw` output).

## [0.1.0] - 2026-05-28

Initial public release.

### Added
- Bitcask-style storage engine: append-only segments, in-memory keydir,
  manifest + hint files, single-writer with group commit, background
  compaction.
- Length-prefixed binary wire protocol (see `docs/SPEC.md`). Supported
  ops: `PING`, `PUT`, `GET`, `DELETE`, `BATCH`, `READKEYRANGE`, `STATS`.
- TCP server (`little-db serve`) with bounded range-scan concurrency
  and structured `slog` logging (`--log-level`, `--log-format`).
- Pure-stdlib Go client library (`internal/client`).
- `little-db` CLI with subcommands: `serve`, `ping`, `put`, `get`,
  `delete`, `batch`, `range`, `stats`. See `docs/cli.md`.
- Test fixtures under `testdata/` and a generator script for bulk
  NDJSON inputs.

[Unreleased]: https://github.com/codeekage/little-db/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/codeekage/little-db/releases/tag/v0.1.0
