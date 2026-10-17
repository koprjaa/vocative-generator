# vocative-generator

Converts Czech names to the vocative case in bulk using the form at [sklonuj.cz](https://sklonuj.cz/). Reads UTF-8 CSV in bounded chunks, runs concurrent batches, and saves resumable progress.

![rust](https://img.shields.io/badge/rust-stable-DEA584?style=flat-square&logo=rust&logoColor=white)
![license](https://img.shields.io/badge/license-MIT-A31F34?style=flat-square)
[![ci](https://github.com/koprjaa/vocative-generator/actions/workflows/ci.yml/badge.svg)](https://github.com/koprjaa/vocative-generator/actions/workflows/ci.yml)


## Install

Install the stable Rust toolchain, then run:

```sh
cargo install --path .
```

## Use

Create `names.csv` in the current directory:

```csv
Name
Jan Dvořák
Petr Kučera
```

```sh
vocative-generator
```

The executable uses the current directory. It writes `names_with_vocative.csv` and `checkpoint.json`. Without `names.csv`, it creates the same 20-row sample input as the original. An optional `ID` column is preserved; otherwise IDs are assigned before empty names are removed. Extra input columns are discarded. The output columns, in order, are:

```csv
ID,Name,Vocative,Vocative First Name,Vocative Last Name,Error
```

CSV uses UTF-8 without a BOM, commas, standard CSV quoting, and native platform line endings. Names are trimmed; empty names and pandas' default missing-value tokens are removed. Returned vocatives are split at the final whitespace-separated word. An unchanged form counts as failure for metrics, but remains in the output. Failed lookups retain the original name and put the failure in `Error`.

Press Ctrl+C to request shutdown; SIGTERM is also supported on Unix. Run again to resume. Progress is tied to a SHA-256 hash of the entire input, so changing even one byte starts a fresh run. Mismatched checkpoints remain untouched when loaded, then are replaced when new progress is saved. Corrupt checkpoints are removed. JSON uses compact UTF-8 and retains the original keys and legacy `last_batch` reader support.

There are no CLI switches, environment settings, or external configuration files, matching the original. Arguments, including `--help`, are ignored. The process logs errors to stderr, prints `Application has exited.` to stdout, and exits with status zero even on a processing error, as before. Configuration is explicit Rust data: `pipeline::Config` for file paths/chunks/batches/workers/delay, `client::NameService` for HTTP parameters. The executable uses their original defaults.

## Request behavior

Each name makes a GET followed by an `inpJmena` form POST at `https://sklonuj.cz/generator-osloveni/`. Each attempt chooses one of the original five browser User-Agents and uses the original browser headers, Origin and Referer. Cookies from the GET accompany the POST. Both requests have a 20-second total timeout. The default pipeline runs 30 concurrent batches (adaptive bounds 20–100), with sequential names inside each batch and a per-name delay initially 0.01 seconds (bounds 0.01–5). Chunks contain at most 100,000 input records. Batches start at 2,000 names (bounds 500–4,000) and are fixed for the current chunk.

HTTP 429, 5xx, timeouts and connection errors use six total attempts. Shared backoff multiplies by 2, 1.8 or 1.5, capped at 20, with a uniform 0–50% jitter added to the wait. Even the final failed attempt waits. After a 60-second cooldown and 50 consecutive successful POSTs, each further success reduces backoff by 0.85, down to 1. HTTP 401/403 stops immediately; other HTTP client errors retry without backoff. Empty HTML results are logged and fall back to the original name without retrying.

The parser uses the first `table.table`, skips its first row, considers at most ten rows, and returns the second cell of the first remaining row with two cells. It depends on the site's layout and can return the wrong case if the site reorders rows.

## Changes from the Python version

- Installation and execution use Cargo and a native executable. Python modules and mutable module configuration are replaced by Rust library APIs; there is no Python import interface.
- Cookies are local to each GET/POST attempt rather than stored in a shared mutable field. This fixes cookies being replaced by concurrent lookups. Cookies are not shared across names or attempts, and manual cookies are only collected from the final GET response; redirect-set session cookies are not persisted.
- Every duplicate input row receives its result. The original name-to-row map updated only the last occurrence.
- Resume reconstructs every chunk from the global cache instead of skipping a partial batch cursor. This fixes missing vocatives in skipped batches and incorrect cursors from batches completing out of order. The JSON cursor is conservatively written as zero; older cursor values are read but not used to skip rows. Adaptive statistics include cached results during reconstruction.
- Output is built in a sibling `.csv.tmp` file and replaces the destination only after a complete run. Resume rebuilds the output, including when the previous output was deleted. This prevents partial chunk duplication and the gap between a complete checkpoint and CSV append. A shutdown leaves the previous published output intact; results saved in the checkpoint are recovered on the next run. Like the original cache, it stores vocative and ID rather than error details, so resumed errors become unchanged-name results.
- Shutdown stops submitting new work, allows current requests to finish, saves completed batches, and waits up to 30 seconds for outstanding work before cancelling remaining work. The original wrapper cancelled the main task immediately. Results inside an unfinished cancelled batch can still require another request on resume.
- The incorrect progress estimate (number of pandas chunks minus one) is removed. Final totals count actual cleaned rows. Progress and lifecycle logs retain the original timestamp/name/level format and core request/checkpoint messages, but omit the incorrect estimated percentages, Python task cancellation messages and tracebacks. Rust errors use their native descriptions. Concurrent completion order and timings are naturally nondeterministic.
- The HTML parser follows HTML5 recovery rules (`scraper`) rather than BeautifulSoup's `html.parser`; malformed or nested tables can therefore differ. CSV input stays textual, preserving IDs such as `007` and numeric names rather than applying pandas numeric inference. A UTF-8 BOM is accepted. Invalid UTF-8 and inconsistent row widths are reported as errors; pandas-specific duplicate-column renaming, numeric inference and permissive decoding are not reproduced. Existing result columns are recalculated instead of retained.
- Checkpoint write failures propagate to the run's error handler instead of being logged and ignored. Writes are flushed to disk before replacement. A legacy checkpoint without a non-null fingerprint is always rejected, even when input is missing.
- HTTP uses reqwest/rustls rather than aiohttp/OpenSSL. TLS fingerprints, default transport headers, redirect/error descriptions, connection pooling and decompression details can differ. There is no separate pool-wide limit of 200; the pipeline bounds active requests to at most 100, preserving effective single-host concurrency. Proxy environment variables are ignored, as with the original session's default `trust_env=False`.

## Development

```sh
cargo build --release
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

Tests cover the original parser, adaptive bands, checkpoint and output regressions, plus CLI execution, HTTP headers/cookies/retries, concurrency, backoff, duplicate rows and resume recovery. All HTTP tests use a local `wiremock` server; no tests contact live sites. CI runs formatting, lint, tests and release builds on Ubuntu and Windows. Fixtures have byte-preserving Git attributes.

Production dependencies each serve one purpose: Tokio for scheduling/signals, reqwest for HTTP/TLS/compression, scraper for HTML, serde/serde_json for checkpoint data, csv for streaming records, sha2 for input identity, rand for User-Agent/jitter, and chrono for local timestamp formatting. Test-only tempfile isolates workspaces; wiremock supplies local HTTP fixtures. No XLSX/XML formats existed in the original.

## License

[MIT](LICENSE)
