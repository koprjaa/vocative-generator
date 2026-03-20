# Vocative Generator
![](https://img.shields.io/badge/License-MIT-blue?style=flat-square) ![](https://img.shields.io/badge/Python--lightgrey?style=flat-square&logo=python&logoColor=3776AB) ![](https://img.shields.io/badge/aiohttp-3.9.3-lightgrey?style=flat-square) ![](https://img.shields.io/badge/pandas-2.2.0-lightgrey?style=flat-square)

## What it does

The program reads names from a CSV, submits each name to the public Czech declension site sklonuj.cz by automating its HTML form (GET for session cookies, then POST with field `inpJmena`), parses the vocative from the returned HTML table, and writes columns for vocative text and split first or last fragments to an output CSV. It processes input in large pandas chunks, overlaps many names in flight via asyncio tasks, persists progress in `checkpoint.json`, and can resume after interruption. If the configured input file is missing, `main.py` generates a small dummy `names.csv` so the pipeline still runs.

## Why it was built

The motivation is to support cleaning personal data held in a database: the tool batch-generates Czech vocatives for names so large extracts or update batches do not require hand-declining every row.

## Architecture

`main.py` drives the loop: it counts or estimates input rows, optionally creates an empty output CSV with headers, reads the input in chunks of `CHUNK_SIZE` from `src/config.py`, cleans rows, and delegates each chunk to `BatchService`. `BatchService` splits the chunk into batches sized by `AdaptiveBatchSize`, skips work already present in `CheckpointService`, schedules `NameService.process_single_name` under a cap from `AdaptiveWorkers`, merges results back into the chunk DataFrame, and calls `CheckpointService.save_checkpoint` on an interval and when a chunk finishes. `NameService` holds per-name retry and backoff state and uses BeautifulSoup with fixed table class selectors to extract the vocative cell. `AdaptiveDelay`, `AdaptiveWorkers`, and `AdaptiveBatchSize` in `src/adapters.py` adjust delay, concurrency, and batch size from batch-level success rates. `GracefulShutdownHandler` and `create_aiohttp_session` in `src/utils.py` register SIGINT or SIGTERM handling and supply a shared `aiohttp.ClientSession`. `NameResult` in `src/models.py` carries parsed fields and a `success` flag based on whether the returned string differs from the input (case-insensitive).

## Key decisions

- Async I/O with aiohttp suits many sequential HTTP round-trips to one host rather than a synchronous loop.
- Chunked pandas reads bound memory for large CSVs while `CheckpointService` stores a growing JSON map of all processed names for resume, trading disk and serialization cost for global deduplication across chunks.
- Two layers of backoff appear in the design: `NameService` scales waits on 429, 5xx, and connection errors, while adapters tune delay, workers, and batch size from success ratios.
- User-Agent strings are rotated from a list in config and set per request because the session skips aiohttp’s default User-Agent header.
- The aiohttp connector sets `ssl=False` even though `API_CONFIG` uses an `https` base URL.

## Trade-offs

- Checkpoint JSON retains every processed name in one object, which simplifies resume and reuse across batches but grows with the dataset size and rewrite cost.
- HTML scraping depends on sklonuj.cz markup; a stable API contract was not used.
- Concurrency is tuned with several magic thresholds in adapters (for example 0.95 and 0.8 success rates) without external calibration documented in the repo.
- `MEMORY_LIMIT` exists in `HTTP_CONFIG` but is not referenced elsewhere in the codebase.

## Limitations

- There is no test directory or test files in the repository.
- Behavior is coupled to sklonuj.cz availability, rate limiting, and unchanged form URLs and HTML table structure.
- After a chunk completes, `BatchService` advances `last_chunk_fully_processed_index` to that chunk index; `main.py` then evaluates whether to append the chunk to the output CSV using `last_chunk_fully_processed_index >= current_chunk_index`, which suppresses the append when shutdown has not been requested, so successful runs may leave `names_with_vocative.csv` with only headers unless that condition is not hit.
- Dummy input creation on missing `names.csv` makes dry runs easy but can overwrite expectations if a run starts without the intended file.
- Python version is not pinned; dependencies are pinned in `requirements.txt` to specific package versions.

## How to run

Create a virtual environment, install dependencies with `pip install -r requirements.txt`, supply a CSV whose configured name column exists (`INPUT_COLUMN_NAME` in `src/config.py`, default `Name`), and use `FILE_CONFIG` paths (default `names.csv` input and `names_with_vocative.csv` output). If `ID` is absent, `main.py` synthesizes sequential IDs per chunk. Run `python main.py`. Logging uses `LOGGING_CONFIG` (console handler).
