# vocative-generator

**Batch-declines Czech first names into the vocative case by automating sklonuj.cz — async aiohttp, pandas chunks, resumable checkpoints.**

![python](https://img.shields.io/badge/python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![license](https://img.shields.io/badge/license-MIT-A31F34?style=flat-square)
![status](https://img.shields.io/badge/status-active-22863A?style=flat-square)
![aiohttp](https://img.shields.io/badge/aiohttp-3.9-2C5BB4?style=flat-square)
![pandas](https://img.shields.io/badge/pandas-2.2-150458?style=flat-square&logo=pandas&logoColor=white)
![beautifulsoup4](https://img.shields.io/badge/bs4-4.12-777?style=flat-square)

Czech is a heavily-inflected language and the vocative ("Hello **Jene**", not "Hello **Jan**") is a correctness requirement for any personalised email, CRM, or DB-driven customer communication. There's no clean public API, so this tool scripts the form at [sklonuj.cz](https://sklonuj.cz/): GET session cookies, POST `inpJmena=<name>`, parse the vocative cell out of the returned HTML table.

Runs it at scale: pandas chunked reads, a shared aiohttp session, configurable worker semaphore, adaptive delay/concurrency/batch-size based on per-batch success rates, and a JSON checkpoint that allows resuming after `Ctrl+C` without re-requesting anything.

## Run

```bash
uv venv
uv pip install -r requirements.txt
# put names into names.csv with a column matching INPUT_COLUMN_NAME (default 'Name')
python main.py
```

If `names.csv` is missing, `main.py` generates a small dummy so the pipeline still runs end-to-end.

Output lands in `names_with_vocative.csv` with columns for the vocative text and the split first/last fragments.

## Architecture

```
main.py           → reads CHUNK_SIZE rows from CSV, delegates to BatchService
BatchService      → splits chunk into AdaptiveBatchSize batches, skips already-done names
NameService       → one request per name via aiohttp + BeautifulSoup, with per-name retry/backoff
CheckpointService → persists every processed (name → result) pair to checkpoint.json
AdaptiveDelay/Workers/BatchSize → tune the three knobs based on recent success ratios
```

Two layers of backoff: `NameService` scales waits on 429/5xx/connection errors, while the adapters tune the whole pipeline's aggressiveness based on batch-level success rates (thresholds at 0.95 and 0.8).

## Key design choices

- **aiohttp over requests** — hundreds of small round-trips to one host dominate total time, and async wins there.
- **Chunked pandas reads** bound memory on multi-million-row CSVs while the checkpoint serves as a global deduplication map across chunks.
- **User-Agent rotation** from a config list, set per request; the aiohttp session is constructed without the default UA.
- **Runs on Windows too** — signal handler registration has a fallback for the Windows ProactorEventLoop which doesn't support `add_signal_handler` (ships a `signal.signal()` path for SIGINT).

## Known limits

- Depends entirely on sklonuj.cz's HTML structure. Markup change = the scraper breaks.
- The checkpoint JSON grows linearly with the dataset — for tens of millions of names the per-flush serialization cost becomes non-trivial.
- No tests. Validation is manual: spot-check `names_with_vocative.csv` against known declensions.
- Several adaptive thresholds are magic numbers in `src/adapters.py` without external calibration.

## License

[MIT](LICENSE)
