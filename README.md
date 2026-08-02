# vocative-generator

Converts Czech first names into the vocative case in bulk. It automates the form at [sklonuj.cz](https://sklonuj.cz/) with aiohttp, reads the input in pandas chunks, and writes a checkpoint so a stopped run can continue.

![python](https://img.shields.io/badge/python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![license](https://img.shields.io/badge/license-MIT-A31F34?style=flat-square)
![status](https://img.shields.io/badge/status-active-22863A?style=flat-square)
[![ci](https://github.com/koprjaa/vocative-generator/actions/workflows/ci.yml/badge.svg)](https://github.com/koprjaa/vocative-generator/actions/workflows/ci.yml)

## What it does

Czech inflects names. A personalized email says "Hello Jene", not "Hello Jan". Any CRM or mail campaign that addresses Czech customers needs the vocative form. No clean public API exists, so this tool drives the sklonuj.cz form: it gets the session cookies, posts `inpJmena=<name>`, and reads the vocative cell out of the returned HTML table.

## Install

```bash
uv venv
uv pip install -r requirements.txt
```

## Use

Put the names into `names.csv` in a column that matches `INPUT_COLUMN_NAME`. The default column name is `Name`.

```bash
python main.py
```

If `names.csv` does not exist, `main.py` writes a small sample file so the pipeline still runs end to end.

The result goes to `names_with_vocative.csv`. It holds the vocative text and the split first and last name parts.

Press Ctrl+C to stop. The next run reads `checkpoint.json` and skips every name it already processed.

## How it works

```
main.py             Reads CHUNK_SIZE rows from the CSV and calls BatchService.
BatchService        Splits the chunk into batches and skips finished names.
NameService         One request per name with aiohttp, with retry and backoff.
CheckpointService   Writes every processed name and result to checkpoint.json.
src/parsing.py      Reads the vocative out of a results page.
src/adapters.py     One tunable number, used for the delay, the workers and the batch size.
```

Backoff works on two levels. `NameService` grows the wait on 429, 5xx, and connection errors. Three `AdaptiveValue` instances tune the pipeline from the recent success rate.

Each one holds a number between a floor and a ceiling and a table of bands. The first band the success rate reaches decides the multiplier, and a factor of 1.0 holds the value still. That middle band matters: without it the value would react to every wobble around a threshold. The delay shortens on a clean run and lengthens on a bad one, the workers and the batch size move the other way.

Three decisions are worth stating.

1. aiohttp instead of requests. Hundreds of small round trips to one host dominate the run time, and async wins there.
2. Chunked pandas reads bound the memory use on files with millions of rows. The checkpoint acts as the deduplication map across chunks.
3. The tool rotates the User-Agent from a config list and builds the aiohttp session without the default agent.

The signal handler falls back to `signal.signal()` on Windows, because the ProactorEventLoop does not support `add_signal_handler`.

## Limits

- The parser depends on the HTML of sklonuj.cz. A markup change breaks it.
- `checkpoint.json` grows with the dataset. At tens of millions of names the serialization cost per flush becomes significant.
- The adaptive bands in `src/adapters.py` are fixed numbers. They come from watching the site rather than from measurement.
- The parser takes the first data row of the results table. A layout change that reorders the cases would return the wrong form rather than nothing.

## Development

```bash
uv run --extra dev ruff check .
uv run --extra dev pytest -q
```

The suite covers the parsing and the adaptive bands and reaches no network. CI
runs both on Python 3.10, 3.11, and 3.12, across Linux and Windows.

## License

[MIT](LICENSE)
