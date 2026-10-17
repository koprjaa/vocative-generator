// Project: vocative-generator
// File: pipeline.rs
// Description: Chunked CSV input, concurrent sequential batches and ordered output.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use crate::{
    adapters::{make_batch_size, make_delay, make_workers},
    checkpoint::{Cache, Checkpoint},
    client::NameService,
    log, NameResult,
};
use std::{
    error::Error,
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::task::JoinSet;
pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
struct TemporaryOutput(PathBuf);
impl Drop for TemporaryOutput {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
pub const OUTPUT_COLUMNS: [&str; 6] = [
    "ID",
    "Name",
    "Vocative",
    "Vocative First Name",
    "Vocative Last Name",
    "Error",
];
#[derive(Debug, Clone)]
pub struct Row {
    pub id: String,
    pub name: String,
    pub result: Option<NameResult>,
}
pub fn missing_name(value: &str) -> bool {
    matches!(
        value,
        "" | "#N/A"
            | "#N/A N/A"
            | "#NA"
            | "-1.#IND"
            | "-1.#QNAN"
            | "-NaN"
            | "-nan"
            | "1.#IND"
            | "1.#QNAN"
            | "<NA>"
            | "N/A"
            | "NA"
            | "NULL"
            | "NaN"
            | "None"
            | "n/a"
            | "nan"
            | "null"
    ) || value.trim().is_empty()
}
pub fn clean_rows(
    headers: &csv::StringRecord,
    records: &[csv::StringRecord],
    processed: usize,
) -> Vec<Row> {
    let Some(name) = headers.iter().position(|h| h == "Name") else {
        return vec![];
    };
    let id = headers.iter().position(|h| h == "ID");
    records
        .iter()
        .enumerate()
        .filter_map(|(i, r)| {
            let n = r.get(name).unwrap_or("");
            if missing_name(n) {
                return None;
            }
            Some(Row {
                id: id
                    .map(|j| r.get(j).unwrap_or("").to_string())
                    .unwrap_or_else(|| (processed + i + 1).to_string()),
                name: n.trim().into(),
                result: None,
            })
        })
        .collect()
}
pub fn write_rows(path: impl AsRef<Path>, rows: &[Row], header: bool) -> Result<()> {
    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .append(!header)
        .truncate(header)
        .open(path)?;
    let mut w = csv::WriterBuilder::new()
        .terminator(if cfg!(windows) {
            csv::Terminator::CRLF
        } else {
            csv::Terminator::Any(b'\n')
        })
        .from_writer(f);
    if header {
        w.write_record(OUTPUT_COLUMNS)?;
    }
    for row in rows {
        let r = row.result.as_ref();
        w.write_record([
            row.id.as_str(),
            &row.name,
            r.map(|r| r.vocative.as_str()).unwrap_or(""),
            r.map(|r| r.first_name.as_str()).unwrap_or(""),
            r.map(|r| r.surname.as_str()).unwrap_or(""),
            r.and_then(|r| r.error_message.as_deref()).unwrap_or(""),
        ])?;
    }
    w.flush()?;
    w.get_ref().sync_all()?;
    Ok(())
}
#[derive(Clone)]
pub struct Config {
    pub input: PathBuf,
    pub output: PathBuf,
    pub checkpoint: PathBuf,
    pub chunk_size: usize,
    pub batch_size: usize,
    pub workers: usize,
    pub delay: f64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            input: "names.csv".into(),
            output: "names_with_vocative.csv".into(),
            checkpoint: "checkpoint.json".into(),
            chunk_size: 100000,
            batch_size: 2000,
            workers: 30,
            delay: 0.01,
        }
    }
}
pub async fn run(config: Config, service: NameService, shutdown: Arc<AtomicBool>) -> Result<usize> {
    if config.chunk_size == 0
        || config.batch_size == 0
        || config.workers == 0
        || !config.delay.is_finite()
        || config.delay < 0.0
    {
        return Err("Chunk size, batch size and workers must be positive; delay must be finite and nonnegative".into());
    }
    let start = Instant::now();
    log(
        "__main__",
        "INFO",
        "Application starting name processing...",
    );
    if !config.input.exists() {
        log(
            "__main__",
            "ERROR",
            format!("Input file {} not found. Exiting.", config.input.display()),
        );
        log(
            "__main__",
            "INFO",
            format!(
                "Creating dummy input file {} for testing.",
                config.input.display()
            ),
        );
        let mut w = csv::WriterBuilder::new()
            .terminator(if cfg!(windows) {
                csv::Terminator::CRLF
            } else {
                csv::Terminator::Any(b'\n')
            })
            .from_path(&config.input)?;
        w.write_record(["ID", "Name"])?;
        for i in 1..=20 {
            w.write_record([i.to_string(), format!("First Name Last Name {i}")])?;
        }
        w.flush()?;
    }
    let mut cp = Checkpoint::load(&config.input, &config.checkpoint)?;
    if !cp.processed_names.is_empty() {
        log(
            "__main__",
            "INFO",
            format!(
                "Flushing checkpoint before start: {} processed names.",
                cp.processed_names.len()
            ),
        );
        cp.save(
            cp.last_chunk_fully_processed_index,
            cp.last_batch_completed_for_current_chunk,
            Cache::new(),
            cp.last_batch_completed_for_current_chunk == 0,
        )?;
        log(
            "__main__",
            "INFO",
            "Checkpoint flushed successfully before start.",
        );
    }
    // Reconstruct the output from cached names on resume. Publishing only a finished run avoids duplicate partial chunks.
    let output_temp = config.output.with_extension("csv.tmp");
    let _temporary_output = TemporaryOutput(output_temp.clone());
    write_rows(&output_temp, &[], true)?;
    let delay = Arc::new(Mutex::new(make_delay(
        config.delay,
        config.delay.min(0.01),
        5.0,
    )));
    let mut workers = make_workers(config.workers, config.workers.min(20), 100);
    let mut batch_size = make_batch_size(
        config.batch_size,
        config.batch_size.min(500),
        config.batch_size.max(4000),
    );
    let mut reader = csv::ReaderBuilder::new().from_path(&config.input)?;
    let mut headers = reader.headers()?.clone();
    if let Some(first) = headers.get(0) {
        if first.starts_with('\u{feff}') {
            let mut h = csv::StringRecord::new();
            h.push_field(first.trim_start_matches('\u{feff}'));
            for x in headers.iter().skip(1) {
                h.push_field(x);
            }
            headers = h;
        }
    }
    let mut records = reader.records();
    let mut total = 0;
    let mut chunk_index = 0_i64;
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let chunk = records
            .by_ref()
            .take(config.chunk_size)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if chunk.is_empty() {
            break;
        }
        log(
            "__main__",
            "INFO",
            format!(
                "\n--- Processing Chunk {chunk_index} ({} records) ---",
                chunk.len()
            ),
        );
        if !headers.iter().any(|h| h == "Name") {
            log(
                "__main__",
                "ERROR",
                format!("Chunk {chunk_index} does not contain 'Name' column. Skipping."),
            );
            chunk_index += 1;
            continue;
        }
        if !headers.iter().any(|h| h == "ID") {
            log("__main__","WARNING",format!("Chunk {chunk_index} does not contain 'ID' column. Generating sequential IDs for this chunk."));
        }
        let mut rows = clean_rows(&headers, &chunk, total);
        if rows.len() != chunk.len() {
            log(
                "__main__",
                "INFO",
                format!(
                    "Cleaned chunk {chunk_index}: removed {} empty/NaN 'Name's.",
                    chunk.len() - rows.len()
                ),
            );
        }
        if rows.is_empty() {
            chunk_index += 1;
            continue;
        }
        let size = batch_size.value as usize;
        let batches: Vec<_> = rows
            .chunks(size)
            .map(|rs| rs.iter().map(|r| r.name.clone()).collect::<Vec<_>>())
            .collect();
        log(
            "BatchService",
            "INFO",
            format!(
                "Starting processing for chunk {chunk_index} with {} names.",
                rows.len()
            ),
        );
        log(
            "BatchService",
            "INFO",
            format!(
                "Chunk {chunk_index} split into {} batches of (up to) {size} names.",
                batches.len()
            ),
        );
        let mut tasks = JoinSet::new();
        let name_ids: std::collections::HashMap<_, _> = rows
            .iter()
            .map(|r| (r.name.clone(), r.id.clone()))
            .collect();
        let mut next = 0;
        let mut completed = 0;
        let mut pending = Cache::new();
        let mut since_save = 0;
        let mut results = std::collections::HashMap::new();
        let mut shutdown_deadline = None;
        while next < batches.len() || !tasks.is_empty() {
            while next < batches.len()
                && tasks.len() < workers.value as usize
                && !shutdown.load(Ordering::SeqCst)
            {
                let names = batches[next].clone();
                let cache: std::collections::HashMap<_, _> = names
                    .iter()
                    .filter_map(|name| {
                        cp.processed_names
                            .get(name)
                            .and_then(|v| v.get("vocative"))
                            .and_then(|v| v.as_str())
                            .map(|v| (name.clone(), v.to_string()))
                    })
                    .collect();
                let svc = service.clone();
                let d = delay.clone();
                let stop = shutdown.clone();
                let batch = next + 1;
                tasks.spawn(async move {
                    let mut out = Vec::new();
                    for name in names {
                        if stop.load(Ordering::SeqCst) {
                            break;
                        }
                        if let Some(v) = cache.get(&name) {
                            out.push(NameResult::from_vocative(&name, v));
                            continue;
                        }
                        let wait = d.lock().unwrap().value;
                        tokio::time::sleep(Duration::from_secs_f64(wait)).await;
                        out.push(svc.process_single_name(&name).await);
                    }
                    let success = out.iter().filter(|r| r.success).count();
                    if !out.is_empty() {
                        d.lock()
                            .unwrap()
                            .adjust(Some(success as f64 / out.len() as f64));
                    }
                    (batch, out)
                });
                next += 1;
            }
            if tasks.is_empty() {
                break;
            }
            if shutdown.load(Ordering::SeqCst) {
                let deadline = shutdown_deadline
                    .get_or_insert_with(|| Instant::now() + Duration::from_secs(30));
                if Instant::now() >= *deadline {
                    tasks.abort_all();
                    break;
                }
            }
            let joined =
                match tokio::time::timeout(Duration::from_millis(100), tasks.join_next()).await {
                    Ok(joined) => joined,
                    Err(_) => continue,
                };
            if let Some(joined) = joined {
                let (batch, out) = joined?;
                let success = out.iter().filter(|r| r.success).count();
                log("BatchService","INFO",format!("Batch statistics: {success}/{} successful ({:.1}%). Current delay: {:.2}s, workers: {}, batch size: {}",out.len(),success as f64/out.len().max(1) as f64*100.0,delay.lock().unwrap().value,workers.value,batch_size.value));
                for r in out {
                    workers.record(r.success);
                    batch_size.record(r.success);
                    let id = name_ids.get(&r.original_name).cloned().unwrap_or_default();
                    pending.insert(
                        r.original_name.clone(),
                        serde_json::json!({"vocative":r.vocative,"id":id}),
                    );
                    results.insert(r.original_name.clone(), r);
                }
                completed += 1;
                since_save += 1;
                if since_save >= 5 {
                    cp.save(chunk_index, 0, std::mem::take(&mut pending), false)?;
                    since_save = 0;
                }
                log("BatchService","INFO",format!("Chunk {chunk_index} Progress: {:.1}% ({completed}/{} batches). Speed: {:.2} batch/s. Delay: {:.2}s, Workers: {}, BatchSizeCfg: {}",completed as f64/batches.len() as f64*100.0,batches.len(),completed as f64/start.elapsed().as_secs_f64(),delay.lock().unwrap().value,workers.value,batch_size.value));
                let _ = batch;
                workers.adjust(None);
                batch_size.adjust(None);
            }
        }
        cp.save(chunk_index, 0, pending, false)?;
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        for row in &mut rows {
            row.result = results.get(&row.name).cloned();
        }
        log(
            "__main__",
            "INFO",
            format!(
                "Appending processed chunk {chunk_index} to {}...",
                config.output.display()
            ),
        );
        write_rows(&output_temp, &rows, false)?;
        // A complete checkpoint is safe because the cache can reconstruct every row, including duplicates.
        cp.save(chunk_index, 0, Cache::new(), true)?;
        log(
            "__main__",
            "INFO",
            format!("Chunk {chunk_index} successfully appended."),
        );
        total += rows.len();
        chunk_index += 1;
    }
    if shutdown.load(Ordering::SeqCst) {
        let _ = fs::remove_file(output_temp);
    } else {
        fs::rename(output_temp, &config.output)?;
    }
    log("__main__", "INFO", "=== Total Processing Finished ===");
    log(
        "__main__",
        "INFO",
        format!(
            "Total execution time: {:.2} seconds",
            start.elapsed().as_secs_f64()
        ),
    );
    log(
        "__main__",
        "INFO",
        format!("Total records processed across all chunks: {total}"),
    );
    Ok(total)
}
