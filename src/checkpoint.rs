// Project: vocative-generator
// File: checkpoint.rs
// Description: Input fingerprinting and resumable compact JSON checkpoints.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use crate::log;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
pub type Cache = serde_json::Map<String, serde_json::Value>;
pub fn fingerprint_input(path: impl AsRef<Path>) -> io::Result<Option<String>> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut digest = Sha256::new();
    let mut block = vec![0; 1024 * 1024];
    loop {
        let n = file.read(&mut block)?;
        if n == 0 {
            break;
        }
        digest.update(&block[..n]);
    }
    Ok(Some(format!("{:x}", digest.finalize())))
}
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
pub struct Checkpoint {
    pub input_fingerprint: Option<String>,
    pub last_chunk_fully_processed_index: i64,
    #[serde(alias = "last_batch")]
    pub last_batch_completed_for_current_chunk: usize,
    pub processed_names: Cache,
    #[serde(skip)]
    pub path: PathBuf,
}
impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            input_fingerprint: None,
            last_chunk_fully_processed_index: -1,
            last_batch_completed_for_current_chunk: 0,
            processed_names: Cache::new(),
            path: PathBuf::from("checkpoint.json"),
        }
    }
}
impl Checkpoint {
    pub fn load(input: impl AsRef<Path>, path: impl AsRef<Path>) -> io::Result<Self> {
        let fresh = Self {
            input_fingerprint: fingerprint_input(input)?,
            path: path.as_ref().into(),
            ..Self::default()
        };
        if !fresh.path.exists() {
            return Ok(fresh);
        }
        match fs::read(&fresh.path)
            .and_then(|b| serde_json::from_slice::<Self>(&b).map_err(io::Error::other))
        {
            Ok(mut saved)
                if saved.input_fingerprint.is_some()
                    && saved.input_fingerprint == fresh.input_fingerprint =>
            {
                saved.path = fresh.path;
                log("CheckpointService","INFO",format!("Checkpoint loaded: {} processed names, last_chunk_idx: {}, last_batch_in_chunk: {}",saved.processed_names.len(),saved.last_chunk_fully_processed_index,saved.last_batch_completed_for_current_chunk));
                Ok(saved)
            }
            Ok(_) => {
                log("CheckpointService","WARNING",format!("Checkpoint was built from a different input file. Starting from the beginning and leaving {} alone.",fresh.path.display()));
                Ok(fresh)
            }
            Err(_) => {
                log("CheckpointService", "ERROR", "Error loading checkpoint");
                fs::remove_file(&fresh.path)?;
                log(
                    "CheckpointService",
                    "INFO",
                    "Corrupted checkpoint file removed.",
                );
                Ok(fresh)
            }
        }
    }
    pub fn save(
        &mut self,
        chunk: i64,
        batch: usize,
        data: Cache,
        complete: bool,
    ) -> io::Result<()> {
        for (name, value) in data {
            if !name.is_empty()
                && value
                    .get("vocative")
                    .and_then(|v| v.as_str())
                    .is_some_and(|v| !v.is_empty())
            {
                self.processed_names.insert(name, value);
            }
        }
        let mut next = self.clone();
        if complete {
            next.last_chunk_fully_processed_index = chunk;
            next.last_batch_completed_for_current_chunk = 0;
        } else {
            next.last_batch_completed_for_current_chunk = batch;
        }
        let temp = self.path.with_extension("tmp");
        let mut f = File::create(&temp)?;
        f.write_all(&serde_json::to_vec(&next)?)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&temp, &self.path)?;
        *self = next;
        log("CheckpointService","INFO",format!("Checkpoint saved. Chunk_idx: {chunk}, batch_in_chunk: {batch}, is_chunk_complete: {}. Total processed: {}.",if complete {"True"} else {"False"},self.processed_names.len()));
        Ok(())
    }
    pub fn resume_info(&self, chunk: i64) -> Option<usize> {
        if chunk <= self.last_chunk_fully_processed_index {
            None
        } else if chunk == self.last_chunk_fully_processed_index + 1 {
            Some(self.last_batch_completed_for_current_chunk)
        } else {
            Some(0)
        }
    }
    pub fn clear(&mut self) -> io::Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        self.processed_names.clear();
        self.last_chunk_fully_processed_index = -1;
        self.last_batch_completed_for_current_chunk = 0;
        log(
            "CheckpointService",
            "INFO",
            "Checkpoint cleared for a new run.",
        );
        Ok(())
    }
}
