// Project: vocative-generator
// File: checkpoint.rs
// Description: Equivalents of fingerprint and resume tests with corruption and cursor coverage.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use std::fs;
use tempfile::tempdir;
use vocative_generator::checkpoint::*;
fn progress(cp: &mut Checkpoint) {
    let mut cache = Cache::new();
    cache.insert(
        "Jan Dvořák".into(),
        serde_json::json!({"vocative":"Jane Dvořáku"}),
    );
    cp.save(0, 1, cache, true).unwrap();
}
#[test]
fn fingerprints() {
    let d = tempdir().unwrap();
    let input = d.path().join("names.csv");
    assert_eq!(fingerprint_input(&input).unwrap(), None);
    fs::write(&input, "Name\nJan Dvorak\n").unwrap();
    let first = fingerprint_input(&input).unwrap();
    assert_eq!(first, fingerprint_input(&input).unwrap());
    fs::write(&input, "Name\nJan Dvorak\n").unwrap();
    assert_eq!(first, fingerprint_input(&input).unwrap());
    for text in ["Name\nJan Dvorbk\n", "Name\nPetr Kučera\n"] {
        fs::write(&input, text).unwrap();
        assert_ne!(first, fingerprint_input(&input).unwrap());
    }
}
#[test]
fn same_input_resumes_and_writes_fingerprint() {
    let d = tempdir().unwrap();
    let input = d.path().join("names.csv");
    let path = d.path().join("checkpoint.json");
    fs::write(&input, "Name\nJan Dvořák\n").unwrap();
    let mut cp = Checkpoint::load(&input, &path).unwrap();
    assert_eq!(cp.last_chunk_fully_processed_index, -1);
    progress(&mut cp);
    let saved: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        saved["input_fingerprint"].as_str(),
        fingerprint_input(&input).unwrap().as_deref()
    );
    let cp = Checkpoint::load(&input, &path).unwrap();
    assert_eq!(cp.last_chunk_fully_processed_index, 0);
    assert!(cp.processed_names.contains_key("Jan Dvořák"));
    assert_eq!(cp.resume_info(0), None);
    assert_eq!(cp.resume_info(1), Some(0));
}
#[test]
fn mismatch_and_legacy_checkpoint_are_left_alone() {
    let d = tempdir().unwrap();
    let input = d.path().join("names.csv");
    let path = d.path().join("checkpoint.json");
    fs::write(&input, "Name\nJan Dvořák\n").unwrap();
    let mut cp = Checkpoint::load(&input, &path).unwrap();
    progress(&mut cp);
    let before = fs::read(&path).unwrap();
    fs::write(&input, "Name\nMarie Nováková\nTomáš Svoboda\n").unwrap();
    let cp = Checkpoint::load(&input, &path).unwrap();
    assert_eq!(cp.last_chunk_fully_processed_index, -1);
    assert!(cp.processed_names.is_empty());
    assert_eq!(fs::read(&path).unwrap(), before);
    fs::write(
        &path,
        r#"{"last_chunk_fully_processed_index":5,"processed_names":{"a":{}}}"#,
    )
    .unwrap();
    assert_eq!(
        Checkpoint::load(&input, &path)
            .unwrap()
            .last_chunk_fully_processed_index,
        -1
    );
}
#[test]
fn corrupt_partial_clear_and_cache_filter() {
    let d = tempdir().unwrap();
    let input = d.path().join("names.csv");
    let path = d.path().join("checkpoint.json");
    fs::write(&input, "Name\nJan\n").unwrap();
    fs::write(&path, "not json").unwrap();
    let mut cp = Checkpoint::load(&input, &path).unwrap();
    assert!(!path.exists());
    let mut data = Cache::new();
    data.insert("".into(), serde_json::json!({"vocative":"x"}));
    data.insert("Jan".into(), serde_json::json!({"vocative":""}));
    cp.save(0, 3, data, false).unwrap();
    assert!(cp.processed_names.is_empty());
    assert_eq!(cp.resume_info(0), Some(3));
    assert_eq!(cp.resume_info(2), Some(0));
    cp.clear().unwrap();
    assert!(!path.exists());
    assert_eq!(cp.resume_info(0), Some(0));
}
