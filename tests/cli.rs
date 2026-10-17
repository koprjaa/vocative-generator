// Project: vocative-generator
// File: cli.rs
// Description: Offline CLI defaults, ignored arguments, logging and exit status.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use std::{fs, process::Command};
use tempfile::tempdir;
#[test]
fn defaults_ignored_arguments_and_stdout() {
    let d = tempdir().unwrap();
    fs::write(d.path().join("names.csv"), "Name\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_vocative-generator"))
        .args(["--help", "--unknown"])
        .current_dir(d.path())
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"Application has exited.\n");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("Starting main_wrapper..."));
    assert!(stderr.contains("Application shutdown sequence complete."));
    assert!(d.path().join("names_with_vocative.csv").exists());
}
#[test]
fn malformed_input_logs_error_and_retains_zero_exit() {
    let d = tempdir().unwrap();
    fs::write(d.path().join("names.csv"), "Name,Other\nJan,x,extra\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_vocative-generator"))
        .current_dir(d.path())
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("Main processing task failed with an exception"));
    assert_eq!(output.stdout, b"Application has exited.\n");
}

#[test]
fn missing_input_creates_original_twenty_row_sample_without_network() {
    use vocative_generator::checkpoint::{Cache, Checkpoint};
    let d = tempdir().unwrap();
    let input = d.path().join("names.csv");
    let mut writer = csv::WriterBuilder::new()
        .terminator(if cfg!(windows) {
            csv::Terminator::CRLF
        } else {
            csv::Terminator::Any(b'\n')
        })
        .from_path(&input)
        .unwrap();
    writer.write_record(["ID", "Name"]).unwrap();
    for i in 1..=20 {
        writer
            .write_record([i.to_string(), format!("First Name Last Name {i}")])
            .unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    let expected = fs::read(&input).unwrap();
    let mut cp = Checkpoint::load(&input, d.path().join("checkpoint.json")).unwrap();
    let mut cache = Cache::new();
    for i in 1..=20 {
        cache.insert(
            format!("First Name Last Name {i}"),
            serde_json::json!({"vocative":format!("First Name Last Name {i}"),"id":i.to_string()}),
        );
    }
    cp.save(-1, 0, cache, false).unwrap();
    fs::remove_file(&input).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_vocative-generator"))
        .current_dir(d.path())
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(fs::read(input).unwrap(), expected);
    assert_eq!(
        csv::Reader::from_path(d.path().join("names_with_vocative.csv"))
            .unwrap()
            .records()
            .count(),
        20
    );
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("Creating dummy input file names.csv for testing."));
}
