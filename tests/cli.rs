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
fn malformed_input_logs_error_and_exits_nonzero() {
    let d = tempdir().unwrap();
    fs::write(d.path().join("names.csv"), "Name,Other\nJan,x,extra\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_vocative-generator"))
        .current_dir(d.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("Main processing task failed with an exception"));
    assert_eq!(output.stdout, b"Application has exited.\n");
}

#[test]
fn missing_input_fails_without_creating_sample_or_calling_service() {
    let d = tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_vocative-generator"))
        .current_dir(d.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!d.path().join("names.csv").exists());
    assert!(!d.path().join("names_with_vocative.csv").exists());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("Input file names.csv not found"));
}
#[test]
fn non_utf8_input_gives_readable_error() {
    let d = tempdir().unwrap();
    fs::write(d.path().join("names.csv"), b"Name\nJan Kop\xf8iva\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_vocative-generator"))
        .current_dir(d.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("input must be UTF-8"));
}
