// Project: vocative-generator
// File: output.rs
// Description: Original CSV regression tests plus duplicate, resume and shutdown fixes.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use std::{
    fs,
    sync::{atomic::AtomicBool, Arc},
};
use tempfile::tempdir;
use vocative_generator::{
    checkpoint::{Cache, Checkpoint},
    client::NameService,
    pipeline::*,
};
fn config(d: &std::path::Path) -> Config {
    Config {
        input: d.join("names.csv"),
        output: d.join("out.csv"),
        checkpoint: d.join("checkpoint.json"),
        delay: 0.0,
        ..Config::default()
    }
}
fn seed(c: &Config) {
    let mut cp = Checkpoint::load(&c.input, &c.checkpoint).unwrap();
    let mut data = Cache::new();
    for (name, vocative) in [
        ("Jan Dvořák", "Jane Dvořáku"),
        ("Petr Kučera", "Petře Kučero"),
        ("Marie Nováková", "Marie Nováková"),
        ("Tomáš Svoboda", "Tomáši Svobodo"),
        ("A", "A!"),
        ("B", "B!"),
        ("C", "C!"),
        ("D", "D!"),
    ] {
        data.insert(
            name.into(),
            serde_json::json!({"vocative":vocative,"id":"1"}),
        );
    }
    cp.save(-1, 0, data, false).unwrap();
}
async fn execute(c: Config, stop: bool) -> usize {
    run(
        c,
        NameService::new("http://127.0.0.1:1").unwrap(),
        Arc::new(AtomicBool::new(stop)),
    )
    .await
    .unwrap()
}
fn rows(c: &Config) -> Vec<csv::StringRecord> {
    csv::Reader::from_path(&c.output)
        .unwrap()
        .records()
        .map(|r| r.unwrap())
        .collect()
}
#[tokio::test]
async fn finished_chunk_values_names_header_order_counts_and_no_repeat() {
    let d = tempdir().unwrap();
    let c = config(d.path());
    fs::write(&c.input, "Name\nJan Dvořák\nPetr Kučera\nTomáš Svoboda\n").unwrap();
    seed(&c);
    assert_eq!(execute(c.clone(), false).await, 3);
    let result = rows(&c);
    assert_eq!(result.len(), 3);
    assert_eq!(&result[0][0], "1");
    assert_eq!(&result[0][1], "Jan Dvořák");
    assert_eq!(&result[0][2], "Jane Dvořáku");
    assert_eq!(&result[2][1], "Tomáš Svoboda");
    assert_eq!(&result[0][3], "Jane");
    assert_eq!(&result[0][4], "Dvořáku");
    assert_eq!(&result[0][5], "");
    assert_eq!(
        csv::Reader::from_path(&c.output)
            .unwrap()
            .headers()
            .unwrap(),
        &csv::StringRecord::from(OUTPUT_COLUMNS.to_vec())
    );
    assert_eq!(
        fs::read_to_string(&c.output)
            .unwrap()
            .matches("Vocative First Name")
            .count(),
        1
    );
    let before = fs::read(&c.output).unwrap();
    execute(c.clone(), false).await;
    assert_eq!(fs::read(&c.output).unwrap(), before);
}
#[tokio::test]
async fn id_and_no_id_column_order_matches() {
    let mut results = Vec::new();
    for text in ["Name\nJan Dvořák\n", "ID,Name\n1,Jan Dvořák\n"] {
        let d = tempdir().unwrap();
        let c = config(d.path());
        fs::write(&c.input, text).unwrap();
        seed(&c);
        execute(c.clone(), false).await;
        results.push(fs::read(&c.output).unwrap());
    }
    assert_eq!(results[0], results[1]);
}
#[tokio::test]
async fn every_row_reaches_output() {
    for names in ["Jan Dvořák\n", "A\nB\nC\nD\n"] {
        let d = tempdir().unwrap();
        let c = config(d.path());
        fs::write(&c.input, format!("Name\n{names}")).unwrap();
        seed(&c);
        let n = execute(c.clone(), false).await;
        assert_eq!(n, names.lines().count());
        assert_eq!(rows(&c).len(), n);
    }
}
#[tokio::test]
async fn empty_names_dropped_and_generated_ids_before_cleaning() {
    let d = tempdir().unwrap();
    let c = config(d.path());
    fs::write(&c.input, "Name\nJan Dvořák\n   \n\"\"\nNA\nPetr Kučera\n").unwrap();
    seed(&c);
    execute(c.clone(), false).await;
    let rows = rows(&c);
    assert_eq!(rows.len(), 2);
    assert_eq!(&rows[0][1], "Jan Dvořák");
    assert_eq!(&rows[1][0], "5");
}
#[tokio::test]
async fn shutdown_before_chunk_writes_nothing() {
    let d = tempdir().unwrap();
    let c = config(d.path());
    fs::write(&c.input, "Name\nJan Dvořák\n").unwrap();
    assert_eq!(execute(c.clone(), true).await, 0);
    assert!(!c.output.exists());
    assert!(!c.output.with_extension("csv.tmp").exists());
}
#[tokio::test]
async fn duplicates_and_partial_cursor_restore_every_row() {
    let d = tempdir().unwrap();
    let mut c = config(d.path());
    c.chunk_size = 2;
    c.batch_size = 1;
    fs::write(&c.input, "Name\nJan Dvořák\nJan Dvořák\nPetr Kučera\n").unwrap();
    seed(&c);
    let mut cp = Checkpoint::load(&c.input, &c.checkpoint).unwrap();
    cp.save(0, 1, Cache::new(), false).unwrap();
    execute(c.clone(), false).await;
    assert_eq!(rows(&c).len(), 3);
    assert!(rows(&c).iter().all(|r| !r[2].is_empty()));
    let before = fs::read(&c.output).unwrap();
    fs::remove_file(&c.output).unwrap();
    execute(c.clone(), false).await;
    assert_eq!(fs::read(&c.output).unwrap(), before);
}
#[tokio::test]
async fn missing_column_and_empty_input() {
    for input in ["Other\nJan\n", "Name\n"] {
        let d = tempdir().unwrap();
        let c = config(d.path());
        fs::write(&c.input, input).unwrap();
        execute(c.clone(), false).await;
        assert!(rows(&c).is_empty());
    }
}
#[test]
fn quoting_unicode_and_existing_ids() {
    let d = tempdir().unwrap();
    let p = d.path().join("out.csv");
    let rows = vec![Row {
        id: "007".into(),
        name: "Jan, \"Pavel\"".into(),
        result: Some(vocative_generator::NameResult::error(
            "Jan, \"Pavel\"",
            "Error\nmessage",
        )),
    }];
    write_rows(&p, &rows, true).unwrap();
    let r = csv::Reader::from_path(p)
        .unwrap()
        .records()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(&r[0], "007");
    assert_eq!(&r[1], "Jan, \"Pavel\"");
    assert_eq!(&r[5], "Error\nmessage");
}

#[tokio::test]
async fn concurrent_batches_keep_output_order_and_sequential_names() {
    use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_millis(50)))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(include_str!("fixtures/result.html")),
        )
        .mount(&server)
        .await;
    let d = tempdir().unwrap();
    let mut c = config(d.path());
    c.workers = 2;
    c.batch_size = 2;
    fs::write(&c.input, "Name\nJan\nPetr\nMarie\nTomáš\n").unwrap();
    run(
        c.clone(),
        NameService::new(&server.uri()).unwrap(),
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    let rows = rows(&c);
    assert_eq!(
        rows.iter().map(|r| &r[1]).collect::<Vec<_>>(),
        vec!["Jan", "Petr", "Marie", "Tomáš"]
    );
    assert!(rows.iter().all(|r| r[2] == *"Jane Dvořáku"));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 8);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[1].method, "GET");
}

#[tokio::test]
async fn shutdown_during_batch_saves_completed_name_and_resume_recovers_all() {
    use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    Mock::given(method("GET"))
        .respond_with(move |_: &wiremock::Request| {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            ResponseTemplate::new(200)
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(include_str!("fixtures/result.html")),
        )
        .mount(&server)
        .await;
    let d = tempdir().unwrap();
    let mut c = config(d.path());
    c.workers = 1;
    c.batch_size = 2;
    fs::write(&c.input, "Name\nJan\nPetr\n").unwrap();
    fs::write(&c.output, "previous output").unwrap();
    run(c.clone(), NameService::new(&server.uri()).unwrap(), stop)
        .await
        .unwrap();
    assert_eq!(fs::read_to_string(&c.output).unwrap(), "previous output");
    let cp = Checkpoint::load(&c.input, &c.checkpoint).unwrap();
    assert!(cp.processed_names.contains_key("Jan"));
    assert_eq!(cp.last_chunk_fully_processed_index, -1);
    server.reset().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(include_str!("fixtures/result.html")),
        )
        .mount(&server)
        .await;
    run(
        c.clone(),
        NameService::new(&server.uri()).unwrap(),
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    assert_eq!(rows(&c).len(), 2);
    assert!(rows(&c).iter().all(|r| !r[2].is_empty()));
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}
#[tokio::test]
async fn failed_lookup_is_reported_and_not_cached() {
    use wiremock::{
        matchers::{body_string_contains, method},
        Mock, MockServer, ResponseTemplate,
    };
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_string_contains("Broken"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<h1>Nothing</h1>"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(include_str!("fixtures/result.html")),
        )
        .mount(&server)
        .await;
    let d = tempdir().unwrap();
    let c = config(d.path());
    fs::write(&c.input, "ID,Name\n1,Jan Dvořák\n2,Broken\n").unwrap();
    let svc = NameService::new(&server.uri()).unwrap();
    run(c.clone(), svc, Arc::new(AtomicBool::new(false)))
        .await
        .unwrap();
    let cp = Checkpoint::load(&c.input, &c.checkpoint).unwrap();
    assert!(cp.processed_names.contains_key("Jan Dvořák"));
    assert!(!cp.processed_names.contains_key("Broken"));
    let out = fs::read_to_string(&c.output).unwrap();
    assert!(out.contains("No vocative found"));
}
