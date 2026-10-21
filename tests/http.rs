// Project: vocative-generator
// File: http.rs
// Description: Offline form, headers, cookies, concurrency and retry verification.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use vocative_generator::client::{Backoff, NameService, USER_AGENTS};
use wiremock::{
    matchers::{body_string_contains, header, method, path},
    Mock, MockServer, ResponseTemplate,
};
const HTML: &str = include_str!("fixtures/result.html");
async fn get(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/generator-osloveni/"))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Set-Cookie", "session=unique; Path=/"),
        )
        .mount(server)
        .await;
}
#[tokio::test]
async fn form_headers_cookie_and_user_agent() {
    let server = MockServer::start().await;
    get(&server).await;
    Mock::given(method("POST"))
        .and(header("cookie", "session=unique"))
        .and(header("origin", server.uri()))
        .and(header(
            "referer",
            format!("{}/generator-osloveni/", server.uri()),
        ))
        .and(header("sec-fetch-site", "same-origin"))
        .and(header("content-type", "application/x-www-form-urlencoded"))
        .and(body_string_contains("inpJmena=Jan+Dvo%C5%99%C3%A1k"))
        .respond_with(ResponseTemplate::new(200).set_body_string(HTML))
        .expect(1)
        .mount(&server)
        .await;
    let svc = NameService::new(&server.uri()).unwrap();
    let r = svc.process_single_name("Jan Dvořák").await;
    assert_eq!(r.vocative, "Jane Dvořáku");
    assert!(r.success);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let ua = requests[0]
        .headers
        .get("user-agent")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(USER_AGENTS.contains(&ua));
    assert_eq!(
        requests[0].headers.get("user-agent"),
        requests[1].headers.get("user-agent")
    );
    assert_eq!(
        requests[0].headers["accept-language"],
        "cs,en-US;q=0.7,en;q=0.3"
    );
    assert_eq!(requests[0].headers["accept-encoding"], "gzip, deflate, br");
}
#[tokio::test]
async fn status_retries_six_attempts_and_terminal_auth() {
    for (status, expected) in [(429, 6), (500, 6), (503, 6), (404, 6), (401, 1), (403, 1)] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status))
            .expect(expected)
            .mount(&server)
            .await;
        let mut svc = NameService::new(&server.uri()).unwrap();
        svc.backoff_seconds = 0.0;
        let result = svc.process_single_name("Jan").await;
        assert!(!result.success);
        assert!(result.error_message.is_some());
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            expected as usize
        );
    }
}
#[tokio::test]
async fn post_retries_and_empty_result() {
    let server = MockServer::start().await;
    get(&server).await;
    let count = Arc::new(AtomicUsize::new(0));
    let n = count.clone();
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            if n.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(429)
            } else {
                ResponseTemplate::new(200).set_body_string(HTML)
            }
        })
        .mount(&server)
        .await;
    let mut svc = NameService::new(&server.uri()).unwrap();
    svc.backoff_seconds = 0.0;
    assert!(svc.process_single_name("Jan Dvořák").await.success);
    assert_eq!(count.load(Ordering::SeqCst), 2);
    server.reset().await;
    get(&server).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<h1>Nothing</h1>"))
        .mount(&server)
        .await;
    let result = svc.process_single_name("Jan").await;
    assert_eq!(result.vocative, "Jan");
    assert!(!result.success);
    assert!(result.error_message.is_some());
}
#[tokio::test]
async fn concurrent_cookie_pairs_do_not_leak() {
    let server = MockServer::start().await;
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    Mock::given(method("GET"))
        .respond_with(move |_: &wiremock::Request| {
            ResponseTemplate::new(200).insert_header(
                "set-cookie",
                format!("session={}; Path=/", c.fetch_add(1, Ordering::SeqCst)),
            )
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(HTML))
        .mount(&server)
        .await;
    let svc = NameService::new(&server.uri()).unwrap();
    let (a, b) = tokio::join!(
        svc.process_single_name("Jan"),
        svc.process_single_name("Petr")
    );
    assert!(a.success && b.success);
    let req = server.received_requests().await.unwrap();
    let cookies: Vec<_> = req
        .iter()
        .filter(|r| r.method == "POST")
        .map(|r| r.headers["cookie"].to_str().unwrap())
        .collect();
    assert_ne!(cookies[0], cookies[1]);
}
#[test]
fn backoff_cap_cooldown_and_repeated_success_reduction() {
    let mut b = Backoff::default();
    assert_eq!(b.failure("rate_limit", 1), 2.0);
    assert_eq!(b.failure("server_error", 2), 3.6);
    assert!((b.failure("connection_error", 3) - 5.4).abs() < 1e-12);
    for i in 0..10 {
        b.failure("rate_limit", i);
    }
    assert_eq!(b.factor, 20.0);
    for _ in 0..60 {
        b.success();
    }
    assert_eq!(b.factor, 20.0);
    b.last_increase = Some(Instant::now() - Duration::from_secs(61));
    b.success();
    assert_eq!(b.factor, 17.0);
    b.success();
    assert_eq!(b.factor, 14.45);
    for _ in 0..100 {
        b.success();
    }
    assert_eq!(b.factor, 1.0);
}

#[tokio::test]
async fn post_exhaustion_is_six_fresh_get_post_pairs() {
    let server = MockServer::start().await;
    get(&server).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .expect(6)
        .mount(&server)
        .await;
    let mut svc = NameService::new(&server.uri()).unwrap();
    svc.backoff_seconds = 0.0;
    let r = svc.process_single_name("Jan").await;
    assert_eq!(
        r.error_message.as_deref(),
        Some("Failed POST after 5 retries")
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 12);
    for pair in requests.chunks(2) {
        assert_eq!(pair[0].method, "GET");
        assert_eq!(pair[1].method, "POST");
    }
}

#[tokio::test]
async fn connection_errors_use_backoff_and_exhaust_attempts() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let mut svc = NameService::new(&uri).unwrap();
    svc.backoff_seconds = 0.0;
    let r = svc.process_single_name("Jan").await;
    assert_eq!(
        r.error_message.as_deref(),
        Some("Failed GET after 5 retries")
    );
    assert_eq!(svc.backoff.lock().unwrap().total_errors, 6);
}

#[tokio::test]
async fn request_timeout_is_twenty_seconds() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(25)))
        .mount(&server)
        .await;
    let mut svc = NameService::new(&server.uri()).unwrap();
    svc.max_retries = 0;
    svc.backoff_seconds = 0.0;
    let start = Instant::now();
    let r = svc.process_single_name("Jan").await;
    assert_eq!(
        r.error_message.as_deref(),
        Some("Failed GET after 0 retries")
    );
    assert!(start.elapsed() >= Duration::from_secs(19));
    assert_eq!(svc.backoff.lock().unwrap().total_errors, 1);
}
