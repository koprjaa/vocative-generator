// Project: vocative-generator
// File: client.rs
// Description: Cookie paired form requests with shared adaptive backoff.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use crate::{extract_vocative, log, NameResult};
use rand::Rng;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
pub const USER_AGENTS:[&str;5]=[
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"];
pub fn headers() -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in [
        ("user-agent", USER_AGENTS[rand::rng().random_range(0..5)]),
        (
            "accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        ),
        ("accept-language", "cs,en-US;q=0.7,en;q=0.3"),
        ("accept-encoding", "gzip, deflate, br"),
        ("connection", "keep-alive"),
        ("upgrade-insecure-requests", "1"),
        ("sec-fetch-dest", "document"),
        ("sec-fetch-mode", "navigate"),
        ("sec-fetch-site", "none"),
        ("sec-fetch-user", "?1"),
        ("cache-control", "max-age=0"),
    ] {
        h.insert(k, HeaderValue::from_str(v).unwrap());
    }
    h
}
#[derive(Debug)]
pub struct Backoff {
    pub factor: f64,
    pub successes: usize,
    pub total_requests: usize,
    pub total_errors: usize,
    pub last_increase: Option<Instant>,
}
impl Default for Backoff {
    fn default() -> Self {
        Self {
            factor: 1.0,
            successes: 0,
            total_requests: 0,
            total_errors: 0,
            last_increase: None,
        }
    }
}
impl Backoff {
    pub fn failure(&mut self, kind: &str, attempt: usize) -> f64 {
        self.successes = 0;
        self.total_errors += 1;
        let old = self.factor;
        self.factor = (self.factor
            * match kind {
                "rate_limit" => 2.0,
                "server_error" => 1.8,
                _ => 1.5,
            })
        .min(20.0);
        if self.factor > old {
            self.last_increase = Some(Instant::now());
            log(
                "NameService",
                "WARNING",
                format!(
                    "Backoff factor increased: {old:.1} -> {:.1} ({kind}, attempt {attempt})",
                    self.factor
                ),
            );
        }
        self.factor
    }
    pub fn success(&mut self) {
        self.successes += 1;
        self.total_requests += 1;
        if self
            .last_increase
            .is_some_and(|i| i.elapsed() < Duration::from_secs(60))
        {
            return;
        }
        if self.successes >= 50 {
            let old = self.factor;
            self.factor = (old * 0.85).max(1.0);
            if self.factor < old {
                log(
                    "NameService",
                    "INFO",
                    format!(
                        "Backoff factor reduced: {old:.1} -> {:.1} after {} consecutive successes",
                        self.factor, self.successes
                    ),
                );
            }
        }
    }
}
#[derive(Clone)]
pub struct NameService {
    client: Client,
    pub base_url: String,
    pub endpoint: String,
    pub max_retries: usize,
    pub backoff: Arc<Mutex<Backoff>>,
    pub backoff_seconds: f64,
}
impl NameService {
    pub fn new(base_url: &str) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(20))
                .pool_max_idle_per_host(100)
                .no_proxy()
                .build()?,
            base_url: base_url.into(),
            endpoint: "/generator-osloveni/".into(),
            max_retries: 5,
            backoff: Arc::new(Mutex::new(Backoff::default())),
            backoff_seconds: 1.0,
        })
    }
    async fn delay(&self, kind: &str, attempt: usize) {
        let factor = self.backoff.lock().unwrap().failure(kind, attempt);
        let delay = self.backoff_seconds * factor * rand::rng().random_range(1.0..=1.5);
        log("NameService","INFO",format!("Rate limit/server error handling: waiting {delay:.2}s (backoff factor: {factor:.1}, error type: {kind}, attempt: {attempt})"));
        tokio::time::sleep(Duration::from_secs_f64(delay)).await;
    }
    pub async fn process_single_name(&self, name: &str) -> NameResult {
        let url = match reqwest::Url::parse(&self.base_url).and_then(|u| u.join(&self.endpoint)) {
            Ok(u) => u,
            Err(e) => return NameResult::error(name, format!("Unexpected error: {e}")),
        };
        for attempt in 1..=self.max_retries + 1 {
            // Each lookup owns its cookie jar so simultaneous GET/POST pairs cannot overwrite cookies.
            let mut h = headers();
            let mut failure = None;
            for method in ["GET", "POST"] {
                let request = if method == "GET" {
                    self.client.get(url.clone()).headers(h.clone())
                } else {
                    h.insert(
                        "content-type",
                        HeaderValue::from_static("application/x-www-form-urlencoded"),
                    );
                    h.insert("origin", HeaderValue::from_str(&self.base_url).unwrap());
                    h.insert("referer", HeaderValue::from_str(url.as_str()).unwrap());
                    h.insert("sec-fetch-site", HeaderValue::from_static("same-origin"));
                    self.client
                        .post(url.clone())
                        .headers(h.clone())
                        .form(&[("inpJmena", name)])
                };
                match request.send().await {
                    Err(e) => {
                        if e.is_timeout() {
                            log(
                                "NameService",
                                "WARNING",
                                if method == "GET" {
                                    format!("Timeout on GET {url} (Attempt: {attempt}).")
                                } else {
                                    format!("Timeout on POST for '{name}' (Attempt: {attempt}).")
                                },
                            );
                        } else {
                            log("NameService","WARNING",format!("ClientError on {method} for '{name}': {e} (Attempt: {attempt})."));
                        }
                        self.delay("connection_error", attempt).await;
                        failure = Some(format!("Failed {method} after {} retries", attempt - 1));
                        break;
                    }
                    Ok(response) => {
                        let status = response.status();
                        if status.as_u16() == 429 || status.is_server_error() {
                            self.delay(
                                if status.as_u16() == 429 {
                                    "rate_limit"
                                } else {
                                    "server_error"
                                },
                                attempt,
                            )
                            .await;
                            failure =
                                Some(format!("Failed {method} after {} retries", attempt - 1));
                            break;
                        }
                        if status.is_client_error() {
                            let message = format!(
                                "HTTP {}: {}",
                                status.as_u16(),
                                status.canonical_reason().unwrap_or("")
                            );
                            if [401, 403].contains(&status.as_u16()) {
                                log(
                                    "NameService",
                                    "WARNING",
                                    format!(
                                        "Non-recoverable HTTP error for '{name}': {} {}",
                                        status.as_u16(),
                                        status.canonical_reason().unwrap_or("")
                                    ),
                                );
                                return NameResult::error(name, message);
                            }
                            failure = Some(message);
                            break;
                        }
                        if method == "GET" {
                            let cookies = response
                                .cookies()
                                .map(|c| format!("{}={}", c.name(), c.value()))
                                .collect::<Vec<_>>()
                                .join("; ");
                            if !cookies.is_empty() {
                                if let Ok(v) = HeaderValue::from_str(&cookies) {
                                    h.insert("cookie", v);
                                }
                            }
                        } else {
                            let html = match response.text().await {
                                Ok(t) => t,
                                Err(_) => {
                                    self.delay("connection_error", attempt).await;
                                    failure =
                                        Some(format!("Failed POST after {} retries", attempt - 1));
                                    break;
                                }
                            };
                            let voc = extract_vocative(&html);
                            if voc.is_empty() {
                                log(
                                    "NameService",
                                    "WARNING",
                                    format!(
                                        "No vocative for '{name}'. HTML preview (1000 chars):\n{}",
                                        html.chars().take(1000).collect::<String>()
                                    ),
                                );
                            }
                            let rate = {
                                let mut state = self.backoff.lock().unwrap();
                                state.success();
                                (state.total_requests as f64 - state.total_errors as f64)
                                    / state.total_requests.max(1) as f64
                                    * 100.0
                            };
                            log(
                                "NameService",
                                "INFO",
                                format!(
                                    "Processed '{name}'. Vocative: '{}'. Success rate: {rate:.1}%",
                                    if voc.is_empty() { "(empty)" } else { &voc }
                                ),
                            );
                            return NameResult::from_vocative(
                                name,
                                if voc.is_empty() { name } else { &voc },
                            );
                        }
                    }
                }
            }
            if attempt > self.max_retries {
                return NameResult::error(
                    name,
                    failure
                        .unwrap_or_else(|| format!("Failed to process after {attempt} attempts")),
                );
            }
        }
        unreachable!()
    }
}
