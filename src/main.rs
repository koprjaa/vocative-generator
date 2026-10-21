// Project: vocative-generator
// File: main.rs
// Description: Fixed default command line and graceful platform signal handling.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use vocative_generator::{
    client::NameService,
    log,
    pipeline::{run, Config},
};
#[tokio::main]
async fn main() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = shutdown.clone();
    let signal = tokio::spawn(async move {
        #[cfg(unix)]
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        loop {
            #[cfg(unix)]
            let signum =
                tokio::select! {_ =tokio::signal::ctrl_c()=>"SIGINT",_=term.recv()=>"SIGTERM"};
            #[cfg(not(unix))]
            let signum = {
                let _ = tokio::signal::ctrl_c().await;
                "SIGINT"
            };
            if flag.swap(true, Ordering::SeqCst) {
                log(
                    "GracefulShutdownHandler",
                    "WARNING",
                    "Shutdown already in progress.",
                );
            } else {
                log(
                    "GracefulShutdownHandler",
                    "WARNING",
                    format!("Received signal {signum}. Requesting graceful shutdown..."),
                );
            }
        }
    });
    log("__main__", "INFO", "Starting main_wrapper...");
    let mut failed = false;
    match NameService::new("https://sklonuj.cz") {
        Ok(service) => match run(Config::default(), service, shutdown).await {
            Ok(_) => log("__main__", "INFO", "Main processing task completed."),
            Err(e) => {
                failed = true;
                log(
                    "__main__",
                    "ERROR",
                    format!("Main processing task failed with an exception: {e}"),
                )
            }
        },
        Err(e) => {
            failed = true;
            log(
                "__main__",
                "CRITICAL",
                format!("Critical error at top level: {e}"),
            )
        }
    }
    signal.abort();
    log(
        "__main__",
        "INFO",
        "Main_wrapper finishing. Performing final cleanup of tasks...",
    );
    log(
        "__main__",
        "INFO",
        "Application shutdown sequence complete.",
    );
    println!("Application has exited.");
    if failed {
        std::process::exit(1);
    }
}
