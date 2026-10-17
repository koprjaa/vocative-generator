// Project: vocative-generator
// File: lib.rs
// Description: Bulk Czech vocative conversion and persistence.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT

pub mod adapters;
pub mod checkpoint;
pub mod client;
pub mod pipeline;

use serde::{Deserialize, Serialize};

pub fn log(name: &str, level: &str, message: impl AsRef<str>) {
    eprintln!(
        "{} - {} - {} - {}",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
        name,
        level,
        message.as_ref()
    );
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NameResult {
    pub original_name: String,
    pub vocative: String,
    pub first_name: String,
    pub surname: String,
    pub success: bool,
    pub error_message: Option<String>,
}
impl NameResult {
    pub fn from_vocative(name: &str, vocative: &str) -> Self {
        let (first_name, surname) = Self::split_vocative(vocative);
        Self {
            original_name: name.into(),
            vocative: if vocative.is_empty() {
                name.into()
            } else {
                vocative.trim().into()
            },
            first_name,
            surname,
            success: !vocative.is_empty()
                && vocative.trim().to_lowercase() != name.trim().to_lowercase(),
            error_message: None,
        }
    }
    pub fn error(name: &str, message: impl Into<String>) -> Self {
        Self {
            original_name: name.into(),
            vocative: name.into(),
            first_name: String::new(),
            surname: String::new(),
            success: false,
            error_message: Some(message.into()),
        }
    }
    pub fn split_vocative(value: &str) -> (String, String) {
        let parts: Vec<_> = value.split_whitespace().collect();
        match parts.len() {
            0 => (String::new(), String::new()),
            1 => (parts[0].into(), String::new()),
            n => (parts[..n - 1].join(" "), parts[n - 1].into()),
        }
    }
}
pub fn extract_vocative(html: &str) -> String {
    let doc = scraper::Html::parse_document(html);
    let table = scraper::Selector::parse("table.table").unwrap();
    let row = scraper::Selector::parse("tr").unwrap();
    let cell = scraper::Selector::parse("td").unwrap();
    if let Some(table) = doc.select(&table).next() {
        for row in table.select(&row).take(10).skip(1) {
            if let Some(cell) = row.select(&cell).nth(1) {
                return cell.text().collect::<String>().trim().into();
            }
        }
    }
    String::new()
}
