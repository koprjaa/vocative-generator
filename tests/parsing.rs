// Project: vocative-generator
// File: parsing.rs
// Description: Equivalents of all original HTML parser tests and model edge cases.
// Author: Jan Alexandr Kopřiva
// Based on vocative-generator by Jan Alexandr Kopřiva
// License: MIT
use vocative_generator::{extract_vocative, NameResult};
fn page(class: &str, body: &str) -> String {
    format!("<table class='{class}'><tr><th>pád</th><th>tvar</th></tr>{body}</table>")
}
#[test]
fn first_data_row_header_and_later_cases() {
    let html = page(
        "table",
        "<tr><td>5. pád</td><td>Jene</td></tr><tr><td>1. pád</td><td>Jan</td></tr>",
    );
    assert_eq!(extract_vocative(&html), "Jene");
    assert_ne!(extract_vocative(&html), "tvar");
    assert_ne!(extract_vocative(&html), "Jan");
}
#[test]
fn whitespace_diacritics_classes() {
    for class in [
        "table",
        "table table-hover table-striped table-bordered",
        "table custom-thing",
    ] {
        assert_eq!(
            extract_vocative(&page(class, "<tr><td>5</td><td>  Kopřivo \n </td></tr>")),
            "Kopřivo"
        );
    }
}
#[test]
fn missing_and_invalid_results() {
    for html in [
        "",
        "not html at all",
        "<html></html>",
        "<html><body><p>Nenalezeno</p></body></html>",
        "<html><body><h1>429 Too Many Requests</h1></body></html>",
        "<table><tr><td>a</td><td>Jene</td></tr></table>",
    ] {
        assert_eq!(extract_vocative(html), "");
    }
    assert_eq!(extract_vocative(&page("table", "")), "");
}
#[test]
fn short_rows_are_skipped_first_table_wins() {
    let first = page(
        "table",
        "<tr><td>alone</td></tr><tr><td>5</td><td>první</td></tr>",
    );
    assert_eq!(
        extract_vocative(&format!(
            "{first}{}",
            page("table", "<tr><td>5</td><td>druhá</td></tr>")
        )),
        "první"
    );
}
#[test]
fn row_limit_and_empty_cell() {
    let body = format!(
        "{}<tr><td>5</td><td>Jene</td></tr>",
        "<tr><td>alone</td></tr>".repeat(9)
    );
    assert_eq!(extract_vocative(&page("table", &body)), "");
    assert_eq!(
        extract_vocative(&page(
            "table",
            "<tr><td>5</td><td></td></tr><tr><td>1</td><td>Jan</td></tr>"
        )),
        ""
    );
    assert_eq!(
        extract_vocative(&page(
            "table",
            "<tr><td>5</td><td><b>Je</b>ne &amp; synu</td></tr>"
        )),
        "Jene & synu"
    );
}
#[test]
fn model_edges() {
    for (s, first, last) in [
        ("", "", ""),
        (" \n ", "", ""),
        ("Jene", "Jene", ""),
        ("  Jane   Pavle Nováku ", "Jane Pavle", "Nováku"),
    ] {
        assert_eq!(NameResult::split_vocative(s), (first.into(), last.into()));
    }
    assert!(!NameResult::from_vocative("Jan", " jan ").success);
    assert!(NameResult::from_vocative("Jan", "Jene").success);
    assert_eq!(NameResult::from_vocative("Jan", "").vocative, "Jan");
    let e = NameResult::error("Jan", "failure");
    assert!(!e.success);
    assert_eq!(e.first_name, "");
    assert_eq!(e.error_message.as_deref(), Some("failure"));
}
