"""Tests for reading the vocative out of a sklonuj.cz results page.

The markup follows the real page: a results table whose first data row holds the
nominative in one cell and the vocative in the next.
"""

import pytest
from bs4 import BeautifulSoup

from src.parsing import extract_vocative, find_result_table


def page(rows, table_class="table table-hover table-striped table-bordered"):
    body = "".join(f"<tr><td>{a}</td><td>{b}</td></tr>" for a, b in rows)
    return (
        f'<html><body><table class="{table_class}">'
        f"<tr><th>pád</th><th>tvar</th></tr>{body}"
        f"</table></body></html>"
    )


# The page lists the vocative first, which is what the tool is after. The other
# cases follow below it and are ignored.
CASES = [("5. pád", "Jene"), ("1. pád", "Jan")]


# --- extract_vocative -------------------------------------------------------


def test_the_vocative_is_read_from_the_first_data_row():
    assert extract_vocative(page(CASES)) == "Jene"


def test_the_header_row_is_skipped():
    """Reading row zero would return the column label instead of a name."""
    assert extract_vocative(page(CASES)) != "tvar"


def test_the_later_cases_are_ignored():
    assert extract_vocative(page(CASES)) != "Jan"


def test_whitespace_around_the_form_is_stripped():
    assert extract_vocative(page([("1. pád", "  Jene \n ")])) == "Jene"


def test_a_form_carrying_diacritics_survives():
    assert extract_vocative(page([("5. pád", "Kopřivo")])) == "Kopřivo"


@pytest.mark.parametrize(
    "table_class",
    ["table", "table table-hover table-striped table-bordered", "table custom-thing"],
)
def test_any_bootstrap_table_markup_is_read(table_class):
    """class_ matches one css class, so extra classes on the tag do not matter."""
    assert extract_vocative(page(CASES, table_class)) == "Jene"


def test_a_page_with_no_table_gives_empty():
    assert extract_vocative("<html><body><p>Nenalezeno</p></body></html>") == ""


def test_a_table_with_only_a_header_gives_empty():
    assert extract_vocative(page([])) == ""


def test_a_row_with_one_cell_is_skipped():
    markup = (
        '<table class="table">'
        "<tr><th>pád</th></tr><tr><td>osamocená</td></tr>"
        "<tr><td>5. pád</td><td>Jene</td></tr></table>"
    )
    assert extract_vocative(markup) == "Jene"


@pytest.mark.parametrize("html", ["", None, "not html at all", "<html></html>"])
def test_a_page_that_is_not_a_result_gives_empty(html):
    assert extract_vocative(html) == ""


def test_an_error_page_gives_empty_rather_than_a_wrong_name():
    """A rate limit page still parses as HTML, so it must not yield a form."""
    assert extract_vocative("<html><body><h1>429 Too Many Requests</h1></body></html>") == ""


def test_only_the_first_data_row_is_returned():
    rows = [("5. pád", "Jene"), ("1. pád", "Jan")]
    assert extract_vocative(page(rows)) == "Jene"


# --- find_result_table ------------------------------------------------------


def test_the_first_table_on_the_page_is_used():
    markup = (
        "<html><body>"
        '<table class="table"><tr><td>a</td><td>první</td></tr></table>'
        '<table class="table"><tr><td>a</td><td>druhá</td></tr></table>'
        "</body></html>"
    )
    table = find_result_table(BeautifulSoup(markup, "html.parser"))
    assert "první" in table.get_text()


def test_a_table_without_the_bootstrap_class_is_ignored():
    markup = "<html><body><table><tr><td>a</td><td>Jene</td></tr></table></body></html>"
    assert find_result_table(BeautifulSoup(markup, "html.parser")) is None


def test_no_table_gives_none():
    assert find_result_table(BeautifulSoup("<html></html>", "html.parser")) is None
