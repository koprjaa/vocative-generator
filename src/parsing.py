# Project: Vocative Generator
# File:    src/parsing.py
#
# Description:
# Reads the vocative form out of a sklonuj.cz results page.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# License: MIT

from bs4 import BeautifulSoup

# The site renders its results in a Bootstrap table. class_ matches a single
# css class rather than the whole attribute, so "table" covers both the current
# markup, which also carries table-hover and friends, and the plainer markup
# older versions of the site used.
RESULT_TABLE_CLASS = "table"

# Rows read before giving up. A results table holds a header and the seven
# cases, so anything past this belongs to a different table.
MAX_ROWS = 10


def find_result_table(soup: BeautifulSoup):
    """The first results table on the page, or None when there is none."""
    return soup.find("table", class_=RESULT_TABLE_CLASS)


def extract_vocative(html: str) -> str:
    """Vocative form from a results page, or empty when it is not there.

    The first data row holds the declined form in its second cell. An empty
    result means the page did not carry one, which the caller reports as a
    failed lookup rather than as a name with no vocative.
    """
    if not html:
        return ""

    table = find_result_table(BeautifulSoup(html, "html.parser"))
    if table is None:
        return ""

    rows = table.find_all("tr", limit=MAX_ROWS)
    for row in rows[1:]:
        cells = row.find_all("td", limit=2)
        if len(cells) >= 2:
            return cells[1].get_text().strip()
    return ""
