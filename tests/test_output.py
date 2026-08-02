#
# Project: vocative-generator
# File:    test_output.py
#
# Description:
# Tests that a finished chunk actually reaches the output CSV.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# License: MIT
#

"""The run used to report success and write nothing.

process_chunk_data advances the checkpoint index, and the append was guarded on
that same index still being behind. The guard was therefore never true on a
clean run, so every result stayed in checkpoint.json and the CSV kept only its
header. These tests pin the write down.
"""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from main import OUTPUT_COLUMNS, _process_one_input_chunk
from src.config import FILE_CONFIG

NAME_COL = FILE_CONFIG["INPUT_COLUMN_NAME"]


def chunk(names, with_id=False):
    """A chunk as read from the input CSV.

    The default is name-only, which is what the documented input looks like. The
    ID column is then appended by the code under test, so the frame ends up with
    its columns in a different order than the output header.
    """
    if with_id:
        return pd.DataFrame({"ID": range(1, len(names) + 1), NAME_COL: names})
    return pd.DataFrame({NAME_COL: names})


class Batch:
    """Stands in for BatchService: fills the vocative and advances the checkpoint.

    Advancing the checkpoint is the part that matters. The real service does it,
    and doing it here is what made the old guard fall through.
    """

    def __init__(self, checkpoint, vocatives=None):
        self.checkpoint = checkpoint
        self.vocatives = vocatives

    async def process_chunk_data(self, chunk_df, index):
        chunk_df["Vocative"] = self.vocatives or [f"{n}!" for n in chunk_df[NAME_COL]]
        self.checkpoint.last_chunk_fully_processed_index = index


def run_one(tmp_path, names, *, already_done=-1, shutdown=False, vocatives=None, with_id=False):
    tmp_path = Path(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    out = tmp_path / "out.csv"
    pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(out, index=False, encoding="utf-8")
    checkpoint = SimpleNamespace(
        last_chunk_fully_processed_index=already_done, last_batch_completed_for_current_chunk=0
    )
    processed, stop = asyncio.run(
        _process_one_input_chunk(
            chunk(names, with_id),
            0,
            0,
            SimpleNamespace(shutdown_requested=shutdown),
            checkpoint,
            Batch(checkpoint, vocatives),
            str(out),
            len(names),
        )
    )
    return out, processed, stop


def rows(path):
    return pd.read_csv(path, encoding="utf-8")


def test_a_finished_chunk_is_written_to_the_output(tmp_path):
    out, _, _ = run_one(tmp_path, ["Jan Dvořák", "Petr Kučera"])
    assert len(rows(out)) == 2


def test_the_written_rows_carry_the_vocative(tmp_path):
    out, _, _ = run_one(tmp_path, ["Jan Dvořák"], vocatives=["Jane Dvořáku"])
    assert rows(out)["Vocative"].tolist() == ["Jane Dvořáku"]


def test_the_name_column_survives_the_round_trip(tmp_path):
    out, _, _ = run_one(tmp_path, ["Tomáš Svoboda"])
    assert rows(out)[NAME_COL].tolist() == ["Tomáš Svoboda"]


def test_values_land_under_the_header_they_belong_to(tmp_path):
    """An input without an ID column gets one appended at the end of the frame.

    Writing the frame in its own order then put the name under ID and the ID
    under the name, so every row was shifted against its header.
    """
    out, _, _ = run_one(tmp_path, ["Jan Dvořák"], vocatives=["Jane Dvořáku"])
    row = rows(out).iloc[0]
    assert row["ID"] == 1
    assert row[NAME_COL] == "Jan Dvořák"
    assert row["Vocative"] == "Jane Dvořáku"


def test_the_column_order_is_the_same_whether_the_input_carries_an_id(tmp_path):
    without = rows(run_one(tmp_path / "a", ["Jan Dvořák"])[0])
    with_id = rows(run_one(tmp_path / "b", ["Jan Dvořák"], with_id=True)[0])
    assert without.columns.tolist() == with_id.columns.tolist()
    assert without.iloc[0][NAME_COL] == with_id.iloc[0][NAME_COL] == "Jan Dvořák"


def test_the_header_is_not_repeated(tmp_path):
    out, _, _ = run_one(tmp_path, ["Jan Dvořák"])
    assert out.read_text(encoding="utf-8").count("Vocative First Name") == 1


def test_the_row_count_matches_what_the_run_reports(tmp_path):
    names = ["Jan Dvořák", "Petr Kučera", "Marie Nováková"]
    out, processed, _ = run_one(tmp_path, names)
    assert processed == len(names) == len(rows(out))


def test_a_chunk_an_earlier_run_finished_is_not_written_twice(tmp_path):
    """The guard for that case is the early return, not the append condition."""
    out, _, _ = run_one(tmp_path, ["Jan Dvořák"], already_done=0)
    assert len(rows(out)) == 0


@pytest.mark.parametrize("names", [["Jan Dvořák"], ["A", "B", "C", "D"]])
def test_every_input_row_reaches_the_output(tmp_path, names):
    out, _, _ = run_one(tmp_path, names)
    assert len(rows(out)) == len(names)


def test_rows_with_an_empty_name_are_dropped_before_writing(tmp_path):
    out, _, _ = run_one(tmp_path, ["Jan Dvořák", "   ", ""])
    assert rows(out)[NAME_COL].tolist() == ["Jan Dvořák"]


def test_a_shutdown_before_the_chunk_starts_writes_nothing(tmp_path):
    out, _, stop = run_one(tmp_path, ["Jan Dvořák"], shutdown=True)
    assert stop is True
    assert len(rows(out)) == 0
