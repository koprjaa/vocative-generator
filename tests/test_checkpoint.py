#
# Project: vocative-generator
# File:    test_checkpoint.py
#
# Description:
# Tests that a checkpoint is only resumed for the input it was built from.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# License: MIT
#

"""A checkpoint records progress as a chunk number, which means nothing on its
own. A leftover checkpoint full of placeholder names made a completely different
input file report "chunk 0 was already fully processed" and the run did nothing.
The checkpoint now carries a hash of the input it belongs to.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import FILE_CONFIG
from src.services import CheckpointService, fingerprint_input


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setitem(FILE_CONFIG, "CHECKPOINT_FILE", "checkpoint.json")
    monkeypatch.setitem(FILE_CONFIG, "INPUT_FILE", "names.csv")
    return tmp_path


def write_input(workspace, names):
    (workspace / "names.csv").write_text(
        "Name\n" + "\n".join(names) + "\n", encoding="utf-8"
    )


def save_progress(service, chunk=0):
    service.save_checkpoint(chunk, 1, {"Jan Dvořák": {"vocative": "Jane Dvořáku"}}, True)


# --- fingerprint_input -------------------------------------------------------


def test_a_missing_input_has_no_fingerprint(workspace):
    assert fingerprint_input(workspace / "nothing.csv") is None


def test_the_same_bytes_give_the_same_fingerprint(workspace):
    write_input(workspace, ["Jan Dvořák"])
    first = fingerprint_input(workspace / "names.csv")
    write_input(workspace, ["Jan Dvořák"])
    assert fingerprint_input(workspace / "names.csv") == first


def test_different_bytes_give_a_different_fingerprint(workspace):
    write_input(workspace, ["Jan Dvořák"])
    first = fingerprint_input(workspace / "names.csv")
    write_input(workspace, ["Petr Kučera"])
    assert fingerprint_input(workspace / "names.csv") != first


def test_a_one_name_change_is_noticed(workspace):
    """Same row count, same length, one letter apart."""
    write_input(workspace, ["Jan Dvorak"])
    first = fingerprint_input(workspace / "names.csv")
    write_input(workspace, ["Jan Dvorbk"])
    assert fingerprint_input(workspace / "names.csv") != first


# --- resuming ----------------------------------------------------------------


def test_progress_on_the_same_input_is_resumed(workspace):
    write_input(workspace, ["Jan Dvořák"])
    save_progress(CheckpointService())

    resumed = CheckpointService()
    assert resumed.last_chunk_fully_processed_index == 0
    assert "Jan Dvořák" in resumed.processed_names


def test_progress_on_a_different_input_is_not_resumed(workspace):
    write_input(workspace, ["Jan Dvořák"])
    save_progress(CheckpointService())

    write_input(workspace, ["Marie Nováková", "Tomáš Svoboda"])
    fresh = CheckpointService()
    assert fresh.last_chunk_fully_processed_index == -1
    assert fresh.processed_names == {}


def test_the_checkpoint_file_is_left_alone_when_it_does_not_match(workspace):
    """It belongs to another run. Deleting it would throw that run's work away."""
    write_input(workspace, ["Jan Dvořák"])
    save_progress(CheckpointService())
    before = (workspace / "checkpoint.json").read_text(encoding="utf-8")

    write_input(workspace, ["Marie Nováková"])
    CheckpointService()
    assert (workspace / "checkpoint.json").read_text(encoding="utf-8") == before


def test_a_checkpoint_without_a_fingerprint_is_not_resumed(workspace):
    """Checkpoints written before this field existed cannot be trusted either."""
    write_input(workspace, ["Jan Dvořák"])
    (workspace / "checkpoint.json").write_text(
        json.dumps({"last_chunk_fully_processed_index": 5, "processed_names": {"a": {}}}),
        encoding="utf-8",
    )
    assert CheckpointService().last_chunk_fully_processed_index == -1


def test_the_fingerprint_is_written_into_the_checkpoint(workspace):
    write_input(workspace, ["Jan Dvořák"])
    save_progress(CheckpointService())
    saved = json.loads((workspace / "checkpoint.json").read_text(encoding="utf-8"))
    assert saved["input_fingerprint"] == fingerprint_input(workspace / "names.csv")


def test_no_checkpoint_file_starts_from_the_beginning(workspace):
    write_input(workspace, ["Jan Dvořák"])
    assert CheckpointService().last_chunk_fully_processed_index == -1
