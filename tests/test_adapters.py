#
# Project: vocative-generator
# File:    test_adapters.py
#
# Description:
# Tests for the value the pipeline tunes from recent success rates.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# License: MIT
#

"""Tests for the value the pipeline tunes from recent success rates.

One class drives the request delay, the worker ceiling and the batch size. They
differ only in their bands, so the bands are tested as data and the movement is
tested once.
"""

import pytest

from src.adapters import (
    BATCH_BANDS,
    DELAY_BANDS,
    WORKER_BANDS,
    AdaptiveValue,
    make_batch_size,
    make_delay,
    make_workers,
)


def ready(value: AdaptiveValue) -> AdaptiveValue:
    """Move the clock past the interval so the next adjust is allowed."""
    value.last_adjustment = 0.0
    return value


def counter(initial=10, minimum=1, maximum=100, bands=None, integer=True):
    return AdaptiveValue(
        initial, minimum, maximum, bands or WORKER_BANDS, "Test", integer=integer, interval=0.0
    )


# --- bands ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rate", "expected"),
    [(1.0, 0.85), (0.95, 0.85), (0.94, 1.0), (0.80, 1.0), (0.79, 1.8), (0.0, 1.8)],
)
def test_the_delay_bands(rate, expected):
    """A good batch speeds up, a bad one backs off, the middle holds still."""
    assert AdaptiveValue(1.0, 0.1, 5, DELAY_BANDS, "d").factor_for(rate) == expected


@pytest.mark.parametrize(
    ("rate", "expected"),
    [(1.0, 1.1), (0.9, 1.1), (0.89, 1.0), (0.8, 1.0), (0.79, 0.8), (0.0, 0.8)],
)
def test_the_worker_bands(rate, expected):
    assert AdaptiveValue(10, 1, 100, WORKER_BANDS, "w").factor_for(rate) == expected


@pytest.mark.parametrize(
    ("rate", "expected"),
    [(1.0, 1.1), (0.95, 1.1), (0.94, 1.0), (0.75, 1.0), (0.74, 0.8), (0.0, 0.8)],
)
def test_the_batch_bands(rate, expected):
    assert AdaptiveValue(100, 10, 200, BATCH_BANDS, "b").factor_for(rate) == expected


@pytest.mark.parametrize("bands", [DELAY_BANDS, WORKER_BANDS, BATCH_BANDS])
def test_every_band_table_ends_with_a_catch_all(bands):
    """Without a zero threshold a total failure would match nothing."""
    assert bands[-1][0] == 0.0


@pytest.mark.parametrize("bands", [DELAY_BANDS, WORKER_BANDS, BATCH_BANDS])
def test_band_thresholds_descend(bands):
    """The first match wins, so an out of order table would shadow a band."""
    thresholds = [threshold for threshold, _ in bands]
    assert thresholds == sorted(thresholds, reverse=True)


@pytest.mark.parametrize("bands", [WORKER_BANDS, BATCH_BANDS, DELAY_BANDS])
def test_every_band_table_has_a_hold_still_step(bands):
    """Without it the value would react to every wobble around a threshold."""
    assert any(factor == 1.0 for _, factor in bands)


# --- movement ---------------------------------------------------------------


def test_a_good_run_grows_the_value():
    value = ready(counter(initial=30))
    value.adjust(1.0)
    assert value.value == 33


def test_a_bad_run_shrinks_the_value():
    value = ready(counter(initial=30))
    value.adjust(0.0)
    assert value.value == 24


def test_a_middling_run_leaves_the_value_alone():
    value = ready(counter(initial=30))
    value.adjust(0.85)
    assert value.value == 30


def test_the_value_never_passes_the_ceiling():
    value = ready(counter(initial=99, maximum=100))
    value.adjust(1.0)
    assert value.value == 100


def test_the_value_never_drops_below_the_floor():
    value = ready(counter(initial=2, minimum=1))
    for _ in range(5):
        ready(value).adjust(0.0)
    assert value.value == 1


def test_a_small_integer_still_moves_up():
    """int(5 * 1.1) is 5, so rounding alone would freeze a small count."""
    value = ready(counter(initial=5))
    value.adjust(1.0)
    assert value.value == 6


def test_a_small_integer_still_moves_down():
    value = ready(counter(initial=3))
    value.adjust(0.0)
    assert value.value == 2


def test_a_float_value_is_not_rounded():
    delay = AdaptiveValue(1.0, 0.1, 5.0, DELAY_BANDS, "d", integer=False, interval=0.0)
    ready(delay).adjust(1.0)
    assert delay.value == pytest.approx(0.85)


# --- the interval -----------------------------------------------------------


def test_an_adjustment_inside_the_interval_is_skipped():
    value = AdaptiveValue(30, 1, 100, WORKER_BANDS, "w", integer=True, interval=3600)
    value.adjust(1.0)
    assert value.value == 30


def test_the_interval_is_measured_from_the_last_adjustment():
    value = ready(AdaptiveValue(30, 1, 100, WORKER_BANDS, "w", integer=True, interval=3600))
    value.adjust(1.0)
    value.adjust(1.0)  # too soon, the first adjustment reset the clock
    assert value.value == 33


# --- recorded counts --------------------------------------------------------


def test_recorded_results_drive_the_adjustment():
    value = ready(counter(initial=30))
    for _ in range(19):
        value.record(True)
    value.record(False)  # 95 percent
    value.adjust()
    assert value.value == 33


def test_recorded_failures_shrink_the_value():
    value = ready(counter(initial=30))
    for _ in range(10):
        value.record(False)
    value.adjust()
    assert value.value == 24


def test_the_counts_reset_after_an_adjustment():
    value = ready(counter(initial=30))
    value.record(True)
    value.adjust()
    assert (value.successes, value.errors) == (0, 0)


def test_an_adjustment_with_nothing_recorded_does_nothing():
    value = ready(counter(initial=30))
    value.adjust()
    assert value.value == 30


def test_an_explicit_rate_beats_the_recorded_counts():
    value = ready(counter(initial=30))
    for _ in range(10):
        value.record(False)
    value.adjust(1.0)
    assert value.value == 33


# --- the three configured values --------------------------------------------


def test_the_delay_is_a_float_between_its_bounds():
    delay = make_delay(0.5, 0.01, 5.0)
    assert delay.integer is False
    assert delay.minimum == 0.01
    assert delay.maximum == 5.0


@pytest.mark.parametrize("factory", [make_workers, make_batch_size])
def test_the_worker_and_batch_values_are_whole_numbers(factory):
    assert factory(30, 10, 100).integer is True


def test_the_delay_moves_the_other_way_from_the_worker_count():
    """A good run should shorten the wait and raise the worker ceiling."""
    delay = ready(make_delay(1.0, 0.1, 5.0))
    workers = ready(make_workers(30, 10, 100))
    delay.adjust(1.0)
    workers.adjust(1.0)
    assert delay.value < 1.0
    assert workers.value > 30
