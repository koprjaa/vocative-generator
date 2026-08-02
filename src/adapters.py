# Project: Vocative Generator
# File:    src/adapters.py
#
# Description:
# One number that moves between bounds as the recent success rate changes.
# Used for the delay between requests, the worker ceiling and the batch size.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# Created: 2025-12-14
#
# License: MIT

import asyncio
import logging
import time

from .config import HTTP_CONFIG


def _format(value: float) -> str:
    return f"{value:.3f}" if isinstance(value, float) else str(value)


class AdaptiveValue:
    """A number the pipeline tunes from how well recent requests went.

    `bands` is read top to bottom and the first threshold the success rate
    reaches decides the multiplier. A factor of 1.0 holds the value still, which
    is how a band that should do nothing is written. Without such a band the
    value would react to every wobble around the thresholds.

    An integer value always moves by at least one step, so a small worker count
    is not held in place by rounding.
    """

    def __init__(
        self,
        initial: float,
        minimum: float,
        maximum: float,
        bands: list[tuple[float, float]],
        name: str,
        integer: bool = False,
        interval: float | None = None,
    ):
        self.value = initial
        self.minimum = minimum
        self.maximum = maximum
        self.bands = bands
        self.name = name
        self.integer = integer
        self.interval = (
            interval if interval is not None else HTTP_CONFIG.get("WORKER_SCALE_INTERVAL", 3.0)
        )
        self.successes = 0
        self.errors = 0
        self.last_adjustment = time.time()
        self.logger = logging.getLogger(self.__class__.__name__)

    def record(self, success: bool) -> None:
        """Count one request towards the next adjustment."""
        if success:
            self.successes += 1
        else:
            self.errors += 1

    def factor_for(self, success_rate: float) -> float:
        """Multiplier the bands give for this success rate."""
        for threshold, factor in self.bands:
            if success_rate >= threshold:
                return factor
        return 1.0

    def _clamp(self, value: float) -> float:
        return max(self.minimum, min(self.maximum, value))

    def _next_value(self, factor: float) -> float:
        if factor == 1.0:
            return self.value
        candidate = self.value * factor
        if self.integer:
            # int() truncates, so a factor near 1 would leave a small value
            # unchanged. Step by at least one in the direction the factor asks.
            candidate = int(candidate)
            if candidate == int(self.value):
                candidate = int(self.value) + (1 if factor > 1 else -1)
        return self._clamp(candidate)

    def adjust(self, success_rate: float | None = None) -> None:
        """Move the value, at most once per interval.

        With no rate given, the counts recorded since the last adjustment are
        used and then reset.
        """
        if time.time() - self.last_adjustment < self.interval:
            return

        if success_rate is None:
            total = self.successes + self.errors
            if total == 0:
                return
            success_rate = self.successes / total

        previous = self.value
        self.value = self._next_value(self.factor_for(success_rate))
        self.successes = 0
        self.errors = 0
        self.last_adjustment = time.time()

        if previous != self.value:
            self.logger.info(
                "%s adjusted: %s -> %s (success: %.1f%%)",
                self.name, _format(previous), _format(self.value), success_rate * 100,
            )

    async def wait(self) -> None:
        """Sleep for the current value. Only meaningful for the delay."""
        await asyncio.sleep(self.value)


# A batch that mostly worked shortens the wait, a batch that mostly failed
# lengthens it. In between the pace holds rather than chase noise.
DELAY_BANDS = [(0.95, 0.85), (0.80, 1.0), (0.0, 1.8)]

# Workers and batch size grow while the site keeps up and shrink when it does
# not. The batch size waits for a cleaner run before it grows, and tolerates a
# worse one before it shrinks.
WORKER_BANDS = [(0.9, 1.1), (0.8, 1.0), (0.0, 0.8)]
BATCH_BANDS = [(0.95, 1.1), (0.75, 1.0), (0.0, 0.8)]


def make_delay(initial: float, minimum: float, maximum: float) -> AdaptiveValue:
    return AdaptiveValue(
        initial, minimum, maximum, DELAY_BANDS, "Delay",
        interval=HTTP_CONFIG.get("WORKER_SCALE_INTERVAL", 1.0),
    )


def make_workers(initial: int, minimum: int, maximum: int) -> AdaptiveValue:
    return AdaptiveValue(initial, minimum, maximum, WORKER_BANDS, "Workers", integer=True)


def make_batch_size(initial: int, minimum: int, maximum: int) -> AdaptiveValue:
    return AdaptiveValue(initial, minimum, maximum, BATCH_BANDS, "Batch size", integer=True)
