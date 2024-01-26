"""Helpers related to time measurements
"""

import time


class Timer:
    """A very basic class for measuring the elapsed time for some operation."""

    def __init__(self, start: bool = True) -> None:
        self._start_time: float | None = None
        if start:
            self.start()

    def start(self) -> None:
        """Start the timer running"""
        self._start_time = time.perf_counter()

    def stop(self) -> float:
        """Stop the timer from running and return the elapsed time"""
        if self._start_time is None:
            raise RuntimeError("Attempted to stop Timer which was never started.")

        elapsed = time.perf_counter() - self._start_time
        self._start_time = None
        return elapsed
