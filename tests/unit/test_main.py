import threading

import pytest

from warehouse_delivery.__main__ import run_loops


def test_run_loops__one_loop_raises__others_stopped_and_error_raised() -> None:
    # Given one loop that fails and one that runs until told to stop
    stop = threading.Event()

    def failing() -> None:
        raise RuntimeError("postgres down")

    told_to_stop: list[bool] = []

    def until_stopped() -> None:
        told_to_stop.append(stop.wait(timeout=5))

    # When / Then
    with pytest.raises(RuntimeError, match="postgres down"):
        run_loops([failing, until_stopped], stop)
    assert told_to_stop == [True]
