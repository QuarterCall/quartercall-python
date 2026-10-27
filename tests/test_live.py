"""Against data.quartercall.com itself. Run with QUARTERCALL_LIVE=1."""

import os

import pytest

from quartercall import quarters

pytestmark = pytest.mark.skipif(
    not os.environ.get("QUARTERCALL_LIVE"), reason="set QUARTERCALL_LIVE=1 to read the live manifest"
)


def test_the_series_runs_newest_first_to_2019q1():
    frame = quarters()

    # The contract's series is every cycle from 2019Q1, newest first (§2).
    assert frame["quarter"].iloc[-1] == "2019Q1"
    assert frame["quarter"].is_monotonic_decreasing
    assert set(frame["status"]) <= {"open", "dormant"}
