"""Against data.quartercall.com itself. Run with QUARTERCALL_LIVE=1."""

import os
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pytest

from quartercall import accounts, accounts_text, profiles, quarters

pytestmark = pytest.mark.skipif(
    not os.environ.get("QUARTERCALL_LIVE"), reason="set QUARTERCALL_LIVE=1 to read the live manifest"
)


def test_the_series_runs_newest_first_to_2019q1():
    frame = quarters()

    # The contract's series is every cycle from 2019Q1, newest first (§2).
    assert frame["quarter"].iloc[-1] == "2019Q1"
    assert frame["quarter"].is_monotonic_decreasing
    assert set(frame["status"]) <= {"open", "dormant"}


def test_reads_verified_live_accounts_with_exact_decimals(tmp_path):
    frame = accounts("2026Q2", codes=["A010", "A998"], cuid=5163, exact=True, cache_dir=tmp_path)

    assert not frame.empty
    assert set(frame["cuid"]) == {5163}
    assert "A010" in set(frame["account_code"])
    assert frame["account_value"].dtype == pd.ArrowDtype(pa.decimal128(24, 8))
    assert all(isinstance(value, Decimal) for value in frame["account_value"].dropna())


def test_reads_live_profiles_and_text():
    profile = profiles("2026Q2", cuid=5163, columns=["cu_number", "credit_union_name", "state"])
    assert len(profile) == 1
    text = accounts_text("2026Q2", codes="A700", cu_number=int(profile["cu_number"].iloc[0]))
    assert not text.empty
    assert set(text["account_code"]) == {"A700"}
    assert text["account_text"].notna().all()


def test_a_quarter_without_text_has_the_live_text_types(site):
    live = accounts_text("2026Q2", codes="A700")
    no_text = {"quarter": "2026Q2", "accounts_path": "curated/accounts/quarter=2026Q2/"}
    empty = accounts_text("2026Q2", base_url=site.publish({"schema_version": "1.1", "quarters": [no_text]}))

    assert empty.dtypes.to_dict() == live.dtypes.to_dict()
