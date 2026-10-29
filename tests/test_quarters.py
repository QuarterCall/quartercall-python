from datetime import date
import urllib.error

import pandas as pd
import pyarrow as pa
import pytest

import quartercall
from quartercall import UnsupportedSchemaError, quarters


def manifest(*entries, schema_version="1.1"):
    return {"schema_version": schema_version, "quarters": list(entries)}


def entry(quarter, **fields):
    """A published quarter: one with an accounts file."""
    return {"quarter": quarter, "accounts_path": f"curated/accounts/quarter={quarter}/", **fields}


def test_lists_only_quarters_with_an_accounts_file(site):
    held = {"quarter": "2026Q4", "status": "open"}  # collected, not yet built (CONTRACT.md §2)
    base = site.publish(manifest(held, entry("2026Q3"), entry("2026Q2")))

    assert quarters(base_url=base)["quarter"].tolist() == ["2026Q3", "2026Q2"]


def test_lists_newest_first_whatever_the_manifest_order(site):
    base = site.publish(manifest(entry("2019Q1"), entry("2026Q3"), entry("2022Q4")))

    assert quarters(base_url=base)["quarter"].tolist() == ["2026Q3", "2022Q4", "2019Q1"]


def test_reports_an_unrecognised_or_missing_status_as_open(site):
    base = site.publish(manifest(
        entry("2026Q3", status="settling"), entry("2026Q2"), entry("2019Q1", status="dormant")))

    assert quarters(base_url=base)["status"].tolist() == ["open", "open", "dormant"]


def test_refuses_a_newer_major_schema_version(site):
    base = site.publish(manifest(entry("2026Q3"), schema_version="2.0"))

    with pytest.raises(UnsupportedSchemaError, match="2.0"):
        quarters(base_url=base)


def test_reads_a_newer_minor_schema_version(site):
    base = site.publish(manifest(entry("2026Q3"), schema_version="1.7"))

    assert quarters(base_url=base)["quarter"].tolist() == ["2026Q3"]


def test_types_dates_counts_and_times(site):
    base = site.publish(manifest(
        entry("2026Q3", cycle_date="2026-09-30", cu_count=1816, updated_at="2026-10-27T10:34:44Z"),
        entry("2026Q2", cycle_date="2026-06-30")))

    frame = quarters(base_url=base)

    # Arrow-backed, like the readers' frames, so the two merge on quarter and cycle_date.
    assert frame["quarter"].dtype == pd.ArrowDtype(pa.string())
    assert frame["cycle_date"].dtype == pd.ArrowDtype(pa.date32())
    assert frame["cycle_date"].tolist() == [date(2026, 9, 30), date(2026, 6, 30)]
    assert frame["cu_count"].dtype == pd.ArrowDtype(pa.int64())
    assert frame["cu_count"].iloc[0] == 1816 and frame["cu_count"].isna().iloc[1]
    assert frame["updated_at"].dtype == pd.ArrowDtype(pa.timestamp("us", tz="UTC"))
    assert frame["updated_at"].iloc[0] == pd.Timestamp("2026-10-27T10:34:44Z")
    assert pd.isna(frame["updated_at"].iloc[1])


def test_no_published_quarter_is_an_empty_frame_with_every_column(site):
    base = site.publish(manifest({"quarter": "2026Q3", "status": "open"}))

    frame = quarters(base_url=base)

    assert frame.empty
    assert frame.columns.tolist() == ["quarter", "cycle_date", "status", "cu_count", "updated_at"]


def test_an_http_failure_raises_rather_than_returning_no_quarters(site):
    base = site.publish({"error": "unavailable"}, status=503)

    with pytest.raises(urllib.error.HTTPError):
        quarters(base_url=base)


def test_names_itself_in_the_user_agent(site):
    quarters(base_url=site.publish(manifest(entry("2026Q3"))))

    assert site.seen[0]["User-Agent"] == f"quartercall-python/{quartercall.__version__}"
