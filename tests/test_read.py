from datetime import date, datetime
from decimal import Decimal
import hashlib
import time
import urllib.error

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import quartercall as qc


@pytest.fixture
def tables():
    accounts = pa.table({
        "quarter": ["2026Q2"] * 7,
        "cycle_date": pa.array([date(2026, 6, 30)] * 7, type=pa.date32()),
        "cu_number": pa.array([100] * 5 + [200, 300], type=pa.int32()),
        "submission_id": pa.array([9007199254740993] * 5 + [2, 3], type=pa.int64()),
        "version": pa.array([2] * 5 + [1, 1], type=pa.int16()),
        "account_code": ["A010", "A998", "A700", "A876", "002", "A010", "A010"],
        "account_value": pa.array([
            Decimal("9007199254740993.12345678"), Decimal("0.1472"), None, None,
            Decimal("0"), Decimal("42"), Decimal("7"),
        ], type=pa.decimal128(24, 8)),
        "ingested_at": pa.array([datetime(2026, 7, 1, 1, 2, 3)] * 7, type=pa.timestamp("us")),
        "cuid": pa.array([10] * 5 + [20, None], type=pa.int32()),
    })
    profiles = pa.table({
        "quarter": ["2026Q2", "2026Q2"],
        "cycle_date": pa.array([date(2026, 6, 30)] * 2, type=pa.date32()),
        "cu_number": pa.array([100, 200], type=pa.int32()),
        "cuid": pa.array([10, 20], type=pa.int32()),
        "submission_id": pa.array([9007199254740993, 2], type=pa.int64()),
        "version": pa.array([2, 1], type=pa.int16()),
        "observed_at": pa.array([datetime(2026, 7, 1)] * 2, type=pa.timestamp("us")),
        "observed_at_source": ["first_seen", "archived"],
        "credit_union_name": ["Example CU", "Another CU"],
        "state": ["CA", "WA"],
        "date_insured": ["1/1/1900", None],
        "is_low_income": ["Yes", "No"],
        "rssd_number": ["00123", "00456"],
        "profile_charter_number": pa.array([101, 200], type=pa.int32()),
    })
    text = pa.table({
        "quarter": ["2026Q2"],
        "cycle_date": pa.array([date(2026, 6, 30)], type=pa.date32()),
        "cu_number": pa.array([100], type=pa.int32()),
        "submission_id": pa.array([9007199254740993], type=pa.int64()),
        "account_code": ["A700"],
        "account_text": ["Well Capitalized"],
        # The live file's unit: test_live.py holds the typed empty frame to it.
        "ingested_at": pa.array([datetime(2026, 7, 1)], type=pa.timestamp("ns")),
    })
    return {"accounts": accounts, "profiles": profiles, "accounts_text": text}


def publish(site, tables, *, schema_version="1.2"):
    entry = {"quarter": "2026Q2", "status": "open"}
    bodies = {}
    for name, table in tables.items():
        # Deliberately differ from the conventional URL: the manifest is authoritative.
        path = f"published/{name}/q2/"
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, row_group_size=2)
        body = sink.getvalue().to_pybytes()
        entry[f"{name}_path"] = path
        entry[f"{name}_sha256"] = hashlib.sha256(body).hexdigest()
        entry[f"{name}_bytes"] = len(body)
        site.serve(f"/{path}data.parquet", body)
        bodies[name] = body
    manifest = {"schema_version": schema_version, "quarters": [entry]}
    return site.publish(manifest), manifest, bodies


def test_money_is_float_by_default_and_missing_stays_missing(site, tables):
    base, _, _ = publish(site, tables)
    frame = qc.accounts("2026Q2", codes=["A010", "A998", "A700"], cu_number=100, base_url=base)

    assert frame["account_value"].dtype == pd.ArrowDtype(pa.float64())
    assert frame["account_value"].iloc[1] == 0.1472
    assert frame["account_value"].isna().tolist() == [False, False, True]


def test_default_money_supports_ratios_and_ranking(site, tables):
    base, _, _ = publish(site, tables)
    assets = qc.accounts("2026Q2", codes="A010", base_url=base)

    assert assets.nlargest(2, "account_value")["cu_number"].tolist() == [100, 200]
    assert (assets["account_value"] / 1000).iloc[1] == 0.042


def test_exact_accounts_preserve_decimals_identifiers_dates_and_nulls(site, tables):
    base, _, _ = publish(site, tables)
    frame = qc.accounts("2026Q2", exact=True, base_url=base)

    assert frame.columns.tolist() == tables["accounts"].column_names
    assert frame["account_value"].dtype == pd.ArrowDtype(pa.decimal128(24, 8))
    assert frame["account_value"].iloc[0] == Decimal("9007199254740993.12345678")
    assert frame["account_value"].iloc[1] == Decimal("0.1472")
    assert frame["account_value"].isna().tolist() == [False, False, True, True, False, False, False]
    assert frame["account_value"].iloc[4] == 0
    assert frame["submission_id"].iloc[0] == 9007199254740993
    assert frame["cuid"].dtype == pd.ArrowDtype(pa.int32())
    assert pd.isna(frame["cuid"].iloc[-1])
    assert frame["cycle_date"].iloc[0] == date(2026, 6, 30)
    assert frame["ingested_at"].iloc[0] == datetime(2026, 7, 1, 1, 2, 3)


def test_filters_apply_before_projection_and_combine_with_and(site, tables):
    base, _, _ = publish(site, tables)
    frame = qc.accounts("2026Q2", codes=["A010", "A998"], cu_number=[100, 200],
                        cuid=10, columns=["account_value", "account_code"], base_url=base)

    assert frame.columns.tolist() == ["account_value", "account_code"]
    assert frame["account_code"].tolist() == ["A010", "A998"]
    assert frame.index.tolist() == [0, 1]


def test_codes_are_strings_as_filed_and_missing_codes_are_not_filled(site, tables):
    base, _, _ = publish(site, tables)
    frame = qc.accounts("2026Q2", codes="002", cu_number=100, base_url=base)

    assert frame["account_code"].tolist() == ["002"]
    assert frame["account_value"].tolist() == [0.0]
    assert qc.accounts("2026Q2", codes="NOT_FILED", base_url=base).empty


@pytest.mark.parametrize("exact,money", [(False, pa.float64()), (True, pa.decimal128(24, 8))])
@pytest.mark.parametrize("selection", [{"codes": []}, {"cu_number": []}, {"cuid": []}, {"cuid": 99}])
def test_empty_selection_keeps_column_types(site, tables, selection, exact, money):
    base, _, _ = publish(site, tables)
    frame = qc.accounts("2026Q2", **selection, exact=exact, base_url=base)

    assert frame.empty
    assert frame.columns.tolist() == tables["accounts"].column_names
    assert frame["account_value"].dtype == pd.ArrowDtype(money)


def test_empty_column_selection_keeps_matching_row_count(site, tables):
    base, _, _ = publish(site, tables)
    frame = qc.accounts("2026Q2", codes="A010", columns=[], base_url=base)

    assert frame.shape == (3, 0)


def test_profiles_preserve_as_filed_strings_and_do_not_invent_missing_profiles(site, tables):
    base, _, _ = publish(site, tables)
    frame = qc.profiles("2026Q2", base_url=base)

    assert frame["cu_number"].tolist() == [100, 200]
    assert frame["profile_charter_number"].tolist() == [101, 200]
    assert frame["is_low_income"].tolist() == ["Yes", "No"]
    assert frame["rssd_number"].tolist() == ["00123", "00456"]
    assert frame["date_insured"].iloc[0] == "1/1/1900"
    assert pd.isna(frame["date_insured"].iloc[1])
    selected = qc.profiles("2026Q2", cuid=[10], cu_number=100,
                           columns=["state", "credit_union_name"], base_url=base)
    assert selected.to_dict("records") == [{"state": "CA", "credit_union_name": "Example CU"}]


def test_text_rows_distinguish_text_from_unreported_numeric_values(site, tables):
    base, _, _ = publish(site, tables)
    accounts = qc.accounts("2026Q2", codes=["A700", "A876"], base_url=base)
    text = qc.accounts_text("2026Q2", codes=["A700", "A876"], cu_number=[100], base_url=base)
    joined = accounts.merge(text, how="left",
                            on=["quarter", "cu_number", "submission_id", "account_code"])

    assert joined["account_value"].isna().all()
    assert joined["account_text"].iloc[0] == "Well Capitalized"
    assert pd.isna(joined["account_text"].iloc[1])
    assert qc.accounts_text("2026Q2", codes="A700", cu_number=200, base_url=base).empty


def test_omitted_text_file_is_typed_empty_without_requesting_a_file(site, tables):
    base, manifest, _ = publish(site, tables)
    for suffix in ("path", "sha256", "bytes"):
        del manifest["quarters"][0][f"accounts_text_{suffix}"]
    site.publish(manifest)

    frame = qc.accounts_text("2026Q2", base_url=base)

    assert frame.empty
    assert frame.columns.tolist() == tables["accounts_text"].column_names
    assert all(frame[name].dtype == pd.ArrowDtype(field.type)
               for name, field in zip(frame.columns, tables["accounts_text"].schema))
    assert site.paths == ["/index/manifest.json"]
    selected = qc.accounts_text("2026Q2", columns=["account_text"], base_url=base)
    assert selected.columns.tolist() == ["account_text"]
    with pytest.raises(ValueError, match="Unknown columns"):
        qc.accounts_text("2026Q2", columns=["typo"], base_url=base)


@pytest.mark.parametrize("quarter", ["2026Q1", "2026Q3"])
def test_unpublished_quarters_raise_instead_of_returning_empty(site, tables, quarter):
    base, manifest, _ = publish(site, tables)
    manifest["quarters"].append({"quarter": "2026Q3"})  # Held but not published.
    site.publish(manifest)
    with pytest.raises(qc.DataUnavailableError, match=quarter):
        qc.accounts(quarter, base_url=base)


def test_unpublished_profiles_raise(site, tables):
    base, manifest, _ = publish(site, tables)
    del manifest["quarters"][0]["profiles_path"]
    site.publish(manifest)
    with pytest.raises(qc.DataUnavailableError, match="profiles"):
        qc.profiles("2026Q2", base_url=base)


@pytest.mark.parametrize("quarter", ["2026q2", "2026Q5", "26Q2", "2026Q2/", None, 2026])
def test_invalid_quarter_fails_before_fetching(site, quarter):
    with pytest.raises(ValueError, match="quarter"):
        qc.accounts(quarter, base_url=site.base_url)
    assert not site.paths


@pytest.mark.parametrize("kwargs", [
    {"codes": [10]}, {"codes": [""]}, {"codes": 10}, {"columns": "account_value"},
    {"cuid": "10"}, {"cuid": [None]}, {"cu_number": True}, {"cu_number": -1},
    {"cu_number": [1.0]}, {"cuid": 2**31},
])
def test_invalid_filters_fail_before_fetching(site, kwargs):
    with pytest.raises((TypeError, ValueError)):
        qc.accounts("2026Q2", **kwargs, base_url=site.base_url)
    assert not site.paths


def test_unknown_column_raises(site, tables):
    base, _, _ = publish(site, tables)
    with pytest.raises(ValueError, match="Unknown columns: typo"):
        qc.accounts("2026Q2", columns=["typo"], base_url=base)


def test_new_major_schema_is_refused_before_a_dataset_request(site, tables):
    base, _, _ = publish(site, tables, schema_version="2.0")
    with pytest.raises(qc.UnsupportedSchemaError):
        qc.accounts("2026Q2", base_url=base)
    assert site.paths == ["/index/manifest.json"]


@pytest.mark.parametrize("version", ["1.0", "1.1"])
def test_older_manifests_without_digests_are_read_but_not_cached(site, tables, tmp_path, version):
    base, manifest, _ = publish(site, tables, schema_version=version)
    entry = manifest["quarters"][0]
    del entry["accounts_sha256"], entry["accounts_bytes"]
    site.publish(manifest)

    assert len(qc.accounts("2026Q2", base_url=base, cache_dir=tmp_path)) == 7
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("version", ["1.2", "1.7"])
@pytest.mark.parametrize("update", [
    {"accounts_sha256": None, "accounts_bytes": None},
    {"accounts_sha256": "invalid"}, {"accounts_bytes": None}, {"accounts_bytes": True},
])
def test_required_digests_are_validated_before_download(site, tables, version, update):
    base, manifest, _ = publish(site, tables, schema_version=version)
    manifest["quarters"][0].update(update)
    site.publish(manifest)
    with pytest.raises(qc.DataIntegrityError, match="SHA-256 and byte count"):
        qc.accounts("2026Q2", base_url=base)
    assert site.paths == ["/index/manifest.json"]


@pytest.mark.parametrize("field,value", [("accounts_sha256", "0" * 64), ("accounts_bytes", 1)])
def test_persistent_mismatch_is_bounded_and_never_cached(site, tables, tmp_path, monkeypatch, field, value):
    base, manifest, _ = publish(site, tables)
    manifest["quarters"][0][field] = value
    site.publish(manifest)
    monkeypatch.setattr("quartercall._read.time.sleep", lambda _: None)

    # Curate replaces objects before it publishes the manifest naming them: say so.
    with pytest.raises(qc.DataIntegrityError, match="does not match.*try again in a few minutes"):
        qc.accounts("2026Q2", base_url=base, cache_dir=tmp_path)

    assert site.paths.count("/index/manifest.json") == 3
    assert site.paths.count("/published/accounts/q2/data.parquet") == 3
    assert not list(tmp_path.iterdir())


def test_publication_race_refetches_manifest_and_uses_its_new_path(site, tables, monkeypatch):
    base, manifest, bodies = publish(site, tables)
    monkeypatch.setattr("quartercall._read.time.sleep", lambda _: None)

    def replace_manifest():
        manifest["quarters"][0]["accounts_path"] = "new/accounts/"
        site.serve("/new/accounts/data.parquet", bodies["accounts"])
        site.publish(manifest)
        return b"a different build"

    site.serve("/published/accounts/q2/data.parquet", replace_manifest)
    frame = qc.accounts("2026Q2", codes="A998", base_url=base)

    assert frame["account_value"].tolist() == [0.1472]
    assert site.paths == ["/index/manifest.json", "/published/accounts/q2/data.parquet",
                          "/index/manifest.json", "/new/accounts/data.parquet"]


def test_cache_reuses_verified_file_but_checks_manifest_every_time(site, tables, tmp_path):
    base, _, _ = publish(site, tables)
    qc.accounts("2026Q2", codes="A010", base_url=base, cache_dir=tmp_path)
    result = qc.accounts("2026Q2", codes="A998", base_url=base, cache_dir=tmp_path)

    assert result["account_value"].tolist() == [0.1472]
    assert site.paths.count("/index/manifest.json") == 2
    assert site.paths.count("/published/accounts/q2/data.parquet") == 1
    assert len(list(tmp_path.glob("*.parquet"))) == 1


def test_amendment_invalidates_cache_even_for_dormant_quarter(site, tables, tmp_path):
    base, manifest, _ = publish(site, tables)
    manifest["quarters"][0]["status"] = "dormant"
    site.publish(manifest)
    qc.accounts("2026Q2", base_url=base, cache_dir=tmp_path)
    tables["accounts"] = tables["accounts"].slice(1)
    publish(site, tables)
    frame = qc.accounts("2026Q2", base_url=base, cache_dir=tmp_path)

    assert frame["account_code"].iloc[0] == "A998"
    assert len(frame) == 6
    assert site.paths.count("/published/accounts/q2/data.parquet") == 2


def test_corrupt_cache_is_replaced_with_verified_download(site, tables, tmp_path):
    base, manifest, bodies = publish(site, tables)
    cached = tmp_path / (manifest["quarters"][0]["accounts_sha256"] + ".parquet")
    cached.write_bytes(b"broken")
    assert len(qc.accounts("2026Q2", base_url=base, cache_dir=tmp_path)) == 7
    assert cached.read_bytes() == bodies["accounts"]
    assert list(tmp_path.iterdir()) == [cached]


@pytest.mark.parametrize("reader,dataset", [(qc.accounts, "accounts"), (qc.profiles, "profiles"),
                                          (qc.accounts_text, "accounts_text")])
def test_missing_published_file_is_an_http_error_not_empty(site, tables, reader, dataset):
    base, _, _ = publish(site, tables)
    site.serve(f"/published/{dataset}/q2/data.parquet", b"not found", status=404)
    with pytest.raises(urllib.error.HTTPError):
        reader("2026Q2", base_url=base)


def test_a_stalled_response_raises_timeout_error(site, tables):
    base, _, _ = publish(site, tables)
    site.serve("/published/accounts/q2/data.parquet", lambda: time.sleep(0.5) or b"")
    # urllib does not wrap a read timeout in URLError; both are OSError.
    with pytest.raises(TimeoutError):
        qc.accounts("2026Q2", base_url=base, timeout=0.1)


def test_quarters_merge_with_a_reader_on_quarter_and_cycle_date(site, tables):
    base, manifest, _ = publish(site, tables)
    manifest["quarters"][0]["cycle_date"] = "2026-06-30"
    site.publish(manifest)
    assets = qc.accounts("2026Q2", codes="A010", base_url=base)

    merged = qc.quarters(base_url=base).merge(assets, on=["quarter", "cycle_date"])

    assert len(merged) == 3


def test_file_requests_identify_package(site, tables):
    base, _, _ = publish(site, tables)
    qc.accounts("2026Q2", base_url=base)
    assert len(site.seen) == 2
    assert all(headers["User-Agent"] == f"quartercall-python/{qc.__version__}" for headers in site.seen)


@pytest.mark.parametrize("path", [None, "", "https://elsewhere.test/file/", "../file/", "/file/"])
def test_invalid_paths_are_not_read_or_treated_as_empty(site, tables, path):
    base, manifest, _ = publish(site, tables)
    manifest["quarters"][0]["accounts_text_path"] = path
    site.publish(manifest)
    with pytest.raises(qc.DataIntegrityError, match="path"):
        qc.accounts_text("2026Q2", base_url=base)
    assert site.paths == ["/index/manifest.json"]


def test_truncated_response_is_retried_and_never_cached(site, tables, tmp_path, monkeypatch):
    base, _, bodies = publish(site, tables)
    site.serve("/published/accounts/q2/data.parquet", bodies["accounts"][:100],
               content_length=len(bodies["accounts"]))
    monkeypatch.setattr("quartercall._read.time.sleep", lambda _: None)
    with pytest.raises(qc.DataIntegrityError, match="Incomplete download"):
        qc.accounts("2026Q2", base_url=base, cache_dir=tmp_path)
    assert site.paths.count("/index/manifest.json") == 3
    assert not list(tmp_path.iterdir())
