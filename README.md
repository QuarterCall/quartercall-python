# quartercall

Read QuarterCall Data — the NCUA Form 5300 Call Reports, republished as Parquet at
`data.quartercall.com` — into pandas. No account or API key is needed.

```sh
pip install quartercall
```

```python
import quartercall

quartercall.quarters()
```

`quarters()` returns one row per published quarter, newest first:

| Column | Holds |
|---|---|
| `quarter` | the cycle, such as `2026Q3` |
| `cycle_date` | the cycle's last day |
| `status` | `open` while filings may still arrive; `dormant` once NCUA has validated none for over a year |
| `cu_count` | credit unions that have filed for the cycle |
| `updated_at` | when the quarter's files were last rebuilt, in UTC |

No quarter is final: NCUA accepts an amendment to a cycle of any age, and a `dormant` quarter
becomes `open` again when one arrives. A recent quarter is published while many credit unions
have yet to file.

## Read a quarter

```python
import quartercall as qc

# Assets and members for one institution, from its latest filing for the quarter.
accounts = qc.accounts("2026Q2", codes=["A010", "A083"], cuid=5163)

# The ten largest credit unions by total assets.
largest = qc.accounts("2026Q2", codes="A010").nlargest(10, "account_value")

profiles = qc.profiles(
    "2026Q2",
    columns=["quarter", "cu_number", "submission_id", "cuid", "credit_union_name", "state"],
)

# Non-numeric values, such as the filed net-worth classification.
text = qc.accounts_text("2026Q2", codes="A700")
```

Each reader returns a pandas DataFrame with the published column names and Arrow-backed types,
as does `quarters()`, so their frames merge on `quarter` and `cycle_date`. A missing value is
`<NA>`, never zero. `account_value` is a float64, which arithmetic, ranking, plotting and export
expect; `exact=True` keeps the published `decimal128(24, 8)`, whose values are Python `Decimal`
objects, for reconciling to the cent. Nullable integers stay integers. Dates and timestamps
retain their Parquet types, and NCUA profile strings stay strings, including dates, leading
zeros and `Yes`/`No` values.

| Argument | Readers | Behavior |
|---|---|---|
| `quarter` | all | One explicit quarter, such as `"2026Q2"` |
| `codes` | accounts, accounts_text | One raw account-code string or a sequence, preserving leading zeros |
| `cu_number` | all | One charter number or a sequence of integers |
| `cuid` | accounts, profiles | One institution identifier or a sequence of integers |
| `columns` | all | A sequence of output column names, in the requested order; omitted means all |
| `exact` | accounts | `True` returns `account_value` as the published decimal; the default is float64 |
| `cache_dir` | all | Optional directory for verified files; omitted means no disk cache |
| `base_url`, `timeout` | all | Alternate data root and timeout in seconds per network operation; defaults match `quarters()` |

Filters combine with AND, and an empty sequence selects no rows. Filtering works even when its
column is absent from `columns`. A missing account or value is never filled with zero. A numeric
NULL means *published as text* only when a matching `accounts_text` row exists. A published quarter
without a text file returns a typed empty DataFrame.

Use `cuid` to follow an institution across charter conversions; `cu_number` identifies a charter.
The text table has no `cuid`, so filter it by charter. Profile names and `profile_charter_number`
reflect when NCUA supplied the profile and do not necessarily describe the historical quarter.

Some filings have no profile. Keep them with a left join on the filing's keys:

```python
named_accounts = accounts.merge(
    profiles,
    on=["quarter", "cu_number", "submission_id"],
    how="left",
    suffixes=("", "_profile"),
    validate="many_to_one",
)
```

Each call reads its own manifest, so separate calls may see different amendments. Including
`submission_id` in the join prevents attaching another filing's profile. For a text join, use
those same keys plus `account_code`.

## Downloads and caching

Readers follow paths in the manifest and download the **whole quarter file** before applying
filters and column selection. Selection reduces the returned data, not the network download.
They check the file's SHA-256 and byte count before reading Parquet. If a publication changes
mid-read, they fetch a fresh manifest and retry, at most three attempts total.

```python
assets = qc.accounts("2026Q2", codes="A010", cache_dir=".quartercall-cache")
# Reuses the verified quarter file if its manifest digest has not changed.
members = qc.accounts("2026Q2", codes="A083", cache_dir=".quartercall-cache")
```

The manifest is fetched on every call, even for dormant quarters. Cached files are checked before
reuse and retained by digest; old versions are not automatically removed. The cache can be deleted
when no reader is using it. It does not provide offline access.

Schema 1.2 and later require digests and byte counts. Older 1.0/1.1 manifests without them remain
readable, but those files cannot be verified against the manifest and are not cached.

- `DataUnavailableError`: the requested quarter or table has not been published.
- `DataIntegrityError`: invalid digest metadata, an incomplete download, or a mismatch that
  persists after retries. Files are replaced before the manifest that names them is published,
  so a mismatch most often means the quarter is being republished: try again in a few minutes.
- `UnsupportedSchemaError`: a major data schema version this package cannot read.
- `ValueError` / `TypeError`: invalid quarter, filters or column selection.
- `OSError`: a network or HTTP failure, as `urllib.error.URLError` (including `HTTPError`), or as
  `TimeoutError` when a response stalls. A missing file that the manifest says exists raises an
  error; it is never returned as an empty table.

Malformed Parquet raises the underlying PyArrow error. Filesystem errors from an explicitly
requested cache are also propagated.

QuarterCall is a registered trademark of QuarterCall Holdings Inc. The code is licensed under the
Apache License 2.0.
