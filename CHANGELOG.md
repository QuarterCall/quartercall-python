# Changelog

## Unreleased

- `accounts()`, `profiles()` and `accounts_text()`: read one quarter into pandas, with account-code
  and credit-union filters and column selection. Arrow-backed types preserve nullable identifiers
  and as-filed strings. `account_value` is float64; `exact=True` keeps the published decimal.
- **Changed:** `quarters()` returns Arrow-backed types, like the readers, so the frames merge on
  `quarter` and `cycle_date`: `cycle_date` is a date, not a timestamp, and a missing `cu_count` or
  `updated_at` is `<NA>`.
- Parquet readers verify manifest SHA-256 digests and byte counts, retry publication mismatches,
  and optionally cache verified files by digest. The manifest is refreshed on every call.
- `DataUnavailableError` and `DataIntegrityError` distinguish unpublished data from failed
  verification. A quarter with no text file returns typed empty text columns.
- PyArrow 21 or later is now a runtime dependency.
- The PyPI page links to the source, issues and this changelog.
- Python 3.11 to 3.14 are listed as supported, and each is tested.

## 0.0.1 — 2026-10-28

- `quarters()`: the published quarters, newest first, with each quarter's status, the credit unions
  that have filed for it, and when its files were last rebuilt.
