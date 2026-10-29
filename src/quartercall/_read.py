"""Read the manifest's per-quarter Parquet files without losing their types."""

from collections.abc import Sequence
import hashlib
import http.client
from numbers import Integral
import os
from pathlib import Path
import re
import tempfile
import time
import urllib.request
from urllib.parse import urlsplit

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from quartercall._manifest import BASE_URL, _fetch_manifest
from quartercall._version import __version__


class DataUnavailableError(LookupError):
    """The requested quarter or dataset has not been published."""


class DataIntegrityError(RuntimeError):
    """A file cannot be verified against the manifest that describes it."""


# Unlike profiles, this table has a small fixed schema, and an omitted path means
# no text values (§3). Keep the same typed columns when returning that empty table.
# §3 says only TIMESTAMP; the unit is the published files' (test_live.py checks it).
_TEXT_SCHEMA = pa.schema([
    ("quarter", pa.string()),
    ("cycle_date", pa.date32()),
    ("cu_number", pa.int32()),
    ("submission_id", pa.int64()),
    ("account_code", pa.string()),
    ("account_text", pa.string()),
    ("ingested_at", pa.timestamp("ns")),
])


def _strings(value, name, *, scalar=False):
    if value is None:
        return None
    if scalar and isinstance(value, str):
        value = [value]
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be a sequence of strings")
    if any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{name} must contain non-empty strings")
    return list(dict.fromkeys(value))


def _ids(value, name):
    if value is None:
        return None
    if isinstance(value, Integral):
        value = [value]
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be an integer or a sequence of integers")
    if any(isinstance(item, bool) or not isinstance(item, Integral)
           or not 0 < item < 2**31 for item in value):
        raise ValueError(f"{name} must contain positive 32-bit integers")
    return list(dict.fromkeys(int(item) for item in value))


def _file_url(base_url, path):
    if not isinstance(path, str) or not path:
        raise DataIntegrityError("The manifest's dataset path must be a non-empty string")
    parts = urlsplit(path)
    if (parts.scheme or parts.netloc or parts.query or parts.fragment
            or path.startswith("/") or "\\" in path or ".." in path.split("/")):
        raise DataIntegrityError(f"The manifest's dataset path must be relative: {path!r}")
    return base_url.rstrip("/") + "/" + path.rstrip("/") + "/data.parquet"


def _digest(manifest, entry, dataset):
    sha = entry.get(f"{dataset}_sha256")
    size = entry.get(f"{dataset}_bytes")
    version = str(manifest["schema_version"]).split(".")
    required = len(version) > 1 and int(version[1]) >= 2
    if sha is None and size is None and not required:
        return None, None
    if (not isinstance(sha, str) or re.fullmatch(r"[0-9a-f]{64}", sha) is None
            or type(size) is not int or size < 0):
        raise DataIntegrityError(
            f"The manifest must provide a SHA-256 and byte count for {dataset}"
        )
    return sha, size


def _matches(body, sha, size):
    return len(body) == size and hashlib.sha256(body).hexdigest() == sha


def _download(url, timeout, sha, size, cache_dir):
    # Only content-addressed, verified files are cached. The manifest is always
    # fetched again: even dormant quarters can receive amendments.
    cache_path = Path(cache_dir) / f"{sha}.parquet" if cache_dir is not None and sha else None
    if cache_path is not None:
        try:
            body = cache_path.read_bytes()
        except FileNotFoundError:
            pass
        else:
            if _matches(body, sha, size):
                return body

    request = urllib.request.Request(url, headers={"User-Agent": f"quartercall-python/{__version__}"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
    except http.client.IncompleteRead as exc:
        raise DataIntegrityError(f"Incomplete download of {url}") from exc
    if sha is not None and not _matches(body, sha, size):
        # Most often a newer build: objects are replaced before the manifest naming them.
        raise DataIntegrityError(
            f"Downloaded file does not match the manifest: {url}. The quarter may be being "
            "republished; try again in a few minutes"
        )

    if cache_path is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        # Close before replacing, including on Windows. Concurrent readers see
        # either the previous complete file or this one, never a partial write.
        output = tempfile.NamedTemporaryFile(dir=cache_path.parent, delete=False)
        temporary = Path(output.name)
        try:
            with output:
                output.write(body)
            os.replace(temporary, cache_path)
        finally:
            temporary.unlink(missing_ok=True)
    return body


def _check_columns(columns, available):
    if columns is not None:
        unknown = [column for column in columns if column not in available]
        if unknown:
            raise ValueError(f"Unknown columns: {', '.join(unknown)}")


def _frame(table, columns, exact):
    if columns is not None:
        table = table.select(columns)
    if not exact:
        # float64 is what pandas arithmetic, ranking, plotting and export expect.
        for i, field in enumerate(table.schema):
            if pa.types.is_decimal(field.type):
                table = table.set_column(i, field.name, pc.cast(table.column(i), pa.float64()))
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def _read(quarter, dataset, *, codes=None, cu_number=None, cuid=None, columns=None,
          exact=True, base_url=BASE_URL, timeout=30, cache_dir=None):
    if not isinstance(quarter, str) or re.fullmatch(r"[0-9]{4}Q[1-4]", quarter) is None:
        raise ValueError("quarter must have the form '2026Q2'")
    columns = _strings(columns, "columns")
    selections = {
        "account_code": _strings(codes, "codes", scalar=True),
        "cu_number": _ids(cu_number, "cu_number"),
        "cuid": _ids(cuid, "cuid"),
    }
    filters = [(key, "in", values) for key, values in selections.items() if values is not None]

    # An object can be replaced before the manifest is published. Restart from
    # the manifest on a mismatch, with at most three complete attempts.
    for attempt in range(3):
        manifest = _fetch_manifest(base_url, timeout)
        entry = next((q for q in manifest.get("quarters", []) if q["quarter"] == quarter), None)
        if entry is None or "accounts_path" not in entry:
            raise DataUnavailableError(f"Quarter {quarter!r} has not been published; see quarters()")
        if f"{dataset}_path" not in entry:
            if dataset != "accounts_text":
                raise DataUnavailableError(f"{dataset} has not been published for {quarter}")
            _check_columns(columns, _TEXT_SCHEMA.names)
            return _frame(_TEXT_SCHEMA.empty_table(), columns, exact)

        url = _file_url(base_url, entry[f"{dataset}_path"])
        sha, size = _digest(manifest, entry, dataset)
        try:
            body = _download(url, timeout, sha, size, cache_dir)
        except DataIntegrityError:
            if attempt == 2:
                raise
            time.sleep(0.25 * 2**attempt)
            continue
        break

    source = pa.BufferReader(body)
    schema = pq.read_schema(source)
    _check_columns(columns, schema.names)
    if any(values == [] for values in selections.values()):
        # Arrow cannot bind an untyped empty IN-list to every column type.
        table = schema.empty_table()
    else:
        source.seek(0)
        table = pq.read_table(source, columns=columns, filters=filters or None)
    return _frame(table, columns, exact)


def accounts(
    quarter: str, *, codes: str | Sequence[str] | None = None,
    cu_number: int | Sequence[int] | None = None, cuid: int | Sequence[int] | None = None,
    columns: Sequence[str] | None = None, exact: bool = False, base_url: str = BASE_URL,
    timeout: float = 30, cache_dir: str | os.PathLike[str] | None = None,
) -> pd.DataFrame:
    """Numeric accounts from the latest filing for each credit union in a quarter.

    Returns one row per account code and filing, with Arrow-backed pandas types.
    ``account_value`` is float64; ``exact=True`` keeps the published decimal(24, 8)
    for reconciling to the cent. Missing values remain missing, never zero.
    Codes are raw NCUA strings, including leading zeros. ``cuid`` identifies an
    institution; ``cu_number`` identifies a charter. Each filter accepts one value
    or a sequence; filters combine with AND and an empty sequence matches no rows.
    ``columns`` selects output columns in the requested order, independently of filters.

    Reads the manifest and downloads the whole quarter file before filtering.
    Checks SHA-256 and byte count when published (required by schema 1.2+), retrying
    a mismatch up to twice with a fresh manifest. ``cache_dir`` optionally retains
    verified files by digest; every call still fetches the manifest. ``timeout``
    is in seconds per network operation. Schema 1.0/1.1 files without digests are
    read without verification or caching.

    Raises ``DataUnavailableError`` for an unpublished quarter, ``DataIntegrityError``
    for invalid digest metadata or a persistent mismatch, ``ValueError`` for invalid
    selections, and ``OSError`` for network failures: ``urllib.error.URLError``, or
    ``TimeoutError`` for a stalled response.
    """
    return _read(quarter, "accounts", codes=codes, cu_number=cu_number, cuid=cuid,
                 columns=columns, exact=exact, base_url=base_url, timeout=timeout,
                 cache_dir=cache_dir)


def profiles(
    quarter: str, *, cu_number: int | Sequence[int] | None = None,
    cuid: int | Sequence[int] | None = None, columns: Sequence[str] | None = None,
    base_url: str = BASE_URL, timeout: float = 30,
    cache_dir: str | os.PathLike[str] | None = None,
) -> pd.DataFrame:
    """Credit Union Profile rows for the latest filings in a quarter.

    Filters, column selection, Arrow-backed types, download verification, caching
    and errors follow ``accounts()``. Every column keeps its published type: NCUA
    string fields remain strings, including dates and Yes/No flags. Names and
    ``profile_charter_number`` describe the time of collection, not necessarily the
    quarter. Join accounts to profiles LEFT on
    ``quarter``, ``cu_number`` and ``submission_id``: some filings have no profile.
    An unpublished profiles table raises ``DataUnavailableError``.
    """
    return _read(quarter, "profiles", cu_number=cu_number, cuid=cuid, columns=columns,
                 base_url=base_url, timeout=timeout, cache_dir=cache_dir)


def accounts_text(
    quarter: str, *, codes: str | Sequence[str] | None = None,
    cu_number: int | Sequence[int] | None = None, columns: Sequence[str] | None = None,
    base_url: str = BASE_URL, timeout: float = 30,
    cache_dir: str | os.PathLike[str] | None = None,
) -> pd.DataFrame:
    """Non-numeric account values as filed, from the latest filings in a quarter.

    Filters, column selection, Arrow-backed types, download verification, caching
    and errors follow ``accounts()``. This table has no ``cuid`` column; filter by
    ``cu_number``. Join to accounts on ``quarter``, ``cu_number``, ``submission_id``
    and ``account_code``. A numeric NULL means published as text only if a matching
    text row exists. Missing values are never replaced with zero.

    A published quarter without an accounts_text path returns an empty DataFrame
    with typed columns: the contract uses an omitted path for no text values.
    """
    return _read(quarter, "accounts_text", codes=codes, cu_number=cu_number, columns=columns,
                 base_url=base_url, timeout=timeout, cache_dir=cache_dir)
