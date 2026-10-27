"""The manifest: which quarters are published, and how settled each is.

The rules are the data contract's (CONTRACT.md for data.quartercall.com), §2 and §6.
"""

import json
import urllib.request

import pandas as pd

from quartercall._version import __version__

BASE_URL = "https://data.quartercall.com/"
SUPPORTED_MAJOR = "1"
STATUSES = {"open", "dormant"}
COLUMNS = ["quarter", "cycle_date", "status", "cu_count", "updated_at"]


class UnsupportedSchemaError(RuntimeError):
    """The manifest's major schema version is one this package cannot read."""


def _fetch_manifest(base_url, timeout):
    request = urllib.request.Request(
        base_url.rstrip("/") + "/index/manifest.json",
        headers={"User-Agent": f"quartercall-python/{__version__}"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        manifest = json.load(response)
    # A major bump is a breaking change; a minor only adds fields (§6).
    version = str(manifest.get("schema_version", ""))
    if version.split(".")[0] != SUPPORTED_MAJOR:
        raise UnsupportedSchemaError(
            f"data.quartercall.com publishes schema {version!r}; this package reads "
            f"{SUPPORTED_MAJOR}.x. Upgrade it: pip install --upgrade quartercall"
        )
    return manifest


def quarters(*, base_url: str = BASE_URL, timeout: float = 30) -> pd.DataFrame:
    """The published quarters, newest first.

    One row per quarter whose files can be read, with columns ``quarter``, ``cycle_date``,
    ``status`` (``open`` or ``dormant``), ``cu_count`` (credit unions that have filed) and
    ``updated_at`` (when the quarter's files were last rebuilt, UTC). No quarter is final:
    a ``dormant`` quarter becomes ``open`` again when an amendment arrives.

    Raises ``UnsupportedSchemaError`` if the data has moved to a schema this version cannot
    read, and ``urllib.error.URLError`` if the manifest cannot be fetched.
    """
    manifest = _fetch_manifest(base_url, timeout)
    rows = [
        {
            "quarter": q["quarter"],
            "cycle_date": q.get("cycle_date"),
            # A status this package does not recognise is to be read as open (§2).
            "status": q.get("status") if q.get("status") in STATUSES else "open",
            "cu_count": q.get("cu_count"),
            "updated_at": q.get("updated_at"),
        }
        for q in manifest.get("quarters", [])
        # A quarter is listed once held; its accounts file is what makes it readable (§2).
        if "accounts_path" in q
    ]
    frame = pd.DataFrame(rows, columns=COLUMNS)
    frame["cycle_date"] = pd.to_datetime(frame["cycle_date"])
    frame["cu_count"] = frame["cu_count"].astype("Int64")
    frame["updated_at"] = pd.to_datetime(frame["updated_at"], utc=True)
    return frame.sort_values("quarter", ascending=False, ignore_index=True)
