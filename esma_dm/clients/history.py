"""
FIRDS historical database client.

Orchestrates the full ETL pipeline for the esma_hist database:
  - Initial load from cached FULINS files (baseline)
  - Incremental daily updates via DLTINS delta files
  - Point-in-time and current-state queries

The client uses FIRDSFileManager for file discovery and download, and
HistoryStore for all database operations.

Example:
    >>> from esma_dm.clients.history import HistoryClient
    >>>
    >>> client = HistoryClient()
    >>> client.init(asset_types=['E'])                     # load FULINS baseline
    >>> client.update(date_from='2026-01-04', date_to='2026-01-10')  # apply deltas
    >>> info = client.query('GB00B1YW4409')               # current state
    >>> snap = client.query('GB00B1YW4409', as_of='2026-01-05')  # point-in-time
"""

import logging
import re
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from esma_dm.config import default_config
from esma_dm.file_manager.firds import FIRDSFileManager
from esma_dm.storage.history import HistoryStore

FULINS_PATTERN = re.compile(r"FULINS_([A-Z])_(\d{8})_\d+of\d+_data\.csv$")
DLTINS_PATTERN = re.compile(r"DLTINS_(\d{8})_\d+of\d+_data\.csv$")

ALL_ASSET_TYPES = list("CDEFHIJORS")


def _parse_date(date_str: str) -> date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


class HistoryClient:
    """
    High-level client for the FIRDS historical database.

    Combines FIRDSFileManager (file discovery/download) with HistoryStore
    (bulk SQL operations) to build and maintain a historical instrument
    database that supports point-in-time queries.

    Args:
        db_path: Override the default esma_hist.duckdb path.
        cache_dir: Override the default FIRDS cache directory.
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        cache_dir: Optional[Path] = None,
    ):
        self.logger = logging.getLogger(__name__)
        self._db_path = db_path or str(default_config.get_hist_database_path())
        self._cache_dir = cache_dir or (default_config.downloads_path / "firds")
        self._store: Optional[HistoryStore] = None

    @property
    def store(self) -> HistoryStore:
        if self._store is None:
            self._store = HistoryStore(db_path=self._db_path)
            self._store.initialize()
        return self._store

    def _file_manager(self, date_from: str = "2018-01-01", date_to: Optional[str] = None) -> FIRDSFileManager:
        return FIRDSFileManager(
            cache_dir=self._cache_dir,
            date_from=date_from,
            date_to=date_to or _date_str(date.today()),
        )

    # ------------------------------------------------------------------
    # Initial FULINS load
    # ------------------------------------------------------------------

    def init(
        self,
        asset_types: Optional[List[str]] = None,
        download: bool = False,
    ) -> Dict[str, Any]:
        """
        Build the baseline from cached FULINS files.

        Finds the latest FULINS CSV for each requested asset type in the
        local cache.  Optionally downloads fresh files from ESMA first.

        Args:
            asset_types: List of CFI first-character codes.  Defaults to all
                         asset types found in the local cache.
            download: If True, download the latest FULINS from ESMA before
                      loading.  Default is False (use what is already cached).

        Returns:
            Dict with per-file load results and aggregate totals.
        """
        if download:
            self.logger.info("Downloading latest FULINS files from ESMA...")
            mgr = self._file_manager()
            types_to_download = asset_types or ALL_ASSET_TYPES
            for at in types_to_download:
                try:
                    mgr.download_latest_full_files(asset_type=at, update=True)
                except Exception as exc:
                    self.logger.warning(f"Could not download FULINS for {at}: {exc}")

        # Find latest FULINS CSV per asset type in cache
        fulins_files = self._find_latest_fulins_in_cache(asset_types)
        if not fulins_files:
            return {"status": "no_files", "message": "No FULINS files found in cache."}

        results = []
        total_isins = 0
        total_listings = 0

        for csv_path in fulins_files:
            try:
                result = self.store.bulk_load_fulins(csv_path)
                results.append(result)
                total_isins += result.get("isins_inserted", result.get("records_inserted", 0))
                total_listings += result.get("listings_inserted", 0)
            except Exception as exc:
                self.logger.error(f"Failed to load {csv_path.name}: {exc}")
                results.append({"file": csv_path.name, "status": "error", "message": str(exc)})

        return {
            "status": "done",
            "files_processed": len(results),
            "total_isins_inserted": total_isins,
            "total_listings_inserted": total_listings,
            "details": results,
        }

    def _find_latest_fulins_in_cache(
        self, asset_types: Optional[List[str]] = None
    ) -> List[Path]:
        """Return the latest FULINS CSV parts for each requested asset type."""
        all_files = sorted(self._cache_dir.glob("FULINS_*_data.csv"))
        if not all_files:
            return []

        # Group by (asset_type, date) and keep latest date per asset type
        groups: Dict[str, Dict[str, List[Path]]] = {}
        for f in all_files:
            m = FULINS_PATTERN.search(f.name)
            if not m:
                continue
            at = m.group(1)
            dt = m.group(2)
            if asset_types and at not in asset_types:
                continue
            groups.setdefault(at, {}).setdefault(dt, []).append(f)

        selected: List[Path] = []
        for at, date_groups in groups.items():
            latest_date = max(date_groups.keys())
            selected.extend(sorted(date_groups[latest_date]))
        return selected

    # ------------------------------------------------------------------
    # Delta update
    # ------------------------------------------------------------------

    def update(
        self,
        asset_types: Optional[List[str]] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        period: Optional[str] = None,
        download: bool = True,
    ) -> Dict[str, Any]:
        """
        Download and apply DLTINS delta files for a date range.

        Period shortcuts (relative to the last processed DLTINS date):
          ``week``  → last + 1 day to last + 7 days
          ``month`` → last + 1 day to last + 30 days

        If neither ``period`` nor ``date_from`` is given the range runs from
        the day after the last processed file to today.

        Already-applied files are skipped automatically.

        Args:
            asset_types: Not used for filtering (DLTINS files contain all types)
                         but logged for reference.
            date_from: Explicit start date (YYYY-MM-DD).
            date_to: Explicit end date (YYYY-MM-DD, inclusive).  Defaults to today.
            period: ``'week'`` or ``'month'`` — shortcut based on last processed date.
            download: Download missing files from ESMA.  Default True.

        Returns:
            Dict with files_applied, files_skipped, aggregate record counts.
        """
        effective_from, effective_to = self._resolve_date_range(date_from, date_to, period)
        self.logger.info(f"Delta update: {effective_from} to {effective_to}")

        if download:
            self._download_dltins(effective_from, effective_to)

        dltins_files = self._find_dltins_in_cache(effective_from, effective_to)
        if not dltins_files:
            return {
                "status": "no_files",
                "message": f"No DLTINS files found in cache for {effective_from} to {effective_to}.",
            }

        totals: Dict[str, int] = {
            "files_applied": 0,
            "files_skipped": 0,
            "new": 0,
            "modified": 0,
            "terminated": 0,
            "cancelled": 0,
            "errors": 0,
        }
        results = []

        for csv_path in dltins_files:
            try:
                result = self.store.apply_delta_file(csv_path)
                results.append(result)

                if result.get("status") == "already_processed":
                    totals["files_skipped"] += 1
                else:
                    totals["files_applied"] += 1
                    for key in ("new", "modified", "terminated", "cancelled", "errors"):
                        totals[key] += result.get(key, 0)
            except Exception as exc:
                self.logger.error(f"Failed to apply {csv_path.name}: {exc}")
                results.append({"file": csv_path.name, "status": "error", "message": str(exc)})

        return {"status": "done", **totals, "details": results}

    def _resolve_date_range(
        self,
        date_from: Optional[str],
        date_to: Optional[str],
        period: Optional[str],
    ):
        today = date.today()
        end = _parse_date(date_to) if date_to else today

        if date_from:
            start = _parse_date(date_from)
        else:
            last = self.store.get_last_processed_date()
            if last:
                start = _parse_date(last) + timedelta(days=1)
            else:
                # Fall back to baseline date if no deltas applied yet
                baseline = self.store.get_baseline_date()
                start = (_parse_date(baseline) + timedelta(days=1)) if baseline else today

        if period == "week":
            end = start + timedelta(days=6)
        elif period == "month":
            end = start + timedelta(days=29)

        return _date_str(start), _date_str(end)

    def _download_dltins(self, date_from: str, date_to: str) -> None:
        """Download DLTINS files from ESMA for the given date range."""
        try:
            mgr = self._file_manager(date_from=date_from, date_to=date_to)
            files_df = mgr.list_files(file_type="DLTINS", fetch_all=True)
            if files_df.empty:
                self.logger.info("No DLTINS files found on ESMA for the requested range.")
                return

            for _, row in files_df.iterrows():
                url = row.get("download_link")
                fname = row.get("file_name", "")
                # Skip if already cached (parsed CSV exists)
                stem = fname.replace(".zip", "")
                cached = self._cache_dir / f"{stem}_data.csv"
                if cached.exists():
                    continue
                try:
                    self.logger.info(f"Downloading {fname}...")
                    mgr.download_file(url)
                except Exception as exc:
                    self.logger.warning(f"Could not download {fname}: {exc}")
        except Exception as exc:
            self.logger.warning(f"DLTINS download step failed: {exc}")

    def _find_dltins_in_cache(self, date_from: str, date_to: str) -> List[Path]:
        """Return sorted DLTINS CSV paths within the date range from local cache."""
        from_d = _parse_date(date_from)
        to_d = _parse_date(date_to)

        files = []
        for f in sorted(self._cache_dir.glob("DLTINS_*_data.csv")):
            m = DLTINS_PATTERN.search(f.name)
            if not m:
                continue
            file_date = _parse_date(f"{m.group(1)[:4]}-{m.group(1)[4:6]}-{m.group(1)[6:8]}")
            if from_d <= file_date <= to_d:
                files.append(f)
        return files

    # ------------------------------------------------------------------
    # Status and query
    # ------------------------------------------------------------------

    def status(self) -> Dict[str, Any]:
        """Return a structured summary of the history database."""
        return self.store.get_status()

    def query(self, isin: str, as_of: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Look up an instrument in the historical database.

        Args:
            isin: 12-character ISIN (ISO 6166).
            as_of: Optional date (YYYY-MM-DD) for point-in-time lookup.
                   If None, returns the current (latest) state.

        Returns:
            Dict with instrument fields, or None if not found.
        """
        return self.store.query_instrument(isin, as_of)

    def version_history(self, isin: str) -> pd.DataFrame:
        """Return all tracked versions for an ISIN (both active and historical)."""
        return self.store.get_version_history(isin)
