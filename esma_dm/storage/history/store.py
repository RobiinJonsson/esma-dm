"""
DuckDB storage backend for the FIRDS historical database (esma_hist.duckdb).

Implements ESMA Section 8 version management using bulk SQL operations.
Each DLTINS file is applied once; processed files are tracked in processing_log.

Record type mapping per ESMA Section 8.2:
  NewRcrd     -> insert new instrument or open a new version on existing ISIN
  ModfdRcrd   -> close current version, open a new one with updated data
  TermntdRcrd -> close current version (valid_to_date set), mark inactive
  CancRcrd    -> remove instrument from active set, log in cancellations
"""

import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

from .schema import initialize_history_schema

DLTINS_PREFIXES = {
    "NEW": "NewRcrd",
    "MODIFIED": "ModfdRcrd",
    "TERMINATED": "TermntdRcrd",
    "CANCELLED": "CancRcrd",
}

FULINS_DATE_PATTERN = re.compile(r"FULINS_([A-Z])_(\d{8})_")
DLTINS_DATE_PATTERN = re.compile(r"DLTINS_(\d{8})_")


def _extract_fulins_meta(filename: str):
    """Return (asset_type, pub_date_str) from a FULINS filename."""
    m = FULINS_DATE_PATTERN.search(filename)
    if not m:
        raise ValueError(f"Cannot extract date from FULINS filename: {filename}")
    asset_type = m.group(1)
    raw = m.group(2)
    pub_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return asset_type, pub_date


def _extract_dltins_date(filename: str) -> str:
    """Return publication date (YYYY-MM-DD) from a DLTINS filename."""
    m = DLTINS_DATE_PATTERN.search(filename)
    if not m:
        raise ValueError(f"Cannot extract date from DLTINS filename: {filename}")
    raw = m.group(1)
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def _find_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """Return first matching column name or None."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


class HistoryStore:
    """
    DuckDB storage for the FIRDS historical database.

    Database path defaults to the package's storage/duckdb/database/esma_hist.duckdb.
    All bulk operations use DuckDB's columnar engine to handle files with
    millions of records efficiently.

    Example:
        >>> store = HistoryStore()
        >>> store.initialize()
        >>> store.bulk_load_fulins(Path('downloads/data/firds/FULINS_E_20260117_01of02_data.csv'))
        >>> store.apply_delta_file(Path('downloads/data/firds/DLTINS_20260118_01of01_data.csv'))
    """

    def __init__(self, db_path: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        if db_path is None:
            from esma_dm.config import default_config
            db_path = str(default_config.get_hist_database_path())
        self.db_path = db_path
        self._con: Optional[duckdb.DuckDBPyConnection] = None

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            self._con = duckdb.connect(self.db_path)
        return self._con

    def initialize(self) -> None:
        """Create schema if not already present (idempotent)."""
        initialize_history_schema(self.con)
        self.logger.info(f"History schema initialized at {self.db_path}")

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    # ------------------------------------------------------------------
    # FULINS initial load
    # ------------------------------------------------------------------

    def bulk_load_fulins(self, csv_path: Path) -> Dict[str, Any]:
        """
        Load a single FULINS CSV file into instruments and listings tables.

        FULINS files have one row per (ISIN, TradingVenue).  This method:
          - Inserts one row per unique ISIN into ``instruments`` (ON CONFLICT DO NOTHING).
          - Inserts all rows into ``listings`` (ON CONFLICT DO NOTHING), capturing
            every trading venue record for each ISIN.

        Already-loaded files (tracked in baseline_info) are skipped.

        Args:
            csv_path: Path to a *_data.csv file produced by the FIRDS parser.

        Returns:
            Dict with file, date, asset_type, isins_total, isins_inserted,
            listings_total, listings_inserted, status.
        """
        asset_type, pub_date = _extract_fulins_meta(csv_path.name)

        already = self.con.execute(
            "SELECT COUNT(*) FROM baseline_info WHERE file_name = ?",
            [csv_path.name],
        ).fetchone()[0]
        if already:
            self.logger.info(f"Skipping already-loaded file: {csv_path.name}")
            return {
                "file": csv_path.name,
                "date": pub_date,
                "asset_type": asset_type,
                "isins_total": 0,
                "isins_inserted": 0,
                "listings_total": 0,
                "listings_inserted": 0,
                "status": "already_loaded",
            }

        self.logger.info(f"Loading FULINS {csv_path.name} ({pub_date}, asset={asset_type})")
        df = pd.read_csv(csv_path, low_memory=False)

        isin_col       = _find_col(df, "Id")
        cfi_col        = _find_col(df, "RefData_FinInstrmGnlAttrbts_ClssfctnTp")
        name_col       = _find_col(df, "RefData_FinInstrmGnlAttrbts_FullNm")
        short_name_col = _find_col(df, "RefData_FinInstrmGnlAttrbts_ShrtNm")
        issuer_col     = _find_col(df, "RefData_Issr")
        currency_col   = _find_col(df, "RefData_FinInstrmGnlAttrbts_NtnlCcy")
        ca_col         = _find_col(df, "RefData_TechAttrbts_RlvntCmptntAuthrty")
        pub_date_col   = _find_col(df, "RefData_TechAttrbts_PblctnPrd_FrDt")
        venue_col      = _find_col(df, "RefData_TradgVnRltdAttrbts_Id")
        issuer_req_col = _find_col(df, "RefData_TradgVnRltdAttrbts_IssrReq")
        first_trd_col  = _find_col(df, "RefData_TradgVnRltdAttrbts_FrstTradDt")
        term_dt_col    = _find_col(df, "RefData_TradgVnRltdAttrbts_TermntnDt")
        admsn_col      = _find_col(df, "RefData_TradgVnRltdAttrbts_AdmssnApprvlDtByIssr")
        req_admsn_col  = _find_col(df, "RefData_TradgVnRltdAttrbts_ReqForAdmssnDt")

        if not isin_col:
            raise ValueError(f"No ISIN column (Id) found in {csv_path.name}")

        now_ts = datetime.now().isoformat()
        df = df.dropna(subset=[isin_col])
        df = df[df[isin_col].astype(str).str.len() == 12]
        total_rows = len(df)

        # ---- instruments (one per unique ISIN — keep first occurrence) ----
        df_isin = df.drop_duplicates(subset=[isin_col], keep="first")
        instruments_df = pd.DataFrame({
            "isin":                df_isin[isin_col].astype(str),
            "cfi_code":            df_isin[cfi_col]        if cfi_col        else None,
            "instrument_type":     df_isin[cfi_col].str[:1]  if cfi_col        else asset_type,
            "full_name":           df_isin[name_col]       if name_col       else None,
            "short_name":          df_isin[short_name_col] if short_name_col else None,
            "issuer":              df_isin[issuer_col]     if issuer_col     else None,
            "currency":            df_isin[currency_col]   if currency_col   else None,
            "competent_authority": df_isin[ca_col]         if ca_col         else None,
            "publication_date":    df_isin[pub_date_col]   if pub_date_col   else None,
            "valid_from_date":     pub_date,
            "latest_record_flag":  True,
            "record_type":         "NEW",
            "version_number":      1,
            "source_file":         csv_path.name,
            "source_file_type":    "FULINS",
            "indexed_at":          now_ts,
        })

        self.con.register("_hist_fulins_instr", instruments_df)
        try:
            before_instr = self.con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
            self.con.execute("""
                INSERT INTO instruments (
                    isin, cfi_code, instrument_type, full_name, short_name,
                    issuer, currency, competent_authority, publication_date,
                    valid_from_date, latest_record_flag,
                    record_type, version_number,
                    source_file, source_file_type, indexed_at
                )
                SELECT
                    isin, cfi_code, instrument_type, full_name, short_name,
                    issuer, currency, competent_authority, publication_date,
                    valid_from_date, latest_record_flag,
                    record_type, version_number,
                    source_file, source_file_type, indexed_at
                FROM _hist_fulins_instr
                ON CONFLICT (isin) DO NOTHING
            """)
            after_instr = self.con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
            isins_inserted = after_instr - before_instr
        finally:
            self.con.unregister("_hist_fulins_instr")

        # ---- listings (one per ISIN + trading venue) ----
        listings_df = pd.DataFrame({
            "isin":                       df[isin_col].astype(str),
            "trading_venue_id":            df[venue_col]      if venue_col      else None,
            "first_trade_date":            df[first_trd_col]  if first_trd_col  else None,
            "termination_date":            df[term_dt_col]    if term_dt_col    else None,
            "admission_approval_date":     df[admsn_col]      if admsn_col      else None,
            "request_for_admission_date":  df[req_admsn_col]  if req_admsn_col  else None,
            "issuer_request":              df[issuer_req_col] if issuer_req_col else None,
            "source_file":                csv_path.name,
            "indexed_at":                 now_ts,
        })

        self.con.register("_hist_fulins_listings", listings_df)
        try:
            before_lst = self.con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
            self.con.execute("""
                INSERT INTO listings (
                    isin, trading_venue_id, first_trade_date, termination_date,
                    admission_approval_date, request_for_admission_date,
                    issuer_request, source_file, indexed_at
                )
                SELECT
                    isin, trading_venue_id, first_trade_date, termination_date,
                    admission_approval_date, request_for_admission_date,
                    issuer_request, source_file, indexed_at
                FROM _hist_fulins_listings
                WHERE isin IN (SELECT isin FROM instruments)
                ON CONFLICT (isin, trading_venue_id) DO NOTHING
            """)
            after_lst = self.con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
            listings_inserted = after_lst - before_lst
        finally:
            self.con.unregister("_hist_fulins_listings")

        self.con.execute(
            """
            INSERT INTO baseline_info (file_name, file_date, asset_type, loaded_at, records_loaded)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (file_name) DO NOTHING
            """,
            [csv_path.name, pub_date, asset_type, now_ts, total_rows],
        )

        self.logger.info(
            f"Loaded {csv_path.name}: {isins_inserted:,} ISINs, "
            f"{listings_inserted:,} listings (from {total_rows:,} rows)"
        )
        return {
            "file": csv_path.name,
            "date": pub_date,
            "asset_type": asset_type,
            "isins_total": len(df_isin),
            "isins_inserted": isins_inserted,
            "listings_total": total_rows,
            "listings_inserted": listings_inserted,
            "status": "loaded",
        }

    # ------------------------------------------------------------------
    # DLTINS delta application
    # ------------------------------------------------------------------

    def is_file_processed(self, filename: str) -> bool:
        """Return True if the file has already been applied."""
        result = self.con.execute(
            "SELECT COUNT(*) FROM processing_log WHERE file_name = ?",
            [filename],
        ).fetchone()[0]
        return result > 0

    def apply_delta_file(self, csv_path: Path) -> Dict[str, Any]:
        """
        Apply a single DLTINS CSV file per ESMA Section 8.2.

        Processes records in type order: NEW → MODIFIED → TERMINATED → CANCELLED.
        Already-applied files (tracked in processing_log) are skipped.

        Args:
            csv_path: Path to a DLTINS *_data.csv file.

        Returns:
            Dict with record counts per type and status.
        """
        if self.is_file_processed(csv_path.name):
            self.logger.info(f"Skipping already-processed file: {csv_path.name}")
            return {"file": csv_path.name, "status": "already_processed"}

        pub_date = _extract_dltins_date(csv_path.name)
        close_date = (
            datetime.strptime(pub_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        self.logger.info(f"Applying delta {csv_path.name} (date={pub_date})")
        df = pd.read_csv(csv_path, low_memory=False)

        if "_record_type" not in df.columns or "Id" not in df.columns:
            self.logger.error(f"Unexpected format in {csv_path.name}")
            return {"file": csv_path.name, "status": "error", "message": "Missing required columns"}

        stats: Dict[str, int] = {
            "new": 0,
            "modified": 0,
            "terminated": 0,
            "cancelled": 0,
            "skipped": 0,
            "errors": 0,
        }

        for record_type in ("NEW", "MODIFIED", "TERMINATED", "CANCELLED"):
            subset = df[df["_record_type"] == record_type].copy()
            if subset.empty:
                continue

            prefix = DLTINS_PREFIXES[record_type]
            normalized = self._normalize_dltins(subset, prefix)

            if normalized.empty:
                continue

            try:
                if record_type == "NEW":
                    stats["new"] += self._apply_new(normalized, pub_date, close_date, csv_path.name)
                elif record_type == "MODIFIED":
                    stats["modified"] += self._apply_modified(normalized, pub_date, close_date, csv_path.name)
                elif record_type == "TERMINATED":
                    stats["terminated"] += self._apply_terminated(normalized, pub_date, csv_path.name)
                elif record_type == "CANCELLED":
                    stats["cancelled"] += self._apply_cancelled(normalized, pub_date, csv_path.name)
            except Exception as exc:
                self.logger.error(f"Error processing {record_type} records from {csv_path.name}: {exc}")
                stats["errors"] += len(normalized)

        self._log_processed_file(csv_path.name, pub_date, stats)
        self.logger.info(
            f"Applied {csv_path.name}: "
            f"NEW={stats['new']} MOD={stats['modified']} "
            f"TERM={stats['terminated']} CANC={stats['cancelled']} "
            f"ERR={stats['errors']}"
        )
        return {"file": csv_path.name, "date": pub_date, "status": "applied", **stats}

    # ------------------------------------------------------------------
    # Internal delta helpers
    # ------------------------------------------------------------------

    def _normalize_dltins(
        self, df: pd.DataFrame, prefix: str
    ) -> pd.DataFrame:
        """
        Build a standard-named DataFrame from a DLTINS record-type subset.

        Returns one row per (ISIN, trading_venue_id) — the natural granularity
        of DLTINS files — with instrument-level attributes carried on each row.
        Callers that need one-row-per-ISIN should deduplicate on 'isin'.
        """
        fc = lambda *cols: _find_col(df, *cols)

        cfi_col        = fc(f"{prefix}_FinInstrmGnlAttrbts_ClssfctnTp")
        name_col       = fc(f"{prefix}_FinInstrmGnlAttrbts_FullNm")
        short_name_col = fc(f"{prefix}_FinInstrmGnlAttrbts_ShrtNm")
        issuer_col     = fc(f"{prefix}_Issr")
        currency_col   = fc(f"{prefix}_FinInstrmGnlAttrbts_NtnlCcy")
        ca_col         = fc(f"{prefix}_TechAttrbts_RlvntCmptntAuthrty")
        venue_col      = fc(f"{prefix}_TradgVnRltdAttrbts_Id")
        first_trd_col  = fc(f"{prefix}_TradgVnRltdAttrbts_FrstTradDt")
        term_dt_col    = fc(f"{prefix}_TradgVnRltdAttrbts_TermntnDt")
        admsn_col      = fc(f"{prefix}_TradgVnRltdAttrbts_AdmssnApprvlDtByIssr")
        req_admsn_col  = fc(f"{prefix}_TradgVnRltdAttrbts_ReqForAdmssnDt")
        issuer_req_col = fc(f"{prefix}_TradgVnRltdAttrbts_IssrReq")

        normalized = pd.DataFrame({"isin": df["Id"].astype(str)})
        normalized["cfi_code"]            = df[cfi_col]        if cfi_col        else None
        normalized["instrument_type"]     = normalized["cfi_code"].str[:1] if cfi_col else None
        normalized["full_name"]           = df[name_col]       if name_col       else None
        normalized["short_name"]          = df[short_name_col] if short_name_col else None
        normalized["issuer"]              = df[issuer_col]     if issuer_col     else None
        normalized["currency"]            = df[currency_col]   if currency_col   else None
        normalized["competent_authority"] = df[ca_col]         if ca_col         else None
        normalized["trading_venue_id"]     = df[venue_col]      if venue_col      else None
        normalized["first_trade_date"]    = df[first_trd_col]  if first_trd_col  else None
        normalized["termination_date"]    = df[term_dt_col]    if term_dt_col    else None
        normalized["admission_approval_date"]    = df[admsn_col]      if admsn_col      else None
        normalized["request_for_admission_date"] = df[req_admsn_col]  if req_admsn_col  else None
        normalized["issuer_request"]      = df[issuer_req_col] if issuer_req_col else None

        normalized = normalized.dropna(subset=["isin"])
        normalized = normalized[normalized["isin"].str.len() == 12]
        return normalized.reset_index(drop=True)

    def _apply_new(
        self,
        records: pd.DataFrame,
        pub_date: str,
        close_date: str,
        source_file: str,
    ) -> int:
        """
        Process NEW records.

        Each row is one (ISIN, TradingVenue) listing.
        - Truly new ISINs: inserted into instruments with version=1.
        - Late-arrival NEWs (ISIN already in instruments): archived and re-versioned.
        - All rows: upserted into listings.

        Note: uses physical TEMP TABLEs (not registered pandas views) to avoid
        a DuckDB bug where UPDATE ... WHERE isin IN (SELECT from pandas view)
        raises an internal assertion error on large batches.
        """
        now_ts = datetime.now().isoformat()
        by_isin = records.drop_duplicates(subset=["isin"], keep="last")

        self.con.execute("DROP TABLE IF EXISTS _new_recs")
        self.con.execute("DROP TABLE IF EXISTS _new_listings")
        self.con.execute("CREATE TEMP TABLE _new_recs AS SELECT * FROM ?", [by_isin])
        self.con.execute("CREATE TEMP TABLE _new_listings AS SELECT * FROM ?", [records])
        try:
            # Archive late-arrival NEWs (ISIN already in instruments)
            self.con.execute(
                """
                INSERT INTO instrument_history
                    (isin, version_number, valid_from_date, valid_to_date,
                     record_type, cfi_code, full_name, issuer,
                     source_file, source_file_type, indexed_at)
                SELECT i.isin, i.version_number, i.valid_from_date, ?,
                       i.record_type, i.cfi_code, i.full_name, i.issuer,
                       i.source_file, i.source_file_type, i.indexed_at
                FROM instruments i
                INNER JOIN _new_recs r ON i.isin = r.isin
                WHERE i.latest_record_flag = TRUE
                ON CONFLICT (isin, version_number) DO NOTHING
                """,
                [close_date],
            )

            # Update version for existing ISINs
            self.con.execute(
                """
                UPDATE instruments
                SET
                    version_number = instruments.version_number + 1,
                    valid_from_date = ?,
                    valid_to_date = NULL,
                    latest_record_flag = TRUE,
                    record_type = 'NEW',
                    source_file = ?,
                    source_file_type = 'DLTINS',
                    last_update_timestamp = ?
                WHERE isin IN (SELECT isin FROM _new_recs)
                  AND latest_record_flag = TRUE
                """,
                [pub_date, source_file, now_ts],
            )

            # Insert truly new ISINs
            self.con.execute(
                """
                INSERT INTO instruments (
                    isin, cfi_code, instrument_type, full_name, short_name,
                    issuer, currency, competent_authority,
                    valid_from_date, latest_record_flag,
                    record_type, version_number,
                    source_file, source_file_type, indexed_at
                )
                SELECT
                    r.isin, r.cfi_code, r.instrument_type, r.full_name, r.short_name,
                    r.issuer, r.currency, r.competent_authority,
                    ?, TRUE, 'NEW', 1, ?, 'DLTINS', ?
                FROM _new_recs r
                WHERE r.isin NOT IN (SELECT isin FROM instruments)
                ON CONFLICT (isin) DO NOTHING
                """,
                [pub_date, source_file, now_ts],
            )

            # Upsert all listings
            self.con.execute(
                """
                INSERT INTO listings (
                    isin, trading_venue_id, first_trade_date, termination_date,
                    admission_approval_date, request_for_admission_date,
                    issuer_request, source_file, indexed_at
                )
                SELECT
                    isin, trading_venue_id, first_trade_date, termination_date,
                    admission_approval_date, request_for_admission_date,
                    issuer_request, ?, ?
                FROM _new_listings
                WHERE isin IN (SELECT isin FROM instruments)
                ON CONFLICT (isin, trading_venue_id) DO NOTHING
                """,
                [source_file, now_ts],
            )
        finally:
            self.con.execute("DROP TABLE IF EXISTS _new_recs")
            self.con.execute("DROP TABLE IF EXISTS _new_listings")

        return len(records)

    def _apply_modified(
        self,
        records: pd.DataFrame,
        pub_date: str,
        close_date: str,
        source_file: str,
    ) -> int:
        """
        Process MODIFIED records.

        Each row is one (ISIN, TradingVenue) listing.
        - Instrument attributes are updated from the first row per ISIN.
        - All listing rows are upserted into the listings table.
        """
        now_ts = datetime.now().isoformat()
        by_isin = records.drop_duplicates(subset=["isin"], keep="last")

        self.con.execute("DROP TABLE IF EXISTS _mod_recs")
        self.con.execute("DROP TABLE IF EXISTS _mod_listings")
        self.con.execute("CREATE TEMP TABLE _mod_recs AS SELECT * FROM ?", [by_isin])
        self.con.execute("CREATE TEMP TABLE _mod_listings AS SELECT * FROM ?", [records])
        try:
            # Archive current active version before overwriting
            self.con.execute(
                """
                INSERT INTO instrument_history
                    (isin, version_number, valid_from_date, valid_to_date,
                     record_type, cfi_code, full_name, issuer,
                     source_file, source_file_type, indexed_at)
                SELECT i.isin, i.version_number, i.valid_from_date, ?,
                       i.record_type, i.cfi_code, i.full_name, i.issuer,
                       i.source_file, i.source_file_type, i.indexed_at
                FROM instruments i
                INNER JOIN _mod_recs r ON i.isin = r.isin
                WHERE i.latest_record_flag = TRUE
                ON CONFLICT (isin, version_number) DO NOTHING
                """,
                [close_date],
            )

            # Update instruments with new attributes and bump version
            self.con.execute(
                """
                UPDATE instruments
                SET
                    cfi_code              = r.cfi_code,
                    instrument_type       = r.instrument_type,
                    full_name             = r.full_name,
                    short_name            = r.short_name,
                    issuer                = r.issuer,
                    currency              = r.currency,
                    competent_authority   = r.competent_authority,
                    valid_from_date       = ?,
                    valid_to_date         = NULL,
                    latest_record_flag    = TRUE,
                    record_type           = 'MODIFIED',
                    version_number        = instruments.version_number + 1,
                    source_file           = ?,
                    source_file_type      = 'DLTINS',
                    last_update_timestamp = ?,
                    updated_at            = ?
                FROM _mod_recs r
                WHERE instruments.isin = r.isin
                  AND instruments.latest_record_flag = TRUE
                """,
                [pub_date, source_file, now_ts, now_ts],
            )

            # Upsert all listing rows
            self.con.execute(
                """
                INSERT INTO listings (
                    isin, trading_venue_id, first_trade_date, termination_date,
                    admission_approval_date, request_for_admission_date,
                    issuer_request, source_file, indexed_at
                )
                SELECT
                    isin, trading_venue_id, first_trade_date, termination_date,
                    admission_approval_date, request_for_admission_date,
                    issuer_request, ?, ?
                FROM _mod_listings
                WHERE isin IN (SELECT isin FROM instruments)
                ON CONFLICT (isin, trading_venue_id) DO UPDATE SET
                    first_trade_date           = EXCLUDED.first_trade_date,
                    termination_date           = EXCLUDED.termination_date,
                    admission_approval_date    = EXCLUDED.admission_approval_date,
                    request_for_admission_date = EXCLUDED.request_for_admission_date,
                    source_file                = EXCLUDED.source_file
                """,
                [source_file, now_ts],
            )
        finally:
            self.con.execute("DROP TABLE IF EXISTS _mod_recs")
            self.con.execute("DROP TABLE IF EXISTS _mod_listings")

        return len(records)

    def _apply_terminated(
        self,
        records: pd.DataFrame,
        pub_date: str,
        source_file: str,
    ) -> int:
        """
        Process TERMINATED records.

        Each row is one (ISIN, TradingVenue) listing being terminated.
        - The specific listings are updated with termination_date.
        - When ALL listings for an ISIN are now terminated the instrument itself
          is marked with valid_to_date = pub_date and latest_record_flag = FALSE.
          Instruments with at least one active listing remain active.
        """
        now_ts = datetime.now().isoformat()
        by_isin = records.drop_duplicates(subset=["isin"], keep="last")

        self.con.execute("DROP TABLE IF EXISTS _term_recs")
        self.con.execute("DROP TABLE IF EXISTS _term_listings")
        self.con.execute("CREATE TEMP TABLE _term_recs AS SELECT * FROM ?", [by_isin])
        self.con.execute("CREATE TEMP TABLE _term_listings AS SELECT * FROM ?", [records])
        try:
            # Update terminated listings with termination date
            self.con.execute(
                """
                UPDATE listings
                SET termination_date = ?,
                    source_file = ?
                FROM _term_listings r
                WHERE listings.isin = r.isin
                  AND (listings.trading_venue_id = r.trading_venue_id
                       OR r.trading_venue_id IS NULL)
                """,
                [pub_date, source_file],
            )

            # Archive instrument version for fully-terminated ISINs
            self.con.execute(
                """
                INSERT INTO instrument_history
                    (isin, version_number, valid_from_date, valid_to_date,
                     record_type, cfi_code, full_name, issuer,
                     source_file, source_file_type, indexed_at)
                SELECT i.isin, i.version_number, i.valid_from_date, ?,
                       'TERMINATED', i.cfi_code, i.full_name, i.issuer,
                       i.source_file, i.source_file_type, i.indexed_at
                FROM instruments i
                WHERE i.isin IN (SELECT isin FROM _term_recs)
                  AND i.latest_record_flag = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM listings l
                      WHERE l.isin = i.isin
                        AND (l.termination_date IS NULL OR l.termination_date > ?)
                  )
                ON CONFLICT (isin, version_number) DO NOTHING
                """,
                [pub_date, pub_date],
            )

            # Mark fully-terminated instruments (keep row for point-in-time queries)
            self.con.execute(
                """
                UPDATE instruments
                SET
                    valid_to_date         = ?,
                    latest_record_flag    = FALSE,
                    record_type           = 'TERMINATED',
                    source_file           = ?,
                    last_update_timestamp = ?
                WHERE isin IN (SELECT isin FROM _term_recs)
                  AND latest_record_flag = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM listings l
                      WHERE l.isin = instruments.isin
                        AND (l.termination_date IS NULL OR l.termination_date > ?)
                  )
                """,
                [pub_date, source_file, now_ts, pub_date],
            )
        finally:
            self.con.execute("DROP TABLE IF EXISTS _term_recs")
            self.con.execute("DROP TABLE IF EXISTS _term_listings")

        return len(records)

    def _apply_cancelled(
        self,
        records: pd.DataFrame,
        pub_date: str,
        source_file: str,
    ) -> int:
        """
        Process CANCELLED records.

        Instrument is inserted into the cancellations log and removed from
        instruments, listings, and instrument_history (per ESMA guidance).
        """
        self.con.execute("DROP TABLE IF EXISTS _canc_recs")
        self.con.execute("CREATE TEMP TABLE _canc_recs AS SELECT * FROM ?", [records])
        try:
            self.con.execute(
                """
                INSERT INTO cancellations
                    (isin, cancellation_date, source_file, indexed_at)
                SELECT DISTINCT r.isin, ?, ?, now()
                FROM _canc_recs r
                WHERE r.isin IS NOT NULL
                """,
                [pub_date, source_file],
            )

            self.con.execute(
                "DELETE FROM listings WHERE isin IN (SELECT DISTINCT isin FROM _canc_recs)"
            )
            self.con.execute(
                "DELETE FROM instrument_history WHERE isin IN (SELECT DISTINCT isin FROM _canc_recs)"
            )
            self.con.execute(
                "DELETE FROM instruments WHERE isin IN (SELECT DISTINCT isin FROM _canc_recs)"
            )
        finally:
            self.con.execute("DROP TABLE IF EXISTS _canc_recs")

        return len(records)

    def _log_processed_file(
        self, filename: str, file_date: str, stats: Dict[str, int]
    ) -> None:
        """Insert a row into processing_log."""
        self.con.execute(
            """
            INSERT INTO processing_log
                (file_name, file_date, applied_at,
                 records_new, records_modified, records_terminated,
                 records_cancelled, records_skipped, records_error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (file_name) DO NOTHING
            """,
            [
                filename,
                file_date,
                datetime.now().isoformat(),
                stats.get("new", 0),
                stats.get("modified", 0),
                stats.get("terminated", 0),
                stats.get("cancelled", 0),
                stats.get("skipped", 0),
                stats.get("errors", 0),
            ],
        )

    # ------------------------------------------------------------------
    # Status and query
    # ------------------------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        """Return a summary of the history database state."""
        status: Dict[str, Any] = {"db_path": self.db_path}

        # Baseline info
        baseline = self.con.execute(
            "SELECT asset_type, file_date, records_loaded FROM baseline_info ORDER BY file_date"
        ).fetchdf()
        status["baseline_files"] = baseline.to_dict("records")
        status["baseline_asset_types"] = sorted(baseline["asset_type"].unique().tolist()) if not baseline.empty else []
        status["baseline_date"] = str(baseline["file_date"].min())[:10] if not baseline.empty else None

        # Processing log
        plog = self.con.execute(
            """
            SELECT COUNT(*) as files,
                   MIN(file_date) as from_date,
                   MAX(file_date) as to_date,
                   SUM(records_new) as total_new,
                   SUM(records_modified) as total_modified,
                   SUM(records_terminated) as total_terminated,
                   SUM(records_cancelled) as total_cancelled
            FROM processing_log
            """
        ).fetchone()
        if plog:
            status["delta_files_applied"] = plog[0]
            status["delta_from_date"] = str(plog[1])[:10] if plog[1] else None
            status["delta_to_date"] = str(plog[2])[:10] if plog[2] else None
            status["total_new"] = plog[3] or 0
            status["total_modified"] = plog[4] or 0
            status["total_terminated"] = plog[5] or 0
            status["total_cancelled"] = plog[6] or 0

        # Active instruments
        counts = self.con.execute(
            """
            SELECT
                COUNT(*) AS active,
                SUM(CASE WHEN latest_record_flag THEN 1 ELSE 0 END) AS latest,
                COUNT(DISTINCT instrument_type) AS asset_types
            FROM instruments
            """
        ).fetchone()
        if counts:
            status["instruments_active"] = counts[0]
            status["instruments_latest"] = counts[1]
            status["asset_type_count"] = counts[2]

        # History table
        hist_count = self.con.execute(
            "SELECT COUNT(*) FROM instrument_history"
        ).fetchone()[0]
        status["history_versions"] = hist_count

        return status

    def get_processed_files(self) -> List[str]:
        """Return list of DLTINS filenames already in processing_log."""
        rows = self.con.execute(
            "SELECT file_name FROM processing_log ORDER BY file_date"
        ).fetchall()
        return [r[0] for r in rows]

    def get_last_processed_date(self) -> Optional[str]:
        """Return the latest file_date in processing_log (or None)."""
        row = self.con.execute(
            "SELECT CAST(MAX(file_date) AS VARCHAR) FROM processing_log"
        ).fetchone()
        if not row or not row[0]:
            return None
        return str(row[0])[:10]  # Ensure YYYY-MM-DD format

    def get_baseline_date(self) -> Optional[str]:
        """Return the earliest FULINS file_date from baseline_info."""
        row = self.con.execute(
            "SELECT CAST(MIN(file_date) AS VARCHAR) FROM baseline_info"
        ).fetchone()
        if not row or not row[0]:
            return None
        return str(row[0])[:10]  # Ensure YYYY-MM-DD format

    def query_instrument(self, isin: str, as_of_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Point-in-time lookup for a single ISIN.

        If as_of_date is None, returns the current (latest) version.
        If as_of_date is provided (YYYY-MM-DD), returns the version active
        on that date per ESMA Section 9 query pattern.
        """
        if as_of_date:
            row = self.con.execute(
                """
                SELECT isin, cfi_code, instrument_type, full_name, short_name,
                       issuer, currency, competent_authority,
                       valid_from_date, valid_to_date, latest_record_flag,
                       record_type, version_number, source_file
                FROM instruments
                WHERE isin = ?
                  AND valid_from_date <= ?
                  AND (valid_to_date IS NULL OR valid_to_date >= ?)

                UNION ALL

                SELECT isin, cfi_code, instrument_type, full_name, short_name,
                       issuer, currency, NULL as competent_authority,
                       valid_from_date, valid_to_date, FALSE,
                       record_type, version_number, source_file
                FROM instrument_history
                WHERE isin = ?
                  AND valid_from_date <= ?
                  AND (valid_to_date IS NULL OR valid_to_date >= ?)

                ORDER BY version_number DESC
                LIMIT 1
                """,
                [isin, as_of_date, as_of_date, isin, as_of_date, as_of_date],
            ).fetchone()
        else:
            row = self.con.execute(
                """
                SELECT isin, cfi_code, instrument_type, full_name, short_name,
                       issuer, currency, competent_authority,
                       valid_from_date, valid_to_date, latest_record_flag,
                       record_type, version_number, source_file
                FROM instruments
                WHERE isin = ?
                LIMIT 1
                """,
                [isin],
            ).fetchone()

        if not row:
            return None

        keys = [
            "isin", "cfi_code", "instrument_type", "full_name", "short_name",
            "issuer", "currency", "competent_authority",
            "valid_from_date", "valid_to_date", "latest_record_flag",
            "record_type", "version_number", "source_file",
        ]
        return dict(zip(keys, row))

    def get_version_history(self, isin: str) -> pd.DataFrame:
        """Return all versions for an ISIN from both instruments and instrument_history."""
        return self.con.execute(
            """
            SELECT isin, version_number, valid_from_date, valid_to_date,
                   record_type, cfi_code, full_name, source_file
            FROM instrument_history
            WHERE isin = ?

            UNION ALL

            SELECT isin, version_number, valid_from_date, valid_to_date,
                   record_type, cfi_code, full_name, source_file
            FROM instruments
            WHERE isin = ?

            ORDER BY version_number
            """,
            [isin, isin],
        ).fetchdf()
