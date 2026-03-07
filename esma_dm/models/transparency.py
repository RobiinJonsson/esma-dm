"""
Transparency record models for ESMA FITRS data.

FITRS (Financial Instruments Transparency System) publishes pre- and post-trade
transparency thresholds for equity and non-equity instruments under MiFID II.

File type taxonomy:
  FULECR / DLTECR  - Equity Calculation Results   (ISIN-level, CFI E/C/R)
  FULNCR / DLTNCR  - Non-Equity Calculation Results (ISIN-level, all CFI types)
"""
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class EquityTransparencyRecord:
    """
    ISIN-level equity transparency result (FULECR / DLTECR).

    One record per ISIN per assessment period. Fields correspond to the
    CSV columns produced by parsing ESMA FITRS equity XML files via
    Utils.download_and_parse_file().

    Column mapping (CSV name → field name):
        TechRcrdId              → record_id
        Id                      → isin
        FinInstrmClssfctn       → classification
        FrDt                    → period_from
        ToDt                    → period_to
        Lqdty                   → liquid_market
        Mthdlgy                 → methodology
        AvrgDalyTrnvr           → average_daily_turnover
        LrgInScale              → large_in_scale_threshold
        AvrgDalyNbOfTxs         → average_daily_transactions
        Id_2                    → most_relevant_market
        AvrgDalyNbOfTxs_3       → waiver_threshold_transactions
        AvrgTxVal               → average_transaction_value
        StdMktSz                → standard_market_size
        TtlNbOfTxsExctd         → total_transactions_executed
        TtlVolOfTxsExctd        → total_volume_executed
        Sttstcs                 → statistics (present in E files only)
    """

    record_id: Optional[str] = None
    """Technical record identifier (TechRcrdId)"""

    isin: Optional[str] = None
    """Instrument ISIN (Id)"""

    classification: Optional[str] = None
    """CFI classification code (FinInstrmClssfctn), e.g. SHRS, ETFS"""

    period_from: Optional[date] = None
    """Start of the assessment period (FrDt)"""

    period_to: Optional[date] = None
    """End of the assessment period (ToDt)"""

    liquid_market: Optional[bool] = None
    """Whether the instrument is in a liquid market (Lqdty)"""

    methodology: Optional[str] = None
    """Calculation methodology: SINT, YEAR, ESTM, or FFWK (Mthdlgy)"""

    average_daily_turnover: Optional[float] = None
    """Average daily turnover in EUR (AvrgDalyTrnvr)"""

    large_in_scale_threshold: Optional[float] = None
    """Large-in-Scale pre-trade threshold in EUR (LrgInScale)"""

    average_daily_transactions: Optional[float] = None
    """Average daily number of transactions (AvrgDalyNbOfTxs)"""

    most_relevant_market: Optional[str] = None
    """MIC of the most relevant market in terms of liquidity (Id_2)"""

    waiver_threshold_transactions: Optional[float] = None
    """Number of transactions above which the waiver applies (AvrgDalyNbOfTxs_3)"""

    average_transaction_value: Optional[float] = None
    """Average transaction value in EUR (AvrgTxVal)"""

    standard_market_size: Optional[float] = None
    """Standard market size in EUR (StdMktSz)"""

    total_transactions_executed: Optional[float] = None
    """Total number of transactions executed in the period (TtlNbOfTxsExctd)"""

    total_volume_executed: Optional[float] = None
    """Total volume of transactions executed in the period (TtlVolOfTxsExctd)"""

    statistics: Optional[str] = None
    """Additional statistics blob; present in equity (E) files only (Sttstcs)"""

    @classmethod
    def get_schema(cls) -> dict:
        """Get field schema metadata."""
        return {
            'record_id': {'type': 'str', 'source': 'TechRcrdId', 'description': 'Technical record identifier'},
            'isin': {'type': 'str', 'source': 'Id', 'description': 'Instrument ISIN'},
            'classification': {'type': 'str', 'source': 'FinInstrmClssfctn', 'description': 'CFI classification code'},
            'period_from': {'type': 'date', 'source': 'FrDt', 'description': 'Assessment period start date'},
            'period_to': {'type': 'date', 'source': 'ToDt', 'description': 'Assessment period end date'},
            'liquid_market': {'type': 'bool', 'source': 'Lqdty', 'description': 'Liquid market indicator'},
            'methodology': {'type': 'str', 'source': 'Mthdlgy', 'description': 'Calculation methodology: SINT, YEAR, ESTM, FFWK'},
            'average_daily_turnover': {'type': 'float', 'source': 'AvrgDalyTrnvr', 'description': 'Average daily turnover (EUR)'},
            'large_in_scale_threshold': {'type': 'float', 'source': 'LrgInScale', 'description': 'Large-in-Scale pre-trade threshold (EUR)'},
            'average_daily_transactions': {'type': 'float', 'source': 'AvrgDalyNbOfTxs', 'description': 'Average daily number of transactions'},
            'most_relevant_market': {'type': 'str', 'source': 'Id_2', 'description': 'Most relevant market MIC'},
            'waiver_threshold_transactions': {'type': 'float', 'source': 'AvrgDalyNbOfTxs_3', 'description': 'Waiver threshold: transaction count'},
            'average_transaction_value': {'type': 'float', 'source': 'AvrgTxVal', 'description': 'Average transaction value (EUR)'},
            'standard_market_size': {'type': 'float', 'source': 'StdMktSz', 'description': 'Standard market size (EUR)'},
            'total_transactions_executed': {'type': 'float', 'source': 'TtlNbOfTxsExctd', 'description': 'Total transactions executed'},
            'total_volume_executed': {'type': 'float', 'source': 'TtlVolOfTxsExctd', 'description': 'Total volume executed'},
            'statistics': {'type': 'str', 'source': 'Sttstcs', 'description': 'Additional statistics (equity files only)'},
        }


@dataclass
class NonEquityTransparencyRecord:
    """
    ISIN-level non-equity transparency result (FULNCR / DLTNCR).

    One record per ISIN per assessment period. The non-equity schema is more
    complex than equity: thresholds are expressed as (criterion_name, criterion_value,
    threshold_eur) triples. The exact number of criteria fields varies by asset class
    (R and F have fewer criteria than D/E/S/H/J/O).

    Options (O) and futures (F) files additionally include trade/unit counts (Nb_, Nb__2).

    Column mapping (CSV name → field name):
        TechRcrdId              → record_id
        ISIN                    → isin
        Desc                    → description
        CritNm                  → criterion_name_1
        CritVal                 → criterion_value_1
        CritNm_2                → criterion_name_2       (absent for R, F single-crit)
        CritVal_3               → criterion_value_2      (absent for R, F single-crit)
        FinInstrmClssfctn       → classification
        FrDt                    → period_from
        ToDt                    → period_to
        Lqdty                   → liquid_market
        Amt_EUR                 → threshold_eur_1        (pre-trade SSTI / illiquid)
        Amt_EUR_2               → threshold_eur_2        (post-trade LIS)
        Amt_EUR_4               → threshold_eur_3        (pre-trade LIS, absent for R)
        TtlNbOfTxsExctd         → total_transactions_executed  (absent for S/H)
        TtlVolOfTxsExctd        → total_volume_executed        (absent for S/H)
        Nb_                     → trades_count           (O and F only)
        Nb__2                   → units_count            (O and F only)
    """

    record_id: Optional[str] = None
    """Technical record identifier (TechRcrdId)"""

    isin: Optional[str] = None
    """Instrument ISIN (ISIN)"""

    description: Optional[str] = None
    """Instrument description (Desc)"""

    classification: Optional[str] = None
    """CFI classification code (FinInstrmClssfctn)"""

    period_from: Optional[date] = None
    """Start of the assessment period (FrDt)"""

    period_to: Optional[date] = None
    """End of the assessment period (ToDt)"""

    liquid_market: Optional[bool] = None
    """Whether the instrument is in a liquid market (Lqdty)"""

    # Threshold criteria (first pair)
    criterion_name_1: Optional[str] = None
    """First threshold criterion name (CritNm)"""

    criterion_value_1: Optional[str] = None
    """First threshold criterion value (CritVal)"""

    # Threshold criteria (second pair — absent in R and some F files)
    criterion_name_2: Optional[str] = None
    """Second threshold criterion name (CritNm_2)"""

    criterion_value_2: Optional[str] = None
    """Second threshold criterion value (CritVal_3)"""

    # Threshold amounts in EUR
    threshold_eur_1: Optional[float] = None
    """First threshold amount in EUR — pre-trade SSTI or illiquid threshold (Amt_EUR)"""

    threshold_eur_2: Optional[float] = None
    """Second threshold amount in EUR — post-trade LIS threshold (Amt_EUR_2)"""

    threshold_eur_3: Optional[float] = None
    """Third threshold amount in EUR — pre-trade LIS threshold (Amt_EUR_4); absent for R"""

    # Transaction statistics (absent for S, H)
    total_transactions_executed: Optional[float] = None
    """Total number of transactions executed in the period (TtlNbOfTxsExctd)"""

    total_volume_executed: Optional[float] = None
    """Total volume of transactions executed in the period (TtlVolOfTxsExctd)"""

    # Count fields (O and F asset types only)
    trades_count: Optional[float] = None
    """Number of trades (Nb_); present in O and F files only"""

    units_count: Optional[float] = None
    """Number of units (Nb__2); present in O and F files only"""

    @classmethod
    def get_schema(cls) -> dict:
        """Get field schema metadata."""
        return {
            'record_id': {'type': 'str', 'source': 'TechRcrdId', 'description': 'Technical record identifier'},
            'isin': {'type': 'str', 'source': 'ISIN', 'description': 'Instrument ISIN'},
            'description': {'type': 'str', 'source': 'Desc', 'description': 'Instrument description'},
            'classification': {'type': 'str', 'source': 'FinInstrmClssfctn', 'description': 'CFI classification code'},
            'period_from': {'type': 'date', 'source': 'FrDt', 'description': 'Assessment period start date'},
            'period_to': {'type': 'date', 'source': 'ToDt', 'description': 'Assessment period end date'},
            'liquid_market': {'type': 'bool', 'source': 'Lqdty', 'description': 'Liquid market indicator'},
            'criterion_name_1': {'type': 'str', 'source': 'CritNm', 'description': 'First threshold criterion name'},
            'criterion_value_1': {'type': 'str', 'source': 'CritVal', 'description': 'First threshold criterion value'},
            'criterion_name_2': {'type': 'str', 'source': 'CritNm_2', 'description': 'Second threshold criterion name'},
            'criterion_value_2': {'type': 'str', 'source': 'CritVal_3', 'description': 'Second threshold criterion value'},
            'threshold_eur_1': {'type': 'float', 'source': 'Amt_EUR', 'description': 'Pre-trade SSTI / illiquid threshold (EUR)'},
            'threshold_eur_2': {'type': 'float', 'source': 'Amt_EUR_2', 'description': 'Post-trade LIS threshold (EUR)'},
            'threshold_eur_3': {'type': 'float', 'source': 'Amt_EUR_4', 'description': 'Pre-trade LIS threshold (EUR); absent for R'},
            'total_transactions_executed': {'type': 'float', 'source': 'TtlNbOfTxsExctd', 'description': 'Total transactions executed'},
            'total_volume_executed': {'type': 'float', 'source': 'TtlVolOfTxsExctd', 'description': 'Total volume executed'},
            'trades_count': {'type': 'float', 'source': 'Nb_', 'description': 'Trade count (O and F files only)'},
            'units_count': {'type': 'float', 'source': 'Nb__2', 'description': 'Unit count (O and F files only)'},
        }
