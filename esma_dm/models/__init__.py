"""
ESMA Reference Data Models

Normalized data models for ESMA FIRDS reference data and FITRS transparency data.
"""
from .base import Instrument, TradingVenueAttributes, TechnicalAttributes, RecordType
from .debt import DebtInstrument
from .equity import EquityInstrument
from .derivative import DerivativeInstrument, OptionAttributes, FutureAttributes
from .mapper import InstrumentMapper
from .transparency import EquityTransparencyRecord, NonEquityTransparencyRecord
from .subtypes import (
    EquitySwap,
    Swaption,
    EquityOption,
    MiniFuture,
    StructuredEquity,
    StructuredDebt,
    CommodityFuture,
    FXForward,
    get_output_model,
    parse_instrument,
)

__all__ = [
    # FIRDS reference data models
    'Instrument',
    'TradingVenueAttributes',
    'TechnicalAttributes',
    'RecordType',
    'DebtInstrument',
    'EquityInstrument',
    'DerivativeInstrument',
    'OptionAttributes',
    'FutureAttributes',
    'InstrumentMapper',
    # FITRS transparency models
    'EquityTransparencyRecord',
    'NonEquityTransparencyRecord',
    # Subtype output models
    'EquitySwap',
    'Swaption',
    'EquityOption',
    'MiniFuture',
    'StructuredEquity',
    'StructuredDebt',
    'CommodityFuture',
    'FXForward',
    'get_output_model',
    'parse_instrument',
]
