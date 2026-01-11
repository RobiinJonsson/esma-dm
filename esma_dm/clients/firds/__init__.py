"""
FIRDS (Financial Instruments Reference Data System) client - modular implementation.

This module provides access to ESMA's FIRDS dataset with a clean, modular architecture.
"""

from .client import FIRDSClient
from .enums import FileType, AssetType, CommodityBaseProduct, OptionType, ExerciseStyle, DeliveryType, BondSeniority
from .models import FIRDSFile
from .downloader import FIRDSDownloader
from .parser import FIRDSParser
from .delta_processor import FIRDSDeltaProcessor

__all__ = [
    'FIRDSClient',
    'FileType', 
    'AssetType',
    'CommodityBaseProduct',
    'OptionType', 
    'ExerciseStyle',
    'DeliveryType',
    'BondSeniority',
    'FIRDSFile',
    'FIRDSDownloader',
    'FIRDSParser', 
    'FIRDSDeltaProcessor'
]