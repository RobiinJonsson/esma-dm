"""
DuckDB storage backend - compatibility import.

The DuckDB storage implementation has been modularized into esma_dm.storage.duckdb.
This file provides backward compatibility for existing imports.
"""

from .duckdb import DuckDBStorage

__all__ = ['DuckDBStorage']
