"""
FIRDS history storage module.

Provides HistoryStore for the esma_hist DuckDB database using
ESMA Section 8 version management with bulk SQL operations.
"""

from .store import HistoryStore

__all__ = ["HistoryStore"]
