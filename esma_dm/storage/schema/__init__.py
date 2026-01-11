"""
Schema definitions for ESMA data structures.
"""

from .firds_schema import initialize_schema
from .fitrs_schema import initialize_fitrs_schema

__all__ = ['initialize_schema', 'initialize_fitrs_schema']