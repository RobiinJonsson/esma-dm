"""
Abstract base class for storage backends.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd


class StorageBackend(ABC):
    """
    Abstract base class for FIRDS data storage backends.
    
    Implementations can use different storage mechanisms (JSON, DuckDB, etc.)
    while maintaining a consistent interface.
    """
    
    def __init__(self, cache_dir: Path):
        """
        Initialize storage backend.
        
        Args:
            cache_dir: Directory for storing data files
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def get_instrument(self, isin: str) -> Optional[Dict]:
        """
        Retrieve instrument data by ISIN.
        
        Args:
            isin: ISIN code to lookup
        
        Returns:
            Dictionary with instrument data, or None if not found
        """
        pass
    
    @abstractmethod
    def get_instrument_history(self, isin: str) -> List[Dict]:
        """
        Retrieve historical states for an instrument.
        
        Args:
            isin: ISIN code to lookup
        
        Returns:
            List of instrument states ordered by date (newest first)
        """
        pass
    
    @abstractmethod
    def index_csv_file(self, csv_path: Path, delete_after: bool = False) -> int:
        """
        Index a CSV file into the storage backend.
        
        Args:
            csv_path: Path to CSV file
            delete_after: Delete CSV after successful indexing
        
        Returns:
            Number of instruments indexed
        """
        pass
    
    @abstractmethod
    def index_all_csv_files(self, cache_dir: Optional[Path] = None, 
                           delete_csv: bool = True) -> Dict:
        """
        Index all CSV files in directory.
        
        Args:
            cache_dir: Directory containing CSV files
            delete_csv: Delete CSVs after indexing
        
        Returns:
            Dictionary with indexing results
        """
        pass
    
    @abstractmethod
    def search_instruments(self, **filters) -> List[Dict]:
        """
        Search instruments by field values.
        
        Args:
            **filters: Field name and value pairs
        
        Returns:
            List of matching instruments
        """
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict:
        """
        Get storage statistics.
        
        Returns:
            Dictionary with statistics (total_instruments, files_processed, etc.)
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close any open connections or resources."""
        pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
