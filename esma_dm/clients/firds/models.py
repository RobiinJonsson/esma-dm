"""
FIRDS data models and structures.
"""

from dataclasses import dataclass
from typing import Optional
import pandas as pd


@dataclass
class FIRDSFile:
    """Metadata for a FIRDS file."""
    file_name: str
    file_type: str  # "Full" or "Delta"
    publication_date: str
    download_link: str
    asset_type: Optional[str] = None
    date_extracted: Optional[str] = None
    part_number: Optional[int] = None
    total_parts: Optional[int] = None
    
    @classmethod
    def from_row(cls, row: pd.Series) -> 'FIRDSFile':
        """Create FIRDSFile from DataFrame row."""
        file_name = row.get('file_name', '')
        
        # Extract asset type and date from filename
        # Format: FULINS_E_20240101_1of2.zip or DLTINS_D_20240101_1of1.zip
        asset_type = None
        date_extracted = None
        part_number = None
        total_parts = None
        
        if '_' in file_name:
            parts = file_name.replace('.zip', '').split('_')
            if len(parts) >= 3:
                asset_type = parts[1]
                date_extracted = parts[2]
                if len(parts) >= 4 and 'of' in parts[3]:
                    part_info = parts[3].split('of')
                    part_number = int(part_info[0]) if part_info[0].isdigit() else None
                    total_parts = int(part_info[1]) if len(part_info) > 1 and part_info[1].isdigit() else None
        
        return cls(
            file_name=file_name,
            file_type=row.get('file_type', ''),
            publication_date=row.get('publication_date', ''),
            download_link=row.get('download_link', ''),
            asset_type=asset_type,
            date_extracted=date_extracted,
            part_number=part_number,
            total_parts=total_parts
        )