"""
FIRDS delta processing and version management (history mode only).
"""

import logging
from datetime import datetime
from typing import Dict, Optional
import pandas as pd

from ..enums import AssetType


class FIRDSDeltaProcessor:
    """Handles delta file processing and version management for history mode."""
    
    def __init__(self, config, data_store, downloader):
        """Initialize delta processor."""
        self.config = config
        self.data_store = data_store
        self.downloader = downloader
        self.logger = logging.getLogger(__name__)
    
    def process_delta_files(
        self,
        asset_type: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        update: bool = False
    ) -> Dict:
        """
        Download and process delta files with version management.
        
        This method is only available in 'history' mode.
        
        This method:
        1. Downloads DLTINS files for the date range
        2. Extracts record types (NEW, MODIFIED, TERMINATED, CANCELLED)
        3. Applies version management per ESMA Section 8.2
        4. Updates instrument_history table
        
        Args:
            asset_type: CFI first character (C, D, E, F, H, I, J, O, R, S)
            date_from: Start date (defaults to instance date_from)
            date_to: End date (defaults to instance date_to)
            update: Force re-download of files (default: False, use cached data)
        
        Returns:
            Dictionary with processing statistics
        
        Raises:
            ValueError: If called in 'current' mode
        
        Example:
            >>> processor = FIRDSDeltaProcessor(config, data_store, downloader)
            >>> # Process recent equity deltas
            >>> stats = processor.process_delta_files(
            ...     asset_type='E',
            ...     date_from='2025-01-01',
            ...     date_to='2025-01-31'
            ... )
            >>> print(f"Processed {stats['records_processed']} delta records")
            >>> print(f"New: {stats['new']}, Modified: {stats['modified']}")
        """
        if self.data_store.mode != 'history':
            raise ValueError(
                "process_delta_files() is only available in 'history' mode. "
                "Initialize client with FIRDSClient(mode='history')."
            )
        
        self.logger.info(f"Processing delta files for asset type {asset_type}")
        
        # Download delta files using the downloader
        df = self.downloader.get_delta_files(
            asset_type=asset_type,
            date_from=date_from,
            date_to=date_to,
            update=update
        )
        
        if df.empty:
            self.logger.warning("No delta records found")
            return {
                'records_processed': 0,
                'new': 0,
                'modified': 0,
                'terminated': 0,
                'cancelled': 0,
                'errors': 0
            }
        
        # Check if _record_type column exists (from XML parsing)
        if '_record_type' not in df.columns:
            self.logger.error("Delta file missing record type information")
            return {
                'records_processed': 0,
                'new': 0,
                'modified': 0,
                'terminated': 0,
                'cancelled': 0,
                'errors': len(df)
            }
        
        # Extract publication date from filename or use current date
        publication_date = datetime.today().strftime("%Y-%m-%d")
        
        # Process each record
        stats = {
            'records_processed': 0,
            'new': 0,
            'modified': 0,
            'terminated': 0,
            'cancelled': 0,
            'errors': 0
        }
        
        for idx, row in df.iterrows():
            try:
                isin = row.get('Id')
                record_type = row.get('_record_type')
                
                if not isin or not record_type:
                    stats['errors'] += 1
                    continue
                
                # Convert row to dict for record_data
                record_data = {
                    'full_name': row.get('FinInstrmGnlAttrbts_FullNm', row.get('FullNm')),
                    'cfi_code': row.get('FinInstrmGnlAttrbts_ClssfctnTp', row.get('ClssfctnTp')),
                    'issuer': row.get('Issr', ''),
                    'trading_venue_id': row.get('TradgVnId', ''),
                    'cancellation_reason': row.get('CancellationReason', '')
                }
                
                # Process with version management
                result = self.data_store.process_delta_record(
                    isin=isin,
                    record_type=record_type,
                    record_data=record_data,
                    publication_date=publication_date,
                    source_file=f"DLTINS_{asset_type}_{publication_date}"
                )
                
                stats['records_processed'] += 1
                if result['status'] == 'inserted':
                    stats['new'] += 1
                elif result['status'] == 'updated':
                    stats['modified'] += 1
                elif result['status'] == 'terminated':
                    stats['terminated'] += 1
                elif result['status'] == 'cancelled':
                    stats['cancelled'] += 1
                else:
                    stats['errors'] += 1
                    
            except Exception as e:
                self.logger.error(f"Error processing record {idx}: {e}")
                stats['errors'] += 1
        
        self.logger.info(f"Delta processing complete: {stats['records_processed']} records processed")
        self.logger.info(f"  NEW: {stats['new']}, MODIFIED: {stats['modified']}, "
                        f"TERMINATED: {stats['terminated']}, CANCELLED: {stats['cancelled']}, "
                        f"ERRORS: {stats['errors']}")
        
        return stats