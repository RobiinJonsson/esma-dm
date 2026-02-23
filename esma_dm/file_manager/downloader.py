"""
Shared file download operations for ESMA data sources.

Provides generic HTTP download, caching, and file management functionality
that can be used by FIRDS, FITRS, and other ESMA data sources.
"""

import logging
import zipfile
from pathlib import Path
from typing import Optional, Dict, Any
import requests
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn

from esma_dm.utils.constants import HTTP_TIMEOUT, HTTP_MAX_RETRIES, HTTP_RETRY_DELAY


class FileDownloader:
    """
    Generic file downloader for ESMA data sources.
    
    Handles HTTP operations, progress tracking, and file caching.
    Can be used by FIRDS, FITRS, SSR, and other ESMA data sources.
    """
    
    def __init__(self, cache_dir: Path, logger: Optional[logging.Logger] = None):
        """
        Initialize downloader.
        
        Args:
            cache_dir: Directory for caching downloaded files
            logger: Optional logger instance
        """
        self.cache_dir = cache_dir
        self.logger = logger or logging.getLogger(__name__)
        
        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def download_file(
        self,
        url: str,
        filename: str,
        force: bool = False,
        show_progress: bool = True
    ) -> Optional[Path]:
        """
        Download a file from URL with optional caching.
        
        Args:
            url: URL to download from
            filename: Target filename in cache
            force: Force re-download even if cached
            show_progress: Show download progress bar
        
        Returns:
            Path to downloaded file, or None if download failed
        """
        file_path = self.cache_dir / filename
        
        # Check cache
        if file_path.exists() and not force:
            self.logger.info(f"Using cached file: {filename}")
            return file_path
        
        # Download file
        try:
            self.logger.info(f"Downloading {filename} from {url}")
            
            response = requests.get(url, stream=True, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            if show_progress and total_size > 0:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    DownloadColumn(),
                    TransferSpeedColumn()
                ) as progress:
                    task = progress.add_task(f"Downloading {filename}", total=total_size)
                    
                    with open(file_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                progress.update(task, advance=len(chunk))
            else:
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            self.logger.info(f"Downloaded {filename} ({total_size / 1024 / 1024:.2f} MB)")
            return file_path
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to download {filename}: {e}")
            if file_path.exists():
                file_path.unlink()  # Remove partial download
            return None
    
    def extract_zip(self, zip_path: Path, extract_dir: Optional[Path] = None) -> Optional[Path]:
        """
        Extract a ZIP file.
        
        Args:
            zip_path: Path to ZIP file
            extract_dir: Directory to extract to (defaults to same directory as ZIP)
        
        Returns:
            Path to extracted directory, or None if extraction failed
        """
        if extract_dir is None:
            extract_dir = zip_path.parent / zip_path.stem
        
        try:
            self.logger.info(f"Extracting {zip_path.name}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            self.logger.info(f"Extracted to {extract_dir}")
            return extract_dir
            
        except zipfile.BadZipFile as e:
            self.logger.error(f"Failed to extract {zip_path.name}: {e}")
            return None
    
    def get_cached_files(self, pattern: str = "*") -> list[Path]:
        """
        List cached files matching a pattern.
        
        Args:
            pattern: Glob pattern for filtering (default: all files)
        
        Returns:
            List of matching file paths
        """
        return sorted(self.cache_dir.glob(pattern))
    
    def clear_cache(self, pattern: str = "*", keep_newest: int = 0) -> int:
        """
        Clear cached files matching a pattern.
        
        Args:
            pattern: Glob pattern for filtering (default: all files)
            keep_newest: Number of newest files to keep (default: 0 - delete all)
        
        Returns:
            Number of files deleted
        """
        files = self.get_cached_files(pattern)
        
        if keep_newest > 0:
            # Sort by modification time, newest first
            files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
            files = files[keep_newest:]  # Keep only files after the newest N
        
        count = 0
        for file_path in files:
            try:
                file_path.unlink()
                count += 1
                self.logger.info(f"Deleted {file_path.name}")
            except OSError as e:
                self.logger.warning(f"Failed to delete {file_path.name}: {e}")
        
        return count
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about cached files.
        
        Returns:
            Dictionary with cache statistics (count, total_size, files)
        """
        files = self.get_cached_files()
        total_size = sum(f.stat().st_size for f in files)
        
        return {
            'count': len(files),
            'total_size': total_size,
            'total_size_mb': total_size / (1024 * 1024),
            'files': [
                {
                    'name': f.name,
                    'size': f.stat().st_size,
                    'size_mb': f.stat().st_size / (1024 * 1024),
                    'modified': f.stat().st_mtime
                }
                for f in files
            ]
        }
