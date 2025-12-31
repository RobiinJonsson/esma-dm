"""
Shared utility functions for ESMA Data Manager
"""
import os
import re
import hashlib
import logging
import functools
import tempfile
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.models import Response
from tqdm import tqdm

from .config import default_config


class Utils:
    """
    Shared utility class providing common functionality for ESMA data operations.
    
    This class includes methods for:
    - File hashing and caching
    - XML parsing and processing
    - HTTP request handling
    - Logging configuration
    - File download and extraction
    """
    
    # XML namespaces used in ESMA files
    NAMESPACES = {
        "BizData": "urn:iso:std:iso:20022:tech:xsd:head.003.001.01",
        "AppHdr": "urn:iso:std:iso:20022:tech:xsd:head.001.001.01",
        "Document": "urn:iso:std:iso:20022:tech:xsd:auth.017.001.02",
    }
    
    @staticmethod
    def _hash(string: str) -> str:
        """Generate MD5 hash from a string."""
        h = hashlib.new("md5")
        h.update(string.encode("utf-8"))
        return h.hexdigest()
    
    @staticmethod
    @functools.lru_cache(maxsize=None)
    def _warning_cached_data(file: str):
        """Warn about previously saved data being used."""
        logger = Utils.set_logger("EsmaDataUtils")
        logger.warning(
            f"Previously saved data used:\n{file}\n"
            "Set update=True to get the most up-to-date data"
        )
    
    @staticmethod
    def extract_file_name_from_url(url: str) -> str:
        """Extract the file name from a URL."""
        file_name_raw = url.split("/")[-1]
        file_name = file_name_raw.split(".")[0]
        return file_name
    
    @staticmethod
    def set_logger(name: str, level: str = None) -> logging.Logger:
        """
        Set up a logger for the specified name.
        
        Args:
            name: Logger name
            level: Log level (DEBUG, INFO, WARNING, ERROR)
        
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
            level = level or default_config.log_level
            logger.setLevel(getattr(logging, level))
        
        return logger
    
    @staticmethod
    def parse_xml_response(request: Response) -> pd.DataFrame:
        """
        Parse an XML response to a DataFrame.
        
        Args:
            request: HTTP response containing XML data
        
        Returns:
            DataFrame containing parsed XML data
        """
        xml = BeautifulSoup(request.text, "xml")
        list_of_dicts = []
        
        for doc in xml.find_all("doc"):
            record_dict = {}
            for element in doc.find_all():
                if element.get("name"):
                    record_dict[element.get("name")] = element.text
            list_of_dicts.append(record_dict)
        
        return pd.DataFrame.from_records(list_of_dicts)
    
    @staticmethod
    def clean_inner_tags(root: ET.Element):
        """Clean XML inner tags by stripping namespaces."""
        pattern_tag = r"\{[^}]*\}(\S+)"
        
        for elem in root.iter():
            match = re.search(pattern_tag, elem.tag)
            if match:
                clean_tag = match.group(1)
                if clean_tag in ["Amt", "Nb"]:
                    elem.tag = f"{clean_tag}_{elem.get('Ccy', '')}"
                else:
                    elem.tag = clean_tag
    
    @staticmethod
    def clean_inner_tags_firds(root: ET.Element):
        """Clean XML inner tags specifically for FIRDS format."""
        pattern_tag = r"\{[^}]*\}(\S+)"
        
        for elem in root.iter():
            match = re.search(pattern_tag, elem.tag)
            if match:
                elem.tag = match.group(1)
    
    @staticmethod
    def process_tags(child: ET.Element) -> dict:
        """Process XML tags and map values into a dictionary for FITRS data."""
        mini_tags = defaultdict(list)
        list_additional_vals = [deque(range(2, 101)) for _ in range(15)]
        mini_tags_list_map = defaultdict(int)
        
        for elem in child.iter():
            if str(elem.text).strip():
                tag = elem.tag
                if tag not in mini_tags:
                    mini_tags[tag].append(elem.text)
                else:
                    idx = mini_tags_list_map[tag]
                    suffix = list_additional_vals[idx].popleft()
                    mini_tags[f"{tag}_{suffix}"].append(elem.text)
                    mini_tags_list_map[tag] = idx + 1
        
        return mini_tags
    
    @staticmethod
    def process_tags_firds(child: ET.Element) -> dict:
        """Process XML tags by building complete paths for FIRDS data."""
        mini_tags = defaultdict(list)
        
        def process_element(elem, current_path=[]):
            """Recursively process elements and build path."""
            # Special case for ISIN ID field
            if elem.tag == "Id" and current_path and current_path[-1] == "FinInstrmGnlAttrbts":
                if elem.text is not None:
                    mini_tags["Id"].append(elem.text.strip())
                return
            
            path = current_path + [elem.tag]
            
            # Store any non-empty value with its full path
            if elem.text is not None and str(elem.text).strip() and str(elem.text).strip().lower() != "nan":
                path_key = "_".join(path)
                mini_tags[path_key].append(elem.text.strip())
            
            # Process children
            for child_elem in elem:
                process_element(child_elem, path)
        
        process_element(child)
        return mini_tags
    
    @staticmethod
    def download_and_parse_file(url: str, data_type: str = "fitrs", update: bool = False) -> pd.DataFrame:
        """
        Download a file from URL, extract, and parse into DataFrame.
        
        Args:
            url: URL to download from
            data_type: Type of data ('firds' or 'fitrs')
            update: Force re-download if True
        
        Returns:
            Parsed DataFrame
        """
        logger = Utils.set_logger("EsmaDataUtils")
        logger.info(f"Downloading from URL: {url}")
        
        file_name = Utils.extract_file_name_from_url(url)
        
        # Check cache first
        if not update and default_config.cache_enabled:
            cache_dir = default_config.downloads_path / data_type
            cache_file = cache_dir / f"{file_name}_data.csv"
            
            if cache_file.exists():
                Utils._warning_cached_data(str(cache_file))
                return pd.read_csv(cache_file)
        
        # Download file
        r = requests.get(url)
        logger.info(f"Download status code: {r.status_code}")
        
        if r.status_code != 200:
            raise Exception(f"Failed to download file: {r.status_code}")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / file_name
            data_dir.mkdir(exist_ok=True)
            
            file_path = data_dir / f"file_{file_name}"
            
            # Save and extract
            with open(file_path, "wb") as f:
                f.write(r.content)
            
            # Extract if ZIP
            if url.endswith('.zip'):
                import zipfile
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(data_dir)
            
            # Find XML file
            xml_files = list(data_dir.glob("*.xml"))
            if not xml_files:
                raise Exception("No XML file found in archive")
            
            xml_file = xml_files[0]
            
            # Parse XML
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # Detect format and parse accordingly
            if root.find(".//Document:RefData", Utils.NAMESPACES) is not None:
                # FIRDS FULINS format (full instrument files)
                logger.info("Detected FIRDS FULINS format")
                df = Utils._parse_firds_xml(root, logger, element_name="RefData")
            elif list(root.iter()):
                # Check for DLTINS format (delta files)
                for elem in root.iter():
                    if 'FinInstrmRptgRefDataDltaRpt' in elem.tag:
                        logger.info("Detected FIRDS DLTINS format")
                        df = Utils._parse_firds_xml(root, logger, element_name="FinInstrmRptgRefDataDltaRpt")
                        break
                else:
                    # FITRS/DVCAP format
                    logger.info("Detected FITRS/DVCAP format")
                    df = Utils._parse_fitrs_xml(root, logger)
            else:
                # FITRS/DVCAP format
                logger.info("Detected FITRS/DVCAP format")
                df = Utils._parse_fitrs_xml(root, logger)
            
            # Cache result
            if default_config.cache_enabled:
                cache_dir = default_config.downloads_path / data_type
                cache_file = cache_dir / f"{file_name}_data.csv"
                df.to_csv(cache_file, index=False)
                logger.info(f"Cached data to: {cache_file}")
            
            return df
    
    @staticmethod
    def _parse_firds_xml(root: ET.Element, logger: logging.Logger, element_name: str = "RefData") -> pd.DataFrame:
        """Parse FIRDS XML format (both FULINS and DLTINS)."""
        Utils.clean_inner_tags_firds(root)
        
        # For DLTINS files, we need to find FinInstrm elements within FinInstrmRptgRefDataDltaRpt
        # For FULINS files, we find multiple RefData elements
        if element_name == "FinInstrmRptgRefDataDltaRpt":
            # DLTINS format: Find the single report element, then get all FinInstrm children
            report_element = root.find(f".//{element_name}")
            if report_element is None:
                raise Exception(f"No {element_name} element found")
            
            # Get all FinInstrm children
            root_list = list(report_element.iter("FinInstrm"))
            if not root_list:
                raise Exception("No FinInstrm elements found in DLTINS file")
        else:
            # FULINS format: Find all RefData elements directly
            root_list = list(root.iter(element_name))
            if not root_list:
                raise Exception(f"No {element_name} elements found")
        
        logger.info(f"Found {len(root_list)} records to process")
        
        list_dicts = []
        for child in tqdm(root_list, desc="Parsing FIRDS file", position=0, leave=True):
            list_dicts.append(Utils.process_tags_firds(child))
        
        df = pd.DataFrame.from_records(list_dicts)
        
        # Clean DataFrame
        df = df.map(lambda x: x[0] if isinstance(x, list) else x)
        df = df.replace(r"^\s*$", pd.NA, regex=True)
        df = df.dropna(axis=1, how="all")
        
        logger.info(f"Final FIRDS DataFrame shape: {df.shape}")
        return df
    
    @staticmethod
    def _parse_fitrs_xml(root: ET.Element, logger: logging.Logger) -> pd.DataFrame:
        """Parse FITRS/DVCAP XML format."""
        Utils.clean_inner_tags(root)
        
        # Try different root element names
        root_list = list(root.iter("NonEqtyTrnsprncyData"))
        if not root_list:
            root_list = list(root.iter("EqtyTrnsprncyData"))
        if not root_list:
            raise Exception("No transparency data elements found")
        
        logger.info(f"Found {len(root_list)} records to process")
        
        list_dicts = []
        for child in tqdm(root_list, desc="Parsing FITRS file", position=0, leave=True):
            list_dicts.append(Utils.process_tags(child))
        
        df = pd.DataFrame.from_records(list_dicts)
        df = df.map(lambda x: x[0] if isinstance(x, list) else x)
        
        logger.info(f"Final FITRS DataFrame shape: {df.shape}")
        return df


def save_df_cache(data_type: str):
    """
    Decorator to cache DataFrame results.
    
    Args:
        data_type: Type of data being cached (e.g., 'firds', 'fitrs')
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract URL or identifier for caching
            url = kwargs.get('url') or (args[1] if len(args) > 1 else None)
            update = kwargs.get('update', False)
            
            if not url:
                return func(*args, **kwargs)
            
            # Generate cache filename
            file_name = Utils.extract_file_name_from_url(url)
            cache_dir = default_config.downloads_path / data_type
            cache_file = cache_dir / f"{file_name}_data.csv"
            
            # Check cache
            if not update and default_config.cache_enabled and cache_file.exists():
                Utils._warning_cached_data(str(cache_file))
                return pd.read_csv(cache_file)
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Cache result
            if default_config.cache_enabled and isinstance(result, pd.DataFrame):
                result.to_csv(cache_file, index=False)
            
            return result
        return wrapper
    return decorator
