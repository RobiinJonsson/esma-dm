"""
SSR (Short Selling Regulation) Client

This module provides access to ESMA's SSR exempted shares register,
which contains information about shares exempt from short selling restrictions.
"""
from datetime import datetime
from typing import Optional, Any

import pandas as pd
import requests
from tqdm import tqdm

from ..utils import Utils
from esma_dm.config import default_config
from ..utils.constants import SSR_SOLR_URL


class SSRClient:
    """
    Client for accessing ESMA SSR (Short Selling Regulation) exempted shares data.
    
    The SSR register contains information about shares that are exempt from
    short selling restrictions across European countries.
    
    Example:
        >>> from esma_dm import SSRClient
        >>> 
        >>> # Get current SSR exempted shares
        >>> ssr = SSRClient()
        >>> exempted_today = ssr.get_exempted_shares(today_only=True)
        >>> 
        >>> # Get all SSR exempted shares
        >>> all_exempted = ssr.get_exempted_shares(today_only=False)
        >>> 
        >>> # Get exempted shares for specific country
        >>> uk_exempted = ssr.get_exempted_shares_by_country('GB')
    """
    
    BASE_URL = (
        f"{SSR_SOLR_URL}?"
        "q=({{!parent%20which=%27type_s:parent%27}})&wt=json&indent=true&rows=150000"
        "&fq=(shs_countryCode:{country})"
    )
    
    # European countries covered by SSR
    COUNTRIES = [
        "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
        "FR", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT",
        "NL", "PL", "PT", "RO", "SE", "SI", "SK", "NO", "GB",
    ]
    
    def __init__(self, config: Optional[Any] = None):
        """
        Initialize SSR client.
        
        Args:
            config: Optional custom configuration object
        """
        self.config = config or default_config
        self.logger = Utils.set_logger("SSRClient")
    
    def get_exempted_shares(self, today_only: bool = True) -> pd.DataFrame:
        """
        Retrieve SSR exempted shares data for all European countries.
        
        Args:
            today_only: If True, filter to show only currently active exemptions
        
        Returns:
            DataFrame containing exempted shares with columns:
            - shs_countryCode: Country code
            - shs_isin: ISIN of the exempted share
            - shs_modificationBDate: Modification before date
            - shs_exemptionStartDate: Start date of exemption
            - shs_modificationDateStr: Modification date string
        
        Example:
            >>> ssr = SSRClient()
            >>> 
            >>> # Get currently active exemptions
            >>> active = ssr.get_exempted_shares(today_only=True)
            >>> 
            >>> # Get all exemptions (including expired)
            >>> all_exemptions = ssr.get_exempted_shares(today_only=False)
        """
        list_dfs = []
        
        self.logger.info(f"Requesting SSR exempted shares for {len(self.COUNTRIES)} countries")
        
        with tqdm(total=len(self.COUNTRIES), position=0, leave=True) as pbar:
            for country in self.COUNTRIES:
                pbar.set_description(f"Processing {country}")
                pbar.update(1)
                
                try:
                    df = self._get_country_data(country)
                    if not df.empty:
                        list_dfs.append(df)
                except Exception as e:
                    self.logger.warning(f"Failed to fetch data for {country}: {e}")
                    continue
        
        if not list_dfs:
            self.logger.warning("No SSR data retrieved")
            return pd.DataFrame()
        
        delivery_df = pd.concat(list_dfs, ignore_index=True)
        
        if not today_only:
            self.logger.info(f"Retrieved {len(delivery_df)} total exemptions")
            return delivery_df
        
        # Filter for today's date
        self.logger.info("Filtering for today's date")
        today_date = datetime.today().strftime("%Y-%m-%d")
        
        filtered_data = delivery_df.query(
            "shs_modificationBDate > @today_date and shs_exemptionStartDate <= @today_date"
        )
        
        # Handle duplicates
        duplicates = filtered_data[filtered_data.duplicated(subset="shs_isin", keep=False)]
        duplicates = duplicates[duplicates["shs_modificationDateStr"] <= today_date]
        
        non_duplicates = filtered_data[~filtered_data["shs_isin"].isin(duplicates["shs_isin"])]
        
        final_data = pd.concat([duplicates, non_duplicates]).reset_index(drop=True)
        
        self.logger.info(f"Retrieved {len(final_data)} active exemptions for today")
        return final_data
    
    def get_exempted_shares_by_country(self, country_code: str) -> pd.DataFrame:
        """
        Retrieve SSR exempted shares for a specific country.
        
        Args:
            country_code: Two-letter country code (e.g., 'GB', 'DE', 'FR')
        
        Returns:
            DataFrame containing exempted shares for the specified country
        
        Example:
            >>> ssr = SSRClient()
            >>> uk_exemptions = ssr.get_exempted_shares_by_country('GB')
            >>> de_exemptions = ssr.get_exempted_shares_by_country('DE')
        """
        if country_code not in self.COUNTRIES:
            raise ValueError(
                f"Invalid country code '{country_code}'. "
                f"Must be one of: {', '.join(self.COUNTRIES)}"
            )
        
        self.logger.info(f"Requesting SSR data for country: {country_code}")
        return self._get_country_data(country_code)
    
    def _get_country_data(self, country: str) -> pd.DataFrame:
        """Fetch SSR data for a single country."""
        country_query = self.BASE_URL.format(country=country)
        response = requests.get(country_query)
        
        if response.status_code != 200:
            raise Exception(f"Request failed with status {response.status_code}")
        
        json_data = response.json()["response"]["docs"]
        return pd.DataFrame(json_data)
