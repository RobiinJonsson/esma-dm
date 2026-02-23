"""
Benchmarks API Access - Direct API queries for Benchmarks data.

Note: Benchmarks data is accessed via API endpoints, not file downloads.
Use the CLI commands:
  - 'esma-dm benchmarks administrators' to query administrators
  - 'esma-dm benchmarks search <name>' to search for benchmarks

Or query the BENCHMARKS_ENTITIES_SOLR_URL directly using requests.

Examples:
    >>> import requests
    >>> from esma_dm.utils.constants import BENCHMARKS_ENTITIES_SOLR_URL
    >>> from esma_dm.utils import Utils
    >>> 
    >>> # Query all administrators
    >>> url = f"{BENCHMARKS_ENTITIES_SOLR_URL}?q=type_s:parent&rows=100&wt=xml"
    >>> response = requests.get(url)
    >>> df = Utils().parse_xml_response(response)
    >>> 
    >>> # Search for benchmarks
    >>> url = f"{BENCHMARKS_ENTITIES_SOLR_URL}?q=bm_fullName:*MSCI*&rows=50&wt=xml"
    >>> response = requests.get(url)
    >>> df = Utils().parse_xml_response(response)
"""

# This module is kept for backwards compatibility
# Benchmarks data is accessed via direct API queries (not file downloads)
