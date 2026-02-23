"""
Benchmarks API access module.

Note: Benchmarks data is accessed via API, not file downloads.

Usage:
    Use CLI commands for accessing benchmarks data:
    - esma-dm benchmarks administrators [OPTIONS]
    - esma-dm benchmarks search <name> [OPTIONS]
    
    Or query directly:
    >>> import requests
    >>> from esma_dm.utils.constants import BENCHMARKS_ENTITIES_SOLR_URL
    >>> from esma_dm.utils import Utils
    >>> 
    >>> url = f"{BENCHMARKS_ENTITIES_SOLR_URL}?q=type_s:parent&rows=100&wt=xml"
    >>> response = requests.get(url)
    >>> df = Utils().parse_xml_response(response)
"""

__all__ = []
