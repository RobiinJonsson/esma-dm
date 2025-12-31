# Development Guide

## Setting Up Development Environment

### 1. Clone and Install

```bash
cd c:\Users\robin\Projects\esma-dm
pip install -e ".[dev]"
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Verify Installation

```bash
python verify_package.py
```

## Project Structure

```
esma_dm/
├── __init__.py          # Package exports
├── config.py            # Configuration management
├── utils.py             # Shared utilities
├── firds.py             # FIRDS client
├── fitrs.py             # FITRS client
├── ssr.py               # SSR client
└── benchmarks.py        # Benchmarks client (stub)
```

## Adding a New Dataset Client

### Step 1: Create Client Module

Create `esma_dm/new_dataset.py`:

```python
"""
New Dataset Client

Description of what this dataset provides.
"""
from datetime import datetime
from typing import Optional, Any
import pandas as pd
import requests

from .utils import Utils
from .config import default_config


class NewDatasetClient:
    """
    Client for accessing ESMA New Dataset.
    
    Example:
        >>> from esma_dm import NewDatasetClient
        >>> client = NewDatasetClient()
        >>> data = client.get_data()
    """
    
    BASE_URL = "https://registers.esma.europa.eu/..."
    
    def __init__(self, config: Optional[Any] = None):
        """Initialize client."""
        self.config = config or default_config
        self.logger = Utils.set_logger("NewDatasetClient")
        self._utils = Utils()
    
    def get_data(self) -> pd.DataFrame:
        """Retrieve data."""
        # Implementation
        pass
```

### Step 2: Update Package Exports

In `esma_dm/__init__.py`:

```python
from .new_dataset import NewDatasetClient

__all__ = [
    # ... existing exports
    "NewDatasetClient",
]
```

### Step 3: Add Configuration Support

In `esma_dm/config.py` `__post_init__`:

```python
(self.downloads_path / "new_dataset").mkdir(exist_ok=True)
```

### Step 4: Create Examples

Create `examples/new_dataset_example.py`:

```python
"""
New Dataset Usage Example
"""
from esma_dm import NewDatasetClient

def main():
    client = NewDatasetClient()
    data = client.get_data()
    print(f"Retrieved {len(data)} records")

if __name__ == "__main__":
    main()
```

### Step 5: Add Tests

Create `tests/test_new_dataset.py`:

```python
"""
Unit tests for New Dataset client
"""
import pytest
from esma_dm import NewDatasetClient

class TestNewDatasetClient:
    def test_initialization(self):
        client = NewDatasetClient()
        assert client is not None
```

### Step 6: Update Documentation

Update `README.md` with new dataset information.

## Coding Standards

### Style Guide

Follow PEP 8 and use these tools:

```bash
# Format code
black esma_dm/

# Lint code
flake8 esma_dm/

# Type checking
mypy esma_dm/
```

### Docstrings

Use Google-style docstrings:

```python
def function_name(param1: str, param2: int) -> bool:
    """
    Brief description.
    
    Longer description if needed.
    
    Args:
        param1: Description of param1
        param2: Description of param2
    
    Returns:
        Description of return value
    
    Raises:
        ValueError: When something is wrong
    
    Example:
        >>> result = function_name("test", 42)
        >>> print(result)
        True
    """
```

### Type Hints

Always use type hints:

```python
from typing import List, Optional, Dict, Any
import pandas as pd

def process_data(
    data: pd.DataFrame,
    columns: List[str],
    config: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """Process data."""
    pass
```

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=esma_dm --cov-report=html

# Run specific test
pytest tests/test_firds.py

# Run with verbose output
pytest -v
```

### Writing Tests

```python
import pytest
from unittest.mock import Mock, patch
import pandas as pd

class TestYourFeature:
    
    def test_something(self):
        """Test description."""
        # Arrange
        expected = True
        
        # Act
        result = your_function()
        
        # Assert
        assert result == expected
    
    @patch('esma_dm.your_module.requests.get')
    def test_with_mock(self, mock_get):
        """Test with mocked HTTP request."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        result = your_function()
        assert result is not None
```

## Debugging

### Enable Debug Logging

```python
from esma_dm import Config, FIRDSClient

config = Config(log_level="DEBUG")
firds = FIRDSClient(config=config)
```

### Use Python Debugger

```python
import pdb; pdb.set_trace()  # Set breakpoint
```

### Check Cache

```python
from esma_dm import default_config

print(f"Cache location: {default_config.downloads_path}")
```

## Common Tasks

### Update Dependencies

```bash
# Update requirements.txt
pip freeze > requirements.txt

# Or use pip-tools
pip-compile requirements.in
```

### Build Documentation

```bash
# Generate API documentation
pdoc --html --output-dir docs esma_dm
```

### Release New Version

1. Update version in `esma_dm/__init__.py`
2. Update CHANGELOG.md
3. Commit changes
4. Tag release: `git tag v0.2.0`
5. Build: `python setup.py sdist bdist_wheel`
6. Upload: `twine upload dist/*`

## Performance Optimization

### Profiling

```python
import cProfile
import pstats

cProfile.run('your_function()', 'output.prof')
stats = pstats.Stats('output.prof')
stats.sort_stats('cumulative')
stats.print_stats(10)
```

### Caching Strategy

The package uses file-based caching:

```python
# Force update to bypass cache
data = firds.get_latest_full_files(asset_type='E', update=True)

# Disable caching entirely
config = Config(cache_enabled=False)
firds = FIRDSClient(config=config)
```

## Troubleshooting

### Import Errors

```bash
# Reinstall in development mode
pip install -e .
```

### Test Failures

```bash
# Clear pytest cache
pytest --cache-clear

# Run with verbose output
pytest -vv
```

### Module Not Found

```bash
# Check PYTHONPATH
python -c "import sys; print(sys.path)"

# Add to PYTHONPATH (Windows)
$env:PYTHONPATH = "c:\Users\robin\Projects\esma-dm"
```

## Best Practices

1. **Always add tests** for new features
2. **Update documentation** when adding features
3. **Use type hints** throughout
4. **Follow existing patterns** in the codebase
5. **Keep methods focused** - single responsibility
6. **Handle errors gracefully** with informative messages
7. **Cache expensive operations** appropriately
8. **Log important operations** for debugging
9. **Write examples** for new features
10. **Review migration impact** when changing APIs

## Resources

- **Python Packaging**: https://packaging.python.org/
- **Type Hints**: https://docs.python.org/3/library/typing.html
- **Pytest**: https://docs.pytest.org/
- **Black**: https://black.readthedocs.io/
- **Flake8**: https://flake8.pycqa.org/
