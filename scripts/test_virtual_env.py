#!/usr/bin/env python
"""
Package installation and testing script for virtual environments.

This script helps test the esma-dm package in a clean virtual environment
to ensure all dependencies and modular components work correctly.
"""

import subprocess
import sys
from pathlib import Path


def create_virtual_environment():
    """Create a clean virtual environment for testing."""
    print("Creating virtual environment for testing...")
    
    venv_path = Path("evenv")
    
    # Remove existing test environment
    if venv_path.exists():
        print(f"Removing existing test environment: {venv_path}")
        subprocess.run([sys.executable, "-m", "venv", "--clear", str(venv_path)], check=True)
    else:
        subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)
    
    return venv_path


def get_python_executable(venv_path):
    """Get the Python executable path for the virtual environment."""
    if sys.platform == "win32":
        return venv_path / "Scripts" / "python.exe"
    else:
        return venv_path / "bin" / "python"


def install_package(python_exe):
    """Install the package in development mode."""
    print("Installing package in development mode...")
    
    # Upgrade pip first
    subprocess.run([str(python_exe), "-m", "pip", "install", "--upgrade", "pip"], check=True)
    
    # Install package in development mode
    subprocess.run([str(python_exe), "-m", "pip", "install", "-e", "."], check=True)
    
    # Install dev dependencies for testing
    subprocess.run([str(python_exe), "-m", "pip", "install", "pytest", "pytest-cov"], check=True)


def test_package_functionality(python_exe):
    """Test basic package functionality."""
    print("Testing package functionality...")
    
    # Test basic imports and functionality
    test_script = '''
import esma_dm as edm
from esma_dm import FIRDSClient
from esma_dm.utils import validate_isin, QueryBuilder

# Test utility functions
print("Testing utilities:")
assert edm.utils.validate_isin("US0378331005") == True
assert edm.utils.validate_lei("549300VALTPVHYSYMH70") == True
assert edm.utils.validate_cfi("ESVUFR") == True
assert edm.utils.validate_mic("XNYS") == True
assert edm.utils.validate_mic("FRAB") == True  # Non-X MIC
print("OK - All validators working")

# Test QueryBuilder
qb = QueryBuilder('current')
query = qb.get_instrument_by_isin('test')
assert 'SELECT' in query
print("OK - QueryBuilder working")

# Test client creation
client = FIRDSClient(mode='current')
assert hasattr(client, 'data_store')
assert hasattr(client.data_store, 'queries')
print("OK - Modular client working")

# Test backwards compatibility
assert hasattr(client, 'get_latest_full_files')
assert hasattr(client, 'index_cached_files') 
print("OK - Backwards compatibility maintained")

print("SUCCESS: All functionality tests passed!")
'''
    
    result = subprocess.run([str(python_exe), "-c", test_script], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        print(result.stdout)
        return True
    else:
        print("Package functionality test failed:")
        print(result.stderr)
        return False


def run_test_suite(python_exe):
    """Run the test suite in the virtual environment."""
    print("Running test suite...")
    
    # Run specific utility tests
    test_files = [
        "tests/test_utils.py",
        "tests/test_query_builder.py", 
        "tests/test_validators.py",
    ]
    
    for test_file in test_files:
        if Path(test_file).exists():
            print(f"Running {test_file}...")
            result = subprocess.run([str(python_exe), "-m", "pytest", test_file, "-v"],
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print(f"OK - {test_file} passed")
            else:
                print(f"FAILED - {test_file} failed:")
                print(result.stdout)
                print(result.stderr)
                return False
    
    return True


def main():
    """Main test procedure."""
    print("ESMA Data Manager - Virtual Environment Testing")
    print("=" * 60)
    
    try:
        # Step 1: Create virtual environment
        venv_path = create_virtual_environment()
        python_exe = get_python_executable(venv_path)
        
        # Step 2: Install package
        install_package(python_exe)
        
        # Step 3: Test functionality
        if not test_package_functionality(python_exe):
            return False
        
        # Step 4: Run test suite
        if not run_test_suite(python_exe):
            return False
        
        print("\n" + "=" * 60)
        print("SUCCESS: Virtual environment testing completed successfully!")
        print(f"\nTest environment: {venv_path}")
        print("To activate test environment:")
        if sys.platform == "win32":
            print(f"  {venv_path}\\Scripts\\activate")
        else:
            print(f"  source {venv_path}/bin/activate")
        
        print("\nTo remove test environment:")
        print(f"  Remove-Item -Recurse {venv_path}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        return False
    except Exception as e:
        print(f"Test failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)