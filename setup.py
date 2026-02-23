from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

setup(
    name="esma-dm",
    version="0.3.0",
    author="Robin Jonsson",
    description="ESMA Data Manager - Modular Python package for ESMA financial data with utilities and validators",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/RobiinJonsson/esma-dm",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'esma_dm': ['data/README.md'],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.5.0",
        "requests>=2.28.0",
        "beautifulsoup4>=4.11.0",
        "lxml>=4.9.0",
        "tqdm>=4.64.0",
        "duckdb>=0.9.0",
        "numpy>=1.21.0",
        "click>=8.0.0",
        "rich>=13.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.10.0",
            "pytest-xdist>=3.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "benchmark": [
            "pytest-benchmark>=4.0.0",
            "memory-profiler>=0.60.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "esma-dm=esma_dm.cli:cli",
        ],
    },
    keywords="esma finance mifid firds fitrs trading securities data duckdb iso-standards validators",
    project_urls={
        "Documentation": "https://github.com/RobiinJonsson/esma-dm#readme",
        "Source": "https://github.com/RobiinJonsson/esma-dm",
        "Tracker": "https://github.com/RobiinJonsson/esma-dm/issues",
    },
)
