from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

setup(
    name="esma-dm",
    version="0.1.0",
    author="Robin Jonsson",
    description="ESMA Data Manager - Comprehensive Python wrapper for ESMA published data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/esma-dm",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.3.0",
        "requests>=2.25.0",
        "beautifulsoup4>=4.9.0",
        "lxml>=4.6.0",
        "tqdm>=4.60.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.950",
        ],
    },
    keywords="esma finance mifid firds fitrs trading securities data",
    project_urls={
        "Documentation": "https://github.com/yourusername/esma-dm#readme",
        "Source": "https://github.com/yourusername/esma-dm",
        "Tracker": "https://github.com/yourusername/esma-dm/issues",
    },
)
