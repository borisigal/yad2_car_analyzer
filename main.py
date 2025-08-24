#!/usr/bin/env python3
"""
Main entry point for Yad2 Car Analyzer
This script provides easy access to the main functionality from the root directory
"""

import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.core.scraper.scrapper_entry_point import main

if __name__ == "__main__":
    main() 