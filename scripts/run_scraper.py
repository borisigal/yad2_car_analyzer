#!/usr/bin/env python3
"""
Utility script to run the scraper from the scripts directory
Demonstrates the new package structure
"""

import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.scraper.scrapper_entry_point import main

if __name__ == "__main__":
    print("🚀 Running Yad2 Car Analyzer from scripts directory...")
    print("📁 Using new package structure: src/core/scraper/")
    main() 