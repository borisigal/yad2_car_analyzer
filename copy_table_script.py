#!/usr/bin/env python3
"""
Script to copy table from prod.car_listings to public.car_listings using replace mode
"""
import sys
import os

# Add the src directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.abspath(current_dir)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from src.core.etl.etl import DataEnricher

def main():
    """Copy table from prod.car_listings to public.car_listings"""
    print("🚀 Starting table copy operation")
    print("Source: prod.car_listings")
    print("Target: public.car_listings") 
    print("Mode: replace")
    print("=" * 50)
    
    try:
        # Initialize DataEnricher with Supabase
        enricher = DataEnricher(database_type="supabase")
        
        # Execute table copy
        result = enricher.copy_table(
            source_schema="prod",
            source_table="car_listings",
            target_schema="public",
            target_table="car_listings",
            copy_mode="replace"
        )
        
        if result > 0:
            print("=" * 50)
            print("✅ Table copy completed successfully!")
            print(f"📊 Copied {result} rows")
        else:
            print("⚠️ No rows copied or operation failed")
            
    except Exception as e:
        print(f"❌ Error during table copy: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()