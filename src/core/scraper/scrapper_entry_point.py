#!/usr/bin/env python3
"""
Entry point for Yad2 Car Analyzer
This script resets the database, scrapes fresh data, and populates the database.

Uses VehicleScraperZenRows by default to bypass anti-bot protection.
Original VehicleScraper is kept intact for reference.
"""

import os
import sys
import argparse
import time
from datetime import datetime
from typing import List, Dict

# Add the src directory to the Python path if not already there
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..', '..', '..')
src_path = os.path.abspath(project_root)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from src.core.database.database import CarDatabase
from src.core.scraper.vehicle_scraper import VehicleScraper
from src.core.scraper.vehicle_scraper_zenrows import VehicleScraperZenRows
from src.core.scraper.vehicle_scraper_scrapingbee import VehicleScraperScrapingBee
from src.core.scraper.vehicle_scraper_brightdata import VehicleScraperBrightData
from src.core.etl.etl import DataEnricher



def scrape_cars(manufacturer: str = None, model: str = None, max_listings: int = 10) -> List[Dict]:
    """Scrape cars from Yad2 for specified manufacturer and model
    
    Args:
        manufacturer: Manufacturer key (e.g., 'subaru'). If None, scrapes all manufacturers from config.
        model: Model key (e.g., 'impreza'). If None, scrapes all models for the manufacturer.
        max_listings: Number of listings to scrape per model (default: 10)
    
    Returns:
        List of dictionaries containing all scraped car data
    """
    print(f"🔍 Starting car scraping at {datetime.now().strftime('%H:%M:%S')}...")
    
    # Use ZenRows scraper for bypassing anti-bot protection
    # scraper = VehicleScraperZenRows()
    # print("🚀 Using ZenRows-enhanced scraper to bypass anti-bot protection")

    # Use ScrapingBee scraper for bypassing anti-bot protection
    # scraper = VehicleScraperScrapingBee()
    # print("🐝 Using ScrapingBee scraper to bypass anti-bot protection")

    # Use BrightData scraper for bypassing anti-bot protection
    scraper = VehicleScraperBrightData()
    print("🔍 Using BrightData scraper to bypass anti-bot protection")

    
    # Load manufacturers configuration
    manufacturers = scraper.load_manufacturers()
    
    if 'manufacturers' not in manufacturers:
        print("❌ No manufacturers configuration found")
        return []
    
    all_cars_data = []
    
    # Determine which manufacturers to scrape
    if manufacturer:
        # Scrape specific manufacturer
        if manufacturer not in manufacturers['manufacturers']:
            print(f"❌ Manufacturer '{manufacturer}' not found")
            print(f"Available manufacturers: {list(manufacturers['manufacturers'].keys())}")
            return []
        
        manufacturers_to_scrape = [manufacturer]
    else:
        # Use all manufacturers from config when manufacturer arg is empty
        manufacturers_to_scrape = list(manufacturers['manufacturers'].keys())
        print(f"📋 Using all manufacturers from config: {manufacturers_to_scrape}")
    
    for manufacturer_key in manufacturers_to_scrape:
        manufacturer_config = manufacturers['manufacturers'][manufacturer_key]
        manufacturer_name = manufacturer_config['hebrew']
        print(f"\n📊 Scraping {manufacturer_config['english']} ({manufacturer_name})")
        
        # Determine which models to scrape
        if model:
            # Scrape specific model
            if model not in manufacturer_config.get('models', {}):
                print(f"❌ Model '{model}' not found for manufacturer '{manufacturer_key}'")
                print(f"Available models: {list(manufacturer_config.get('models', {}).keys())}")
                continue
            
            models_to_scrape = {model: manufacturer_config['models'][model]}
        else:
            # Use all models from config when model arg is empty
            models_to_scrape = manufacturer_config.get('models', {})
            print(f"📋 Using all models for {manufacturer_config['english']}: {list(models_to_scrape.keys())}")
        
        for model_key, model_config in models_to_scrape.items():
            print(f"\n🚗 Scraping {model_config['english']} ({model_config['hebrew']})...")
            
            try:
                # Scrape this model
                cars = scraper.scrape_manufacturer(
                    manufacturer_key=manufacturer_key,
                    model_key=model_key,
                    max_listings=max_listings
                )
                
                if cars:
                    print(f"   ✅ Found {len(cars)} {model_config['english']} cars")
                    all_cars_data.extend(cars)
                else:
                    print(f"   ⚠️ No {model_config['english']} cars found")
                    
            except Exception as e:
                print(f"   ❌ Error scraping {model_config['english']}: {e}")
    
    print(f"\n🎉 Scraping completed at {datetime.now().strftime('%H:%M:%S')}!")
    print(f"📊 Total cars found: {len(all_cars_data)}")
    return all_cars_data

def store_cars_in_database(cars_data: List[Dict], database_type: str) -> int:
    """Store scraped cars data in the database
    
    Args:
        cars_data: List of car dictionaries to store
    
    Returns:
        Number of cars successfully stored
    """
    if not cars_data:
        print("⚠️ No cars data to store")
        return 0
    
    print(f"💾 Starting database write at {datetime.now().strftime('%H:%M:%S')}...")
    db = CarDatabase(database_type=database_type)
    
    # Get next run number for raw data tracking
    run_number = db.get_next_run_number()
    print(f"📊 Using run number: {run_number}")
    
    # Group cars by manufacturer
    cars_by_manufacturer = {}
    for car in cars_data:
        manufacturer = car.get('manufacturer', 'Unknown')
        if manufacturer not in cars_by_manufacturer:
            cars_by_manufacturer[manufacturer] = []
        cars_by_manufacturer[manufacturer].append(car)
    
    total_stored = 0
    raw_data_stored = 0
    
    # Store cars for each manufacturer
    for manufacturer_name, cars in cars_by_manufacturer.items():
        try:
            # Store parsed car data
            added_count = db.add_car_listings(manufacturer_name, cars)
            print(f"   💾 Stored {added_count} cars for {manufacturer_name}")
            total_stored += added_count
            
            # Store raw HTML data
            try:
                raw_data_count = db.save_raw_data(manufacturer_name, cars, run_number)
                raw_data_stored += raw_data_count
                print(f"   📄 Stored {raw_data_count} raw HTML pages for {manufacturer_name}")
            except Exception as e:
                print(f"   ⚠️ Error saving raw data for {manufacturer_name}: {e}")
            
        except Exception as e:
            print(f"   ❌ Error storing cars for {manufacturer_name}: {e}")
    
    print(f"✅ Database storage completed at {datetime.now().strftime('%H:%M:%S')}!")
    print(f"📊 Total stored: {total_stored} cars, {raw_data_stored} raw HTML pages")
    return total_stored

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Yad2 Car Analyzer - Scrape and store vehicle data')
    parser.add_argument('--manufacturer', '-m',  default=None,
                       help='Manufacturer key (e.g., subaru). If not specified, scrapes all manufacturers.')
    parser.add_argument('--model', '-md', 
                       help='Model key (e.g., impreza). If not specified, scrapes all models for the manufacturer.')
    parser.add_argument('--listings', '-l', type=int, default=1,
                       help='Number of listings to scrape per model (default: 1)')

    parser.add_argument('--database', '-db', choices=['sqlite', 'supabase'], default='supabase',
                       help='Database type to use (default: sqlite)')
    
    return parser.parse_args()

def main():
    """Main execution function"""
    
    args = parse_args()
    
    overall_start_time = time.time()
    
    print("🚀 Yad2 Car Analyzer - Fresh Scrape")
    print("=" * 50)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Manufacturer: {args.manufacturer or 'All'}")
    print(f"Model: {args.model or 'All'}")
    print(f"Listings per model: {args.listings}")
    print(f"Database: {args.database}")
    print(f"Reset database: Always")
    print("=" * 50)
    
    try:
        # Step 1: Reset database
        db = CarDatabase(database_type=args.database)
        db.reset_database()
        
        # Step 2: Scrape cars
        scraping_start_time = time.time()
        cars_data = scrape_cars(
            manufacturer=args.manufacturer,
            model=args.model,
            max_listings=args.listings
        )
        scraping_end_time = time.time()
        scraping_duration = scraping_end_time - scraping_start_time
        
        # Step 3: Store cars in database
        if cars_data:
            db_start_time = time.time()
            stored_count = store_cars_in_database(cars_data, database_type=args.database)
            db_end_time = time.time()
            db_duration = db_end_time - db_start_time
            
            # Step 4: Enrich data with calculated fields
            enrichment_start_time = time.time()
            print(f"\n🔧 Starting data enrichment at {datetime.now().strftime('%H:%M:%S')}...")
            enricher = DataEnricher(database_type=args.database)
            enriched_count = enricher.enrich_data()
            enrichment_end_time = time.time()
            enrichment_duration = enrichment_end_time - enrichment_start_time
            print(f"✅ Data enrichment completed at {datetime.now().strftime('%H:%M:%S')}!")
            print(f"⏱️ Total enrichment time: {enrichment_duration:.1f} seconds")
            
            overall_end_time = time.time()
            overall_duration = overall_end_time - overall_start_time
            
            print(f"\n✅ All steps completed successfully!")
            print(f"📊 Scraped {len(cars_data)} cars total")
            print(f"📊 Stored {stored_count} cars in database")
            print(f"🔧 Enriched {enriched_count} records with mechanical_age")
            
            # Calculate and display total execution time
            print("\n" + "=" * 60)
            print("⏱️  EXECUTION TIMING SUMMARY")
            print("=" * 60)
            print(f"🔍 Total scraping time: {scraping_duration:.1f}s ({scraping_duration/60:.1f}m)")
            print(f"💾 Total database write time: {db_duration:.1f}s")
            print(f"🔧 Total enrichment time: {enrichment_duration:.1f}s")
            print(f"⏱️ Overall process time: {overall_duration:.1f}s ({overall_duration/60:.1f}m)")
            print("\n" + "=" * 60)
            print("📊 DATA SUMMARY")
            print("=" * 60)
            print(f"📊 Scraped {len(cars_data)} cars total")
            print(f"📊 Stored {stored_count} cars in database")
            print(f"🔧 Enriched {enriched_count} records with mechanical_age")
            print("=" * 60)
            
            print("📊 Check the database for scraped data:")
            if args.database == 'sqlite':
                print("   - cars.db (SQLite database)")
            else:
                print("   - Supabase database")
            print("   - Use explore_db.py to view the data")
        else:
            print("\n❌ No cars were scraped!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Error during execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 