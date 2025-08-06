#!/usr/bin/env python3
"""
Reset Database Script
Deletes the existing database and creates a new one with the updated schema
"""

import os
import sqlite3

def reset_database():
    """Reset the database with new schema"""
    
    db_path = "cars.db"
    
    # Remove existing database
    if os.path.exists(db_path):
        print(f"🗑️  Removing existing database: {db_path}")
        os.remove(db_path)
    
    print("🔄 Creating new database with enhanced schema...")
    
    # Create new database with enhanced schema
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create manufacturers table
    cursor.execute('''
        CREATE TABLE manufacturers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create car_listings table with comprehensive data
    cursor.execute('''
        CREATE TABLE car_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            manufacturer_id INTEGER NOT NULL,
            model TEXT,
            sub_model TEXT,
            price INTEGER NOT NULL,
            year INTEGER NOT NULL,
            age INTEGER NOT NULL,
            mileage INTEGER,
            fuel_type TEXT,
            transmission TEXT,
            engine_size TEXT,
            color TEXT,
            condition TEXT,
            location TEXT,
            current_ownership_type TEXT,
            previous_ownership_type TEXT,
            current_owner_number INTEGER,
            listing_url TEXT,
            listing_title TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (manufacturer_id) REFERENCES manufacturers (id),
            UNIQUE(manufacturer_id, price, year, listing_url)
        )
    ''')
    
    # Create scraping_logs table
    cursor.execute('''
        CREATE TABLE scraping_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            manufacturer_name TEXT NOT NULL,
            cars_found INTEGER NOT NULL,
            scraping_duration REAL,
            status TEXT NOT NULL,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create raw_data table for storing unprocessed scraped data
    cursor.execute('''
        CREATE TABLE raw_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            manufacturer_name TEXT NOT NULL,
            url TEXT NOT NULL,
            run_number INTEGER NOT NULL,
            page_number INTEGER,
            data_type TEXT NOT NULL,  -- 'html', 'json', 'text'
            raw_data TEXT NOT NULL,
            element_count INTEGER,
            extraction_method TEXT,  -- 'requests', 'selenium'
            response_status INTEGER,
            response_time REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    
    print("✅ Database reset complete!")
    print("📊 New schema includes:")
    print("   - Enhanced car_listings table with detailed fields")
    print("   - Model, sub_model, mileage, fuel type, transmission")
    print("   - Engine size, color, condition, location")
    print("   - Current ownership type, previous ownership type")
    print("   - Current owner number")
    print("   - Listing URL, title, and description")
    print("   - Raw data table for debugging scraped content")
    print()
    print("🚀 Ready to run the scraper:")
    print("   python3 run_scraper.py")

if __name__ == "__main__":
    reset_database() 