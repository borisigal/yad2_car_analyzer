#!/usr/bin/env python3
"""
ETL (Extract, Transform, Load) operations for Yad2 Car Analyzer
Handles data enrichment and transformation operations
"""

import sqlite3
from typing import Optional

class DataEnricher:
    """Handles data enrichment operations on the database"""
    
    def __init__(self, db_path: str = "cars.db"):
        self.db_path = db_path
    
    def enrich_data_with_mechanical_age(self):
        """Enrich car_listings table with mechanical_age calculated field"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            print("🔧 Starting data enrichment with mechanical_age...")
                        
            cursor.execute('DROP TABLE IF EXISTS car_listings_enriched')

            # Create new table with mechanical_age column using CREATE TABLE AS SELECT
            cursor.execute('''
                CREATE TABLE car_listings_enriched AS
                SELECT 
                    id,
                    manufacturer_id,
                    model,
                    sub_model,
                    price,
                    year,
                    age,
                    mileage,
                    ROUND(CAST(mileage AS REAL) / 15000.0, 2) AS mechanical_age,
                    ROUND((CAST(mileage AS REAL) / 15000.0) / age, 2) AS mechanical_age_real_age_ratio,
                    fuel_type,
                    transmission,
                    engine_size,
                    color,
                    condition,
                    location,
                    current_ownership_type,
                    previous_ownership_type,
                    current_owner_number,
                    listing_url,
                    listing_title,
                    description,
                    created_at
                FROM car_listings
            ''')
            
            # Rename the original table to _to_delete
            cursor.execute('ALTER TABLE car_listings RENAME TO car_listings_to_delete')

            # Rename the enriched table to the original name
            cursor.execute('ALTER TABLE car_listings_enriched RENAME TO car_listings')
            
            # Drop the original table
            cursor.execute('DROP TABLE car_listings_to_delete')
                   
            # Recreate indexes and constraints
            cursor.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_car_listings_unique 
                ON car_listings (id)
            ''')
            
            updated_count = cursor.rowcount
            conn.commit()
            print(f"✅ Data enrichment completed! Updated {updated_count} records with mechanical_age")
            return updated_count
            
        except Exception as e:
            print(f"❌ Error during data enrichment: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    def enrich_data(self):
        """Run all data enrichment operations"""
        return self.enrich_data_with_mechanical_age() 