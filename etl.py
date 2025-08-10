#!/usr/bin/env python3
"""
ETL (Extract, Transform, Load) operations for Yad2 Car Analyzer
Handles data enrichment and transformation operations
"""

import sqlite3
import os
from typing import Optional, Dict, Any
from environment_variables_loader import load_supabase_credentials
from database import CarDatabase

class DataEnricher:
    """Handles data enrichment operations on the database"""
    
    def __init__(self, db_path: str = "cars.db", database_type: str = "sqlite"):
        self.db_path = db_path
        self.database_type = database_type
        
        # Create database instance
        self.db = CarDatabase(db_path=db_path, database_type=database_type)
        
        if database_type == "supabase":
            try:
                from supabase import create_client, Client
                # Load credentials from environment variables
                supabase_url = load_supabase_credentials()['supabase_url']
                supabase_key = load_supabase_credentials()['supabase_key']
                self.supabase: Client = create_client(supabase_url, supabase_key)
            except ImportError:
                print("❌ Supabase library not installed. Please run: pip install supabase")
                raise
            except ValueError as e:
                print(f"❌ Configuration Error: {e}")
                print("💡 Please ensure your .env file contains SUPABASE_URL and SUPABASE_KEY")
                raise
    
    def enrich_data_with_mechanical_age(self):
        """Enrich car_listings table with mechanical_age calculated field"""
        return self._enrich_data_with_mechanical_age_sql()
    
    def _enrich_data_with_mechanical_age_sql(self):
        """Enrich car_listings table with mechanical_age calculated field using SQL-only approach"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            print("🔧 Starting data enrichment with mechanical_age...")
            
            # Define table names and SQL syntax based on database type
            if self.database_type == "supabase":
                table_name = "prod.car_listings"
                temp_table_name = "prod.car_listings_enriched"
                to_delete_table_name = "prod.car_listings_to_delete"
                timestamp_function = "NOW()"
                drop_if_exists = "DROP TABLE IF EXISTS"
                # PostgreSQL syntax for mechanical age calculation
                mechanical_age_sql = "ROUND((COALESCE(mileage, 0)::NUMERIC) / 15000.0, 2)"
                ratio_sql = "ROUND((COALESCE(mileage, 0)::NUMERIC / 15000.0) / GREATEST(COALESCE(age, 1), 1)::NUMERIC, 2)"
            else:
                table_name = "car_listings"
                temp_table_name = "car_listings_enriched"
                to_delete_table_name = "car_listings_to_delete"
                timestamp_function = "CURRENT_TIMESTAMP"
                drop_if_exists = "DROP TABLE IF EXISTS"
                # SQLite syntax for mechanical age calculation
                mechanical_age_sql = "ROUND(CAST(COALESCE(mileage, 0) AS REAL) / 15000.0, 2)"
                ratio_sql = "ROUND((CAST(COALESCE(mileage, 0) AS REAL) / 15000.0) / CAST(COALESCE(age, 1) AS REAL), 2)"
            
            # Drop temp table if exists
            cursor.execute(f'{drop_if_exists} {temp_table_name}')

            # Create new table with mechanical_age column using CREATE TABLE AS SELECT
            create_table_sql = f'''
                CREATE TABLE {temp_table_name} AS
                SELECT 
                    id,
                    manufacturer_id,
                    model,
                    sub_model,
                    price,
                    year,
                    age,
                    mileage,
                    {mechanical_age_sql} AS mechanical_age,
                    {ratio_sql} AS mechanical_age_real_age_ratio,
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
                    {timestamp_function} as insert_time_utc
                FROM {table_name}
            '''
            cursor.execute(create_table_sql)
            
            # Get count before swapping tables
            cursor.execute(f'SELECT COUNT(*) FROM {temp_table_name}')
            updated_count = cursor.fetchone()[0]
            
            # Rename the original table to _to_delete
            if self.database_type == "supabase":
                cursor.execute('ALTER TABLE prod.car_listings RENAME TO car_listings_to_delete')
                cursor.execute('ALTER TABLE prod.car_listings_enriched RENAME TO car_listings')
                cursor.execute('DROP TABLE prod.car_listings_to_delete')
            else:
                cursor.execute(f'ALTER TABLE {table_name} RENAME TO {to_delete_table_name}')
                cursor.execute(f'ALTER TABLE {temp_table_name} RENAME TO {table_name}')
                cursor.execute(f'DROP TABLE {to_delete_table_name}')
                   
            # Recreate indexes and constraints (SQLite only, PostgreSQL handles this differently)
            if self.database_type == "sqlite":
                cursor.execute('''
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_car_listings_unique 
                    ON car_listings (id)
                ''')
            
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

def main():
    enricher = DataEnricher(database_type="sqlite")
    enricher.enrich_data()

if __name__ == "__main__":
    main()