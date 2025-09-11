#!/usr/bin/env python3
"""
ETL (Extract, Transform, Load) operations for Yad2 Car Analyzer
Handles data enrichment and transformation operations
"""

import sqlite3
import os
import sys
from typing import Optional, Dict, Any

# Add the src directory to the Python path if not already there
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..', '..', '..')
src_path = os.path.abspath(project_root)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from src.core.config.environment_variables_loader import load_supabase_credentials
from src.core.database.database import CarDatabase

class DataEnricher:
    """Handles data enrichment operations on the database"""
    
    def __init__(self, db_path: str = "data/cars.db", database_type: str = "sqlite"):
        self.db_path = db_path
        self.database_type = database_type
        
        # Create database instance
        self.db = CarDatabase(db_path=db_path, database_type=database_type)
    
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
                ratio_sql = "ROUND((COALESCE(mileage, 0)::NUMERIC / 15000.0)*12 / GREATEST(COALESCE(EXTRACT(YEAR FROM AGE(NOW(), TO_DATE(date_on_road, 'MM/YYYY'))) * 12 + EXTRACT(MONTH FROM AGE(NOW(), TO_DATE(date_on_road, 'MM/YYYY'))), 1), 1)::NUMERIC, 2)"
                # PostgreSQL syntax for age_in_months calculation
                age_in_months_sql = "EXTRACT(YEAR FROM AGE(NOW(), TO_DATE(date_on_road, 'MM/YYYY'))) * 12 + EXTRACT(MONTH FROM AGE(NOW(), TO_DATE(date_on_road, 'MM/YYYY')))"
            else:
                table_name = "car_listings"
                temp_table_name = "car_listings_enriched"
                to_delete_table_name = "car_listings_to_delete"
                timestamp_function = "CURRENT_TIMESTAMP"
                drop_if_exists = "DROP TABLE IF EXISTS"
                # SQLite syntax for mechanical age calculation
                mechanical_age_sql = "ROUND(CAST(COALESCE(mileage, 0) AS REAL) / 15000.0, 2)"
                ratio_sql = "ROUND((CAST(COALESCE(mileage, 0) AS REAL) / 15000.0)*12 / CAST(MAX(COALESCE(ROUND((julianday('now') - julianday(substr(date_on_road, 4, 4) || '-' || substr(date_on_road, 1, 2) || '-01')) / 30.44, 0), 1), 1) AS REAL), 2)"
                # SQLite syntax for age_in_months calculation
                age_in_months_sql = "ROUND((julianday('now') - julianday(substr(date_on_road, 4, 4) || '-' || substr(date_on_road, 1, 2) || '-01')) / 30.44, 0)"
            
            # Drop temp table if exists
            cursor.execute(f'{drop_if_exists} {temp_table_name}')

            # Create new table with mechanical_age column using CREATE TABLE AS SELECT
            create_table_sql = f'''
                CREATE TABLE {temp_table_name} AS
                SELECT 
                    id,
                    manufacturer_id,
                    manufacturer,
                    model,
                    price,
                    year,
                    age,
                    date_on_road,
                    {age_in_months_sql} AS age_in_months,
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
                    thumbnail_base64,
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
    
    def copy_table(self, source_schema: str, source_table: str, target_schema: str, target_table: str = None, copy_mode: str = "replace"):
        """Copy table from source to target using SQL only with hot swap
        
        Args:
            source_schema: Source schema name
            source_table: Source table name  
            target_schema: Target schema name
            target_table: Target table name (defaults to source_table if None)
            copy_mode: "replace" or "copy" mode
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            # Default target table name to source table name if not specified
            if target_table is None:
                target_table = source_table
            
            print(f"🔄 Starting table copy: {source_schema}.{source_table} -> {target_schema}.{target_table} ({copy_mode} mode)")
            
            # Define table names based on database type
            if self.database_type == "supabase":
                source_full_name = f'"{source_schema}"."{source_table}"'
                target_full_name = f'"{target_schema}"."{target_table}"'
                temp_table_name = f'"{target_schema}"."{target_table}_temp"'
                to_delete_table_name = f'"{target_schema}"."{target_table}_to_delete"'
                drop_if_exists = "DROP TABLE IF EXISTS"
                # Check if target table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (target_schema, target_table))
            else:
                # For SQLite, ignore schema names (not supported)
                source_full_name = source_table
                target_full_name = target_table  
                temp_table_name = f"{target_table}_temp"
                to_delete_table_name = f"{target_table}_to_delete"
                drop_if_exists = "DROP TABLE IF EXISTS"
                # Check if target table exists
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name = ?
                """, (target_table,))
            
            target_exists = cursor.fetchone() is not None
            
            if copy_mode == "copy" and target_exists:
                print(f"ℹ️ Target table {target_full_name} already exists. Nothing to do.")
                return 0
            elif copy_mode == "replace":
                # Drop temp table if exists
                cursor.execute(f'{drop_if_exists} {temp_table_name}')
                
                # Create temp table as copy of source
                create_sql = f'CREATE TABLE {temp_table_name} AS SELECT * FROM {source_full_name}'
                cursor.execute(create_sql)
                print(f"📋 Created temp table {temp_table_name} from {source_full_name}")
                
                # Get row count before hot swap
                cursor.execute(f'SELECT COUNT(*) FROM {temp_table_name}')
                row_count = cursor.fetchone()[0]
                
                if target_exists:
                    # Hot swap: rename target to _to_delete, temp to target
                    if self.database_type == "supabase":
                        cursor.execute(f'ALTER TABLE {target_full_name} RENAME TO "{target_table}_to_delete"')
                        cursor.execute(f'ALTER TABLE {temp_table_name} RENAME TO "{target_table}"')
                        cursor.execute(f'DROP TABLE {to_delete_table_name}')
                    else:
                        cursor.execute(f'ALTER TABLE {target_full_name} RENAME TO {to_delete_table_name}')
                        cursor.execute(f'ALTER TABLE {temp_table_name} RENAME TO {target_full_name}')
                        cursor.execute(f'DROP TABLE {to_delete_table_name}')
                    print(f"🔄 Hot swapped tables and dropped old version")
                else:
                    # Simply rename temp to target
                    if self.database_type == "supabase":
                        cursor.execute(f'ALTER TABLE {temp_table_name} RENAME TO "{target_table}"')
                    else:
                        cursor.execute(f'ALTER TABLE {temp_table_name} RENAME TO {target_full_name}')
                    print(f"📋 Renamed temp table to {target_full_name}")
                
            elif copy_mode == "copy":
                # Create new table as copy of source
                create_sql = f'CREATE TABLE {target_full_name} AS SELECT * FROM {source_full_name}'
                cursor.execute(create_sql)
                
                # Get row count
                cursor.execute(f'SELECT COUNT(*) FROM {target_full_name}')
                row_count = cursor.fetchone()[0]
                
                print(f"📋 Created new table {target_full_name} from {source_full_name}")
            
            else:
                raise ValueError(f"Invalid copy_mode: {copy_mode}. Must be 'replace' or 'copy'")
            
            conn.commit()
            print(f"✅ Table copy completed! {target_full_name} now has {row_count} rows")
            return row_count
            
        except Exception as e:
            print(f"❌ Error during table copy: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def enrich_data(self):
        """Run all data enrichment operations"""
        return self.enrich_data_with_mechanical_age()

def main():
    enricher = DataEnricher(database_type="supabase")
    enricher.enrich_data()

if __name__ == "__main__":
    main()


#     -- Enable Row Level Security on car_listings table
# ALTER TABLE public.car_listings ENABLE ROW LEVEL SECURITY;

# -- Create policy to allow public read access to car listings
# -- Since this is public car data, we allow anyone to read it
# CREATE POLICY "Allow public read access to car listings" 
# ON public.car_listings 
# FOR SELECT 
# USING (true);

# -- Create policy to allow anonymous read access
# CREATE POLICY "Allow anonymous read access to car listings" 
# ON public.car_listings 
# FOR SELECT 
# TO anon
# USING (true);

# -- Create policy to allow authenticated read access
# CREATE POLICY "Allow authenticated read access to car listings" 
# ON public.car_listings 
# FOR SELECT 
# TO authenticated
# USING (true);

# Fixed! The issue was that Row Level Security (RLS) was disabled on the car_listings table, preventing data access. I've now:

# Enabled RLS on the car_listings table
# Created proper policies to allow public read access to the car data
# Cleaned up the debug code
# Now when you click the green "סע!" button after selecting a manufacturer and model, you should see:

# The interactive scatter plot chart showing price vs age
# The detailed car listings table with pagination
# Market insights in the InsightsPanel
# Try searching for a car now - select a manufacturer (יצרן) and model (דגם), then click the green button!

# Fixed! The issue was that Row Level Security (RLS) was disabled on the car_listings table, preventing data access. I've now:

# Enabled RLS on the car_listings table
# Created proper policies to allow public read access to the car data
# Cleaned up the debug code
# Now when you click the green "סע!" button after selecting a manufacturer and model, you should see:

# The interactive scatter plot chart showing price vs age
# The detailed car listings table with pagination
# Market insights in the InsightsPanel
# Try searching for a car now - select a manufacturer (יצרן) and model (דגם), then click the green button!

# query: "SELECT t.table_schema, t.table_name, 
#        CASE WHEN pt.rowsecurity IS NOT NULL THEN pt.rowsecurity ELSE false END as rls_enabled
# FROM information_schema.tables t
# LEFT JOIN pg_tables pt ON t.table_name = pt.tablename AND t.table_schema = pt.schemaname
# WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
#   AND t.table_type = 'BASE TABLE'
# ORDER BY t.table_schema, t.table_name;"


# query: "SELECT schemaname, tablename, rowsecurity 
# FROM pg_tables 
# WHERE schemaname = 'public' 
# ORDER BY tablename;"


# query: "SELECT * FROM current_setting('pgrst.db_schemas', true) as exposed_schemas;"

# query: SHOW pgrst.db_schemas;


# - Enable RLS on prod schema tables to fix security issue
# -- These tables appear to be source data that should be publicly readable

# -- Enable RLS on prod.car_listings
# ALTER TABLE prod.car_listings ENABLE ROW LEVEL SECURITY;

# -- Enable RLS on prod.manufacturers
# ALTER TABLE prod.manufacturers ENABLE ROW LEVEL SECURITY;

# -- Enable RLS on prod.raw_data
# ALTER TABLE prod.raw_data ENABLE ROW LEVEL SECURITY;

# -- Enable RLS on prod.scraping_logs
# ALTER TABLE prod.scraping_logs ENABLE ROW LEVEL SECURITY;

# -- Create public read policies for car_listings (main data table)
# CREATE POLICY "Allow public read access to prod car listings" 
# ON prod.car_listings 
# FOR SELECT 
# USING (true);

# -- Create public read policies for manufacturers
# CREATE POLICY "Allow public read access to prod manufacturers" 
# ON prod.manufacturers 
# FOR SELECT 
# USING (true);

# -- Create public read policies for raw_data
# CREATE POLICY "Allow public read access to prod raw data" 
# ON prod.raw_data 
# FOR SELECT 
# USING (true);

# -- Create restrictive policy for scraping_logs (should be admin only)
# CREATE POLICY "Deny access to scraping logs" 
# ON prod.scraping_logs 
# FOR SELECT 
# USING (false);

# -- If you need admin access to scraping logs, you can later add:
# -- CREATE POLICY "Allow admin access to scraping logs" 
# -- ON prod.scraping_logs 
# -- FOR SELECT 
# -- USING (auth.jwt() ->> 'role' = 'service_role');
