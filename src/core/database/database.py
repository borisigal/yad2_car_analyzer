#!/usr/bin/env python3
"""
Database layer for Yad2 Car Analyzer
Uses SQLite for local storage and PostgreSQL for Supabase
"""

import sqlite3
import json
import uuid
import os
import sys
from datetime import datetime
from typing import List, Dict, Optional

# Add the src directory to the Python path if not already there
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..', '..', '..')
src_path = os.path.abspath(project_root)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from src.core.config.environment_variables_loader import load_supabase_credentials

class CarDatabase:
    def __init__(self, db_path: str = "data/cars.db", database_type: str = "sqlite"):
        self.db_path = db_path
        self.database_type = database_type
        
        if database_type == "supabase":
            try:
                import psycopg2
                # Load credentials from environment variables
                credentials = load_supabase_credentials()
                self.supabase_credentials = credentials
                print(f"🔗 Supabase credentials loaded for PostgreSQL connection")
            except ImportError:
                print("❌ psycopg2 library not installed. Please run: pip install psycopg2-binary")
                raise
            except ValueError as e:
                print(f"❌ Configuration Error: {e}")
                print(" Please ensure your .env file contains all required Supabase credentials")
                raise
        else:
            self.init_database()
    
    def get_connection(self):
        """Get a database connection"""
        if self.database_type == "supabase":
            import psycopg2
            return psycopg2.connect(
                user=self.supabase_credentials['supabase_user'],
                password=self.supabase_credentials['supabase_password'],
                host=self.supabase_credentials['supabase_host'],
                port=self.supabase_credentials['supabase_port'],
                dbname=self.supabase_credentials['supabase_dbname']
            )
        return sqlite3.connect(self.db_path)
    
    def init_database(self):
        """Initialize the database with proper schema for both SQLite and Supabase"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Create manufacturers table
        if self.database_type == "supabase":
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS prod.manufacturers (
                    id TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    insert_time_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        else:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS manufacturers (
                    id TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    insert_time_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        
        # Create car_listings table with comprehensive data
        if self.database_type == "supabase":
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS prod.car_listings (
                    id TEXT PRIMARY KEY,
                    manufacturer_id TEXT NOT NULL REFERENCES prod.manufacturers(id),
                    manufacturer TEXT,
                    model TEXT,
                    price INTEGER,
                    year INTEGER NOT NULL,
                    age INTEGER NOT NULL,
                    date_on_road TEXT,
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
                    listing_url TEXT UNIQUE,
                    listing_title TEXT,
                    description TEXT,
                    thumbnail_base64 TEXT,
                    mechanical_age DECIMAL(10,2),
                    mechanical_age_real_age_ratio DECIMAL(10,2),
                    age_in_months INTEGER,
                    insert_time_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        else:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS car_listings (
                    id TEXT PRIMARY KEY,
                    manufacturer_id TEXT NOT NULL,
                    manufacturer TEXT,
                    model TEXT,
                    price INTEGER,
                    year INTEGER NOT NULL,
                    age INTEGER NOT NULL,
                    date_on_road TEXT,
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
                    thumbnail_base64 TEXT,
                    age_in_months INTEGER,
                    insert_time_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (manufacturer_id) REFERENCES manufacturers (id),
                    UNIQUE(manufacturer_id, price, year, listing_url)
                )
            ''')
        

        # Create scraping_logs table for tracking scraping sessions
        if self.database_type == "supabase":
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS prod.scraping_logs (
                    id TEXT PRIMARY KEY,
                    manufacturer_name TEXT NOT NULL,
                    cars_found INTEGER NOT NULL,
                    scraping_duration REAL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    insert_time_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        else:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scraping_logs (
                    id TEXT PRIMARY KEY,
                    manufacturer_name TEXT NOT NULL,
                    cars_found INTEGER NOT NULL,
                    scraping_duration REAL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    insert_time_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        
        # Create raw_data table for storing unprocessed scraped data
        if self.database_type == "supabase":
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS prod.raw_data (
                    id TEXT PRIMARY KEY,
                    manufacturer_name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    run_number INTEGER NOT NULL,
                    page_number INTEGER,
                    data_type TEXT NOT NULL,
                    raw_data TEXT NOT NULL,
                    element_count INTEGER,
                    extraction_method TEXT,
                    response_status INTEGER,
                    response_time REAL,
                    insert_time_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        else:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS raw_data (
                    id TEXT PRIMARY KEY,
                    manufacturer_name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    run_number INTEGER NOT NULL,
                    page_number INTEGER,
                    data_type TEXT NOT NULL,
                    raw_data TEXT NOT NULL,
                    element_count INTEGER,
                    extraction_method TEXT,
                    response_status INTEGER,
                    response_time REAL,
                    insert_time_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        
        # Create indexes for better performance (Supabase only)
        if self.database_type == "supabase":
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_car_listings_manufacturer_id ON prod.car_listings(manufacturer_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_car_listings_year ON prod.car_listings(year)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_car_listings_price ON prod.car_listings(price)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_raw_data_manufacturer_name ON prod.raw_data(manufacturer_name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_raw_data_url ON prod.raw_data(url)')
        
        conn.commit()
        conn.close()
    
    def add_manufacturer(self, name: str):
        """Add a manufacturer to the database"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Generate UUID4 for both SQLite and Supabase
            manufacturer_id = str(uuid.uuid4())
            
            if self.database_type == "supabase":
                cursor.execute('INSERT INTO prod.manufacturers (id, name) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING', (manufacturer_id, name))
            else:
                cursor.execute('INSERT OR IGNORE INTO manufacturers (id, name) VALUES (?, ?)', (manufacturer_id, name))
            
            conn.commit()
            
            # Get the manufacturer ID
            if self.database_type == "supabase":
                cursor.execute('SELECT id FROM prod.manufacturers WHERE name = %s', (name,))
            else:
                cursor.execute('SELECT id FROM manufacturers WHERE name = ?', (name,))
            
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            conn.close()
    
    def get_manufacturer_id(self, name: str):
        """Get manufacturer ID by name"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if self.database_type == "supabase":
                cursor.execute('SELECT id FROM prod.manufacturers WHERE name = %s', (name,))
            else:
                cursor.execute('SELECT id FROM manufacturers WHERE name = ?', (name,))
            
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            conn.close()
    
    def get_all_manufacturers(self) -> List[str]:
        """Get all manufacturer names"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if self.database_type == "supabase":
                cursor.execute('SELECT name FROM prod.manufacturers ORDER BY name')
            else:
                cursor.execute('SELECT name FROM manufacturers ORDER BY name')
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def add_car_listings(self, manufacturer_name: str, cars_data: List[Dict]) -> int:
        """Add car listings to the database"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get or create manufacturer
            manufacturer_id = self.add_manufacturer(manufacturer_name)
            
            if not manufacturer_id:
                print(f"Manufacturer '{manufacturer_name}' not found or added. Cannot add car listings.")
                return 0
            
            # Remove existing listings for this manufacturer
            cursor.execute('DELETE FROM prod.car_listings WHERE manufacturer_id = %s' if self.database_type == "supabase" else 'DELETE FROM car_listings WHERE manufacturer_id = ?', (manufacturer_id,))
            
            # Add new listings
            added_count = 0
            for car in cars_data:
                try:
                    # Generate UUID4 for the id column
                    car_id = str(uuid.uuid4())
                    
                    if self.database_type == "supabase":
                        cursor.execute('''
                            INSERT INTO prod.car_listings 
                            (id, manufacturer_id, manufacturer, model, price, year, age, date_on_road, mileage, 
                             fuel_type, transmission, engine_size, color, condition, 
                             location, current_ownership_type, previous_ownership_type, 
                             current_owner_number, listing_url, listing_title, description, thumbnail_base64, insert_time_utc)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (listing_url) DO NOTHING
                        ''', (
                            car_id,
                            manufacturer_id,
                            car.get('manufacturer', ''),
                            car.get('model', ''),
                            car.get('price'),
                            car['year'],
                            car['age'],
                            car.get('date_on_road'),
                            car.get('mileage'),
                            car.get('fuel_type', ''),
                            car.get('transmission', ''),
                            car.get('engine_size', ''),
                            car.get('color', ''),
                            car.get('condition', ''),
                            car.get('location', ''),
                            car.get('current_ownership_type', ''),
                            car.get('previous_ownership_type', ''),
                            car.get('current_owner_number'),
                            car.get('listing_url', ''),
                            car.get('listing_title', ''),
                            car.get('description', ''),
                            car.get('thumbnail_base64'),
                            datetime.utcnow()
                        ))
                    else:
                        cursor.execute('''
                            INSERT OR IGNORE INTO car_listings 
                            (id, manufacturer_id, manufacturer, model, price, year, age, date_on_road, mileage, 
                             fuel_type, transmission, engine_size, color, condition, 
                             location, current_ownership_type, previous_ownership_type, 
                             current_owner_number, listing_url, listing_title, description, thumbnail_base64, insert_time_utc)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            car_id,
                            manufacturer_id,
                            car.get('manufacturer', ''),
                            car.get('model', ''),
                            car.get('price'),
                            car['year'],
                            car['age'],
                            car.get('date_on_road'),
                            car.get('mileage'),
                            car.get('fuel_type', ''),
                            car.get('transmission', ''),
                            car.get('engine_size', ''),
                            car.get('color', ''),
                            car.get('condition', ''),
                            car.get('location', ''),
                            car.get('current_ownership_type', ''),
                            car.get('previous_ownership_type', ''),
                            car.get('current_owner_number'),
                            car.get('listing_url', ''),
                            car.get('listing_title', ''),
                            car.get('description', ''),
                            car.get('thumbnail_base64'),
                            datetime.utcnow()
                        ))
                    added_count += cursor.rowcount
                except Exception as e:
                    print(f"Error adding car listing: {e}")
                    continue
            
            conn.commit()
            return added_count
        finally:
            conn.close()
    
    def get_car_listings(self, manufacturer_name: str) -> List[Dict]:
        """Get car listings for a manufacturer"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            manufacturer_id = self.get_manufacturer_id(manufacturer_name)
            if not manufacturer_id:
                return []
            
            cursor.execute('''
                SELECT * FROM prod.car_listings 
                WHERE manufacturer_id = %s 
                ORDER BY insert_time_utc DESC
            ''' if self.database_type == "supabase" else '''
                SELECT * FROM car_listings 
                WHERE manufacturer_id = ? 
                ORDER BY insert_time_utc DESC
            ''', (manufacturer_id,))
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def get_car_statistics(self, manufacturer_name: str) -> Dict:
        """Get statistics for a manufacturer"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            manufacturer_id = self.get_manufacturer_id(manufacturer_name)
            if not manufacturer_id:
                return {}
            
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_cars,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(year) as avg_year,
                    AVG(age) as avg_age,
                    AVG(mileage) as avg_mileage
                FROM prod.car_listings 
                WHERE manufacturer_id = %s
            ''' if self.database_type == "supabase" else '''
                SELECT 
                    COUNT(*) as total_cars,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(year) as avg_year,
                    AVG(age) as avg_age,
                    AVG(mileage) as avg_mileage
                FROM car_listings 
                WHERE manufacturer_id = ?
            ''', (manufacturer_id,))
            
            result = cursor.fetchone()
            if result:
                return {
                    'total_cars': result[0],
                    'avg_price': result[1] or 0,
                    'min_price': result[2] or 0,
                    'max_price': result[3] or 0,
                    'avg_year': result[4] or 0,
                    'avg_age': result[5] or 0,
                    'avg_mileage': result[6] or 0
                }
            return {}
        finally:
            conn.close()
    
    def save_raw_data(self, manufacturer_name: str, cars_data: List[Dict], run_number: int) -> int:
        """Save raw scraped data to the database"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            added_count = 0
            for car in cars_data:
                try:
                    # Generate UUID4 for the id column
                    raw_data_id = str(uuid.uuid4())
                    
                    if self.database_type == "supabase":
                        cursor.execute('''
                            INSERT INTO prod.raw_data 
                            (id, manufacturer_name, url, run_number, page_number, data_type, 
                             raw_data, element_count, extraction_method, response_status, 
                             response_time, insert_time_utc)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''', (
                            raw_data_id,
                            manufacturer_name,
                            car.get('listing_url', ''),
                            run_number,
                            car.get('page_number', 1),
                            'html',
                            car.get('raw_html', ''),
                            car.get('element_count', 0),
                            car.get('extraction_method', 'requests'),
                            car.get('response_status', 200),
                            car.get('response_time', 0.0),
                            datetime.utcnow()
                        ))
                    else:
                        cursor.execute('''
                            INSERT INTO raw_data 
                            (id, manufacturer_name, url, run_number, page_number, data_type, 
                             raw_data, element_count, extraction_method, response_status, 
                             response_time, insert_time_utc)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            raw_data_id,
                            manufacturer_name,
                            car.get('listing_url', ''),
                            run_number,
                            car.get('page_number', 1),
                            'html',
                            car.get('raw_html', ''),
                            car.get('element_count', 0),
                            car.get('extraction_method', 'requests'),
                            car.get('response_status', 200),
                            car.get('response_time', 0.0),
                            datetime.utcnow()
                        ))
                    added_count += 1
                except Exception as e:
                    print(f"Error saving raw data: {e}")
                    continue
            
            conn.commit()
            return added_count
        finally:
            conn.close()
    
    def log_scraping_session(self, manufacturer_name: str, cars_found: int, 
                           duration: float, status: str, error_message: str = None):
        """Log a scraping session"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Generate UUID4 for the id column
            log_id = str(uuid.uuid4())
            
            if self.database_type == "supabase":
                cursor.execute('''
                    INSERT INTO prod.scraping_logs 
                    (id, manufacturer_name, cars_found, scraping_duration, status, error_message, insert_time_utc)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (log_id, manufacturer_name, cars_found, duration, status, error_message, datetime.utcnow()))
            else:
                cursor.execute('''
                    INSERT INTO scraping_logs 
                    (id, manufacturer_name, cars_found, scraping_duration, status, error_message, insert_time_utc)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (log_id, manufacturer_name, cars_found, duration, status, error_message, datetime.utcnow()))
            conn.commit()
        finally:
            conn.close()
    
    def get_next_run_number(self) -> int:
        """Get the next run number for scraping sessions"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if self.database_type == "supabase":
                cursor.execute('SELECT MAX(run_number) FROM prod.raw_data')
            else:
                cursor.execute('SELECT MAX(run_number) FROM raw_data')
            result = cursor.fetchone()
            return (result[0] or 0) + 1
        finally:
            conn.close()
    
    def get_scraping_status(self) -> Dict:
        """Get scraping status and statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get recent scraping sessions
            if self.database_type == "supabase":
                cursor.execute('''
                    SELECT * FROM prod.scraping_logs 
                    ORDER BY insert_time_utc DESC 
                    LIMIT 10
                ''')
            else:
                cursor.execute('''
                    SELECT * FROM scraping_logs 
                    ORDER BY insert_time_utc DESC 
                    LIMIT 10
                ''')
            
            columns = [description[0] for description in cursor.description]
            recent_sessions = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Get total cars
            if self.database_type == "supabase":
                cursor.execute('SELECT COUNT(*) FROM prod.car_listings')
            else:
                cursor.execute('SELECT COUNT(*) FROM car_listings')
            total_cars = cursor.fetchone()[0]
            
            return {
                'recent_sessions': recent_sessions,
                'total_cars': total_cars,
                'database_type': self.database_type
            }
        finally:
            conn.close()
    
    def truncate_car_listings(self):
        """Truncate car_listings table"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if self.database_type == "supabase":
                cursor.execute('DELETE FROM prod.car_listings')
            else:
                cursor.execute('DELETE FROM car_listings')
            conn.commit()
            print("✅ Truncated car_listings table")
        finally:
            conn.close()
    
    def reset_database(self):
        """Reset the database by dropping and recreating all tables"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if self.database_type == "supabase":
                # For Supabase, drop and recreate tables to ensure correct schema
                cursor.execute('DROP TABLE IF EXISTS prod.car_listings CASCADE')
                cursor.execute('DROP TABLE IF EXISTS prod.raw_data CASCADE')
                cursor.execute('DROP TABLE IF EXISTS prod.scraping_logs CASCADE')
                cursor.execute('DROP TABLE IF EXISTS prod.manufacturers CASCADE')
            else:
                # For SQLite, drop and recreate tables to ensure correct schema
                cursor.execute('DROP TABLE IF EXISTS car_listings')
                cursor.execute('DROP TABLE IF EXISTS raw_data')
                cursor.execute('DROP TABLE IF EXISTS scraping_logs')
                cursor.execute('DROP TABLE IF EXISTS manufacturers')
            
            conn.commit()
            conn.close()
            
            # Recreate tables with correct schema using unified init_database
            self.init_database()
            print("✅ Reset database")
            
        finally:
            # SQLite connections don't have a 'closed' attribute
            if self.database_type == "supabase":
                if not conn.closed:
                    conn.close()
            else:
                try:
                    conn.close()
                except:
                    pass  # Connection might already be closed