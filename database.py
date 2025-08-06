#!/usr/bin/env python3
"""
Database layer for Yad2 Car Analyzer
Uses SQLite for persistent storage
"""

import sqlite3
import json
import uuid
from datetime import datetime
from typing import List, Dict, Optional
import os

class CarDatabase:
    def __init__(self, db_path: str = "cars.db"):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Get a database connection"""
        return sqlite3.connect(self.db_path)
    
    def init_database(self):
        """Initialize the database with proper schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create manufacturers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS manufacturers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create car_listings table with comprehensive data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS car_listings (
                id TEXT PRIMARY KEY,
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
        
        # Create scraping_logs table for tracking scraping sessions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_logs (
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
            CREATE TABLE IF NOT EXISTS raw_data (
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
    
    def add_manufacturer(self, name: str) -> int:
        """Add a manufacturer and return its ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                'INSERT OR IGNORE INTO manufacturers (name) VALUES (?)',
                (name,)
            )
            cursor.execute('SELECT id FROM manufacturers WHERE name = ?', (name,))
            manufacturer_id = cursor.fetchone()[0]
            conn.commit()
            return manufacturer_id
        finally:
            conn.close()
    
    def get_manufacturer_id(self, name: str) -> Optional[int]:
        """Get manufacturer ID by name"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT id FROM manufacturers WHERE name = ?', (name,))
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            conn.close()
    
    def get_all_manufacturers(self) -> List[str]:
        """Get all manufacturer names"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT name FROM manufacturers ORDER BY name')
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def add_car_listings(self, manufacturer_name: str, cars_data: List[Dict]) -> int:
        """Add car listings for a manufacturer"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get or create manufacturer
            manufacturer_id = self.add_manufacturer(manufacturer_name)
            
            # Remove existing listings for this manufacturer
            cursor.execute('DELETE FROM car_listings WHERE manufacturer_id = ?', (manufacturer_id,))
            
            # Add new listings
            added_count = 0
            for car in cars_data:
                try:
                    # Generate UUID4 for the id column
                    car_id = str(uuid.uuid4())
                    
                    cursor.execute('''
                        INSERT OR IGNORE INTO car_listings 
                        (id, manufacturer_id, model, sub_model, price, year, age, mileage, 
                         fuel_type, transmission, engine_size, color, condition, 
                         location, current_ownership_type, previous_ownership_type, 
                         current_owner_number, listing_url, listing_title, description)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        car_id,
                        manufacturer_id,
                        car.get('model', ''),
                        car.get('sub_model', ''),
                        car['price'],
                        car['year'],
                        car['age'],
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
                        car.get('description', '')
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
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            manufacturer_id = self.get_manufacturer_id(manufacturer_name)
            if not manufacturer_id:
                return []
            
            cursor.execute('''
                SELECT id, model, sub_model, price, year, age, mileage, fuel_type, 
                       transmission, engine_size, color, condition, location,
                       current_ownership_type, previous_ownership_type,
                       current_owner_number, listing_url, listing_title, description
                FROM car_listings 
                WHERE manufacturer_id = ?
                ORDER BY year DESC, price DESC
            ''', (manufacturer_id,))
            
            return [
                {
                    'id': row[0],
                    'model': row[1],
                    'sub_model': row[2],
                    'price': row[3],
                    'year': row[4],
                    'age': row[5],
                    'mileage': row[6],
                    'fuel_type': row[7],
                    'transmission': row[8],
                    'engine_size': row[9],
                    'color': row[10],
                    'condition': row[11],
                    'location': row[12],
                    'current_ownership_type': row[13],
                    'previous_ownership_type': row[14],
                    'current_owner_number': row[15],
                    'listing_url': row[16],
                    'listing_title': row[17],
                    'description': row[18]
                }
                for row in cursor.fetchall()
            ]
        finally:
            conn.close()
    
    def get_car_statistics(self, manufacturer_name: str) -> Dict:
        """Get statistics for a manufacturer"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            manufacturer_id = self.get_manufacturer_id(manufacturer_name)
            if not manufacturer_id:
                return {}
            
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_cars,
                    AVG(price) as avg_price,
                    AVG(age) as avg_age,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    MIN(age) as min_age,
                    MAX(age) as max_age
                FROM car_listings 
                WHERE manufacturer_id = ?
            ''', (manufacturer_id,))
            
            row = cursor.fetchone()
            if row and row[0] > 0:
                return {
                    'total_cars': row[0],
                    'avg_price': int(row[1]) if row[1] else 0,
                    'avg_age': round(row[2], 1) if row[2] else 0,
                    'price_range': f"{int(row[3])} - {int(row[4])}" if row[3] and row[4] else "0 - 0",
                    'age_range': f"{int(row[5])} - {int(row[6])}" if row[5] and row[6] else "0 - 0"
                }
            return {}
        finally:
            conn.close()
    
    def save_raw_data(self, manufacturer_name: str, cars_data: List[Dict], run_number: int) -> int:
        """Save raw scraped data for a manufacturer"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Remove existing raw data for this manufacturer and run number
            cursor.execute('DELETE FROM raw_data WHERE manufacturer_name = ? AND run_number = ?', 
                         (manufacturer_name, run_number))
            
            # Prepare and add new raw data entries
            added_count = 0
            for i, car in enumerate(cars_data):
                if 'raw_html' in car:
                    try:
                        cursor.execute('''
                            INSERT OR IGNORE INTO raw_data 
                            (manufacturer_name, url, run_number, page_number, data_type, 
                             raw_data, element_count, extraction_method, response_status, response_time)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            manufacturer_name,
                            car.get('listing_url', ''),
                            run_number,
                            i + 1,
                            'listing_page',
                            car.get('raw_html', ''),
                            len(car),  # Number of extracted fields
                            'beautifulsoup',
                            car.get('response_status', 200),
                            car.get('response_time', 0.0)
                        ))
                        added_count += cursor.rowcount
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
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO scraping_logs 
                (manufacturer_name, cars_found, scraping_duration, status, error_message)
                VALUES (?, ?, ?, ?, ?)
            ''', (manufacturer_name, cars_found, duration, status, error_message))
            conn.commit()
        finally:
            conn.close()
    
    def get_next_run_number(self) -> int:
        """Get the next run number for scraping sessions"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT MAX(run_number) FROM raw_data')
            result = cursor.fetchone()
            return (result[0] or 0) + 1
        finally:
            conn.close()
    
    def get_scraping_status(self) -> Dict:
        """Get overall scraping status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT 
                    COUNT(DISTINCT manufacturer_name) as manufacturers_scraped,
                    SUM(cars_found) as total_cars,
                    MAX(created_at) as last_scraping
                FROM scraping_logs 
                WHERE status = 'success'
            ''')
            
            row = cursor.fetchone()
            return {
                'manufacturers_scraped': row[0] if row[0] else 0,
                'total_cars': row[1] if row[1] else 0,
                'last_scraping': row[2] if row[2] else None
            }
        finally:
            conn.close()
    
    def truncate_car_listings(self):
        """Truncate the car_listings table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM car_listings')
            conn.commit()
            print(f"🗑️ Truncated car_listings table")
        finally:
            conn.close()
    
    def reset_database(self):
        """Reset all database tables by truncating them"""
        print("🗑️ Truncating database tables...")
        
        # Truncate all tables instead of dropping them
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Truncate each table
        tables_to_truncate = ['manufacturers', 'car_listings', 'scraping_logs', 'raw_data']
        for table_name in tables_to_truncate:
            cursor.execute(f"DELETE FROM {table_name}")
            print(f"   Truncated table: {table_name}")
        
        conn.commit()
        conn.close()
        
        # VACUUM must be run outside of a transaction
        try:
            conn = self.get_connection()
            conn.execute('VACUUM;')
            conn.close()
            print("   🧹 Database vacuumed to reclaim space")
        except Exception as e:
            print(f"   ⚠️ VACUUM failed (this is normal): {e}")
        
        print("✅ Database truncation completed!")

# Global database instance
db = CarDatabase() 