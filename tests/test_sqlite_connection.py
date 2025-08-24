#!/usr/bin/env python3
"""
SQLite Connection Test Script
Test SQLite database connection and basic functionality
"""

import sqlite3
import os
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from core.database.database import CarDatabase

def test_sqlite_connection():
    """Test SQLite database connection and basic functionality"""
    
    db_path = 'data/cars.db'
    
    if not os.path.exists(db_path):
        print("❌ Database not found! Run the scraper first:")
        print("   python scrapper_entry_point.py")
        return False
    
    print("🔍 SQLite Connection Test")
    print("=" * 50)
    
    try:
        # Test basic connection
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("✅ Successfully connected to SQLite database")
        
        # Test table access
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"📋 Tables found: {len(tables)}")
        for table in tables:
            print(f"   - {table[0]}")
        print()
        
        # Test each table
        test_table_access(cursor, "manufacturers")
        test_table_access(cursor, "car_listings")
        test_table_access(cursor, "scraping_logs")
        test_table_access(cursor, "raw_data")
        
        # Test database size
        cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
        size_bytes = cursor.fetchone()[0]
        size_mb = size_bytes / (1024 * 1024)
        print(f"💾 Database size: {size_mb:.2f} MB")
        
        conn.close()
        print("\n✅ SQLite connection test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ SQLite connection test failed: {e}")
        return False

def test_table_access(cursor, table_name):
    """Test access to a specific table"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"✅ {table_name}: {count} records")
        
        if count > 0:
            # Show sample data structure
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
            sample = cursor.fetchone()
            if sample:
                print(f"   📝 Sample record has {len(sample)} columns")
        
    except Exception as e:
        print(f"❌ {table_name}: Error - {e}")

def test_car_database_class():
    """Test the CarDatabase class functionality"""
    print("\n🔧 Testing CarDatabase class...")
    
    try:
        # Test SQLite database initialization
        db = CarDatabase(database_type="sqlite")
        print("✅ CarDatabase SQLite initialization successful")
        
        # Test getting all manufacturers
        manufacturers = db.get_all_manufacturers()
        print(f"✅ Retrieved {len(manufacturers)} manufacturers")
        
        # Test getting car listings
        if manufacturers:
            sample_manufacturer = manufacturers[0]
            listings = db.get_car_listings(sample_manufacturer)
            print(f"✅ Retrieved {len(listings)} car listings for {sample_manufacturer}")
        
        print("✅ CarDatabase class test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ CarDatabase class test failed: {e}")
        return False

def run_sql_query(query):
    """Run a custom SQL query for testing"""
    db_path = 'cars.db'
    
    if not os.path.exists(db_path):
        print("❌ Database not found!")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Get column names
        columns = [description[0] for description in cursor.description]
        
        print(f"📋 Query: {query}")
        print("=" * 50)
        
        if results:
            # Print headers
            print(" | ".join(columns))
            print("-" * (len(" | ".join(columns))))
            
            # Print results (limit to first 10 for readability)
            for i, row in enumerate(results[:10]):
                print(" | ".join(str(cell) for cell in row))
            
            if len(results) > 10:
                print(f"... and {len(results) - 10} more rows")
        else:
            print("No results found")
            
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Query error: {e}")
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Run custom query
        query = " ".join(sys.argv[1:])
        success = run_sql_query(query)
    else:
        # Run full connection test
        success1 = test_sqlite_connection()
        success2 = test_car_database_class()
        success = success1 and success2
        
        if success:
            print("\n💡 Quick SQL Examples:")
            print("   python test_sqlite_connection.py 'SELECT * FROM manufacturers'")
            print("   python test_sqlite_connection.py 'SELECT * FROM car_listings LIMIT 5'")
            print("   python test_sqlite_connection.py 'SELECT manufacturer_id, COUNT(*) FROM car_listings GROUP BY manufacturer_id'")
    
    sys.exit(0 if success else 1) 