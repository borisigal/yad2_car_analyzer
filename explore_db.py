#!/usr/bin/env python3
"""
Database Explorer Script
Quick way to explore the cars.db database
"""

import sqlite3
import os
from database import db

def explore_database():
    """Explore the database contents"""
    
    if not os.path.exists('cars.db'):
        print("❌ Database not found! Run the scraper first:")
        print("   python3 run_scraper.py")
        return
    
    print("🔍 Database Explorer")
    print("=" * 50)
    
    # Connect to database
    conn = sqlite3.connect('cars.db')
    cursor = conn.cursor()
    
    # Show tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"📋 Tables found: {len(tables)}")
    for table in tables:
        print(f"   - {table[0]}")
    print()
    
    # Show manufacturers
    cursor.execute("SELECT COUNT(*) FROM manufacturers")
    manufacturer_count = cursor.fetchone()[0]
    print(f"🏭 Manufacturers: {manufacturer_count}")
    
    if manufacturer_count > 0:
        cursor.execute("SELECT name FROM manufacturers ORDER BY name LIMIT 10")
        manufacturers = cursor.fetchall()
        print("   Sample manufacturers:")
        for mfr in manufacturers:
            print(f"     - {mfr[0]}")
        if manufacturer_count > 10:
            print(f"     ... and {manufacturer_count - 10} more")
    print()
    
    # Show car listings
    cursor.execute("SELECT COUNT(*) FROM car_listings")
    car_count = cursor.fetchone()[0]
    print(f"🚗 Car listings: {car_count}")
    
    if car_count > 0:
        cursor.execute("""
            SELECT m.name, COUNT(c.id) as car_count, 
                   AVG(c.price) as avg_price, AVG(c.age) as avg_age
            FROM manufacturers m
            LEFT JOIN car_listings c ON m.id = c.manufacturer_id
            GROUP BY m.id, m.name
            ORDER BY car_count DESC
            LIMIT 10
        """)
        results = cursor.fetchall()
        print("   Top manufacturers by car count:")
        for mfr, count, avg_price, avg_age in results:
            if count > 0:
                print(f"     - {mfr}: {count} cars, avg ₪{int(avg_price):,}, avg {avg_age:.1f} years")
    print()
    
    # Show scraping logs
    cursor.execute("SELECT COUNT(*) FROM scraping_logs")
    log_count = cursor.fetchone()[0]
    print(f"📊 Scraping logs: {log_count}")
    
    if log_count > 0:
        cursor.execute("""
            SELECT manufacturer_name, cars_found, status, created_at
            FROM scraping_logs
            ORDER BY created_at DESC
            LIMIT 5
        """)
        logs = cursor.fetchall()
        print("   Recent scraping sessions:")
        for mfr, cars, status, created in logs:
            print(f"     - {mfr}: {cars} cars, {status}, {created}")
    print()
    
    # Show database size
    cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
    size_bytes = cursor.fetchone()[0]
    size_mb = size_bytes / (1024 * 1024)
    print(f"💾 Database size: {size_mb:.2f} MB")
    
    conn.close()

def run_sql_query(query):
    """Run a custom SQL query"""
    if not os.path.exists('cars.db'):
        print("❌ Database not found!")
        return
    
    conn = sqlite3.connect('cars.db')
    cursor = conn.cursor()
    
    try:
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
            
            # Print results
            for row in results:
                print(" | ".join(str(cell) for cell in row))
        else:
            print("No results found")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Run custom query
        query = " ".join(sys.argv[1:])
        run_sql_query(query)
    else:
        # Show database overview
        explore_database()
        
        print("\n💡 Quick SQL Examples:")
        print("   python3 explore_db.py 'SELECT * FROM manufacturers'")
        print("   python3 explore_db.py 'SELECT * FROM car_listings LIMIT 5'")
        print("   python3 explore_db.py 'SELECT manufacturer_id, COUNT(*) FROM car_listings GROUP BY manufacturer_id'") 