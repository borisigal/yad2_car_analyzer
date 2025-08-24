#!/usr/bin/env python3
"""
Test Supabase connection and table access for prod schema
Using direct SQL connection with psycopg2
"""
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

def test_supabase_connection():
    """Test Supabase connection and table access for prod schema using direct SQL"""
    try:
        # Fetch variables from environment
        USER = os.getenv("SUPABASE_USER")
        PASSWORD = os.getenv("SUPABASE_PASSWORD")
        HOST = os.getenv("SUPABASE_HOST")
        PORT = os.getenv("SUPABASE_PORT")
        DBNAME = os.getenv("SUPABASE_DBNAME")
        
        print("🔗 Connecting to Supabase using direct SQL...")
        print(f"�� Testing connection to: {HOST}:{PORT}")
        
        # Connect to the database
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )
        print("✅ Connection successful!")
        
        # Create a cursor to execute SQL queries
        cursor = connection.cursor()
        
        # Test basic connection
        cursor.execute("SELECT NOW();")
        result = cursor.fetchone()
        print(f"📅 Current database time: {result[0]}")
        
        # Define the tables we expect in our schema
        expected_tables = [
            'manufacturers',
            'car_listings', 
            'scraping_logs',
            'raw_data'
        ]
        
        print(f"\n�� Testing all tables in prod schema...")
        print(f"   Expected tables: {', '.join(expected_tables)}")
        
        # Loop through each table and test count query
        for table_name in expected_tables:
            print(f"\n   🔍 Testing table: {table_name}")
            
            try:
                # Test access to table in prod schema
                cursor.execute(f"SELECT COUNT(*) FROM prod.{table_name}")
                count = cursor.fetchone()[0]
                
                print(f"   ✅ Can access prod.{table_name} table via direct SQL")
                print(f"   📊 Total records: {count}")
                
                # Show sample data if available
                if count > 0:
                    cursor.execute(f"SELECT * FROM prod.{table_name} LIMIT 1")
                    sample = cursor.fetchone()
                    if sample:
                        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'prod' AND table_name = '{table_name}'")
                        columns = [col[0] for col in cursor.fetchall()]
                        print(f"   📝 Sample record columns: {columns}")
                else:
                    print(f"   📝 Table is empty")
                
            except Exception as e:
                print(f"   ❌ Cannot access prod.{table_name} table: {e}")
        
        # Close the cursor and connection
        cursor.close()
        connection.close()
        print("\n🔒 Connection closed.")
        
        print("\n✅ Connection test completed!")
        
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        print("�� Please ensure your .env file contains:")
        print("   SUPABASE_USER, SUPABASE_PASSWORD, SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DBNAME")

if __name__ == "__main__":
    test_supabase_connection()