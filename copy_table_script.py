#!/usr/bin/env python3
"""
General-purpose table copy script
Supports copying between SQLite and Supabase databases with configurable parameters
"""
import sys
import os
import sqlite3
import psycopg2
import argparse
from datetime import datetime

# Add the src directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.abspath(current_dir)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from src.core.database.database import CarDatabase
from src.core.config.environment_variables_loader import load_supabase_credentials

def get_table_columns(cursor, table_name, db_type, schema=None):
    """Get column information for a table"""
    if db_type == "sqlite":
        cursor.execute(f'PRAGMA table_info({table_name})')
        columns = cursor.fetchall()
        return [col[1] for col in columns]  # Column names
    elif db_type == "supabase":
        schema_prefix = f"{schema}." if schema else ""
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            {f"AND table_schema = '{schema}'" if schema else ""}
            ORDER BY ordinal_position
        """)
        return [row[0] for row in cursor.fetchall()]

def copy_table(source_db_type, target_db_type, source_schema, target_schema, 
               source_table, target_table, mode="replace", batch_size=50):
    """
    General-purpose table copy function
    
    Args:
        source_db_type: "sqlite" or "supabase"
        target_db_type: "sqlite" or "supabase" 
        source_schema: Source schema (None for SQLite)
        target_schema: Target schema (None for SQLite)
        source_table: Source table name
        target_table: Target table name
        mode: "replace" (truncate+insert) or "insert" (append only)
        batch_size: Number of records per batch
    """
    print(f"🚀 Starting table copy operation")
    print(f"Source: {source_db_type} {source_schema + '.' if source_schema else ''}{source_table}")
    print(f"Target: {target_db_type} {target_schema + '.' if target_schema else ''}{target_table}")
    print(f"Mode: {mode}")
    print("=" * 70)
    
    source_conn = None
    target_conn = None
    
    try:
        # Initialize source connection
        if source_db_type == "sqlite":
            source_db = CarDatabase(db_path="data/cars.db", database_type="sqlite")
            source_conn = source_db.get_connection()
        elif source_db_type == "supabase":
            source_db = CarDatabase(database_type="supabase")
            source_conn = source_db.get_connection()
        else:
            raise ValueError(f"Unsupported source database type: {source_db_type}")
            
        source_cursor = source_conn.cursor()
        
        # Initialize target connection
        if target_db_type == "sqlite":
            target_db = CarDatabase(db_path="data/cars.db", database_type="sqlite")
            target_conn = target_db.get_connection()
        elif target_db_type == "supabase":
            if source_db_type == "supabase" and source_conn:
                # Reuse connection if both are Supabase
                target_conn = source_conn
            else:
                supabase_credentials = load_supabase_credentials()
                target_conn = psycopg2.connect(
                    user=supabase_credentials['supabase_user'],
                    password=supabase_credentials['supabase_password'],
                    host=supabase_credentials['supabase_host'],
                    port=supabase_credentials['supabase_port'],
                    dbname=supabase_credentials['supabase_dbname']
                )
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")
            
        target_cursor = target_conn.cursor()
        
        # Build full table names
        source_full_table = f"{source_schema}.{source_table}" if source_schema else source_table
        target_full_table = f"{target_schema}.{target_table}" if target_schema else target_table
        
        # Step 1: Count source records
        source_cursor.execute(f"SELECT COUNT(*) FROM {source_full_table}")
        total_records = source_cursor.fetchone()[0]
        print(f"📊 Found {total_records} records in source table")
        
        if total_records == 0:
            print("⚠️ No records to copy")
            return 0
        
        # Step 2: Get column information
        source_columns = get_table_columns(source_cursor, source_table, source_db_type, source_schema)
        target_columns = get_table_columns(target_cursor, target_table, target_db_type, target_schema)
        
        # Find common columns
        common_columns = [col for col in source_columns if col in target_columns]
        print(f"📋 Common columns: {', '.join(common_columns)}")
        
        if not common_columns:
            print("❌ No common columns found between source and target tables")
            return 0
        
        # Step 3: Handle target table based on mode
        if mode == "replace":
            print(f"🗑️ Truncating target table {target_full_table}...")
            target_cursor.execute(f"TRUNCATE TABLE {target_full_table}" if target_db_type == "supabase" else f"DELETE FROM {target_full_table}")
            target_conn.commit()
            print("✅ Truncation completed")
        
        # Step 4: Read source data
        print(f"📥 Reading data from source...")
        columns_sql = ", ".join(common_columns)
        source_cursor.execute(f"SELECT {columns_sql} FROM {source_full_table}")
        records = source_cursor.fetchall()
        print(f"✅ Retrieved {len(records)} records from source")
        
        # Step 5: Insert into target in batches
        print(f"📤 Inserting data into target table...")
        
        # Build insert statement
        placeholders = ", ".join(["%s" if target_db_type == "supabase" else "?" for _ in common_columns])
        insert_sql = f"INSERT INTO {target_full_table} ({columns_sql}) VALUES ({placeholders})"
        
        total_inserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            
            try:
                if target_db_type == "supabase":
                    target_cursor.executemany(insert_sql, batch)
                else:
                    target_cursor.executemany(insert_sql, batch)
                target_conn.commit()
                total_inserted += len(batch)
                print(f"✅ Inserted batch {i//batch_size + 1}: {total_inserted}/{len(records)} records")
            except Exception as e:
                print(f"❌ Error inserting batch {i//batch_size + 1}: {e}")
                target_conn.rollback()
                continue
        
        # Step 6: Verify copy
        print(f"🔍 Verifying data transfer...")
        target_cursor.execute(f"SELECT COUNT(*) FROM {target_full_table}")
        target_count = target_cursor.fetchone()[0]
        
        print(f"\n" + "=" * 70)
        print(f"📊 COPY RESULTS:")
        print(f"   Source records: {total_records}")
        print(f"   Records copied: {total_inserted}")
        print(f"   Target records: {target_count}")
        print(f"   Success rate: {(target_count/total_records)*100:.1f}%")
        
        if target_count == total_records:
            print("✅ Perfect copy - all records transferred successfully!")
        elif target_count > 0:
            print("⚠️ Partial copy - some records transferred")
        else:
            print("❌ Copy failed - no records in target")
        
        return target_count
        
    except Exception as e:
        print(f"❌ Error during copy operation: {e}")
        import traceback
        traceback.print_exc()
        return 0
    finally:
        # Close connections
        if source_conn and source_db_type != target_db_type:
            source_conn.close()
        if target_conn and (source_db_type != target_db_type or source_conn != target_conn):
            target_conn.close()

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description="General-purpose table copy script")
    parser.add_argument("--source-db-type", choices=["sqlite", "supabase"], default="supabase",
                       help="Source database type")
    parser.add_argument("--target-db-type", choices=["sqlite", "supabase"], default="supabase",
                       help="Target database type")
    parser.add_argument("--source-schema", default="prod", help="Source schema (optional for SQLite)")
    parser.add_argument("--target-schema", default="public", help="Target schema (optional for SQLite)")
    parser.add_argument("--source-table", default="car_listings", help="Source table name")
    parser.add_argument("--target-table", default="car_listings", help="Target table name")
    parser.add_argument("--mode", choices=["replace", "insert"], default="replace",
                       help="Copy mode: replace (truncate+insert) or insert (append)")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for inserts")
    
    args = parser.parse_args()
    
    result = copy_table(
        source_db_type=args.source_db_type,
        target_db_type=args.target_db_type,
        source_schema=args.source_schema,
        target_schema=args.target_schema,
        source_table=args.source_table,
        target_table=args.target_table,
        mode=args.mode,
        batch_size=args.batch_size
    )
    
    if result > 0:
        print(f"\n🎉 Copy operation completed successfully! {result} records transferred.")
        sys.exit(0)
    else:
        print(f"\n❌ Copy operation failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()