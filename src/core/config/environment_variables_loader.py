#!/usr/bin/env python3
"""
Configuration management for Yad2 Car Analyzer
Handles environment variable loading and Supabase credentials
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def load_supabase_credentials():
    """Load Supabase credentials from environment variables"""
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    supabase_user = os.getenv('SUPABASE_USER')
    supabase_password = os.getenv('SUPABASE_PASSWORD')
    supabase_host = os.getenv('SUPABASE_HOST')
    supabase_port = os.getenv('SUPABASE_PORT')
    supabase_dbname = os.getenv('SUPABASE_DBNAME')
    credentials = {
        'supabase_url': supabase_url,
        'supabase_key': supabase_key,
        'supabase_user': supabase_user,
        'supabase_password': supabase_password,
        'supabase_host': supabase_host,
        'supabase_port': supabase_port,
        'supabase_dbname': supabase_dbname
    }
    
    if not supabase_url:
        raise ValueError("SUPABASE_URL environment variable is not set. Please check your .env file.")
    if not supabase_key:
        raise ValueError("SUPABASE_KEY environment variable is not set. Please check your .env file.")
    if not supabase_user:
        raise ValueError("SUPABASE_USER environment variable is not set. Please check your .env file.")
    if not supabase_password:
        raise ValueError("SUPABASE_PASSWORD environment variable is not set. Please check your .env file.")
    if not supabase_host:
        raise ValueError("SUPABASE_HOST environment variable is not set. Please check your .env file.")
    if not supabase_port:
        raise ValueError("SUPABASE_PORT environment variable is not set. Please check your .env file.")
    if not supabase_dbname:
        raise ValueError("SUPABASE_DBNAME environment variable is not set. Please check your .env file.")
    return credentials

def get_database_config():
    """Get database configuration from environment variables"""
    return {
        'database_type': os.getenv('DATABASE_TYPE', 'sqlite'),
        'db_path': os.getenv('DB_PATH', 'cars.db'),
        'supabase_url': os.getenv('SUPABASE_URL'),
        'supabase_key': os.getenv('SUPABASE_KEY')
    } 