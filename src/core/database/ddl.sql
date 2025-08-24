-- Yad2 Car Analyzer Database Schema
-- This file contains all DDL statements for creating the database tables

-- Manufacturers table
CREATE TABLE manufacturers (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Car listings table
CREATE TABLE car_listings (
    id TEXT PRIMARY KEY,
    manufacturer_id INTEGER NOT NULL REFERENCES manufacturers(id),
    model TEXT,
    sub_model TEXT,
    price INTEGER NOT NULL,
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
    mechanical_age DECIMAL(10,2),
    mechanical_age_real_age_ratio DECIMAL(10,2),
    age_in_months INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Scraping logs table
CREATE TABLE scraping_logs (
    id SERIAL PRIMARY KEY,
    manufacturer_name TEXT NOT NULL,
    cars_found INTEGER NOT NULL,
    scraping_duration REAL,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw data table
CREATE TABLE raw_data (
    id SERIAL PRIMARY KEY,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better performance
CREATE INDEX idx_car_listings_manufacturer_id ON car_listings(manufacturer_id);
CREATE INDEX idx_car_listings_year ON car_listings(year);
CREATE INDEX idx_car_listings_price ON car_listings(price);
CREATE INDEX idx_raw_data_manufacturer_name ON raw_data(manufacturer_name);
CREATE INDEX idx_raw_data_url ON raw_data(url); 