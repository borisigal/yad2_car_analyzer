#!/usr/bin/env python3
"""
Debug script to test Yad2 scraping logic
"""

import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

def debug_scraping():
    url = "https://www.yad2.co.il/vehicles/cars?manufacturer=35&model=10476"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    try:
        print(f"🔍 Fetching URL: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        print(f"✅ Response status: {response.status_code}")
        print(f"📄 Content length: {len(response.content)} characters")
        
        # Look for any links
        all_links = soup.find_all('a', href=True)
        print(f"🔗 Total links found: {len(all_links)}")
        
        # Look for item links specifically
        item_links = []
        for link in all_links:
            href = link.get('href')
            if href and '/item/' in href:
                item_links.append(href)
        
        print(f"📦 Item links found: {len(item_links)}")
        for i, link in enumerate(item_links[:5]):  # Show first 5
            print(f"  {i+1}. {link}")
        
        # Look for vehicle item links
        vehicle_item_links = []
        for link in all_links:
            href = link.get('href')
            if href and '/vehicles/item/' in href:
                vehicle_item_links.append(href)
        
        print(f"🚗 Vehicle item links found: {len(vehicle_item_links)}")
        for i, link in enumerate(vehicle_item_links[:5]):  # Show first 5
            print(f"  {i+1}. {link}")
        
        # Look for any text containing "item"
        item_texts = soup.find_all(string=re.compile(r'/item/'))
        print(f"📝 Text elements containing '/item/': {len(item_texts)}")
        
        # Check if the page is JavaScript-rendered
        scripts = soup.find_all('script')
        print(f"📜 Script tags found: {len(scripts)}")
        
        # Look for JSON data in scripts
        for script in scripts:
            if script.string and 'props' in script.string:
                print("🔍 Found script with 'props' - likely contains data")
                break
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    debug_scraping() 