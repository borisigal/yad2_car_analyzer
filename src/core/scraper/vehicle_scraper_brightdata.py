#!/usr/bin/env python3
"""
Enhanced Yad2 Vehicle Scraper with BrightData Integration
Handles JavaScript-rendered content and bypasses anti-bot protection using BrightData API
"""

import requests
import json
import re
import time
import random
import base64
import io
import hashlib
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import yaml
import os
import sys

# PIL/Pillow import for image processing
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# Add the src directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..', '..', '..')
src_path = os.path.abspath(project_root)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

class VehicleScraperBrightData:
    def __init__(self):
        # BrightData configuration
        self.brightdata_api_url = "https://api.brightdata.com/request"
        self.brightdata_token = "dfccc7979cef8edc56ab22729ce616c3a5e27ff3c6dcca4d345d0e69af40c229"
        self.brightdata_zone = "yad2_first"
        
        # Base configuration
        self.base_url = "https://www.yad2.co.il"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'he,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        print("🌟 BrightData scraper initialized successfully")

    def is_likely_car_listing_url(self, url: str) -> bool:
        """Check if URL looks like a valid car listing - COPIED FROM ZENROWS"""
        try:
            # Extract item ID from URL
            if '/item/' not in url:
                return False
            
            item_id = url.split('/item/')[1]
            # Remove query parameters if any
            if '?' in item_id:
                item_id = item_id.split('?')[0]
            
            # FILTER FOR WORKING URL PATTERN: Short alphanumeric codes (4-10 chars)
            # Working: 7liq5ya4, 6f8xhc0x, nipalgim, lnlj3vvb, kii3ai7e
            # NOT working: 8648660090940 (long numeric)
            
            # Must be 4-10 characters and contain letters (not purely numeric)
            if not (4 <= len(item_id) <= 10):
                return False
            
            # Must contain at least one letter (not purely numeric)
            if item_id.isdigit():
                return False
            
            # Must be alphanumeric only
            if not item_id.isalnum():
                return False
            
            return True
            
        except Exception:
            return False

    def make_brightdata_request(self, url: str, max_retries: int = 2) -> Optional[requests.Response]:
        """Make request using BrightData API with retry logic"""
        for attempt in range(max_retries):
            try:
                print(f"🌟 Making BrightData request (attempt {attempt + 1}/{max_retries}): {url}")
                
                # BrightData API payload - back to working basic parameters
                payload = {
                    "zone": self.brightdata_zone,
                    "url": url,
                    "format": "raw",
                    "method": "GET"
                }
                
                # BrightData API headers
                headers = {
                    "Authorization": f"Bearer {self.brightdata_token}",
                    "Content-Type": "application/json"
                }
                
                # Ultra-minimal delay between retries
                if attempt > 0:
                    delay = random.uniform(0.1, 0.3)
                    print(f"   ⏳ Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)
                
                # Make BrightData request with longer timeout for JavaScript rendering
                response = requests.post(self.brightdata_api_url, json=payload, headers=headers, timeout=20)
                
                if response.status_code == 200:
                    print(f"   ✅ BrightData request successful: {len(response.content)} chars received")
                    
                    # Quick validation - check if content looks like Yad2 data
                    if b'yad2' in response.content.lower() or b'\u05d9\u05d3\u05d2' in response.content:  # Hebrew "יד2"
                        print(f"   ✅ Content appears to be valid Yad2 data")
                        return response
                    else:
                        print(f"   ⚠️ Content doesn't appear to be Yad2 data, may be blocked")
                        
                    return response
                else:
                    print(f"   ❌ BrightData request failed with status: {response.status_code}")
                    if response.status_code == 422:
                        print(f"   ⚠️ BrightData error (422): Likely blocked URL or invalid parameters")
                    elif response.status_code == 401:
                        print(f"   ⚠️ BrightData error (401): Authentication failed")
                    continue
                    
            except Exception as e:
                print(f"   ❌ BrightData request failed on attempt {attempt + 1}: {e}")
                continue
        
        print(f"   ❌ All BrightData attempts failed for {url}")
        return None

    def extract_listings_and_thumbnails_from_page(self, search_url: str) -> List[Tuple[str, Optional[str]]]:
        """Extract car listing URLs and their thumbnail URLs from search page using BrightData"""
        try:
            print(f"📡 Extracting URLs and thumbnails from search page...")
            
            # Try multiple attempts with increasing wait times for JavaScript rendering
            max_attempts = 3
            wait_times = [5000, 8000, 12000]  # Progressive wait times for React loading
            
            for attempt in range(max_attempts):
                print(f"🔄 Attempt {attempt + 1}/{max_attempts} (wait: {wait_times[attempt]/1000}s for JS loading)")
                
                # Temporarily override wait time for this attempt
                original_payload_method = self.make_brightdata_request
                
                def make_request_with_custom_wait(url):
                    # Use basic working parameters for now - focus on __NEXT_DATA__ parsing
                    payload = {
                        "zone": self.brightdata_zone,
                        "url": url,
                        "format": "raw",
                        "method": "GET"
                    }
                    
                    headers = {
                        "Authorization": f"Bearer {self.brightdata_token}",
                        "Content-Type": "application/json"
                    }
                    
                    try:
                        response = requests.post(self.brightdata_api_url, json=payload, headers=headers, timeout=25)
                        if response.status_code == 200:
                            print(f"   ✅ BrightData request successful: {len(response.content)} chars received")
                            return response
                        else:
                            print(f"   ❌ BrightData request failed with status: {response.status_code}")
                            return None
                    except Exception as e:
                        print(f"   ❌ BrightData request exception: {e}")
                        return None
                
                response = make_request_with_custom_wait(search_url)
                if not response:
                    continue
                
                html_content = response.content.decode('utf-8', errors='ignore')
                
                # Quick check if __NEXT_DATA__ exists before parsing
                if '__NEXT_DATA__' in html_content:
                    print(f"   ✅ Found __NEXT_DATA__ in response - proceeding with extraction")
                    break
                else:
                    print(f"   ❌ No __NEXT_DATA__ found - trying longer wait time")
                    if attempt < max_attempts - 1:
                        continue
                    else:
                        print(f"   💥 All attempts failed to get __NEXT_DATA__ - React not loading properly")
                        return []
            
            # Parse HTML with BeautifulSoup - COPIED FROM ZENROWS
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Enhanced __NEXT_DATA__ detection with multiple strategies
            print(f"🔍 Searching for __NEXT_DATA__ with enhanced detection...")
            
            data = None
            
            # Strategy 1: Direct string search (works with minified content)
            if '__NEXT_DATA__' in html_content:
                print(f"   ✅ Found __NEXT_DATA__ in raw HTML")
                
                # Try multiple regex patterns for different formats
                patterns = [
                    r'__NEXT_DATA__"\s*type="application/json">({.*?})</script>',  # BrightData script tag format
                    r'__NEXT_DATA__\s*=\s*({.*?})\s*(?:</script>|;|\n)',  # Standard format
                    r'__NEXT_DATA__\s*=\s*({.*})',                        # Simple format  
                    r'"__NEXT_DATA__":\s*({.*?}),',                       # JSON property format
                ]
                
                for i, pattern in enumerate(patterns, 1):
                    print(f"   🔍 Trying pattern {i}...")
                    matches = re.findall(pattern, html_content, re.DOTALL)
                    
                    for match in matches:
                        try:
                            # Try to parse each match
                            test_data = json.loads(match)
                            print(f"   ✅ Pattern {i} successful - JSON parsed")
                            data = test_data
                            break
                        except json.JSONDecodeError:
                            continue
                    
                    if data:
                        break
                
                if not data:
                    print(f"   ⚠️ Found __NEXT_DATA__ but couldn't parse JSON - trying alternative extraction")
                    
                    # Alternative: Find the position and extract manually
                    start_pos = html_content.find('__NEXT_DATA__')
                    if start_pos != -1:
                        # Find the opening brace
                        brace_pos = html_content.find('{', start_pos)
                        if brace_pos != -1:
                            # Find matching closing brace (simple bracket counting)
                            brace_count = 0
                            end_pos = brace_pos
                            
                            for i, char in enumerate(html_content[brace_pos:]):
                                if char == '{':
                                    brace_count += 1
                                elif char == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        end_pos = brace_pos + i + 1
                                        break
                            
                            if brace_count == 0:  # Found matching brace
                                json_str = html_content[brace_pos:end_pos]
                                try:
                                    data = json.loads(json_str)
                                    print(f"   ✅ Manual extraction successful")
                                except json.JSONDecodeError as e:
                                    print(f"   ❌ Manual extraction failed: {e}")
            else:
                print(f"   ❌ No __NEXT_DATA__ found in raw HTML content")
            
            if not data:
                print("💥 All __NEXT_DATA__ extraction strategies failed")
                return []
            
            print(f"🎉 Successfully extracted __NEXT_DATA__ - proceeding with car data parsing")
            
            # Extract car listings from JSON structure
            listings_with_thumbnails = []
            
            # Navigate through JSON structure with detailed debugging
            try:
                print(f"🔍 Analyzing JSON structure...")
                print(f"   📋 Root keys: {list(data.keys())}")
                
                # Same path as ZenRows: props > pageProps > apolloState > ROOT_QUERY
                props = data.get('props', {})
                if props:
                    print(f"   ✅ Found props with keys: {list(props.keys())}")
                else:
                    print(f"   ❌ No props found")
                    return []
                
                page_props = props.get('pageProps', {})
                if page_props:
                    print(f"   ✅ Found pageProps with keys: {list(page_props.keys())}")
                else:
                    print(f"   ❌ No pageProps found")
                    return []
                
                # Try both apolloState (ZenRows format) and dehydratedState (BrightData format)
                apollo_state = page_props.get('apolloState', {})
                dehydrated_state = page_props.get('dehydratedState', {})
                
                if apollo_state:
                    print(f"   ✅ Found apolloState with {len(apollo_state)} keys")
                    data_source = apollo_state
                    data_type = "apolloState"
                elif dehydrated_state:
                    print(f"   ✅ Found dehydratedState with {len(dehydrated_state)} keys")
                    # Show structure for debugging
                    dehydrated_keys = list(dehydrated_state.keys())[:5]
                    print(f"   📋 Sample dehydratedState keys: {dehydrated_keys}")
                    data_source = dehydrated_state
                    data_type = "dehydratedState"
                else:
                    print(f"   ❌ No apolloState or dehydratedState found")
                    print(f"   📋 Available pageProps keys: {list(page_props.keys())}")
                    return []
                
                print(f"   🎯 Using {data_type} for car data extraction")
                
                processed_tokens = set()
                
                # Look for car data in the appropriate data source
                if data_type == "apolloState":
                    # Look for ROOT_QUERY entries - ZENROWS LOGIC
                    for key, value in data_source.items():
                        if key.startswith('ROOT_QUERY') and isinstance(value, dict):
                            query_data = value
                            
                            # Process all listing categories - EXACTLY LIKE ZENROWS
                            for category in ['platinum', 'commercial', 'solo', 'private']:
                                if category in query_data and isinstance(query_data[category], list):
                                    
                                    for listing in query_data[category]:
                                        # Extract token and build URL
                                        token = listing.get('token')
                                        if not token or token in processed_tokens:
                                            continue
                                        
                                        processed_tokens.add(token)
                                        listing_url = f"https://www.yad2.co.il/item/{token}"
                                        
                                        # Extract thumbnail with priority: coverImage > first image - ZENROWS LOGIC
                                        thumbnail_url = None
                                        meta_data = listing.get('metaData', {})
                                        
                                        if 'coverImage' in meta_data and meta_data['coverImage']:
                                            thumbnail_url = meta_data['coverImage']
                                        elif 'images' in meta_data and meta_data['images'] and len(meta_data['images']) > 0:
                                            thumbnail_url = meta_data['images'][0]
                                        
                                        # Only add if we have both URL and thumbnail
                                        if thumbnail_url and self.is_likely_car_listing_url(listing_url):
                                            listings_with_thumbnails.append((listing_url, thumbnail_url))
                                            print(f"✅ JSON-matched: {token} → {thumbnail_url[:60] + '...' if thumbnail_url else 'No thumbnail'}")
                
                elif data_type == "dehydratedState":
                    # Process dehydratedState structure (BrightData format)
                    print(f"   🔍 Processing dehydratedState structure...")
                    
                    if 'queries' in data_source:
                        queries = data_source['queries']
                        print(f"   ✅ Found queries with {len(queries)} items")
                        
                        for i, query in enumerate(queries):
                            if isinstance(query, dict) and 'state' in query:
                                query_state = query['state']
                                
                                if 'data' in query_state:
                                    query_data = query_state['data']
                                    
                                    if isinstance(query_data, dict):
                                        print(f"     🔍 Query {i} has data: {list(query_data.keys())[:5]}")
                                        
                                        # Process all listing categories
                                        for category in ['platinum', 'commercial', 'solo', 'private']:
                                            if category in query_data and isinstance(query_data[category], list):
                                                car_count = len(query_data[category])
                                                print(f"       ✅ Found {car_count} cars in {category}")
                                                
                                                # Process each listing for thumbnails
                                                for listing in query_data[category]:
                                                    # Extract token and build URL
                                                    token = listing.get('token')
                                                    if not token or token in processed_tokens:
                                                        continue
                                                    
                                                    processed_tokens.add(token)
                                                    listing_url = f"https://www.yad2.co.il/item/{token}"
                                                    
                                                    # Extract thumbnail with priority: coverImage > first image
                                                    thumbnail_url = None
                                                    meta_data = listing.get('metaData', {})
                                                    
                                                    if 'coverImage' in meta_data and meta_data['coverImage']:
                                                        thumbnail_url = meta_data['coverImage']
                                                    elif 'images' in meta_data and meta_data['images'] and len(meta_data['images']) > 0:
                                                        thumbnail_url = meta_data['images'][0]
                                                    
                                                    # Only add if we have both URL and thumbnail
                                                    if thumbnail_url and self.is_likely_car_listing_url(listing_url):
                                                        listings_with_thumbnails.append((listing_url, thumbnail_url))
                                                        print(f"✅ dehydratedState-matched: {token} → {thumbnail_url[:60] + '...' if thumbnail_url else 'No thumbnail'}")
                                    elif isinstance(query_data, list):
                                        print(f"     ℹ️ Query {i} data is a list with {len(query_data)} items")
                                    else:
                                        print(f"     ℹ️ Query {i} data is {type(query_data)}")
                                else:
                                    print(f"     ❌ No 'data' in query {i}")
                            else:
                                print(f"     ❌ Query {i} doesn't have expected structure")
                    else:
                        print(f"   ❌ No queries found in dehydratedState")
                
            except Exception as e:
                print(f"❌ Error extracting data from JSON: {e}")
                return []
            
            print(f"🎯 JSON extraction found {len(listings_with_thumbnails)} listings with perfect thumbnail matching")
            return listings_with_thumbnails
            
        except Exception as e:
            print(f"❌ Error extracting listings and thumbnails: {e}")
            return []

    def scrape_manufacturer(self, manufacturer_key: str, model_key: str = None, max_listings: int = 10) -> List[Dict]:
        """Scrape cars for a specific manufacturer and optionally model using BrightData"""
        try:
            print(f"🚗 Starting to scrape {manufacturer_key}")
            
            # Load manufacturers configuration
            manufacturers = self.load_manufacturers()
            
            if manufacturer_key not in manufacturers.get('manufacturers', {}):
                print(f"❌ Manufacturer '{manufacturer_key}' not found in configuration")
                return []
            
            manufacturer_config = manufacturers['manufacturers'][manufacturer_key]
            manufacturer_name_hebrew = manufacturer_config['hebrew']
            manufacturer_id = manufacturer_config['manufacturer_id']
            
            print(f"📋 Manufacturer: {manufacturer_config['english']} ({manufacturer_name_hebrew})")
            
            # Get models to scrape
            models_to_scrape = {}
            if model_key:
                if model_key not in manufacturer_config.get('models', {}):
                    print(f"❌ Model '{model_key}' not found for manufacturer '{manufacturer_key}'")
                    return []
                models_to_scrape = {model_key: manufacturer_config['models'][model_key]}
            else:
                models_to_scrape = manufacturer_config.get('models', {})
            
            all_extracted_cars = []
            
            for model_key, model_config in models_to_scrape.items():
                print(f"📋 Model: {model_config['hebrew']} ({model_config['english']})")
                
                model_id = model_config['model_id']
                model_name_hebrew = model_config['hebrew']
                
                # Build search URL
                search_url = f"{self.base_url}/vehicles/cars?manufacturer={manufacturer_id}&model={model_id}"
                
                # Collect listing URLs and thumbnails from search pages
                all_listings_with_thumbnails = self.collect_listings_from_pages(
                    search_url, max_listings, manufacturer_config['english'], model_config['english']
                )
                
                if not all_listings_with_thumbnails:
                    print(f"⚠️ No listings found for {model_config['english']}")
                    continue
                
                # Process each listing to extract detailed car data
                print(f"💾 Processing {len(all_listings_with_thumbnails)} listings for detailed extraction...")
                
                cars_data = []
                used_thumbnails_hashes = set()
                
                for i, (listing_url, thumbnail_url) in enumerate(all_listings_with_thumbnails, 1):
                    print(f"🔍 Processing listing {i}/{len(all_listings_with_thumbnails)}: {listing_url}")
                    
                    car_data = self.extract_car_data_from_listing(
                        listing_url, 
                        manufacturer_name_hebrew, 
                        model_name_hebrew, 
                        thumbnail_url, 
                        used_thumbnails_hashes
                    )
                    
                    if car_data:
                        # Add manufacturer/model info
                        car_data['manufacturer'] = manufacturer_name_hebrew
                        car_data['model'] = model_name_hebrew
                        cars_data.append(car_data)
                        print(f"✅ Extracted data for {manufacturer_name_hebrew}")
                    else:
                        print(f"⚠️ No data extracted from {listing_url}")
                
                all_extracted_cars.extend(cars_data)
            
            print(f"💾 Extracted {len(all_extracted_cars)} cars")
            return all_extracted_cars
            
        except Exception as e:
            print(f"❌ Error in scrape_manufacturer: {e}")
            return []

    def collect_listings_from_pages(self, search_url: str, max_listings: int, manufacturer_name: str, model_name: str) -> List[Tuple[str, Optional[str]]]:
        """Collect listing URLs and thumbnails from multiple pages"""
        print(f"📄 Collecting URLs and thumbnails from pages (target: {max_listings} listings)...")
        
        all_listings_with_thumbnails = []
        max_pages = 10  # Increased limit to allow more pages for higher listing counts
        
        for page in range(1, max_pages + 1):
            if len(all_listings_with_thumbnails) >= max_listings:
                break
            
            # Build page URL with page parameter
            if page == 1:
                page_url = search_url
            else:
                page_url = f"{search_url}&page={page}"
                
            print(f"🔍 Scanning page {page}: {page_url}")
            
            # Extract listings and thumbnails from current page
            page_listings = self.extract_listings_and_thumbnails_from_page(page_url)
            
            if not page_listings:
                print(f"⚠️ No listings found on page {page}, stopping pagination")
                break
                
            # Add new listings (avoid duplicates)
            existing_urls = {url for url, _ in all_listings_with_thumbnails}
            new_listings = [(url, thumb) for url, thumb in page_listings if url not in existing_urls]
            
            all_listings_with_thumbnails.extend(new_listings)
            print(f"📄 Page {page}: Found {len(page_listings)} listings, {len(new_listings)} new, "
                  f"{len([thumb for _, thumb in new_listings if thumb])} with thumbnails, total: {len(all_listings_with_thumbnails)}")
            
            # No delay between pages - maximize speed
            
        # Limit to requested number of listings
        final_listings = all_listings_with_thumbnails[:max_listings]
        print(f"📄 Final collection: {len(final_listings)} listings from {min(page, max_pages)} pages")
        
        # Quick analysis of URL formats
        working_format = len([url for url, _ in final_listings if '/item/' in url])
        other_format = len(final_listings) - working_format
        print(f"📊 Listing Analysis: {working_format} working format, {other_format} other format")
        
        return final_listings

    def extract_car_data_from_listing(self, listing_url: str, manufacturer: str, model: str, thumbnail_url: Optional[str], used_thumbnails_hashes: set) -> Optional[Dict]:
        """Extract detailed car data from individual listing page using BrightData - COPIED FROM ZENROWS"""
        try:
            # Make BrightData request for car details
            response = self.make_brightdata_request(listing_url)
            if not response:
                print(f"❌ BrightData request failed for car data extraction: {listing_url}")
                return None
            
            html_content = response.content.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract car data - ZENROWS LOGIC
            car_data = {
                'manufacturer': manufacturer,
                'listing_url': listing_url,
                'original_url': listing_url,
                'raw_html': response.content,  # Store the raw HTML
                'response_status': response.status_code,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Extract price - look for price elements with specific classes - ZENROWS LOGIC
            price_elem = None
            
            # First, try to find the main price element (usually larger, more prominent)
            main_price_elem = soup.find('span', {'data-testid': 'price'}) or \
                             soup.find(class_=re.compile(r'main.*price|price.*main|price.*large|large.*price')) or \
                             soup.find('h1', class_=re.compile(r'price')) or \
                             soup.find('h2', class_=re.compile(r'price'))
            
            if main_price_elem:
                price_elem = main_price_elem
                print(f"🎯 Found main price element: {main_price_elem.get_text().strip()}")
            
            # If no main price found, look for any price element
            if not price_elem:
                price_elem = soup.find(class_=re.compile(r'price')) or \
                            soup.find(text=re.compile(r'₪\s*\d+'))
                if price_elem:
                    print(f"🔍 Found fallback price element: {price_elem.get_text().strip()}")
            
            # Extract year FIRST to use for price validation - ZENROWS LOGIC
            year_elem = soup.find(text=re.compile(r'\b20\d{2}\b'))
            if year_elem:
                year_match = re.search(r'\b(20\d{2})\b', year_elem)
                if year_match:
                    car_data['year'] = int(year_match.group(1))
                    car_data['age'] = datetime.now().year - car_data['year']
            
            # Extract price with age validation - ZENROWS LOGIC
            if price_elem:
                price_text = price_elem.get_text().strip()
                print(f"🔍 Extracting price from: '{price_text}' (car age: {car_data.get('age', 'Unknown')})")
                # Pass price_text, full HTML, and car age for validation
                car_data['price'] = self.extract_price(price_text, str(soup), car_data.get('age'))
            else:
                # No price element found - try JSON pattern directly
                print("⚠️ No price element found, trying JSON pattern...")
                car_data['price'] = self.extract_price("", str(soup), car_data.get('age'))
            
            # Extract model and sub_model from title - ZENROWS LOGIC
            title_elem = soup.find('h1') or soup.find('h2') or \
                        soup.find(class_=re.compile(r'title|heading'))
            if title_elem:
                title_text = title_elem.get_text().strip()
                car_data['listing_title'] = title_text  # Store the actual title
                car_data['manufacturer'], car_data['model'] = self.extract_model_info(title_text)
            
            # Extract current_owner_number from the specific HTML structure - ZENROWS LOGIC
            # Look for: <span data-testid="term">יד</span><span class="details-item_itemValue__r0R14">3</span>
            term_spans = soup.find_all('span', {'data-testid': 'term'})
            for term_span in term_spans:
                if term_span.get_text().strip() == 'יד':
                    # Find the next sibling span with the value class
                    next_span = term_span.find_next_sibling('span', class_='details-item_itemValue__r0R14')
                    if next_span:
                        owner_number_text = next_span.get_text().strip()
                        try:
                            car_data['current_owner_number'] = int(owner_number_text)
                            break
                        except ValueError:
                            continue
            
            # Extract ownership info (יד 2) - fallback method
            if not car_data.get('current_owner_number'):
                ownership_elem = soup.find(text=re.compile(r'יד\s*\d+'))
                if ownership_elem:
                    ownership_match = re.search(r'יד\s*(\d+)', ownership_elem)
                    if ownership_match:
                        car_data['current_owner_number'] = int(ownership_match.group(1))
            
            # Extract location from pin icon or text - ZENROWS LOGIC
            location_elem = soup.find(text=re.compile(r'[א-ת]+')) or \
                           soup.find(class_=re.compile(r'location|address'))
            if location_elem:
                # Look for location patterns in Hebrew
                location_match = re.search(r'([א-ת]+(?:\s+[א-ת]+)*)', location_elem)
                if location_match:
                    car_data['location'] = location_match.group(1).strip()
            
            # Extract color - ZENROWS LOGIC
            color_elem = soup.find(text=re.compile(r'צבע|לבן|שחור|אדום|כחול|ירוק|צהוב|כתום|סגול|ורוד|חום|אפור|כסף|זהב'))
            if color_elem:
                color_match = re.search(r'([א-ת]+(?:\s+[א-ת]+)*)', color_elem)
                if color_match:
                    car_data['color'] = color_match.group(1).strip()
            
            # Extract transmission type - ZENROWS LOGIC
            transmission_elem = soup.find(text=re.compile(r'אוטומטי|ידני|אוטומט'))
            if transmission_elem:
                car_data['transmission'] = transmission_elem.strip()
            
            # Extract additional specifications - COPIED FROM ZENROWS
            self.extract_specifications(soup, car_data)
            
            # Download and process thumbnail
            if thumbnail_url:
                listing_id = listing_url.split('/')[-1]
                print(f"📥 Downloading thumbnail for listing {listing_id}")
                thumbnail_base64 = self.download_thumbnail_as_base64(thumbnail_url, used_thumbnails_hashes)
                if thumbnail_base64:
                    car_data['thumbnail_base64'] = thumbnail_base64
                else:
                    print(f"❌ Thumbnail download returned None for {listing_id} (but local file may exist)")
                    car_data['thumbnail_base64'] = None
                
            return car_data
            
        except Exception as e:
            print(f"❌ Error extracting car data from {listing_url}: {e}")
            return None

    def extract_price_from_page(self, soup: BeautifulSoup) -> Optional[int]:
        """Extract price from car listing page with validation"""
        try:
            # Look for price elements using various selectors
            price_selectors = [
                '[data-testid*="price"]',
                '.price',
                '[class*="price"]',
                'span:contains("₪")',
                'div:contains("₪")'
            ]
            
            for selector in price_selectors:
                elements = soup.select(selector)
                for element in elements:
                    price_text = element.get_text(strip=True)
                    if '₪' in price_text:
                        print(f"🎯 Found main price element: {price_text}")
                        return self.parse_price_from_text(price_text)
            
            # Fallback: search in page text
            price_patterns = [
                r'(\d{1,3}(?:,\d{3})*)\s*₪',
                r'₪\s*(\d{1,3}(?:,\d{3})*)',
                r'מחיר[:\s]*(\d{1,3}(?:,\d{3})*)',
            ]
            
            page_text = soup.get_text()
            for pattern in price_patterns:
                matches = re.findall(pattern, page_text)
                if matches:
                    price_str = matches[0].replace(',', '')
                    price = int(price_str)
                    if 10000 <= price <= 1000000:  # Reasonable car price range
                        return price
            
            return None
            
        except Exception as e:
            print(f"❌ Error extracting price: {e}")
            return None

    def parse_price_from_text(self, price_text: str) -> Optional[int]:
        """Parse and validate price from text"""
        try:
            # Extract numeric values with commas preserved
            price_numbers = re.findall(r'\d{1,3}(?:,\d{3})*', price_text)
            if not price_numbers:
                return None
                
            # Remove commas and convert to int
            price = int(price_numbers[0].replace(',', ''))
            
            # Validate price range
            if 10000 <= price <= 1000000:
                return price
            else:
                print(f"⚠️ Price {price} outside realistic range (10K-1M)")
                return None
                
        except (ValueError, IndexError):
            return None

    def extract_car_details_from_soup(self, soup: BeautifulSoup) -> Dict:
        """Extract car details from BeautifulSoup object"""
        details = {}
        
        try:
            # Extract year
            year_element = soup.find(string=re.compile(r'שנת יצור|שנה'))
            if year_element:
                year_text = year_element.parent.get_text() if year_element.parent else ''
                year_match = re.search(r'20\d{2}', year_text)
                if year_match:
                    details['year'] = int(year_match.group())
                    details['age'] = datetime.now().year - details['year']
            
            # Extract location
            location_selectors = ['[data-testid*="location"]', '.location', '[class*="location"]']
            for selector in location_selectors:
                element = soup.select_one(selector)
                if element:
                    details['location'] = element.get_text(strip=True)
                    break
            
            # Extract other details using text search
            detail_patterns = {
                'transmission': r'תיבת הילוכים[:\s]*([^\n,]+)',
                'engine_type': r'סוג דלק[:\s]*([^\n,]+)',
                'color': r'צבע[:\s]*([^\n,]+)',
                'mileage': r'קילומטראז[:\s]*(\d{1,3}(?:,\d{3})*)',
                'current_owner_number': r'יד[:\s]*(\d+)'
            }
            
            page_text = soup.get_text()
            for field, pattern in detail_patterns.items():
                match = re.search(pattern, page_text)
                if match:
                    value = match.group(1).strip()
                    if field == 'mileage':
                        details[field] = int(value.replace(',', ''))
                    elif field == 'current_owner_number':
                        details[field] = int(value)
                    else:
                        details[field] = value
                        
        except Exception as e:
            print(f"⚠️ Error extracting some details: {e}")
            
        return details

    def download_thumbnail_as_base64(self, thumbnail_url: str, used_thumbnails_hashes: set = None) -> Optional[str]:
        """Download thumbnail image and convert to base64 - SPEED OPTIMIZED"""
        if not thumbnail_url:
            return None
            
        try:
            # Ultra-fast download with minimal timeout and proper headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Referer': 'https://www.yad2.co.il/',
                'Accept': 'image/jpeg,image/png,image/webp,*/*'
            }
            response = requests.get(thumbnail_url, headers=headers, timeout=2)
            response.raise_for_status()
            
            image_bytes = response.content
            
            
            # ALWAYS process images to target ~50KB average size
            if PIL_AVAILABLE:
                try:
                    image = Image.open(io.BytesIO(image_bytes))
                    
                    # Target ~50KB final base64 size (66,000 chars)
                    # Base64 adds ~33% overhead, so target ~50KB raw image size
                    image.thumbnail((500, 350), Image.Resampling.LANCZOS)  # Much larger dimensions for 50KB target
                    
                    # Convert to RGB if needed
                    if image.mode in ('RGBA', 'LA', 'P'):
                        image = image.convert('RGB')
                    
                    # Try to hit 50KB target by starting with high quality and working down
                    for quality in [95, 90, 85, 80, 75, 70]:  # Very high quality range
                        output_buffer = io.BytesIO()
                        image.save(output_buffer, format='JPEG', quality=quality, optimize=True)
                        test_bytes = output_buffer.getvalue()
                        
                        # Test base64 size (adds ~33% overhead)
                        test_base64_size = len(base64.b64encode(test_bytes).decode('utf-8'))
                        
                        # Target around 50KB base64 (66,000 chars), accept 30-80KB range
                        if 40000 <= test_base64_size <= 106000:  # 30-80KB range
                            image_bytes = test_bytes
                            print(f"🎯 OPTIMIZED: Quality {quality} → {test_base64_size:,} base64 chars (~{test_base64_size//1333}KB)")
                            break
                        elif test_base64_size > 106000:
                            # Too big, try lower quality
                            continue
                        
                    # If we didn't find a good match, use the largest acceptable size
                    if 'image_bytes' not in locals() or len(base64.b64encode(image_bytes).decode('utf-8')) < 40000:
                        # Use quality 80 as fallback for good size/quality balance
                        output_buffer = io.BytesIO()
                        image.save(output_buffer, format='JPEG', quality=80, optimize=True)
                        image_bytes = output_buffer.getvalue()
                        final_size = len(base64.b64encode(image_bytes).decode('utf-8'))
                        print(f"📏 FALLBACK: Quality 80 → {final_size:,} base64 chars (~{final_size//1333}KB)")
                        
                except Exception as e:
                    print(f"❌ PIL processing failed: {e} - using original")
                    # Use original on any PIL error
            
            # Direct base64 conversion with proper data URI format
            base64_string = base64.b64encode(image_bytes).decode('utf-8')
            
            # More lenient size check - allow up to 120KB base64 (90KB actual)
            if len(base64_string) > 160000:  # ~120KB limit as safety for 50KB average target
                print(f"❌ Image too large after processing: {len(base64_string):,} chars")
                return None
            
            # Return properly formatted data URI for HTML display
            final_result = f"data:image/jpeg;base64,{base64_string}"
            print(f"✅ Thumbnail processed: {len(base64_string):,} base64 chars (~{len(base64_string)//1333}KB)")
            return final_result
            
        except Exception as e:
            print(f"❌ Thumbnail download failed: {e}")
            return None

    def extract_specifications(self, soup: BeautifulSoup, car_data: Dict):
        """Extract specifications from the details table - COPIED FROM ZENROWS"""
        # Look for specification table or details
        spec_elements = soup.find_all(text=re.compile(r'(צבע|תיבת הילוכים|סוג מנוע|מושבים|נפח מנוע|קילומטראז׳)'))
        
        for elem in spec_elements:
            text = elem.strip()
            
            # Extract color
            if 'צבע' in text:
                color_match = re.search(r'צבע[:\s]*([א-ת\s]+)', text)
                if color_match:
                    car_data['color'] = color_match.group(1).strip()
            
            # Extract transmission from "תיבת הילוכים"
            if 'תיבת הילוכים' in text:
                transmission_match = re.search(r'תיבת הילוכים[:\s]*([א-ת\s]+)', text)
                if transmission_match:
                    car_data['transmission'] = transmission_match.group(1).strip()
            
            # Extract fuel type from "סוג מנוע"
            if 'סוג מנוע' in text:
                fuel_match = re.search(r'סוג מנוע[:\s]*([א-ת\s]+)', text)
                if fuel_match:
                    car_data['fuel_type'] = fuel_match.group(1).strip()
            
            # Extract engine size
            if 'נפח מנוע' in text:
                engine_match = re.search(r'נפח מנוע[:\s]*([\d,]+)', text)
                if engine_match:
                    car_data['engine_size'] = engine_match.group(1).replace(',', '')
            
            # Extract seats
            if 'מושבים' in text:
                seats_match = re.search(r'מושבים[:\s]*(\d+)', text)
                if seats_match:
                    car_data['seats'] = int(seats_match.group(1))
        
        # Extract mileage from the details table structure
        # Look for the specific structure: <dd>קילומטראז׳</dd><dt>230,000</dt>
        mileage_labels = soup.find_all('dd', text=re.compile(r'קילומטראז׳'))
        for label in mileage_labels:
            # Find the corresponding value in the next <dt> element
            next_dt = label.find_next_sibling('dt')
            if next_dt:
                mileage_text = next_dt.get_text().strip()
                # Remove commas and convert to integer
                mileage_str = mileage_text.replace(',', '')
                try:
                    car_data['mileage'] = int(mileage_str)
                    break
                except ValueError:
                    continue
        
        # Alternative method: Look for mileage in the vehicle details section
        if not car_data.get('mileage'):
            details_items = soup.find_all('div', class_='details-item_detailsItemBox__blPEY')
            for item in details_items:
                item_text = item.get_text()
                mileage_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*ק"מ', item_text)
                if mileage_match:
                    mileage_str = mileage_match.group(1).replace(',', '')
                    try:
                        car_data['mileage'] = int(mileage_str)
                        break
                    except ValueError:
                        continue
        
        # Extract fuel_type from the details table structure
        # Look for the specific structure: <dd>סוג מנוע</dd><dt>בנזין</dt>
        fuel_labels = soup.find_all('dd', text=re.compile(r'סוג מנוע'))
        for label in fuel_labels:
            # Find the corresponding value in the next <dt> element
            next_dt = label.find_next_sibling('dt')
            if next_dt:
                fuel_text = next_dt.get_text().strip()
                car_data['fuel_type'] = fuel_text
                break
        
        # Extract date_on_road from the specifications table
        # Look for the specific structure: <dd>תאריך עליה לכביש</dd><dt>01/2023</dt>
        date_on_road_labels = soup.find_all('dd', text=re.compile(r'תאריך עליה לכביש'))
        for label in date_on_road_labels:
            # Find the corresponding value in the next <dt> element
            next_dt = label.find_next_sibling('dt')
            if next_dt:
                date_on_road_text = next_dt.get_text().strip()
                # Parse the date format MM/YYYY
                date_match = re.search(r'(\d{2}/\d{4})', date_on_road_text)
                if date_match:
                    car_data['date_on_road'] = date_match.group(1)
                    # Extract year for backward compatibility
                    year_match = re.search(r'/(\d{4})', date_match.group(1))
                    if year_match:
                        car_data['year'] = int(year_match.group(1))
                        car_data['age'] = datetime.now().year - car_data['year']
                    break
        
        # Extract transmission from the details table structure
        # Look for the specific structure: <dd>תיבת הילוכים</dd><dt>אוטומטי</dt>
        transmission_labels = soup.find_all('dd', text=re.compile(r'תיבת הילוכים'))
        for label in transmission_labels:
            # Find the corresponding value in the next <dt> element
            next_dt = label.find_next_sibling('dt')
            if next_dt:
                transmission_text = next_dt.get_text().strip()
                car_data['transmission'] = transmission_text
                break
        
        # Extract engine_size from the details table structure
        # Look for the specific structure: <dd>נפח מנוע</dd><dt>1.6</dt>
        engine_size_labels = soup.find_all('dd', text=re.compile(r'נפח מנוע'))
        for label in engine_size_labels:
            # Find the corresponding value in the next <dt> element
            next_dt = label.find_next_sibling('dt')
            if next_dt:
                engine_size_text = next_dt.get_text().strip()
                car_data['engine_size'] = engine_size_text
                break
        
        # Extract color from the details table structure
        # Look for the specific structure: <dd>צבע</dd><dt>כחול</dt>
        color_labels = soup.find_all('dd', text=re.compile(r'צבע'))
        for label in color_labels:
            # Find the corresponding value in the next <dt> element
            next_dt = label.find_next_sibling('dt')
            if next_dt:
                color_text = next_dt.get_text().strip()
                car_data['color'] = color_text
                break
        
        # Extract current_ownership_type from the details table structure
        # Look for the specific structure: <dd>בעלות נוכחית</dd><dt>פרטי</dt>
        current_ownership_labels = soup.find_all('dd', text=re.compile(r'בעלות נוכחית'))
        for label in current_ownership_labels:
            # Find the corresponding value in the next <dt> element
            next_dt = label.find_next_sibling('dt')
            if next_dt:
                current_ownership_text = next_dt.get_text().strip()
                car_data['current_ownership_type'] = current_ownership_text
                break
        
        # Extract previous_ownership_type from the details table structure
        # Look for the specific structure: <dd>בעלות קודמת</dd><dt>פרטי</dt>
        previous_ownership_labels = soup.find_all('dd', text=re.compile(r'בעלות קודמת'))
        for label in previous_ownership_labels:
            # Find the corresponding value in the next <dt> element
            next_dt = label.find_next_sibling('dt')
            if next_dt:
                previous_ownership_text = next_dt.get_text().strip()
                car_data['previous_ownership_type'] = previous_ownership_text
                break
        
        # Extract description from JSON data in script tags
        # Look for description in the JSON structure: props.pageProps.dehydratedState.queries[].state.data.metaData.description
        script_tags = soup.find_all('script')
        for script in script_tags:
            if script.string and 'description' in script.string:
                try:
                    # Try to extract JSON data
                    json_start = script.string.find('{')
                    if json_start != -1:
                        json_str = script.string[json_start:]
                        import json
                        data = json.loads(json_str)
                        
                        # Navigate through the nested structure
                        if 'props' in data and 'pageProps' in data['props']:
                            page_props = data['props']['pageProps']
                            if 'dehydratedState' in page_props and 'queries' in page_props['dehydratedState']:
                                queries = page_props['dehydratedState']['queries']
                                for query in queries:
                                    if 'state' in query and 'data' in query['state']:
                                        query_data = query['state']['data']
                                        if 'metaData' in query_data and 'description' in query_data['metaData']:
                                            description_text = query_data['metaData']['description']
                                            if description_text:
                                                car_data['description'] = description_text
                                                break
                except:
                    continue
        
        # Extract location from JSON data in script tags  
        # Look for location in the JSON structure: props.pageProps.dehydratedState.queries[].state.data.address.city.text
        script_tags = soup.find_all('script')
        for script in script_tags:
            if script.string and 'address' in script.string:
                try:
                    # Try to extract JSON data
                    json_start = script.string.find('{')
                    if json_start != -1:
                        json_str = script.string[json_start:]
                        import json
                        data = json.loads(json_str)
                        
                        # Navigate through the nested structure
                        if 'props' in data and 'pageProps' in data['props']:
                            page_props = data['props']['pageProps']
                            if 'dehydratedState' in page_props and 'queries' in page_props['dehydratedState']:
                                queries = page_props['dehydratedState']['queries']
                                for query in queries:
                                    if 'state' in query and 'data' in query['state']:
                                        query_data = query['state']['data']
                                        if 'address' in query_data and 'city' in query_data['address']:
                                            location_text = query_data['address']['city'].get('text', '')
                                            if location_text:
                                                car_data['location'] = location_text
                                                break
                except:
                    continue

    def load_manufacturers(self) -> Dict:
        """Load manufacturer data from YAML file"""
        try:
            # Get the path to the config directory relative to this file
            import os
            config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'top_car_models_fourty.yml')
            with open(config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print("❌ top_car_models_fourty.yml not found")
            return {}

    def extract_price(self, price_text: str, full_html: str = None, car_age: int = None) -> Optional[int]:
        """Extract price from price text, with JSON pattern fallback and age validation - COPIED FROM ZENROWS"""
        try:
            print(f"🔍 Extracting price from: '{price_text}' (car age: {car_age})")
            
            # Method 1: Try existing price_text extraction first
            if price_text and price_text.strip():
                # Remove currency symbols, commas, and extra whitespace
                price_clean = re.sub(r'[^\d\s]', '', price_text).strip()
                
                # Split by whitespace to separate multiple numbers
                numbers = price_clean.split()
                
                if numbers:
                    # Convert all numbers to integers
                    price_values = []
                    for num_str in numbers:
                        try:
                            price_values.append(int(num_str))
                        except ValueError:
                            continue
                    
                    if price_values:
                        print(f"📊 Found price values from text: {price_values}")
                        
                        # If there's only one number, validate it against car age
                        if len(price_values) == 1:
                            price = price_values[0]
                            if self.is_valid_price_for_age(price, car_age):
                                print(f"✅ Single price found and validated: {price}")
                                return price
                            else:
                                print(f"⚠️ Single price {price} failed age validation (age: {car_age})")
                                # Continue to fallback methods
                        else:
                            # Multiple numbers - use enhanced heuristics with age validation
                            car_prices = [p for p in price_values if len(str(p)) >= 5]
                            funding_amounts = [p for p in price_values if len(str(p)) <= 4]
                            
                            print(f"🚗 Car price candidates (5+ digits): {car_prices}")
                            print(f"💰 Funding amount candidates (4 or fewer digits): {funding_amounts}")
                            
                            # Validate car prices against age
                            valid_car_prices = [p for p in car_prices if self.is_valid_price_for_age(p, car_age)]
                            
                            if valid_car_prices:
                                selected_price = max(valid_car_prices)
                                print(f"✅ Selected car price: {selected_price} (age validated)")
                                return selected_price
                            elif car_prices:
                                # Use car prices even if age validation fails (age might be unknown)
                                selected_price = max(car_prices)
                                print(f"⚠️ Selected car price: {selected_price} (age validation skipped)")
                                return selected_price
                            
                            # Fallback to largest number only if it passes age validation
                            valid_prices = [p for p in price_values if self.is_valid_price_for_age(p, car_age)]
                            if valid_prices:
                                selected_price = max(valid_prices)
                                print(f"⚠️ Using largest age-validated price: {selected_price}")
                                return selected_price
            
            # Method 2: JSON pattern fallback
            if full_html:
                print(f"🔄 Price text extraction failed, trying JSON pattern fallback...")
                json_price = self.extract_price_from_json(full_html)
                if json_price and self.is_valid_price_for_age(json_price, car_age):
                    print(f"✅ Price found via JSON pattern and age validated: {json_price}")
                    return json_price
                elif json_price:
                    print(f"⚠️ JSON price {json_price} failed age validation, but using anyway")
                    return json_price
            
            print(f"⚠️ No valid price found in text: '{price_text}' or JSON patterns")
            return None
            
        except Exception as e:
            print(f"❌ Error extracting price from '{price_text}': {e}")
            return None
    
    def is_valid_price_for_age(self, price: int, car_age: int) -> bool:
        """Validate if price makes sense for car age - COPIED FROM ZENROWS"""
        if car_age is None:
            return True  # Skip validation if age is unknown
        
        # Rule: Cars less than 10 years old should have 5-6 digit prices (50K+)
        # 3-4 digit prices are likely monthly payments, not car prices
        if car_age < 10 and len(str(price)) <= 4:
            print(f"🚫 Price {price} too low for {car_age} year old car (likely monthly payment)")
            return False
        
        # Rule: Very old cars (20+ years) can have lower prices
        if car_age >= 20 and price < 10000:
            print(f"✅ Low price {price} acceptable for old car (age: {car_age})")
            return True
        
        # Rule: General price range validation (10K - 1M)
        if not (10000 <= price <= 1000000):
            print(f"🚫 Price {price} outside realistic range (10K-1M)")
            return False
        
        return True
    
    def extract_price_from_json(self, html_content: str) -> Optional[int]:
        """Extract price from JSON patterns in HTML - COPIED FROM ZENROWS"""
        try:
            # Look for common JSON price patterns
            json_patterns = [
                r'"price":\s*(\d+)',
                r'"Price":\s*(\d+)',
                r'"amount":\s*(\d+)',
                r'"cost":\s*(\d+)',
                r'"value":\s*(\d+)',
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, html_content)
                if matches:
                    prices = [int(match) for match in matches if len(match) >= 4]
                    if prices:
                        price = max(prices)  # Use highest price found
                        print(f"🔍 JSON pattern found price: {price}")
                        return price
            
            return None
            
        except Exception as e:
            print(f"❌ Error extracting price from JSON: {e}")
            return None
    
    def extract_model_info(self, title_text: str) -> tuple:
        """Extract manufacturer and model info from title - COPIED FROM ZENROWS"""
        try:
            # Simple extraction - could be enhanced based on title patterns
            parts = title_text.split()
            if len(parts) >= 2:
                return parts[0], parts[1]
            elif len(parts) == 1:
                return parts[0], ""
            else:
                return "", ""
        except:
            return "", ""

    # NOTE: Only JSON-based extraction used - perfect thumbnail pairing with no distance calculations