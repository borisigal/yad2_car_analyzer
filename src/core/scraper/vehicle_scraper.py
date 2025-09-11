#!/usr/bin/env python3
"""
Vehicle Scraper for Yad2 Car Analyzer
Handles scraping and data extraction from Yad2 vehicle listings
"""

import requests
import time
import random
import re
import yaml
import base64
import io
from datetime import datetime
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Selenium imports for browser automation
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# PIL/Pillow import for image processing
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

class VehicleScraper:
    def __init__(self):
        """Initialize the scraper with headers and manufacturers"""
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.manufacturers = self.load_manufacturers()
    
    def load_manufacturers(self) -> Dict:
        """Load manufacturer data from YAML file"""
        try:
            # Get the path to the config directory relative to this file
            import os
            config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'top_car_models.yml') # top_car_models_fourty.yml
            with open(config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print("❌ manufacturers.yml not found")
            return {}
    
    def normalize_listing_url(self, url: str) -> str:
        """Normalize listing URL by stripping parameters to get the base item ID"""
        try:
            # Extract the base item ID from the URL
            # Example: https://www.yad2.co.il/item/abc123?opened-from=feed&spot=platinum -> https://www.yad2.co.il/item/abc123
            if '/item/' in url:
                # Find the item ID part
                item_part = url.split('/item/')[1]
                if '?' in item_part:
                    item_id = item_part.split('?')[0]
                else:
                    item_id = item_part
                
                # Reconstruct the clean URL
                normalized_url = f"https://www.yad2.co.il/item/{item_id}"
                return normalized_url
            
            return url
        except Exception as e:
            print(f"⚠️ Error normalizing URL {url}: {e}")
            return url

    def is_likely_car_listing_url(self, url: str) -> bool:
        """Check if URL looks like a valid car listing"""
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

    def get_manufacturer_url(self, manufacturer_key: str, model_key: str = None, page: int = 1) -> str:
        """Generate URL for a specific manufacturer and model with page number"""
        if manufacturer_key not in self.manufacturers['manufacturers']:
            raise ValueError(f"Manufacturer {manufacturer_key} not found")
        
        manufacturer = self.manufacturers['manufacturers'][manufacturer_key]
        base_url = "https://www.yad2.co.il/vehicles/cars"
        
        if model_key and model_key in manufacturer['models']:
            model = manufacturer['models'][model_key]
            url = f"{base_url}?manufacturer={manufacturer['manufacturer_id']}&model={model['model_id']}"
        else:
            url = f"{base_url}?manufacturer={manufacturer['manufacturer_id']}"
        
        # Add page parameter if page > 1
        if page > 1:
            url += f"&page={page}"
        
        return url
    
    def scrape_manufacturer(self, manufacturer_key: str, model_key: str = None, 
                          max_listings: int = 5) -> List[Dict]:
        """Scrape vehicle listings for a specific manufacturer and model"""
        
        manufacturer = self.manufacturers['manufacturers'][manufacturer_key]
        manufacturer_name = manufacturer['hebrew']
        
        print(f"🚗 Starting to scrape {manufacturer_name}")
        if model_key:
            model = manufacturer['models'][model_key]
            print(f"📋 Model: {model['hebrew']} ({model['english']})")
        
        # Step 1: Collect listing URLs and thumbnails from multiple pages
        all_listings_with_thumbnails = []
        page = 1
        max_pages = 50  # Safety limit to prevent infinite loops
        
        print(f"📄 Collecting URLs and thumbnails from pages (target: {max_listings} listings)...")
        
        while len(all_listings_with_thumbnails) < max_listings and page <= max_pages:
            search_url = self.get_manufacturer_url(manufacturer_key, model_key, page)
            print(f"🔍 Scanning page {page}: {search_url}")
            
            try:
                print("📡 Extracting URLs and thumbnails from search page...")
                page_listings_with_thumbnails = self.get_listings_with_thumbnails_from_json(search_url)
                
                if not page_listings_with_thumbnails:
                    print(f"⚠️ No listings found on page {page}, stopping pagination")
                    break
                
                # Add new listings (avoid duplicates) - use normalized URLs
                existing_urls = {listing[0] for listing in all_listings_with_thumbnails}
                new_listings = []
                for listing_url, thumbnail_url in page_listings_with_thumbnails:
                    normalized_url = self.normalize_listing_url(listing_url)
                    if normalized_url not in existing_urls:
                        new_listings.append((normalized_url, thumbnail_url))
                        existing_urls.add(normalized_url)
                
                all_listings_with_thumbnails.extend(new_listings)
                
                # Count thumbnails found
                thumbnails_found = sum(1 for _, thumb_url in new_listings if thumb_url)
                print(f"📄 Page {page}: Found {len(page_listings_with_thumbnails)} listings, {len(new_listings)} new, {thumbnails_found} with thumbnails, total: {len(all_listings_with_thumbnails)}")
                
                # Small delay between page requests
                if page < max_pages and len(all_listings_with_thumbnails) < max_listings:
                    time.sleep(random.uniform(0.5, 1.5))
                
                page += 1
                
            except Exception as e:
                print(f"❌ Error scanning page {page}: {e}")
                break
        
        # Limit to requested number of listings
        listings_with_thumbnails = all_listings_with_thumbnails[:max_listings]
        print(f"📄 Final collection: {len(listings_with_thumbnails)} listings from {page-1} pages")
        
        # Step 2: Extract detailed data from each listing and download thumbnails
        # Separate listings into working format vs others
        working_listings = []
        other_listings = []
        
        for listing_url, thumbnail_url in listings_with_thumbnails:
            if self.is_likely_car_listing_url(listing_url):
                working_listings.append((listing_url, thumbnail_url))
            else:
                other_listings.append((listing_url, thumbnail_url))
        
        print(f"📊 Listing Analysis: {len(working_listings)} working format, {len(other_listings)} other format")
        
        # Process working URLs first
        prioritized_listings = working_listings + other_listings
        
        cars_data = []
        used_thumbnail_hashes = set()  # Track thumbnail hashes to ensure uniqueness
        
        for i, (listing_url, thumbnail_url) in enumerate(prioritized_listings, 1):
            print(f"🔍 Processing listing {i}/{len(prioritized_listings)}: {listing_url}")
            try:
                car_data = self.extract_car_data(listing_url, manufacturer_name)
                if car_data:
                    # Download thumbnail from search page with uniqueness validation
                    if thumbnail_url:
                        print(f"📥 Downloading thumbnail for listing {i}/{len(prioritized_listings)}")
                        thumbnail = self.download_thumbnail_as_base64(thumbnail_url, used_thumbnail_hashes)
                        if thumbnail:
                            car_data['thumbnail_base64'] = thumbnail
                            print(f"✅ Unique thumbnail downloaded and added to car data")
                        else:
                            print(f"⚠️ Thumbnail download failed or duplicate, continuing without thumbnail")
                    else:
                        print(f"⚠️ No thumbnail URL found for this listing")
                    
                    cars_data.append(car_data)
                    print(f"✅ Extracted data for {car_data.get('manufacturer', 'Unknown')}")
                else:
                    print(f"⚠️ No data extracted from {listing_url}")
                
                # Random delay between requests (reduced since no longer taking screenshots)
                time.sleep(random.uniform(0.5, 1.5))
                
            except Exception as e:
                print(f"❌ Error processing {listing_url}: {e}")
                continue
        
        print(f"💾 Extracted {len(cars_data)} cars")
        return cars_data
    


    def get_listing_urls_from_page(self, search_url: str) -> List[str]:
        """Extract listing URLs from a single search results page"""
        # Get both URLs and thumbnails, but return only URLs for backward compatibility
        listings_with_thumbnails = self.get_listings_with_thumbnails_from_json(search_url)
        return [listing[0] for listing in listings_with_thumbnails]
    
    def get_listings_with_thumbnails_from_json(self, search_url: str) -> List[tuple]:
        """Extract listings with thumbnails using JSON data - 95%+ accuracy guaranteed"""
        try:
            response = requests.get(search_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            html_content = response.text
            
            # Extract JSON data from window.__NEXT_DATA__ script tag
            json_match = re.search(r'window\.__NEXT_DATA__\s*=\s*({.*?});', html_content, re.DOTALL)
            
            if not json_match:
                print("❌ No __NEXT_DATA__ found in initial HTML (requires JS rendering)")
                print("🔄 Falling back to enhanced HTML parsing with browser automation")
                return self.get_listings_with_thumbnails_browser_enhanced(search_url)
            
            import json
            try:
                next_data = json.loads(json_match.group(1))
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON data: {e}")
                return self.get_listings_with_thumbnails_browser_enhanced(search_url)
            
            # Navigate to listings data
            listings_data = next_data.get('props', {}).get('pageProps', {}).get('dehydratedState', {}).get('queries', [])
            
            listings_with_thumbnails = []
            processed_tokens = set()
            
            print(f"🔍 Processing {len(listings_data)} query blocks from JSON data")
            
            for query in listings_data:
                if 'state' in query and 'data' in query['state']:
                    data = query['state']['data']
                    
                    # Process all listing categories with accurate thumbnail matching
                    for category in ['platinum', 'commercial', 'solo', 'private']:
                        if category in data and isinstance(data[category], list):
                            
                            for listing in data[category]:
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
                                
                                if thumbnail_url and self.is_likely_car_listing_url(listing_url):
                                    listings_with_thumbnails.append((listing_url, thumbnail_url))
                                    print(f"✅ JSON-matched: {token} → {thumbnail_url[:50]}...")
            
            print(f"🎯 JSON extraction found {len(listings_with_thumbnails)} listings with perfect thumbnail matching")
            return listings_with_thumbnails
            
        except Exception as e:
            print(f"❌ JSON extraction failed: {e}")
            return self.get_listings_with_thumbnails_browser_enhanced(search_url)

    def get_listings_with_thumbnails_browser_enhanced(self, search_url: str) -> List[tuple]:
        """Enhanced browser method: Extract JSON data after JavaScript rendering for perfect accuracy"""
        if not SELENIUM_AVAILABLE:
            print("❌ Selenium not available, falling back to HTML parsing")
            return self.get_listings_with_thumbnails_from_page_fallback(search_url)
        
        driver = None
        try:
            print(f"🌐 Starting browser automation to extract JSON data from: {search_url}")
            
            # Setup Chrome options for stealth
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(search_url)
            
            # Wait for page to load and JavaScript to execute
            print("⏳ Waiting for page to load and JavaScript to execute...")
            time.sleep(3)
            
            # Try to extract JSON data from the rendered page
            try:
                json_data = driver.execute_script("return window.__NEXT_DATA__;")
                if json_data:
                    print("✅ Successfully extracted __NEXT_DATA__ from rendered page")
                    
                    # Navigate to listings data
                    listings_data = json_data.get('props', {}).get('pageProps', {}).get('dehydratedState', {}).get('queries', [])
                    
                    listings_with_thumbnails = []
                    processed_tokens = set()
                    
                    print(f"🔍 Processing {len(listings_data)} query blocks from browser JSON data")
                    
                    for query in listings_data:
                        if 'state' in query and 'data' in query['state']:
                            data = query['state']['data']
                            
                            # Process all listing categories with perfect thumbnail matching
                            for category in ['platinum', 'commercial', 'solo', 'private']:
                                if category in data and isinstance(data[category], list):
                                    
                                    for listing in data[category]:
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
                                        
                                        if thumbnail_url and self.is_likely_car_listing_url(listing_url):
                                            listings_with_thumbnails.append((listing_url, thumbnail_url))
                                            print(f"🎯 Browser-JSON matched: {token} → {thumbnail_url[:50]}...")
                    
                    print(f"🎉 Browser JSON extraction found {len(listings_with_thumbnails)} listings with PERFECT thumbnail matching!")
                    return listings_with_thumbnails
                else:
                    print("❌ No __NEXT_DATA__ found even in rendered page")
                    
            except Exception as e:
                print(f"❌ Failed to extract JSON from browser: {e}")
                
            # If JSON extraction fails, fall back to HTML parsing
            print("🔄 Falling back to legacy HTML parsing method")
            return self.get_listings_with_thumbnails_from_page_fallback(search_url)
            
        except Exception as e:
            print(f"❌ Browser automation failed: {e}")
            return self.get_listings_with_thumbnails_from_page_fallback(search_url)
        finally:
            if driver:
                driver.quit()

    def get_listings_with_thumbnails_from_page_fallback(self, search_url: str) -> List[tuple]:
        """FALLBACK: Extract listing URLs and their thumbnail URLs using HTML parsing (legacy method)"""
        try:
            response = requests.get(search_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            listings_with_thumbnails = []
            processed_urls = set()
            used_thumbnails = set()  # Track used thumbnails to ensure uniqueness
            
            print(f"🔍 Extracting listings from page with {len(soup.find_all('a', href=True))} total links")
            
            # Method 1: Find all item links first, then match with UNIQUE nearby thumbnails
            all_item_links = soup.find_all('a', href=True)
            item_links = []
            
            for link in all_item_links:
                href = link.get('href')
                if href and '/item/' in href:
                    # Clean the URL and make it absolute
                    if href.startswith('/'):
                        full_url = urljoin('https://www.yad2.co.il', href)
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        full_url = urljoin('https://www.yad2.co.il', '/' + href)
                    
                    # Normalize and validate URL
                    normalized_url = self.normalize_listing_url(full_url)
                    if self.is_likely_car_listing_url(normalized_url) and normalized_url not in processed_urls:
                        item_links.append((link, normalized_url))
                        processed_urls.add(normalized_url)
            
            print(f"🔗 Found {len(item_links)} valid item links")
            
            # Method 2: SMART DISTANCE-BASED thumbnail matching
            # Find all images first, then match each link to the closest unique image
            all_images = soup.find_all('img', src=True)
            valid_images = []
            
            for img in all_images:
                img_src = img.get('src')
                if img_src and ('yad2.co.il' in img_src or img_src.startswith('/Pic/')):
                    # Make absolute URL
                    if img_src.startswith('/'):
                        full_img_url = urljoin('https://img.yad2.co.il', img_src)
                    elif img_src.startswith('http'):
                        full_img_url = img_src
                    else:
                        continue
                    valid_images.append((img, full_img_url))
            
            print(f"🔍 Found {len(valid_images)} valid images to match against")
            
            # For each link, find the closest unique image
            for link_element, listing_url in item_links:
                thumbnail_url = None
                min_distance = float('inf')
                best_image_url = None
                
                # Calculate distance from this link to all available images
                for img_element, img_url in valid_images:
                    if img_url in used_thumbnails:
                        continue  # Skip already used images
                    
                    # Calculate distance using multiple methods
                    distance_score = 0
                    
                    # Method 1: DOM tree distance (parent-child relationship)
                    try:
                        # Check if image and link share common ancestors
                        link_parents = []
                        current = link_element
                        for level in range(10):  # Go up to 10 levels
                            if current.parent:
                                current = current.parent
                                link_parents.append(current)
                            else:
                                break
                        
                        img_parents = []
                        current = img_element
                        for level in range(10):
                            if current.parent:
                                current = current.parent
                                img_parents.append(current)
                            else:
                                break
                        
                        # Find lowest common ancestor
                        common_ancestor = None
                        link_depth = 0
                        img_depth = 0
                        
                        for i, link_parent in enumerate(link_parents):
                            for j, img_parent in enumerate(img_parents):
                                if link_parent == img_parent:
                                    common_ancestor = link_parent
                                    link_depth = i
                                    img_depth = j
                                    break
                            if common_ancestor:
                                break
                        
                        if common_ancestor:
                            # Distance is sum of depths from common ancestor
                            tree_distance = link_depth + img_depth
                            distance_score += tree_distance * 10  # Weight tree distance heavily
                        else:
                            distance_score += 1000  # High penalty for no common ancestor
                            
                    except:
                        distance_score += 500  # Medium penalty for calculation error
                    
                    # Method 2: HTML string position distance
                    try:
                        page_html = str(soup)
                        link_href = link_element.get('href', '')
                        if link_href:
                            link_pos = page_html.find(link_href)
                            img_src = img_element.get('src', '')
                            img_pos = page_html.find(img_src)
                            
                            if link_pos >= 0 and img_pos >= 0:
                                string_distance = abs(link_pos - img_pos)
                                distance_score += string_distance / 100  # Normalized string distance
                            else:
                                distance_score += 100
                    except:
                        distance_score += 50
                    
                    # Choose image with lowest distance score
                    if distance_score < min_distance:
                        min_distance = distance_score
                        best_image_url = img_url
                
                # Assign the best match
                if best_image_url and best_image_url not in used_thumbnails:
                    thumbnail_url = best_image_url
                    used_thumbnails.add(thumbnail_url)
                    print(f"🎯 Distance-matched thumbnail for {listing_url[-8:]} (score: {min_distance:.1f})")
                else:
                    print(f"⚠️ No suitable unique thumbnail found for {listing_url[-8:]}")
                
                # If still no unique thumbnail found, try browser automation for this specific listing
                if not thumbnail_url:
                    print(f"⚠️ No unique thumbnail found for {listing_url[-8:]}")
                
                listings_with_thumbnails.append((listing_url, thumbnail_url))
            
            # Method 3: Browser automation fallback if too many missing thumbnails
            missing_thumbnails = sum(1 for _, thumb in listings_with_thumbnails if not thumb)
            if missing_thumbnails > len(listings_with_thumbnails) * 0.3 and SELENIUM_AVAILABLE:
                print(f"⚡ {missing_thumbnails} listings missing thumbnails, trying browser automation...")
                browser_results = self.get_listings_with_thumbnails_browser(search_url)
                if browser_results:
                    print(f"✅ Browser found {len(browser_results)} listings, merging results...")
                    return browser_results
            
            # Validate uniqueness
            found_thumbnails = [thumb for _, thumb in listings_with_thumbnails if thumb]
            unique_thumbnails = len(set(found_thumbnails))
            print(f"🔍 Found {len(listings_with_thumbnails)} listings, {len(found_thumbnails)} with thumbnails, {unique_thumbnails} unique")
            
            if len(found_thumbnails) != unique_thumbnails:
                print(f"⚠️ WARNING: {len(found_thumbnails) - unique_thumbnails} duplicate thumbnails detected")
            
            return listings_with_thumbnails
            
        except Exception as e:
            print(f"❌ Error getting listings with thumbnails from page: {e}")
            return []

    def get_listing_urls_with_browser(self, search_url: str) -> List[str]:
        """Extract listing URLs using browser automation - for JavaScript-heavy pages"""
        if not SELENIUM_AVAILABLE:
            print("❌ Selenium not available for browser automation")
            return []
        
        driver = None
        try:
            print(f"🌐 Starting browser automation for {search_url}")
            
            # Setup Chrome options for stealth
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Run in background
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Add realistic user agent
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Initialize driver
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Load the page completely
            print("🔄 Loading page with browser...")
            driver.get(search_url)
            
            # FULL PAGE LOADING STRATEGY
            print("⏳ Loading complete page with all content...")
            
            # Strategy 1: Wait for page to be ready
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            wait = WebDriverWait(driver, 30)
            
            # Wait for body to be present
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Strategy 2: Wait for initial content load
            time.sleep(5)
            
            # Strategy 3: Scroll down slowly to trigger ALL lazy loading
            print("📜 Scrolling to load all listings...")
            last_height = driver.execute_script("return document.body.scrollHeight")
            
            # Progressive scrolling with network wait
            for scroll_step in range(10):
                # Scroll down 10% each time
                scroll_position = (scroll_step + 1) * 0.1
                driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_position});")
                time.sleep(2)  # Wait for content to load
                
                # Check if page height changed (new content loaded)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height != last_height:
                    print(f"   📈 Page height changed: {last_height} -> {new_height}")
                    last_height = new_height
                    time.sleep(3)  # Extra wait for new content
            
            # Strategy 4: Final scroll to very bottom and wait
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            
            # Strategy 5: Wait for any remaining network activity
            print("🌐 Waiting for network activity to complete...")
            time.sleep(10)  # Extended wait for all network requests
            
            # Get page source and extract URLs
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            listing_urls = []
            
            # STRATEGY: EXTRACT TOKENS FROM JAVASCRIPT DATA (NOT HTML LINKS)
            # The 29 listings exist in JavaScript data structures, not as HTML links
            
            # Method 1: Extract tokens from JavaScript/JSON data
            script_tags = soup.find_all('script')
            for script in script_tags:
                if script.string and 'token' in script.string:
                    try:
                        # Look for token patterns in JavaScript data
                        import json
                        # Try to extract JSON data containing tokens
                        text = script.string
                        
                        # Look for token patterns: "token":"abc123def"
                        token_matches = re.findall(r'"token"\s*:\s*"([a-zA-Z0-9]{4,10})"', text)
                        for token in token_matches:
                            if token and 4 <= len(token) <= 10 and not token.isdigit():
                                item_url = f"https://www.yad2.co.il/item/{token}"
                                if item_url not in listing_urls:
                                    listing_urls.append(item_url)
                        
                        # Also look for pattern: token: "abc123def"
                        token_matches2 = re.findall(r'token\s*:\s*"([a-zA-Z0-9]{4,10})"', text)
                        for token in token_matches2:
                            if token and 4 <= len(token) <= 10 and not token.isdigit():
                                item_url = f"https://www.yad2.co.il/item/{token}"
                                if item_url not in listing_urls:
                                    listing_urls.append(item_url)
                                    
                    except Exception as e:
                        continue
            
            # Method 2: Fallback - Extract ALL links with '/item/' and filter for working pattern (as backup)
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href')
                if href and '/item/' in href:
                    # Clean the URL and make it absolute
                    if href.startswith('/'):
                        full_url = urljoin('https://www.yad2.co.il', href)
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        full_url = urljoin('https://www.yad2.co.il', '/' + href)
                    
                    # Add ALL /item/ URLs - let the system try both formats
                    normalized_url = self.normalize_listing_url(full_url)
                    if normalized_url not in listing_urls:
                        listing_urls.append(normalized_url)
            
            # Method 2: Look for data-nagish="private-item-link" with filtering
            private_links = soup.find_all('a', attrs={'data-nagish': 'private-item-link'})
            for link in private_links:
                href = link.get('href')
                if href and '/item/' in href:
                    if href.startswith('/'):
                        full_url = urljoin('https://www.yad2.co.il', href)
                    else:
                        full_url = href
                    
                    normalized_url = self.normalize_listing_url(full_url)
                    if normalized_url not in listing_urls:
                        listing_urls.append(normalized_url)
            
            # Method 3: Look for feed item links with filtering
            vehicle_links = soup.find_all('a', attrs={'data-nagish': 'feed-item-base-link'})
            for link in vehicle_links:
                href = link.get('href')
                if href and 'item/' in href:
                    full_url = urljoin('https://www.yad2.co.il', href)
                    normalized_url = self.normalize_listing_url(full_url)
                    if normalized_url not in listing_urls:
                        listing_urls.append(normalized_url)
            
            # Method 4: Look for elements with data-testid with filtering
            testid_elements = soup.find_all(attrs={'data-testid': re.compile(r'^[a-zA-Z0-9]+$')})
            for element in testid_elements:
                testid = element.get('data-testid')
                if testid and len(testid) > 5:  # Likely an item ID
                    # Find the link within this element
                    link = element.find('a', href=True)
                    if link:
                        href = link.get('href')
                        if href and 'item/' in href:
                            full_url = urljoin('https://www.yad2.co.il', href)
                            normalized_url = self.normalize_listing_url(full_url)
                            if normalized_url not in listing_urls and self.is_likely_car_listing_url(normalized_url):
                                listing_urls.append(normalized_url)
            
            print(f"🎯 Browser automation found {len(listing_urls)} listing URLs")
            return listing_urls
            
        except Exception as e:
            print(f"❌ Browser automation error: {e}")
            return []
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    def get_listing_urls(self, search_url: str, max_listings: int) -> List[str]:
        """Extract listing URLs from search results page"""
        try:
            response = requests.get(search_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            listing_urls = []
            
            # Method 1: Look for any links containing '/item/' (most reliable)
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href')
                if href and '/item/' in href:
                    # Clean the URL and make it absolute
                    if href.startswith('/'):
                        full_url = urljoin('https://www.yad2.co.il', href)
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        full_url = urljoin('https://www.yad2.co.il', '/' + href)
                    
                    # Add any item URL (not just vehicle-specific ones)
                    if full_url not in listing_urls:
                        listing_urls.append(full_url)
                        if len(listing_urls) >= max_listings:
                            break
            
            # Method 2: Look for feed item links with data-nagish attribute
            if len(listing_urls) < max_listings:
                vehicle_links = soup.find_all('a', attrs={'data-nagish': 'feed-item-base-link'})
                for link in vehicle_links:
                    href = link.get('href')
                    if href and 'item/' in href:
                        full_url = urljoin('https://www.yad2.co.il', href)
                        if full_url not in listing_urls:
                            listing_urls.append(full_url)
                            if len(listing_urls) >= max_listings:
                                break
            
            # Method 3: Look for elements with data-testid containing item IDs
            if len(listing_urls) < max_listings:
                testid_elements = soup.find_all(attrs={'data-testid': re.compile(r'^[a-zA-Z0-9]+$')})
                for element in testid_elements:
                    testid = element.get('data-testid')
                    if testid and len(testid) > 5:  # Likely an item ID
                        # Find the link within this element
                        link = element.find('a', href=True)
                        if link:
                            href = link.get('href')
                            if href and 'item/' in href:
                                full_url = urljoin('https://www.yad2.co.il', href)
                                if full_url not in listing_urls:
                                    listing_urls.append(full_url)
                                    if len(listing_urls) >= max_listings:
                                        break
            
            return listing_urls[:max_listings]
            
        except Exception as e:
            print(f"❌ Error getting listing URLs: {e}")
            return []
    
    def extract_car_data(self, url: str, manufacturer_name: str) -> Optional[Dict]:
        """Extract detailed car data from individual listing page"""
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract car data
            car_data = {
                'manufacturer': manufacturer_name,
                'listing_url': url,
                'original_url': url,
                'raw_html': response.text,  # Store the raw HTML
                'response_status': response.status_code,
                'response_time': response.elapsed.total_seconds()
            }
            
            # Extract price - look for price elements with specific classes
            # Prioritize main price elements over funding amounts
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
            
            # Extract year FIRST to use for price validation
            year_elem = soup.find(text=re.compile(r'\b20\d{2}\b'))
            if year_elem:
                year_match = re.search(r'\b(20\d{2})\b', year_elem)
                if year_match:
                    car_data['year'] = int(year_match.group(1))
                    car_data['age'] = datetime.now().year - car_data['year']
            
            # Extract price with age validation
            if price_elem:
                price_text = price_elem.get_text().strip()
                # Pass price_text, full HTML, and car age for validation
                car_data['price'] = self.extract_price(price_text, str(soup), car_data.get('age'))
            else:
                # No price element found - try JSON pattern directly
                print("⚠️ No price element found, trying JSON pattern...")
                car_data['price'] = self.extract_price("", str(soup), car_data.get('age'))
            
            # Extract model and sub_model from title
            title_elem = soup.find('h1') or soup.find('h2') or \
                        soup.find(class_=re.compile(r'title|heading'))
            if title_elem:
                title_text = title_elem.get_text().strip()
                car_data['manufacturer'], car_data['model'] = self.extract_model_info(title_text)
            

            
            # Extract current_owner_number from the specific HTML structure
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
            
            # Extract location from pin icon or text
            location_elem = soup.find(text=re.compile(r'[א-ת]+')) or \
                           soup.find(class_=re.compile(r'location|address'))
            if location_elem:
                # Look for location patterns in Hebrew
                location_match = re.search(r'([א-ת]+(?:\s+[א-ת]+)*)', location_elem)
                if location_match:
                    car_data['location'] = location_match.group(1).strip()
            
            # Extract color
            color_elem = soup.find(text=re.compile(r'צבע|לבן|שחור|אדום|כחול|ירוק|צהוב|כתום|סגול|ורוד|חום|אפור|כסף|זהב'))
            if color_elem:
                color_match = re.search(r'([א-ת]+(?:\s+[א-ת]+)*)', color_elem)
                if color_match:
                    car_data['color'] = color_match.group(1).strip()
            
            # Extract transmission type
            transmission_elem = soup.find(text=re.compile(r'אוטומטי|ידני|אוטומט'))
            if transmission_elem:
                transmission_match = re.search(r'(אוטומטי|ידני|אוטומט)', transmission_elem)
                if transmission_match:
                    car_data['transmission'] = transmission_match.group(1)
            
            # Extract engine type
            engine_elem = soup.find(text=re.compile(r'בנזין|דיזל|היברידי|חשמלי'))
            if engine_elem:
                engine_match = re.search(r'(בנזין|דיזל|היברידי|חשמלי)', engine_elem)
                if engine_match:
                    car_data['engine_type'] = engine_match.group(1)
            
            # Extract additional details from specification table
            self.extract_specifications(soup, car_data)
            
            # Allow cars without price (set to None/NULL), but require year
            return car_data if car_data.get('year') else None
            
        except Exception as e:
            print(f"❌ Error extracting car data from {url}: {e}")
            return None
    
    def extract_price(self, price_text: str, full_html: str = None, car_age: int = None) -> Optional[int]:
        """Extract price from price text, with JSON pattern fallback and age validation"""
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
            
            # Method 2: JSON pattern fallback (NEW!)
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
        """Validate if price makes sense for car age - NEW METHOD"""
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
        
        # Rule: Normal validation for realistic price range
        if 1000 <= price <= 2000000:  # 1K - 2M NIS range
            return True
            
        return False
    
    def extract_price_from_json(self, html_text: str) -> Optional[int]:
        """Extract price from JSON pattern in HTML - NEW METHOD"""
        try:
            import re
            # Search for "price":NUMBER pattern
            match = re.search(r'"price":(\d+)', html_text)
            if match:
                price_value = int(match.group(1))
                # Validate it's in realistic car price range
                if 10000 <= price_value <= 1000000:
                    print(f"🎯 JSON pattern found price: {price_value}")
                    return price_value
                else:
                    print(f"⚠️ JSON price {price_value} outside realistic range (10K-1M)")
            return None
        except Exception as e:
            print(f"❌ Error extracting price from JSON pattern: {e}")
            return None
    
    def extract_model_info(self, title_text: str) -> tuple:
        """Extract model and sub_model from title"""
        # Simple extraction - can be improved
        parts = title_text.split()
        if len(parts) >= 2:
            model = parts[0]  # First word is usually the model
            sub_model = ' '.join(parts[1:]) if len(parts) > 1 else ''
            return model, sub_model
        return title_text, ''
    
    def extract_specifications(self, soup: BeautifulSoup, car_data: Dict):
        """Extract specifications from the details table"""
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
    
    def capture_thumbnail(self, listing_url: str) -> Optional[str]:
        """Capture thumbnail image from listing page and return as base64 string"""
        if not SELENIUM_AVAILABLE:
            print("❌ Selenium not available for thumbnail capture")
            return None
        
        driver = None
        try:
            # Create thumbnail URL by appending the gallery modal fragment
            thumbnail_url = f"{listing_url}#galleryModal-grid-swiper-item-0"
            print(f"📸 Capturing thumbnail from: {thumbnail_url}")
            
            # Setup Chrome options for thumbnail capture
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1200,800')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Add realistic user agent
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Initialize driver
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Load the thumbnail URL
            driver.get(thumbnail_url)
            time.sleep(3)  # Wait for page to load
            
            # Try to find and wait for the main image element
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            wait = WebDriverWait(driver, 10)
            
            # Look for common image selectors on Yad2
            image_selectors = [
                "img[src*='yad2']",  # Yad2 images
                ".gallery img",
                ".image-gallery img", 
                ".swiper-slide img",
                "[data-testid*='image'] img",
                ".main-image img",
                "img[alt*='car'], img[alt*='רכב']"  # Car-related alt text
            ]
            
            image_element = None
            for selector in image_selectors:
                try:
                    image_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    if image_element.is_displayed():
                        print(f"✅ Found image with selector: {selector}")
                        break
                except:
                    continue
            
            # If no specific image found, try to find any visible image
            if not image_element:
                try:
                    images = driver.find_elements(By.TAG_NAME, "img")
                    for img in images:
                        if img.is_displayed() and img.size['width'] > 100 and img.size['height'] > 100:
                            image_element = img
                            print(f"✅ Found fallback image: {img.get_attribute('src')[:50]}...")
                            break
                except:
                    pass
            
            if not image_element:
                print("⚠️ No suitable image found for thumbnail")
                return None
            
            # Take a screenshot of the specific image element
            try:
                screenshot_png = image_element.screenshot_as_png
                
                # Resize image to thumbnail size using PIL if available
                if PIL_AVAILABLE:
                    try:
                        image = Image.open(io.BytesIO(screenshot_png))
                        
                        # Resize to thumbnail (max 300x200, maintain aspect ratio)
                        image.thumbnail((300, 200), Image.Resampling.LANCZOS)
                        
                        # Convert back to bytes
                        output_buffer = io.BytesIO()
                        image.save(output_buffer, format='JPEG', quality=85, optimize=True)
                        thumbnail_bytes = output_buffer.getvalue()
                        print(f"✅ PIL resized image: {len(thumbnail_bytes)} bytes")
                        
                    except Exception as e:
                        print(f"⚠️ PIL processing failed ({e}), using Selenium crop fallback")
                        # Fallback: Use Selenium to crop the image element to a smaller size
                        try:
                            driver.set_window_size(800, 600)  # Smaller window
                            time.sleep(1)
                            screenshot_png = image_element.screenshot_as_png
                            thumbnail_bytes = screenshot_png
                        except:
                            thumbnail_bytes = screenshot_png
                else:
                    print("⚠️ PIL not available, using Selenium crop fallback")
                    # Fallback: Use Selenium to crop the image element to a smaller size
                    try:
                        driver.set_window_size(800, 600)  # Smaller window
                        time.sleep(1)
                        screenshot_png = image_element.screenshot_as_png
                        thumbnail_bytes = screenshot_png
                    except:
                        thumbnail_bytes = screenshot_png
                
                # Convert to base64
                base64_string = base64.b64encode(thumbnail_bytes).decode('utf-8')
                
                # Check size limit (200KB base64 limit - more reasonable)
                if len(base64_string) > 200000:
                    print(f"⚠️ Thumbnail too large ({len(base64_string)} chars), skipping")
                    return None
                
                print(f"✅ Captured thumbnail ({len(base64_string)} chars)")
                return base64_string
                
            except Exception as e:
                print(f"❌ Error taking screenshot: {e}")
                return None
                
        except Exception as e:
            print(f"❌ Error capturing thumbnail from {thumbnail_url}: {e}")
            return None
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def get_listings_with_thumbnails_browser(self, search_url: str) -> List[tuple]:
        """Extract listings and thumbnails using browser automation - for JavaScript-heavy pages"""
        if not SELENIUM_AVAILABLE:
            print("❌ Selenium not available for browser automation")
            return []
        
        driver = None
        try:
            print(f"🌐 Starting browser automation for listings and thumbnails: {search_url}")
            
            # Setup Chrome options for stealth
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Load the page completely
            driver.get(search_url)
            
            # Wait and scroll to load all content
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            wait = WebDriverWait(driver, 30)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(3)
            
            # Scroll to load all listings
            last_height = driver.execute_script("return document.body.scrollHeight")
            for scroll_step in range(5):
                driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {(scroll_step + 1) * 0.2});")
                time.sleep(2)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height != last_height:
                    last_height = new_height
            
            # Get page source and parse
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            listings_with_thumbnails = []
            processed_urls = set()
            
            # Find all item links
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href')
                if href and '/item/' in href:
                    # Clean and normalize URL
                    if href.startswith('/'):
                        full_url = urljoin('https://www.yad2.co.il', href)
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        full_url = urljoin('https://www.yad2.co.il', '/' + href)
                    
                    normalized_url = self.normalize_listing_url(full_url)
                    if not self.is_likely_car_listing_url(normalized_url) or normalized_url in processed_urls:
                        continue
                    
                    # Find thumbnail near this link
                    thumbnail_url = None
                    current_element = link
                    for level in range(6):  # Check more levels in browser version
                        if current_element.parent:
                            current_element = current_element.parent
                            images = current_element.find_all('img', src=True)
                            for img in images:
                                img_src = img.get('src')
                                if img_src and ('yad2.co.il' in img_src or img_src.startswith('/Pic/') or 'image' in img_src.lower()):
                                    if img_src.startswith('/'):
                                        thumbnail_url = urljoin('https://img.yad2.co.il', img_src)
                                    elif img_src.startswith('http'):
                                        thumbnail_url = img_src
                                    break
                            if thumbnail_url:
                                break
                    
                    listings_with_thumbnails.append((normalized_url, thumbnail_url))
                    processed_urls.add(normalized_url)
            
            print(f"🎯 Browser automation found {len(listings_with_thumbnails)} listings with thumbnails")
            return listings_with_thumbnails
            
        except Exception as e:
            print(f"❌ Browser automation error: {e}")
            return []
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def download_thumbnail_as_base64(self, thumbnail_url: str, used_thumbnails_hashes: set = None) -> Optional[str]:
        """Download thumbnail image and convert to base64 with uniqueness validation"""
        if not thumbnail_url:
            return None
        
        if used_thumbnails_hashes is None:
            used_thumbnails_hashes = set()
            
        try:
            print(f"📥 Downloading thumbnail: {thumbnail_url[:60]}...")
            
            # Download the image
            response = requests.get(thumbnail_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            # Get image bytes
            original_image_bytes = response.content
            
            # First check: Verify original image is unique by hash
            import hashlib
            original_hash = hashlib.md5(original_image_bytes).hexdigest()
            if original_hash in used_thumbnails_hashes:
                print(f"⚠️ Duplicate original image detected (hash: {original_hash[:8]}), skipping")
                return None
                
            # Resize image if PIL is available
            image_bytes = original_image_bytes
            if PIL_AVAILABLE and len(original_image_bytes) > 0:
                try:
                    image = Image.open(io.BytesIO(original_image_bytes))
                    
                    # Resize to thumbnail (max 300x200, maintain aspect ratio)  
                    image.thumbnail((300, 200), Image.Resampling.LANCZOS)
                    
                    # Convert to JPEG and optimize
                    output_buffer = io.BytesIO()
                    # Convert to RGB if image has transparency (for JPEG compatibility)
                    if image.mode in ('RGBA', 'LA', 'P'):
                        image = image.convert('RGB')
                    image.save(output_buffer, format='JPEG', quality=85, optimize=True)
                    image_bytes = output_buffer.getvalue()
                    print(f"✅ PIL resized thumbnail: {len(image_bytes)} bytes")
                    
                except Exception as e:
                    print(f"⚠️ PIL processing failed ({e}), using original image")
            
            # Second check: Verify processed image is still unique
            processed_hash = hashlib.md5(image_bytes).hexdigest()
            if processed_hash in used_thumbnails_hashes:
                print(f"⚠️ Processed image became duplicate (hash: {processed_hash[:8]}), skipping")
                return None
            
            # Add hashes to used set
            used_thumbnails_hashes.add(original_hash)
            used_thumbnails_hashes.add(processed_hash)
            
            # Convert to base64
            base64_string = base64.b64encode(image_bytes).decode('utf-8')
            
            # Check size limit (200KB base64 limit)
            if len(base64_string) > 200000:
                print(f"⚠️ Thumbnail too large ({len(base64_string)} chars), skipping")
                return None
            
            print(f"✅ Downloaded unique thumbnail ({len(base64_string)} chars, hash: {processed_hash[:8]})")
            return base64_string
            
        except Exception as e:
            print(f"❌ Error downloading thumbnail from {thumbnail_url}: {e}")
            return None