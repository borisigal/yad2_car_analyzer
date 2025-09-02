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
            config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'manufacturers.yml')
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
        
        # Step 1: Collect listing URLs from multiple pages
        all_listing_urls = []
        page = 1
        max_pages = 50  # Safety limit to prevent infinite loops
        
        print(f"📄 Collecting URLs from pages (target: {max_listings} listings)...")
        
        while len(all_listing_urls) < max_listings and page <= max_pages:
            search_url = self.get_manufacturer_url(manufacturer_key, model_key, page)
            print(f"🔍 Scanning page {page}: {search_url}")
            
            try:
                print("📡 Trying fast requests method first...")
                page_urls = self.get_listing_urls_from_page(search_url)
                
                # HYBRID APPROACH: Use browser automation if we get less than 25 results
                if 0 < len(page_urls) < 10 and SELENIUM_AVAILABLE:
                    print(f"⚡ Found only {len(page_urls)} URLs with requests, trying browser automation...")
                    browser_urls = self.get_listing_urls_with_browser(search_url)
                    if len(browser_urls) > len(page_urls):
                        print(f"✅ Browser found {len(browser_urls)} URLs (vs {len(page_urls)} from requests)")
                        page_urls = browser_urls
                    else:
                        print(f"⚠️ Browser found {len(browser_urls)} URLs, keeping requests result")
                elif len(page_urls) < 20:
                    print("⚠️ Selenium not available, cannot use browser fallback")
                
                if not page_urls:
                    print(f"⚠️ No listings found on page {page}, stopping pagination")
                    break
                
                # Add new URLs (avoid duplicates) - use normalized URLs
                normalized_page_urls = [self.normalize_listing_url(url) for url in page_urls]
                new_urls = [url for url in normalized_page_urls if url not in all_listing_urls]
                all_listing_urls.extend(new_urls)
                
                print(f"📄 Page {page}: Found {len(page_urls)} URLs, {len(new_urls)} new, total: {len(all_listing_urls)}")
                
                # Small delay between page requests
                if page < max_pages and len(all_listing_urls) < max_listings:
                    time.sleep(random.uniform(0.5, 1.5))
                
                page += 1
                
            except Exception as e:
                print(f"❌ Error scanning page {page}: {e}")
                break
        
        # Limit to requested number of listings
        listing_urls = all_listing_urls[:max_listings]
        print(f"📄 Final collection: {len(listing_urls)} listing URLs from {page-1} pages")
        
        # Step 2: Extract detailed data from each listing - prioritize working URL patterns
        # Separate URLs into working format vs others
        working_urls = []
        other_urls = []
        
        for url in listing_urls:
            if self.is_likely_car_listing_url(url):
                working_urls.append(url)
            else:
                other_urls.append(url)
        
        print(f"📊 URL Analysis: {len(working_urls)} working format, {len(other_urls)} other format")
        
        # Process working URLs first
        prioritized_urls = working_urls + other_urls
        
        cars_data = []
        for i, url in enumerate(prioritized_urls, 1):
            print(f"🔍 Processing listing {i}/{len(prioritized_urls)}: {url}")
            try:
                car_data = self.extract_car_data(url, manufacturer_name)
                if car_data:
                    cars_data.append(car_data)
                    print(f"✅ Extracted data for {car_data.get('manufacturer', 'Unknown')}")
                else:
                    print(f"⚠️ No data extracted from {url}")
                
                # Random delay between requests
                time.sleep(random.uniform(0.5, 2))
                
            except Exception as e:
                print(f"❌ Error processing {url}: {e}")
                continue
        
        print(f"💾 Extracted {len(cars_data)} cars")
        return cars_data
    


    def get_listing_urls_from_page(self, search_url: str) -> List[str]:
        """Extract listing URLs from a single search results page"""
        try:
            response = requests.get(search_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            listing_urls = []
            
            # ENHANCED: Extract ALL valid URLs (working format only)
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
                    
                    # ONLY add URLs with working format (short alphanumeric codes)
                    normalized_url = self.normalize_listing_url(full_url)
                    if normalized_url not in listing_urls:
                        listing_urls.append(normalized_url)
            
            # Method 2: Look for feed item links with data-nagish attribute
            if len(listing_urls) == 0:
                vehicle_links = soup.find_all('a', attrs={'data-nagish': 'feed-item-base-link'})
                for link in vehicle_links:
                    href = link.get('href')
                    if href and 'item/' in href:
                        full_url = urljoin('https://www.yad2.co.il', href)
                        if full_url not in listing_urls:
                            listing_urls.append(full_url)
            
            # Method 3: Look for elements with data-testid containing item IDs
            if len(listing_urls) == 0:
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
            
            print(f"🔍 Found {len(listing_urls)} listing URLs on page")
            return listing_urls
            
        except Exception as e:
            print(f"❌ Error getting listing URLs from page: {e}")
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
                    print(f"⚠️ JSON price {price_value} outside realistic range (50K-1M)")
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