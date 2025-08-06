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
            with open('manufacturers.yml', 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print("❌ manufacturers.yml not found")
            return {}
    
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
                page_urls = self.get_listing_urls_from_page(search_url)
                if not page_urls:
                    print(f"⚠️ No listings found on page {page}, stopping pagination")
                    break
                
                # Add new URLs (avoid duplicates)
                new_urls = [url for url in page_urls if url not in all_listing_urls]
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
        
        # Step 2: Extract detailed data from each listing
        cars_data = []
        for i, url in enumerate(listing_urls, 1):
            print(f"🔍 Processing listing {i}/{len(listing_urls)}: {url}")
            try:
                car_data = self.extract_car_data(url, manufacturer_name)
                if car_data:
                    cars_data.append(car_data)
                    print(f"✅ Extracted data for {car_data.get('model', 'Unknown')}")
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
            
            # Look for vehicle links with data-nagish="feed-item-base-link"
            listing_urls = []
            
            # Method 1: Look for feed item links
            vehicle_links = soup.find_all('a', attrs={'data-nagish': 'feed-item-base-link'})
            for link in vehicle_links:
                href = link.get('href')
                if href and 'item/' in href:
                    full_url = urljoin('https://www.yad2.co.il', href)
                    if full_url not in listing_urls:
                        listing_urls.append(full_url)
            
            # Method 2: Look for elements with data-testid containing item IDs
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
            
            # Method 3: Fallback to general item links
            if len(listing_urls) == 0:
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href')
                    if href and '/vehicles/item/' in href:
                        full_url = urljoin('https://www.yad2.co.il', href)
                        if full_url not in listing_urls:
                            listing_urls.append(full_url)
            
            return listing_urls
            
        except Exception as e:
            print(f"❌ Error getting listing URLs from page: {e}")
            return []

    def get_listing_urls(self, search_url: str, max_listings: int) -> List[str]:
        """Extract listing URLs from search results page"""
        try:
            response = requests.get(search_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for vehicle links with data-nagish="feed-item-base-link"
            listing_urls = []
            
            # Method 1: Look for feed item links
            vehicle_links = soup.find_all('a', attrs={'data-nagish': 'feed-item-base-link'})
            for link in vehicle_links:
                href = link.get('href')
                if href and 'item/' in href:
                    full_url = urljoin('https://www.yad2.co.il', href)
                    if full_url not in listing_urls:
                        listing_urls.append(full_url)
                        if len(listing_urls) >= max_listings:
                            break
            
            # Method 2: Look for elements with data-testid containing item IDs
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
            
            # Method 3: Fallback to general item links
            if len(listing_urls) < max_listings:
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href')
                    if href and '/vehicles/item/' in href:
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
            price_elem = soup.find('span', {'data-testid': 'price'}) or \
                        soup.find(class_=re.compile(r'price.*price')) or \
                        soup.find(text=re.compile(r'₪\s*\d+'))
            if price_elem:
                price_text = price_elem.get_text().strip()
                car_data['price'] = self.extract_price(price_text)
            
            # Extract model and sub_model from title
            title_elem = soup.find('h1') or soup.find('h2') or \
                        soup.find(class_=re.compile(r'title|heading'))
            if title_elem:
                title_text = title_elem.get_text().strip()
                car_data['model'], car_data['sub_model'] = self.extract_model_info(title_text)
            
            # Extract year from specifications bar or text
            year_elem = soup.find(text=re.compile(r'\b20\d{2}\b'))
            if year_elem:
                year_match = re.search(r'\b(20\d{2})\b', year_elem)
                if year_match:
                    car_data['year'] = int(year_match.group(1))
                    car_data['age'] = datetime.now().year - car_data['year']
            

            
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
            
            return car_data if car_data.get('price') and car_data.get('year') else None
            
        except Exception as e:
            print(f"❌ Error extracting car data from {url}: {e}")
            return None
    
    def extract_price(self, price_text: str) -> Optional[int]:
        """Extract price from price text"""
        try:
            # Remove currency symbol and commas
            price_clean = re.sub(r'[^\d]', '', price_text)
            return int(price_clean) if price_clean else None
        except:
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