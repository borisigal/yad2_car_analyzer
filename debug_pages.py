#!/usr/bin/env python3
"""
Test multiple pages to find all 29 listings
"""
import sys
sys.path.insert(0, '/Users/barrysigal/yad2_car_analyzer')

from src.core.scraper.vehicle_scraper import VehicleScraper

def main():
    scraper = VehicleScraper()
    
    print('🔍 Testing multiple pages for all 29 listings...')
    
    all_urls = []
    for page in range(1, 6):  # Test pages 1-5
        page_url = f'https://www.yad2.co.il/vehicles/cars?manufacturer=35&model=10481&page={page}'
        print(f'\n📄 Testing page {page}: {page_url}')
        
        urls = scraper.get_listing_urls_from_page(page_url)
        print(f'   Found {len(urls)} URLs on page {page}')
        
        if urls:
            new_urls = [url for url in urls if url not in all_urls]
            all_urls.extend(new_urls)
            print(f'   Added {len(new_urls)} new URLs (total: {len(all_urls)})')
            
            # Show sample URLs
            if new_urls:
                print(f'   Sample new URLs:')
                for i, url in enumerate(new_urls[:3]):
                    print(f'     {i+1}. {url}')
        else:
            print(f'   ❌ No URLs found on page {page}')
            break
    
    print(f'\n🎯 TOTAL RESULTS:')
    print(f'   Total unique URLs found: {len(all_urls)}')
    if len(all_urls) >= 25:
        print(f'   ✅ SUCCESS! Found {len(all_urls)} URLs (target: 25+)')
    else:
        print(f'   ⚠️ Only found {len(all_urls)} URLs, need 25+')
    
    if all_urls:
        print(f'   All URLs:')
        for i, url in enumerate(all_urls):
            print(f'     {i+1}. {url}')

if __name__ == '__main__':
    main()