#!/usr/bin/env python3
"""
Quick debug script for browser automation
"""
import sys
sys.path.insert(0, '/Users/barrysigal/yad2_car_analyzer')

from src.core.scraper.vehicle_scraper import VehicleScraper

def main():
    scraper = VehicleScraper()
    test_url = 'https://www.yad2.co.il/vehicles/cars?manufacturer=35&model=10481'
    
    print('🧪 Testing filtered browser automation...')
    
    # Test requests method first for comparison
    print('📡 Testing requests method...')
    requests_urls = scraper.get_listing_urls_from_page(test_url)
    print(f'Requests found: {len(requests_urls)} URLs')
    if requests_urls:
        print('📝 Sample requests URLs:')
        for i, url in enumerate(requests_urls[:3]):
            print(f'  {i+1}. {url}')
    
    # Test browser automation
    print('\n🌐 Testing browser automation with filtering...')
    browser_urls = scraper.get_listing_urls_with_browser(test_url)
    print(f'Browser found: {len(browser_urls)} URLs')
    if browser_urls:
        print('📝 Sample browser URLs:')
        for i, url in enumerate(browser_urls[:10]):
            print(f'  {i+1}. {url}')
    
    # Test URL validation
    print('\n🔍 Testing URL validation...')
    test_urls = [
        'https://www.yad2.co.il/item/7liq5ya4',  # Should pass
        'https://www.yad2.co.il/item/8648660090940',  # Should fail (long numeric)
        'https://www.yad2.co.il/item/nipalgim',  # Should pass
        'https://www.yad2.co.il/item/6f8xhc0x',  # Should pass
    ]
    for test_url in test_urls:
        valid = scraper.is_likely_car_listing_url(test_url)
        print(f'  {test_url} -> {"✅ PASS" if valid else "❌ FAIL"}')

if __name__ == '__main__':
    main()