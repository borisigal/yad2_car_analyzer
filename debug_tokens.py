#!/usr/bin/env python3
"""
Debug script to examine JavaScript content for tokens
"""
import sys
sys.path.insert(0, '/Users/barrysigal/yad2_car_analyzer')

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import re

def main():
    test_url = 'https://www.yad2.co.il/vehicles/cars?manufacturer=35&model=10481'
    
    print('🔍 Examining JavaScript content for tokens...')
    
    # Setup browser
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        driver.get(test_url)
        time.sleep(5)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Examine all script tags
        script_tags = soup.find_all('script')
        print(f'📜 Found {len(script_tags)} script tags')
        
        tokens_found = []
        for i, script in enumerate(script_tags):
            if script.string and len(script.string) > 100:  # Only examine substantial scripts
                content = script.string
                
                # Look for 'token' keyword
                if 'token' in content.lower():
                    print(f'\n📍 Script {i+1} contains "token" keyword')
                    print(f'   Length: {len(content)} characters')
                    
                    # Look for various token patterns
                    patterns = [
                        r'"token"\s*:\s*"([a-zA-Z0-9]+)"',
                        r'token\s*:\s*"([a-zA-Z0-9]+)"',
                        r'"token":"([a-zA-Z0-9]+)"',
                        r'token:"([a-zA-Z0-9]+)"',
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, content)
                        if matches:
                            print(f'   Pattern {pattern} found {len(matches)} matches: {matches[:5]}')
                            tokens_found.extend(matches)
                    
                    # Show a sample of the content around 'token'
                    token_pos = content.lower().find('token')
                    if token_pos > 0:
                        start = max(0, token_pos - 100)
                        end = min(len(content), token_pos + 200)
                        sample = content[start:end].replace('\n', ' ').replace('\r', ' ')
                        print(f'   Sample content: ...{sample}...')
        
        # Remove duplicates and filter for valid tokens
        unique_tokens = list(set(tokens_found))
        valid_tokens = [t for t in unique_tokens if 4 <= len(t) <= 10 and not t.isdigit()]
        
        print(f'\n🎯 Summary:')
        print(f'   Total tokens found: {len(tokens_found)}')
        print(f'   Unique tokens: {len(unique_tokens)}')
        print(f'   Valid tokens (4-10 chars, not pure numeric): {len(valid_tokens)}')
        
        if valid_tokens:
            print(f'   Valid tokens: {valid_tokens}')
            print(f'   Sample URLs:')
            for token in valid_tokens[:5]:
                print(f'     https://www.yad2.co.il/item/{token}')
        else:
            print('   ❌ No valid tokens found!')
            
    finally:
        driver.quit()

if __name__ == '__main__':
    main()