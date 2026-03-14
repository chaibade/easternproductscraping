import os
import json
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def login_and_save_cookies():
    username = os.environ.get('EASTERN_USERNAME', '')
    password = os.environ.get('EASTERN_PASSWORD', '')
    if not username or not password:
        raise ValueError('EASTERN_USERNAME and EASTERN_PASSWORD env vars required')
    print("username,password", username, password)
    
    print("Setting up Chrome driver...")
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        print(f"Navigating to login page...")
        driver.get('https://pronto.eastdist.com/login')
        
        print("Waiting for page to load...")
        time.sleep(3)
        
        print("Looking for login form elements...")
        wait = WebDriverWait(driver, 15)
        
        username_field = wait.until(
            EC.presence_of_element_located((By.ID, 'user_email_address'))
        )
        password_field = driver.find_element(By.ID, 'user_password')
        
        print("Entering credentials...")
        username_field.clear()
        username_field.send_keys(username)
        
        password_field.clear()
        password_field.send_keys(password)
        
        print("Waiting for form to be ready...")
        time.sleep(2)
        
        print("Submitting login form...")
        submit_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.signin-btn'))
        )
        submit_button.click()
        
        print("Waiting for login to complete...")
        time.sleep(5)
        
        print(f"Current URL after login: {driver.current_url}")
        
        cookies = driver.get_cookies()
        print(f"Retrieved {len(cookies)} cookies")
        
        cookies_file = 'pronto_cookies.json'
        with open(cookies_file, 'w') as f:
            json.dump(cookies, f, indent=2)
        
        print(f"\n✓ Success! Cookies saved to {cookies_file}")
        print(f"\nCookie details:")
        for cookie in cookies:
            print(f"  - {cookie['name']}: {cookie['value'][:20]}..." if len(cookie['value']) > 20 else f"  - {cookie['name']}: {cookie['value']}")
        
    except Exception as e:
        print(f"\n✗ Error during login: {str(e)}")
        print(f"Current URL: {driver.current_url}")
        
        screenshot_file = 'error_screenshot.png'
        driver.save_screenshot(screenshot_file)
        print(f"Screenshot saved to {screenshot_file}")
        
        page_source_file = 'error_page_source.html'
        with open(page_source_file, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print(f"Page source saved to {page_source_file}")
        
    finally:
        driver.quit()
        print("\nBrowser closed")

if __name__ == "__main__":
    print("=" * 60)
    print("Pronto Login and Cookie Saver")
    print("=" * 60)
    login_and_save_cookies()
