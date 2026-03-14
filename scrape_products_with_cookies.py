import os
import json
import csv
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import subprocess

try:
    import psycopg2
except ImportError:
    psycopg2 = None

class ProductScraper:
    def __init__(self, csv_file, cookies_file, output_file, db_config=None):
        self.csv_file = csv_file
        self.cookies_file = cookies_file
        self.output_file = output_file
        self.driver = None
        self.last_cookie_refresh = None
        self.cookie_refresh_interval = 600
        self.products_scraped = 0
        self.db_config = db_config or self._load_db_config()
        self.db_connection = None
        self.db_table = 'eastern_scraped_products'
        self._db_warning_emitted = False
        
    def setup_driver(self):
        print("Setting up Chrome driver...")
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        
        self.driver = webdriver.Chrome(options=chrome_options)
        print("Driver setup complete")
        
    def refresh_cookies(self):
        print("\n" + "="*60)
        print("Refreshing cookies by running login script...")
        print("="*60)
        
        try:
            result = subprocess.run(
                ['python', 'login_and_save_cookies_.py'],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                print("✓ Cookies refreshed successfully")
                self.last_cookie_refresh = datetime.now()
                return True
            else:
                print(f"✗ Cookie refresh failed: {result.stderr}")
                return False
        except Exception as e:
            print(f"✗ Error refreshing cookies: {str(e)}")
            return False
    
    def load_cookies(self):
        print(f"Loading cookies from {self.cookies_file}...")
        
        if not os.path.exists(self.cookies_file):
            print(f"Cookie file not found. Running login script first...")
            if not self.refresh_cookies():
                raise Exception("Failed to generate cookies")
        
        with open(self.cookies_file, 'r') as f:
            cookies = json.load(f)
        
        self.driver.get('https://pronto.eastdist.com')
        time.sleep(2)
        
        for cookie in cookies:
            if 'expiry' in cookie:
                cookie['expiry'] = int(cookie['expiry'])
            try:
                self.driver.add_cookie(cookie)
            except Exception as e:
                print(f"Warning: Could not add cookie {cookie.get('name')}: {str(e)}")
        
        self.driver.get('https://pronto.eastdist.com')
        time.sleep(2)
        
        print(f"✓ Loaded {len(cookies)} cookies and activated session")
        self.last_cookie_refresh = datetime.now()
        
    def check_cookie_expiry(self):
        if self.last_cookie_refresh is None:
            return True
            
        elapsed = (datetime.now() - self.last_cookie_refresh).total_seconds()
        
        if elapsed >= self.cookie_refresh_interval:
            print(f"\n⏰ {self.cookie_refresh_interval/60} minutes elapsed, refreshing cookies...")
            return True
        
        return False
    
    def is_login_page(self):
        current_url = self.driver.current_url
        return 'login' in current_url.lower() or self.driver.current_url == 'https://pronto.eastdist.com/login'

    def _load_db_config(self):
        host = os.getenv('SUPABASE_HOST') or os.getenv('POSTGRES_HOST')
        dbname = os.getenv('SUPABASE_DBNAME') or os.getenv('POSTGRES_DB', 'postgres')
        user = os.getenv('SUPABASE_USER') or os.getenv('POSTGRES_USER')
        password = os.getenv('SUPABASE_PASSWORD') or os.getenv('POSTGRES_PASSWORD')
        port = os.getenv('SUPABASE_PORT') or os.getenv('POSTGRES_PORT', '5432')
        sslmode = os.getenv('POSTGRES_SSLMODE', 'require')

        if not all([host, user, password]):
            raise ValueError(
                'Database env vars required: set SUPABASE_HOST, SUPABASE_USER, SUPABASE_PASSWORD '
                '(or POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD)'
            )
        return {
            'host': host,
            'dbname': dbname,
            'user': user,
            'password': password,
            'port': port,
            'sslmode': sslmode
        }

    def connect_database(self):
        if self.db_connection or self.db_config is None:
            if self.db_config is None and not self._db_warning_emitted:
                print("PostgreSQL configuration not provided; skipping database storage.")
                self._db_warning_emitted = True
            return

        if psycopg2 is None:
            if not self._db_warning_emitted:
                print("psycopg2 is not installed; install psycopg2-binary to enable database storage.")
                self._db_warning_emitted = True
            return

        try:
            self.db_connection = psycopg2.connect(**self.db_config)
            self.db_connection.autocommit = True
            self.ensure_table()
            print("Database connection established.")
        except Exception as exc:
            print(f"Failed to connect to PostgreSQL: {exc}")
            self.db_connection = None

    def ensure_table(self):
        if not self.db_connection:
            return

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.db_table} (
                sku TEXT PRIMARY KEY,
                url TEXT,
                product_name TEXT,
                price TEXT,
                description TEXT,
                stock_status TEXT,
                brand TEXT,
                image_url TEXT,
                pack_weight TEXT,
                available_in TEXT,
                scraped_at TIMESTAMP
            )
        """

        with self.db_connection.cursor() as cursor:
            cursor.execute(create_table_query)

    def clear_existing_database_rows(self):
        """Remove existing rows before inserting new scrape results."""
        if not self.db_connection:
            return
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {self.db_table}")
            print(f"Cleared existing rows from {self.db_table}.")
        except Exception as exc:
            print(f"Warning: Failed to clear {self.db_table}: {exc}")

    def save_product_to_db(self, product_data):
        if not self.db_connection or not product_data.get('sku'):
            return

        insert_query = f"""
            INSERT INTO {self.db_table} (
                sku, url, product_name, price, description, stock_status,
                brand, image_url, pack_weight, available_in, scraped_at
            )
            VALUES (
                %(sku)s, %(url)s, %(product_name)s, %(price)s, %(description)s,
                %(stock_status)s, %(brand)s, %(image_url)s, %(pack_weight)s,
                %(available_in)s, %(scraped_at)s
            )
            ON CONFLICT (sku) DO UPDATE SET
                url = EXCLUDED.url,
                product_name = EXCLUDED.product_name,
                price = EXCLUDED.price,
                description = EXCLUDED.description,
                stock_status = EXCLUDED.stock_status,
                brand = EXCLUDED.brand,
                image_url = EXCLUDED.image_url,
                pack_weight = EXCLUDED.pack_weight,
                available_in = EXCLUDED.available_in,
                scraped_at = EXCLUDED.scraped_at
        """

        with self.db_connection.cursor() as cursor:
            cursor.execute(insert_query, product_data)

    def reset_output_csv(self, fieldnames):
        """Rewrite the CSV file header so each run starts clean."""
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
            print(f"Cleared existing CSV data in {self.output_file}.")
        except Exception as exc:
            print(f"Warning: Failed to reset CSV file {self.output_file}: {exc}")

    def close_database(self):
        if self.db_connection:
            self.db_connection.close()
            self.db_connection = None
    
    def extract_product_details(self, url):
        try:
            if self.check_cookie_expiry():
                print("Refreshing cookies due to time interval...")
                self.driver.quit()
                if self.refresh_cookies():
                    self.setup_driver()
                    self.load_cookies()
                else:
                    print("Warning: Cookie refresh failed, continuing with existing cookies...")
            
            self.driver.get(url)
            time.sleep(1.5)
            
            if self.is_login_page():
                print("⚠ Redirected to login page - cookies may be expired! Refreshing...")
                self.driver.quit()
                if self.refresh_cookies():
                    self.setup_driver()
                    self.load_cookies()
                    self.driver.get(url)
                    time.sleep(1.5)
                else:
                    return None
                    
                if self.is_login_page():
                    print("✗ Still on login page after refresh, skipping product")
                    return None
            
            product_data = {
                'url': url,
                'sku': '',
                'product_name': '',
                'price': '',
                'description': '',
                'stock_status': '',
                'brand': '',
                'image_url': '',
                'pack_weight': '',
                'available_in': ''
            }
            
            try:
                product_data['sku'] = url.split('/')[-1]
            except:
                pass
            
            try:
                json_ld_scripts = self.driver.find_elements(By.CSS_SELECTOR, 'script[type="application/ld+json"]')
                for script in json_ld_scripts:
                    try:
                        data = json.loads(script.get_attribute('innerHTML'))
                        if isinstance(data, dict) and data.get('@type') == 'Product':
                            product_data['product_name'] = data.get('name', '')
                            product_data['description'] = data.get('description', '')
                            product_data['brand'] = data.get('brand', '')
                            product_data['sku'] = data.get('sku', product_data['sku'])
                            
                            if 'offers' in data and isinstance(data['offers'], dict):
                                price = data['offers'].get('price', '')
                                if price:
                                    product_data['price'] = f"${price} (Ex GST)"
                            
                            if 'image' in data:
                                img_path = data.get('image', '')
                                if img_path and not img_path.startswith('http'):
                                    product_data['image_url'] = f"https://pronto.eastdist.com{img_path}"
                                else:
                                    product_data['image_url'] = img_path
                            break
                    except:
                        continue
            except:
                pass
            
            try:
                stock_element = self.driver.find_element(By.CSS_SELECTOR, 'span.stock-status')
                stock_text = stock_element.text.strip()
                if stock_text:
                    product_data['stock_status'] = stock_text
            except:
                pass
            
            try:
                pack_weight_element = self.driver.find_element(By.CSS_SELECTOR, 'span.pack-weight-value')
                product_data['pack_weight'] = pack_weight_element.text.strip() + ' kg/lt'
            except:
                pass
            
            try:
                page_text = self.driver.find_element(By.TAG_NAME, 'body').text
                if 'Available in:' in page_text:
                    lines = page_text.split('\n')
                    for i, line in enumerate(lines):
                        if 'Available in:' in line:
                            if i + 1 < len(lines):
                                product_data['available_in'] = lines[i+1].strip()
                            break
            except:
                pass
            
            if not product_data['product_name']:
                try:
                    title = self.driver.title
                    if '|' in title:
                        product_data['product_name'] = title.split('|')[1].strip()
                except:
                    pass
            
            self.products_scraped += 1
            return product_data
            
        except Exception as e:
            print(f"✗ Error scraping {url}: {str(e)}")
            return None
    
    def scrape_products(self, test_mode=True, test_limit=5):
        print("\n" + "="*60)
        print("STARTING PRODUCT SCRAPER")
        print("="*60)
        
        with open(self.csv_file, 'r') as f:
            reader = csv.DictReader(f)
            product_urls = [row['product_link'] for row in reader]
        
        total_products = len(product_urls)
        print(f"Found {total_products} product URLs to scrape")
        
        if test_mode:
            product_urls = product_urls[:test_limit]
            print(f"TEST MODE: Scraping first {test_limit} products only")
        
        self.setup_driver()
        self.load_cookies()
        self.connect_database()
        self.clear_existing_database_rows()

        fieldnames = ['url', 'sku', 'product_name', 'price', 'description', 'stock_status', 
                     'brand', 'image_url', 'pack_weight', 'available_in', 'scraped_at']
        self.reset_output_csv(fieldnames)

        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                for index, url in enumerate(product_urls, 1):
                    print(f"\n[{index}/{len(product_urls)}] Scraping: {url}")
                    
                    product_data = self.extract_product_details(url)
                    
                    if product_data:
                        product_data['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        writer.writerow(product_data)
                        csvfile.flush()
                        self.save_product_to_db(product_data.copy())
                        
                        print(f"  ✓ Product: {product_data['product_name'][:50]}")
                        print(f"  ✓ Price: {product_data['price']}")
                    else:
                        print(f"  ✗ Failed to scrape this product")
                    
                    if index % 10 == 0:
                        print(f"\n📊 Progress: {index}/{len(product_urls)} products scraped")
                    
                    time.sleep(0.3)
        finally:
            if self.driver:
                self.driver.quit()
            self.close_database()
        
        print("\n" + "="*60)
        print("SCRAPING COMPLETE!")
        print("="*60)
        print(f"✓ Total products scraped: {self.products_scraped}")
        print(f"✓ Data saved to: {self.output_file}")

if __name__ == "__main__":
    import sys
    
    INPUT_CSV = "attached_assets/Eastern_sku_matchedd_rows_1762833767480.csv"
    COOKIES_FILE = "pronto_cookies.json"
    OUTPUT_CSV = "eastern_scraped_data.csv"
    
    scraper = ProductScraper(INPUT_CSV, COOKIES_FILE, OUTPUT_CSV)
    
    print("\n" + "="*60)
    print("PRODUCT SCRAPER - EASTERN DISTRIBUTORS")
    print("="*60)
    print(f"Input CSV: {INPUT_CSV}")
    print(f"Cookies File: {COOKIES_FILE}")
    print(f"Output CSV: {OUTPUT_CSV}")
    print("="*60)
    
    # mode = sys.argv[1] if len(sys.argv) > 1 else 'test'
    mode = 'full'
    
    if mode == 'full':
        print("\n⚠ FULL MODE: Scraping ALL 5229 products")
        scraper.scrape_products(test_mode=False)
    else:
        test_limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        print(f"\nTEST MODE: Scraping first {test_limit} products")
        scraper.scrape_products(test_mode=True, test_limit=test_limit)
