#!/usr/bin/env python3

import os
import sys
import time
import logging
import pandas as pd
import re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pytz

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, NoAlertPresentException, WebDriverException


# =========================
# ENV VARIABLES
# =========================
EMAIL = os.environ.get("COMPANY_EMAIL")
PASSWORD = os.environ.get("COMPANY_PASSWORD")
DATABASE_URL = os.environ.get("DATABASE_URL")

if not EMAIL or not PASSWORD or not DATABASE_URL:
    sys.exit("❌ Missing environment variables")

engine = create_engine(DATABASE_URL)

LOGIN_URL = "https://admin.shurjopayment.com/login"
SETTLEMENT_CREATE_URL = "https://admin.shurjopayment.com/accounts/settlement/create"
TIMEOUT = 120
TICK_INDICATORS = {"x","X","✔","✓","1","TRUE","True","true",True}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("settlement-bot")

# Bangladesh Timezone
BD_TZ = pytz.timezone('Asia/Dhaka')
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


# =========================
# Helper function for BDT
# =========================
def get_bd_now():
    """Get current datetime in Bangladesh Time"""
    return datetime.now(BD_TZ)


def get_bd_today():
    """Get today's date in Bangladesh Time"""
    return get_bd_now().date()


def get_bd_today_name():
    """Get today's weekday name in Bangladesh Time"""
    return get_bd_now().strftime("%A")


def get_bd_yesterday_str(format="%d/%m/%Y"):
    """Get yesterday's date in Bangladesh Time as formatted string"""
    yesterday = get_bd_now() - timedelta(days=1)
    return yesterday.strftime(format)


def clean_day_columns(df: pd.DataFrame):
    """Clean day columns to handle various marker formats"""
    def _c(val):
        if pd.isna(val) or str(val).strip().lower() in {"", "none", "nan"}:
            return ""
        return re.sub(r"[^\w✓✔xX1]", "", str(val)).strip()
    for d in DAYS_OF_WEEK:
        df[d] = df[d].map(_c)
    return df


def capture_screenshot(driver, tag):
    """Take screenshot for debugging"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fn = f"{tag}_{ts}.png"
    driver.save_screenshot(fn)
    logger.debug("Screenshot saved → %s", fn)


# =========================
# WEBDRIVER INIT (DNS Fix for GitHub Actions)
# =========================
def init_webdriver():
    """Initialize Chrome driver with DNS fixes for GitHub Actions"""
    logger.info("Initializing Chrome driver with DNS fixes...")
    
    opts = Options()
    
    # Essential headless options
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    
    # DNS and network fixes for GitHub Actions
    opts.add_argument("--dns-prefetch-disable")
    opts.add_argument("--disable-features=DNS-over-HTTPS")
    opts.add_argument("--disable-web-security")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--disable-software-rasterizer")
    
    # Additional stability options
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-setuid-sandbox")
    opts.add_argument("--remote-debugging-port=9222")
    opts.add_argument("--log-level=3")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--allow-running-insecure-content")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    
    # Set DNS servers explicitly
    opts.add_argument("--host-resolver-rules=MAP * ~NOTFOUND , EXCLUDE localhost")
    
    # Experimental options
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    # Set binary location
    opts.binary_location = "/usr/bin/google-chrome"
    
    # Try multiple approaches
    attempts = [
        {"name": "Standard", "opts": opts},
        {"name": "Legacy Headless", "opts": None},  # Will create new opts
        {"name": "No DNS Prefetch", "opts": None}   # Will create new opts
    ]
    
    for attempt in attempts:
        try:
            if attempt["name"] == "Standard":
                driver = webdriver.Chrome(options=opts)
            elif attempt["name"] == "Legacy Headless":
                new_opts = Options()
                new_opts.add_argument("--headless")
                new_opts.add_argument("--no-sandbox")
                new_opts.add_argument("--disable-dev-shm-usage")
                new_opts.add_argument("--disable-gpu")
                new_opts.add_argument("--window-size=1920,1080")
                new_opts.add_argument("--dns-prefetch-disable")
                new_opts.add_argument("--host-resolver-rules=MAP * ~NOTFOUND , EXCLUDE localhost")
                driver = webdriver.Chrome(options=new_opts)
            else:  # No DNS Prefetch
                new_opts = Options()
                new_opts.add_argument("--headless=new")
                new_opts.add_argument("--no-sandbox")
                new_opts.add_argument("--disable-dev-shm-usage")
                new_opts.add_argument("--disable-gpu")
                new_opts.add_argument("--window-size=1920,1080")
                new_opts.add_argument("--disable-features=DNS-over-HTTPS")
                new_opts.add_argument("--disable-web-security")
                driver = webdriver.Chrome(options=new_opts)
            
            driver.implicitly_wait(5)
            logger.info(f"✅ Chrome driver initialized with {attempt['name']} approach")
            
            # Test DNS resolution
            try:
                driver.get("https://admin.shurjopayment.com")
                logger.info("✅ DNS resolution test passed")
                return driver
            except Exception as dns_error:
                logger.warning(f"DNS test failed with {attempt['name']}: {str(dns_error)}")
                driver.quit()
                continue
                
        except Exception as e:
            logger.warning(f"Attempt {attempt['name']} failed: {str(e)}")
            continue
    
    # If all attempts fail, raise exception
    raise Exception("❌ All Chrome driver initialization attempts failed")


def test_connection(driver):
    """Test internet connection and DNS resolution"""
    try:
        logger.info("Testing connection to shurjopayment.com...")
        driver.set_page_load_timeout(10)
        driver.get("https://admin.shurjopayment.com")
        logger.info("✅ Connection successful")
        return True
    except Exception as e:
        logger.error(f"❌ Connection test failed: {str(e)}")
        return False


# =========================
# DATABASE FUNCTIONS
# =========================
def read_data_from_db():
    with engine.begin() as conn:
        df = pd.read_sql("SELECT * FROM settlement_day", conn)
    return df


def update_from_date(record_id):
    today_date = get_bd_today()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE settlement_day
            SET from_date=:d,
                updated_at=NOW()
            WHERE id=:i
        """), {"d": today_date, "i": record_id})
        logger.info(f"✅ DB updated for ID {record_id}: from_date set to {today_date}")


# =========================
# SELENIUM STEPS
# =========================
def perform_login(driver, wait):
    """Login and open settlement page in new tab with retry"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Login attempt {attempt + 1}/{max_retries}")
            
            driver.get(LOGIN_URL)
            
            # Enter Email
            wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(EMAIL)
            
            # Enter Password
            pw = driver.find_element(By.ID, "password-field")
            pw.send_keys(PASSWORD)
            pw.send_keys(Keys.RETURN)

            # Wait for login to complete
            wait.until(EC.url_changes(LOGIN_URL))
            time.sleep(2)
            logger.info("✅ Login successful")
            
            # Open settlement page in new tab
            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[1])
            driver.get(SETTLEMENT_CREATE_URL)
            wait.until(EC.presence_of_element_located((By.ID, "select2-merchant_id-container")))
            logger.info("✅ Settlement page opened")
            return True
            
        except Exception as e:
            logger.warning(f"Login attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(5)
                # Test connection before retry
                if not test_connection(driver):
                    logger.warning("Connection issue detected, waiting longer...")
                    time.sleep(10)
            else:
                raise e
    
    return False


def select_merchant(driver, wait, merchant_name):
    """Select merchant from dropdown"""
    logger.info(f"Selecting merchant: {merchant_name}")
    wait.until(EC.element_to_be_clickable((By.ID, "select2-merchant_id-container"))).click()
    box = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input.select2-search__field")))
    box.clear()
    box.send_keys(merchant_name)
    time.sleep(2)
    
    try:
        wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//li[contains(@class,'select2-results__option') and text()='{merchant_name}']")
            )
        ).click()
        logger.info(f"Merchant selected: {merchant_name}")
        return True
    except Exception as e:
        logger.error(f"Could not select merchant {merchant_name}: {str(e)}")
        return False


def get_available_stores(driver, wait):
    """Get all available store options from dropdown"""
    try:
        store_select = wait.until(EC.element_to_be_clickable((By.ID, "store_id")))
        select = Select(store_select)
        options = []
        for option in select.options:
            if option.text.strip() and option.text.strip().lower() not in ['select store', '']:
                options.append({
                    'text': option.text.strip(),
                    'value': option.get_attribute('value')
                })
        return options
    except Exception as e:
        logger.error(f"Error getting store options: {str(e)}")
        return []


def select_store_by_name(driver, wait, store_name):
    """Select store by name only"""
    logger.info(f"Attempting to select store by name: '{store_name}'")
    
    # Get all available stores
    available_stores = get_available_stores(driver, wait)
    
    if not available_stores:
        logger.error("No stores available in dropdown")
        return False
    
    # Log available stores for debugging
    logger.info(f"Available stores ({len(available_stores)}):")
    for i, store in enumerate(available_stores[:10]):
        logger.info(f"  {i+1}. '{store['text']}' (value: {store['value']})")
    if len(available_stores) > 10:
        logger.info(f"  ... and {len(available_stores) - 10} more")
    
    # Clean store name for comparison
    store_name_clean = store_name.strip().lower()
    
    # Try exact match first
    for store in available_stores:
        if store['text'].strip().lower() == store_name_clean:
            logger.info(f"Found exact match: '{store['text']}'")
            select = Select(driver.find_element(By.ID, "store_id"))
            select.select_by_visible_text(store['text'])
            return True
    
    # Try partial match
    for store in available_stores:
        if store_name_clean in store['text'].strip().lower():
            logger.info(f"Found partial match: '{store['text']}' contains '{store_name}'")
            select = Select(driver.find_element(By.ID, "store_id"))
            select.select_by_visible_text(store['text'])
            return True
    
    logger.error(f"Store '{store_name}' not found in dropdown")
    return False


def enter_dates(driver, wait, from_d, to_d):
    """Enter from and to dates"""
    f = wait.until(EC.presence_of_element_located((By.ID, "fromDate")))
    f.clear()
    f.send_keys(from_d)
    
    t = driver.find_element(By.ID, "toDate")
    t.clear()
    t.send_keys(to_d)


def submit_and_verify_settlement(driver, wait, original_url):
    """Submit the settlement form and verify result based on popup type"""
    logger.info("Clicking create settlement button...")
    
    # Click the create button
    submit_btn = wait.until(EC.element_to_be_clickable((By.ID, "create_settlement")))
    submit_btn.click()
    
    # ===== STEP 1: First check for immediate popup (No Transactions) =====
    logger.info("Checking for immediate popup...")
    time.sleep(5)
    
    # Check for "No Transactions" warning popup
    try:
        # Check for warning icon
        warning_icon = driver.find_elements(By.XPATH, "//div[contains(@class, 'swal2-icon') and contains(@class, 'swal2-warning')]")
        
        # Check for "No Transactions" title
        no_transaction_title = driver.find_elements(By.XPATH, "//h2[contains(@class, 'swal2-title') and text()='No Transactions']")
        
        # Check for the message about no eligible transactions
        no_transaction_msg = driver.find_elements(By.XPATH, "//div[contains(@class, 'swal2-html-container') and contains(text(), 'No eligible transactions')]")
        
        if warning_icon and (no_transaction_title or no_transaction_msg):
            logger.info("✅ DETECTED: No Transactions popup")
            
            # Click OK button to dismiss popup
            try:
                ok_btn = driver.find_elements(By.XPATH, "//button[contains(@class, 'swal2-confirm') and text()='OK']")
                if not ok_btn:
                    ok_btn = driver.find_elements(By.XPATH, "//button[text()='OK']")
                if not ok_btn:
                    ok_btn = driver.find_elements(By.XPATH, "//div[@class='swal2-actions']//button[text()='OK']")
                
                if ok_btn:
                    ok_btn[0].click()
                    logger.info("Popup dismissed")
                    time.sleep(2)
                else:
                    logger.warning("OK button not found, but popup detected")
            except Exception as e:
                logger.warning(f"Could not click OK button: {str(e)}")
            
            return "no_eligible"
    except Exception as e:
        logger.debug(f"Error checking for warning popup: {str(e)}")
    
    # ===== STEP 2: If no popup, wait for redirect (transactions exist) =====
    logger.info("No immediate popup detected. Waiting up to 60 seconds for redirect...")
    
    max_wait = 60
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        current_url = driver.current_url
        
        # Check if URL changed to a different page (not the create page)
        if current_url != original_url and "create" not in current_url:
            logger.info(f"✅ Redirected to different page after {int(time.time() - start_time)} seconds: {current_url}")
            return "success"
        
        # Also check for any success popup
        try:
            success_icon = driver.find_elements(By.XPATH, "//div[contains(@class, 'swal2-icon') and contains(@class, 'swal2-success')]")
            if success_icon:
                logger.info(f"✅ Success popup detected after {int(time.time() - start_time)} seconds")
                return "success"
        except:
            pass
        
        # Progress update every 10 seconds
        elapsed = int(time.time() - start_time)
        if elapsed % 10 == 0 and elapsed > 0:
            logger.info(f"Still waiting... ({elapsed} seconds elapsed)")
        
        time.sleep(2)
    
    # ===== STEP 3: If we're here after 60 seconds, something is wrong =====
    logger.warning(f"⚠️ No redirect after {max_wait} seconds - manual check required")
    return "uncertain"


def navigate_back_to_settlement_page(driver, wait):
    """Navigate back to settlement page with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info("Navigating back to settlement page (attempt %s/%s)", attempt + 1, max_retries)
            driver.get(SETTLEMENT_CREATE_URL)
            wait.until(EC.presence_of_element_located((By.ID, "select2-merchant_id-container")))
            time.sleep(2)
            return True
        except Exception as nav_error:
            logger.warning("Navigation attempt %s failed: %s", attempt + 1, nav_error)
            if attempt < max_retries - 1:
                time.sleep(5)
                try:
                    driver.refresh()
                    time.sleep(2)
                except:
                    pass
            else:
                logger.error("Failed to navigate back after %s attempts", max_retries)
                return False
    return False


# =========================
# MAIN PROCESS
# =========================
def main():
    logger.info(f"Starting daily settlement at {get_bd_now()}")
    
    # Statistics tracking
    stats = {
        'total_queued': 0,
        'confirmed_success': 0,
        'no_eligible': 0,
        'uncertain': 0,
        'errors': 0
    }
    
    # Track stores for reporting
    confirmed_stores = []
    no_eligible_stores = []
    uncertain_stores = []
    error_stores = []

    driver = None
    try:
        # Initialize driver with retry
        max_init_retries = 3
        for init_attempt in range(max_init_retries):
            try:
                driver = init_webdriver()
                break
            except Exception as e:
                logger.warning(f"Driver init attempt {init_attempt + 1} failed: {str(e)}")
                if init_attempt == max_init_retries - 1:
                    raise e
                time.sleep(5)
        
        wait = WebDriverWait(driver, TIMEOUT)

        # Test connection first
        if not test_connection(driver):
            raise Exception("Cannot connect to shurjopayment.com")

        # Login and setup
        perform_login(driver, wait)

        # Read and prepare data
        df = read_data_from_db()
        df = clean_day_columns(df)
        
        # Get today's weekday
        bd_now = get_bd_now()
        bd_today_name = get_bd_today_name()
        logger.info(f"Current Bangladesh Time: {bd_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"Today's weekday in BD: {bd_today_name}")

        # Filter merchants scheduled for today
        df_today = df[df[bd_today_name].astype(str).str.strip().isin(['1', '✓', '✔', 'x', 'X'])]
        stats['total_queued'] = len(df_today)

        if df_today.empty:
            logger.info(f"No merchants scheduled for {bd_today_name}")
            return

        logger.info(f"{len(df_today)} merchants to process for {bd_today_name}")

        # Process each merchant
        for index, row in df_today.iterrows():
            merchant_name = str(row["merchant_name"]).strip()
            store_name = str(row["store_name"]).strip() if pd.notna(row["store_name"]) else ""
            
            logger.info(f"▶ PROCESSING ({index+1}/{len(df_today)}): {merchant_name}")
            logger.info(f"Store name: '{store_name}'")
            
            from_date = pd.to_datetime(row["from_date"]).strftime("%d/%m/%Y")
            to_date = get_bd_yesterday_str("%d/%m/%Y")
            logger.info(f"Date range: {from_date} to {to_date}")

            original_url = driver.current_url
            record_id = row["id"]
            
            try:
                # Select merchant
                if not select_merchant(driver, wait, merchant_name):
                    logger.error(f"Failed to select merchant: {merchant_name}")
                    stats['errors'] += 1
                    error_stores.append(f"{merchant_name} - Merchant not found")
                    continue
                
                # Wait for store dropdown to populate
                time.sleep(2)
                
                # Select store by name
                store_selected = select_store_by_name(driver, wait, store_name)
                
                if not store_selected:
                    logger.error(f"Failed to select store '{store_name}' for merchant {merchant_name}")
                    stats['errors'] += 1
                    error_stores.append(f"{merchant_name} - Store '{store_name}' not found")
                    capture_screenshot(driver, f"store_not_found_{merchant_name}")
                    continue
                
                # Enter dates
                enter_dates(driver, wait, from_date, to_date)
                
                # Submit and verify settlement creation
                result = submit_and_verify_settlement(driver, wait, original_url)
                
                if result == "success":
                    update_from_date(record_id)
                    stats['confirmed_success'] += 1
                    confirmed_stores.append(f"{merchant_name} - {store_name}")
                    logger.info(f"✅ CONFIRMED SUCCESS: {merchant_name} - {store_name}")
                elif result == "no_eligible":
                    stats['no_eligible'] += 1
                    no_eligible_stores.append(f"{merchant_name} - {store_name}")
                    logger.info(f"ℹ️ No eligible transactions for {merchant_name} - {store_name}")
                else:  # uncertain
                    stats['uncertain'] += 1
                    uncertain_stores.append(f"{merchant_name} - {store_name}")
                    logger.warning(f"⚠️ UNCERTAIN - Manual check required: {merchant_name} - {store_name}")

            except Exception as e:
                logger.error(f"❌ ERROR: {merchant_name} - {store_name} → {str(e)}")
                stats['errors'] += 1
                error_stores.append(f"{merchant_name} - {store_name} ({str(e)[:50]}...)")
                capture_screenshot(driver, f"error_{merchant_name}")

            finally:
                # Navigate back to settlement page
                if not navigate_back_to_settlement_page(driver, wait):
                    logger.critical("Critical navigation failure. Restarting browser...")
                    driver.quit()
                    driver = init_webdriver()
                    wait = WebDriverWait(driver, TIMEOUT)
                    perform_login(driver, wait)

        # Final Report
        logger.info("")
        logger.info("=" * 60)
        logger.info("SETTLEMENT PROCESSING REPORT - %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        logger.info("=" * 60)
        logger.info(f"Total queued for today: {stats['total_queued']}")
        logger.info(f"✅ Confirmed Success (DB updated): {stats['confirmed_success']}")
        logger.info(f"ℹ️ No eligible transactions (DB not updated): {stats['no_eligible']}")
        logger.info(f"⚠️ Uncertain - Manual check required (DB not updated): {stats['uncertain']}")
        logger.info(f"❌ Errors: {stats['errors']}")
        logger.info("=" * 60)
        
        if confirmed_stores:
            logger.info("✅ CONFIRMED SUCCESS - DB UPDATED ({0}):".format(len(confirmed_stores)))
            for store in confirmed_stores:
                logger.info("  ✅ %s", store)
        
        if no_eligible_stores:
            logger.info("ℹ️ NO ELIGIBLE TRANSACTIONS - DB NOT UPDATED ({0}):".format(len(no_eligible_stores)))
            for store in no_eligible_stores:
                logger.info("  ℹ️ %s", store)
        
        if uncertain_stores:
            logger.info("⚠️ NEED MANUAL CHECK - DB NOT UPDATED ({0}):".format(len(uncertain_stores)))
            for store in uncertain_stores:
                logger.info("  ⚠️ %s", store)
        
        if error_stores:
            logger.info("❌ ERRORS ({0}):".format(len(error_stores)))
            for store in error_stores[:10]:
                logger.info("  ❌ %s", store)
            if len(error_stores) > 10:
                logger.info("  ... and %s more", len(error_stores) - 10)
        
        logger.info("=" * 60)
        
        # Summary recommendation
        if stats['uncertain'] > 0:
            logger.info("⚠️ RECOMMENDATION: Please manually check the {0} uncertain settlements in the ShurjoPay admin panel. DB was NOT updated for these.".format(stats['uncertain']))
        if stats['confirmed_success'] > 0:
            logger.info("✅ {0} settlements were successfully created and DB updated.".format(stats['confirmed_success']))
        if stats['no_eligible'] > 0:
            logger.info("ℹ️ {0} merchants had no eligible transactions. DB was NOT updated.".format(stats['no_eligible']))
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error during settlement: {str(e)}")
        if driver:
            capture_screenshot(driver, "fatal_error")
    finally:
        if driver:
            driver.quit()
            logger.info("Browser closed")


if __name__ == "__main__":
    main()
