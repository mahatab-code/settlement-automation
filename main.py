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
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, NoAlertPresentException


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
# WEBDRIVER INIT (Linux Safe)
# =========================
def init_webdriver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--log-level=3")

    driver = webdriver.Chrome(options=opts)
    driver.implicitly_wait(5)
    return driver


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


# =========================
# SELENIUM STEPS (from working script)
# =========================
def perform_login(driver, wait):
    """Login and open settlement page in new tab"""
    driver.get(LOGIN_URL)
    
    # Enter Email
    wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(EMAIL)
    
    # Enter Password
    pw = driver.find_element(By.ID, "password-field")
    pw.send_keys(PASSWORD)
    pw.send_keys(Keys.RETURN)

    wait.until(EC.url_changes(LOGIN_URL))
    time.sleep(1)

    # Open settlement page in new tab
    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[1])
    driver.get(SETTLEMENT_CREATE_URL)
    wait.until(EC.presence_of_element_located((By.ID, "select2-merchant_id-container")))
    logger.info("Login successful & settlement page opened")


def select_merchant(driver, wait, name):
    """Select merchant from dropdown"""
    wait.until(EC.element_to_be_clickable((By.ID, "select2-merchant_id-container"))).click()
    box = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input.select2-search__field")))
    box.clear()
    box.send_keys(name)
    time.sleep(1)
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, f"//li[contains(@class,'select2-results__option') and text()='{name}']")
        )
    ).click()


def select_store(driver, wait, store_name, store_id):
    """
    Select store by name first, if not available fall back to store ID
    """
    store_select = wait.until(EC.element_to_be_clickable((By.ID, "store_id")))
    select = Select(store_select)
    
    # First try to find by store name
    store_name_clean = str(store_name).strip()
    if store_name_clean and store_name_clean.lower() not in ['nan', 'none', '']:
        for option in select.options:
            if option.text.strip() == store_name_clean:
                option.click()
                logger.info("Store selected by name: %s", store_name_clean)
                return True
        
        logger.warning("Store name '%s' not found in dropdown, falling back to store ID", store_name_clean)
    
    # Fall back to store ID
    store_id_clean = str(store_id).strip()
    if store_id_clean and store_id_clean.lower() not in ['nan', 'none', '']:
        try:
            select.select_by_value(store_id_clean)
            logger.info("Store selected by ID: %s", store_id_clean)
            return True
        except NoSuchElementException:
            logger.error("Store ID '%s' also not found in dropdown", store_id_clean)
            return False
    
    logger.error("Neither store name '%s' nor store ID '%s' found", store_name_clean, store_id_clean)
    return False


def enter_dates(driver, wait, from_d, to_d):
    """Enter from and to dates"""
    f = wait.until(EC.presence_of_element_located((By.ID, "fromDate")))
    f.clear()
    f.send_keys(from_d)
    
    t = driver.find_element(By.ID, "toDate")
    t.clear()
    t.send_keys(to_d)


def submit_form(driver, wait):
    """Submit the settlement form"""
    wait.until(EC.element_to_be_clickable((By.ID, "create_settlement"))).click()


def confirm_submission(driver, wait, original_url):
    """Check if submission was successful"""
    cond = EC.any_of(
        EC.url_changes(original_url),
        EC.presence_of_element_located((By.XPATH, "//div[@id='swal2-html-container' and contains(text(),'No eligible transactions')]")),
        EC.alert_is_present(),
    )
    WebDriverWait(driver, 120).until(cond)

    if driver.current_url != original_url:
        return True  # success
    try:
        driver.switch_to.alert.accept()
    except NoAlertPresentException:
        try:
            driver.find_element(By.XPATH, "//button[text()='OK']").click()
        except NoSuchElementException:
            pass
    return False


def navigate_back_to_settlement_page(driver, wait):
    """Navigate back to settlement page with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info("Navigating back to settlement page (attempt %s/%s)", attempt + 1, max_retries)
            driver.get(SETTLEMENT_CREATE_URL)
            wait.until(EC.presence_of_element_located((By.ID, "select2-merchant_id-container")))
            time.sleep(0.8)
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
    # Statistics tracking
    stats = {
        'total_queued': 0,
        'processed_success': 0,
        'no_eligible': 0,
        'errors': 0
    }
    
    # Track processed stores for reporting
    processed_stores = []
    error_stores = []

    driver = init_webdriver()
    wait = WebDriverWait(driver, TIMEOUT)

    try:
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
            merchant = str(row["merchant_name"]).strip()
            store_id = str(row["store_id"]).strip()
            store_name = str(row["store_name"]).strip() if pd.notna(row["store_name"]) else ""
            from_date = pd.to_datetime(row["from_date"]).strftime("%d/%m/%Y")
            to_date = get_bd_yesterday_str("%d/%m/%Y")

            logger.info(f"▶ PROCESSING ({index+1}/{len(df_today)}): {merchant} (Store: '{store_name}', ID: {store_id})")
            logger.info(f"Date range: {from_date} to {to_date}")

            original_url = driver.current_url
            
            try:
                # Select merchant
                select_merchant(driver, wait, merchant)
                wait.until(lambda d: len(Select(d.find_element(By.ID, "store_id")).options) > 1)
                
                # Select store (prioritize store name)
                store_selected = select_store(driver, wait, store_name, store_id)
                
                if not store_selected:
                    logger.error(f"Failed to select store for {merchant} - skipping")
                    stats['errors'] += 1
                    error_stores.append(f"{merchant} - {store_name} (Store not found)")
                    continue
                
                # Enter dates and submit
                enter_dates(driver, wait, from_date, to_date)
                submit_form(driver, wait)

                # Check submission result
                if confirm_submission(driver, wait, original_url):
                    update_from_date(row["id"])
                    stats['processed_success'] += 1
                    processed_stores.append(f"{merchant} - {store_name}")
                    logger.info(f"✅ SUCCESS: {merchant} - {store_name}")
                else:
                    stats['no_eligible'] += 1
                    logger.info(f"ℹ️ No eligible transactions for {merchant} - {store_name}")

            except Exception as e:
                logger.error(f"❌ ERROR: {merchant} / {store_id} → {str(e)}")
                stats['errors'] += 1
                error_stores.append(f"{merchant} - {store_name} ({str(e)[:50]}...)")
                capture_screenshot(driver, f"error_{merchant}_{store_id}")

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
        logger.info(f"Successfully processed: {stats['processed_success']}")
        logger.info(f"No eligible transactions: {stats['no_eligible']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info("=" * 60)
        
        if processed_stores:
            logger.info("SUCCESSFULLY PROCESSED STORES (%s):", len(processed_stores))
            for store in processed_stores:
                logger.info("  ✅ %s", store)
        
        if error_stores:
            logger.info("STORES WITH ERRORS (%s):", len(error_stores))
            for store in error_stores[:10]:
                logger.info("  ❌ %s", store)
            if len(error_stores) > 10:
                logger.info("  ... and %s more", len(error_stores) - 10)
        
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        capture_screenshot(driver, "fatal_error")

    finally:
        driver.quit()
        logger.info("Browser closed")


if __name__ == "__main__":
    main()
