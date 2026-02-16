#!/usr/bin/env python3
"""
Automated settlement creator — weekday-only version
(Updated 2025-12-21) - Added skip.txt feature
"""

import os, sys, time, logging, re
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException,
    NoAlertPresentException, WebDriverException
)

from dotenv import load_dotenv
from logging.handlers import TimedRotatingFileHandler
from webdriver_manager.chrome import ChromeDriverManager  # ✅ webdriver-manager

# ─────────────────────── ENV & CONSTANTS ──────────────────────────
load_dotenv()
EMAIL     = os.getenv("COMPANY_EMAIL")
PASSWORD  = os.getenv("COMPANY_PASSWORD")
if not EMAIL or not PASSWORD:
    sys.exit("❌  Please set COMPANY_EMAIL and COMPANY_PASSWORD in your environment.")

BASE_URL              = "https://admin.shurjopayment.com/"
SETTLEMENT_CREATE_URL = f"{BASE_URL}accounts/settlement/create"
TIMEOUT               = 120

DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday",
                "Thursday", "Friday", "Saturday", "Sunday"]
TICK_INDICATORS = {"x","X","✔","✓","1","TRUE","True","true",True}

# ───────────────────────── LOGGING ────────────────────────────────
logger = logging.getLogger("settlement-bot")
logger.setLevel(logging.INFO)
for h in list(logger.handlers):
    logger.removeHandler(h)

fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file_handler = TimedRotatingFileHandler(
    "automation.log", when="midnight", interval=1,
    backupCount=30, encoding="utf-8"
)
file_handler.suffix = "%Y-%m-%d"
file_handler.setFormatter(fmt)
console_handler = logging.StreamHandler()
console_handler.setFormatter(fmt)
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.info("Logging configured — daily rotation, 30-day retention")

# ─────────────────────── DB HELPERS ───────────────────────────────
def get_db_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")

    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    return create_engine(db_url, pool_pre_ping=True)


def read_data_from_db():
    with get_db_engine().begin() as conn:
        df = pd.read_sql("SELECT * FROM settlement_day", conn)
    logger.info("Loaded %s rows from settlement_day", len(df))
    return df

def update_from_date_in_db(record_id:int, new_date:str):
    new_dt = datetime.strptime(new_date, "%d/%m/%Y").date()
    with get_db_engine().begin() as conn:
        conn.execute(
            text("UPDATE settlement_day SET from_date=:d WHERE id=:i"),
            {"d": new_dt, "i": record_id}
        )
    logger.info("id %s: from_date → %s", record_id, new_dt)

# ─────────────────────── SKIP.TXT HANDLER ─────────────────────────
def load_skip_list():
    """
    Load merchant-store combinations from skip.txt
    Format: merchant_name,store_name
    Example:
      Combined Military Hospital (CMH) Dhaka,OPDCashPoint-1
      Combined Military Hospital (CMH) Dhaka,OPDCashPoint-4
      LTDEZ,  (empty store means skip all stores for this merchant)
    """
    skip_file = "skip.txt"
    if not os.path.exists(skip_file):
        logger.warning("skip.txt not found. No stores will be skipped.")
        logger.info("To skip stores, create skip.txt with format: merchant_name,store_name")
        return set()
    
    try:
        skip_set = set()
        valid_rows = 0
        
        with open(skip_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line_num, line in enumerate(lines, 1):
            try:
                # Skip empty lines and comments
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # Split by comma
                parts = line.split(',', 1)  # Split into max 2 parts
                
                if len(parts) == 2:
                    merchant = parts[0].strip()
                    store = parts[1].strip()
                elif len(parts) == 1:
                    merchant = parts[0].strip()
                    store = ""
                else:
                    logger.warning("Line %s invalid format: %s", line_num, line)
                    continue
                
                # Skip empty merchant
                if not merchant:
                    logger.warning("Line %s has empty merchant: %s", line_num, line)
                    continue
                
                # Add to skip set
                skip_set.add((merchant, store))
                valid_rows += 1
                
                # Log first few entries
                if valid_rows <= 3:
                    store_display = store if store else "(all stores)"
                    logger.debug("Skip entry %s: Merchant='%s', Store='%s'", valid_rows, merchant, store_display)
                    
            except Exception as line_error:
                logger.warning("Error processing line %s: %s - %s", line_num, line, line_error)
                continue
        
        logger.info("Loaded %s valid merchant-store combinations from skip.txt", valid_rows)
        
        # Log summary
        if skip_set:
            empty_store_count = sum(1 for item in skip_set if not item[1])
            
            logger.info("Skip list summary:")
            logger.info("  - Total entries: %s", len(skip_set))
            logger.info("  - Skip all stores for merchant: %s", empty_store_count)
            logger.info("  - Skip specific stores: %s", len(skip_set) - empty_store_count)
            
            # Show first 5 entries
            logger.info("First 5 skip entries:")
            count = 0
            for merchant, store in skip_set:
                if count >= 5:
                    break
                store_display = store if store else "(ALL stores)"
                logger.info("  %s. Merchant: %s, Store: %s", count + 1, merchant, store_display)
                count += 1
        
        return skip_set
        
    except Exception as e:
        logger.error("Error loading skip.txt: %s", e)
        logger.info("Please ensure skip.txt format is:")
        logger.info("  merchant_name,store_name")
        logger.info("Example:")
        logger.info("  Combined Military Hospital (CMH) Dhaka,OPDCashPoint-1")
        logger.info("  Combined Military Hospital (CMH) Dhaka,OPDCashPoint-4")
        logger.info("  LTDEZ,  # empty store means skip all LTDEZ stores")
        return set()

def should_skip_merchant_store(merchant, store_name, skip_set):
    """
    Check if a merchant-store combination should be skipped
    """
    if not skip_set:
        return False
    
    merchant_clean = str(merchant).strip()
    store_clean = str(store_name).strip() if pd.notna(store_name) and str(store_name).strip() else ""
    
    # Check for exact match
    if (merchant_clean, store_clean) in skip_set:
        return True
    
    # Check if all stores for this merchant should be skipped (empty store in skip list)
    if (merchant_clean, "") in skip_set:
        return True
    
    return False

# ─────────────────────── SELENIUM SETUP ───────────────────────────
def init_webdriver():
    opts = Options()
    opts.add_argument("--headless=new")   # for modern Chrome
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--log-level=3")

    # 1️⃣ Main path: webdriver-manager (auto downloads correct driver)
    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
        logger.info("Chrome started via webdriver-manager → %s", service.path)
        driver.implicitly_wait(5)
        return driver
    except Exception as wm_err:
        logger.warning("webdriver-manager failed → %s", wm_err)

    # 2️⃣ Fallback: use local chromedriver.exe if webdriver-manager fails
    driver_path = r"F:\All Settelment\settlement-Python\chromedriver.exe"
    if not os.path.exists(driver_path):
        raise FileNotFoundError(
            f"chromedriver.exe not found at {driver_path} "
            "and webdriver-manager could not fetch a driver."
        )

    # Older Chromes may not understand '--headless=new'
    if "--headless=new" in opts.arguments:
        opts.arguments.remove("--headless=new")
        opts.add_argument("--headless")

    driver = webdriver.Chrome(
        service=ChromeService(driver_path),
        options=opts
    )
    driver.implicitly_wait(5)
    logger.info("Chrome started via local driver (%s)", driver_path)
    return driver

def capture_screenshot(driver, tag):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fn = f"{tag}_{ts}.png"
    driver.save_screenshot(fn)
    logger.debug("Screenshot saved → %s", fn)

# ─────────────────────── UTILITIES ────────────────────────────────
def get_yesterday_date() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

def today_str() -> str:
    return datetime.now().strftime("%d/%m/%Y")

def clean_day_columns(df: pd.DataFrame):
    def _c(val):
        if pd.isna(val) or str(val).strip().lower() in {"", "none", "nan"}:
            return ""
        return re.sub(r"[^\w✓✔xX1]", "", str(val)).strip()
    for d in DAYS_OF_WEEK:
        df[d] = df[d].map(_c)
    return df

def validate_columns(df):
    needed = ["id","merchant_name","store_id","store_name","from_date"] + DAYS_OF_WEEK
    missing = [c for c in needed if c not in df.columns]
    if missing:
        sys.exit(f"❌ Missing columns: {missing}")

def validate_day_markers(df):
    for d in DAYS_OF_WEEK:
        bad = df[(df[d]!="") & ~df[d].isin(TICK_INDICATORS)][d].unique()
        if len(bad):
            logger.warning("Column %s has unknown markers: %s", d, bad)

# ─────────────────────── SELENIUM STEPS ───────────────────────────
def perform_login(driver, wait):
    driver.get(BASE_URL)
    wait.until(EC.presence_of_element_located((By.ID,"email"))).send_keys(EMAIL)
    pw = wait.until(EC.presence_of_element_located((By.ID,"password-field")))
    pw.send_keys(PASSWORD)
    pw.send_keys(Keys.RETURN)

    wait.until(EC.url_changes(BASE_URL))
    time.sleep(1)

    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[1])
    driver.get(SETTLEMENT_CREATE_URL)
    wait.until(EC.presence_of_element_located((By.ID,"select2-merchant_id-container")))
    logger.info("Login successful & settlement page opened")

def select_merchant(driver, wait, name):
    wait.until(EC.element_to_be_clickable((By.ID,"select2-merchant_id-container"))).click()
    box = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR,"input.select2-search__field")))
    box.send_keys(name)
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, f"//li[contains(@class,'select2-results__option') and text()='{name}']"))
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
    f = wait.until(EC.presence_of_element_located((By.ID,"fromDate")))
    f.clear(); f.send_keys(from_d)
    t = driver.find_element(By.ID,"toDate")
    t.clear(); t.send_keys(to_d)

def submit_form(driver, wait):
    wait.until(EC.element_to_be_clickable((By.ID,"create_settlement"))).click()

def confirm_submission(driver, wait, original_url):
    cond = EC.any_of(
        EC.url_changes(original_url),
        EC.presence_of_element_located((By.XPATH,"//div[@id='swal2-html-container' and contains(text(),'No eligible transactions')]")),
        EC.alert_is_present(),
    )
    WebDriverWait(driver, 120).until(cond)

    if driver.current_url != original_url:
        return True  # success
    try:
        driver.switch_to.alert.accept()
    except NoAlertPresentException:
        try:
            driver.find_element(By.XPATH,"//button[text()='OK']").click()
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
            wait.until(EC.presence_of_element_located((By.ID,"select2-merchant_id-container")))
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

# ───────────────────────── MAIN LOOP ──────────────────────────────
def main():
    # Load skip list first
    skip_set = load_skip_list()
    
    driver = init_webdriver()
    wait   = WebDriverWait(driver, TIMEOUT)
    
    # Statistics tracking
    stats = {
        'total_queued': 0,
        'skipped': 0,
        'processed_success': 0,
        'no_eligible': 0,
        'errors': 0
    }
    
    # Track processed stores for reporting
    processed_stores = []
    skipped_stores = []
    error_stores = []

    try:
        perform_login(driver, wait)

        df = read_data_from_db()
        df = clean_day_columns(df)
        validate_columns(df)
        validate_day_markers(df)

        # Filter out rows with default from_date "2030-01-01"
        df = df[~df["from_date"].astype(str).str.contains("2030-01-01")]
        logger.info("Filtered out default rows (2030-01-01), remaining: %s rows", len(df))
        
        today_day = datetime.now().strftime("%A")
        df_today  = df[df[today_day].isin(TICK_INDICATORS)]
        stats['total_queued'] = len(df_today)
        logger.info("%s rows queued for %s", len(df_today), today_day)
        
        if df_today.empty:
            logger.info("No stores to process today.")
            return

        for idx, r in df_today.iterrows():
            merchant   = str(r["merchant_name"]).strip()
            store_id   = str(r["store_id"]).strip()
            store_name = str(r["store_name"]).strip() if pd.notna(r["store_name"]) else ""
            from_date  = pd.to_datetime(r["from_date"]).strftime("%d/%m/%Y")
            to_date    = get_yesterday_date()
            
            # Check if this merchant-store should be skipped
            if should_skip_merchant_store(merchant, store_name, skip_set):
                logger.info("⏭️ SKIPPED: %s (store: '%s', ID: %s) - Listed in skip.txt", 
                           merchant, store_name, store_id)
                stats['skipped'] += 1
                skipped_stores.append(f"{merchant} - {store_name}")
                continue
            
            logger.info("▶ PROCESSING: %s (store: '%s', ID: %s)  %s → %s", 
                       merchant, store_name, store_id, from_date, to_date)

            original_url = driver.current_url
            try:
                select_merchant(driver, wait, merchant)
                wait.until(lambda d: len(Select(d.find_element(By.ID,"store_id")).options) > 1)
                
                # Use the updated select_store function that prioritizes store name
                store_selected = select_store(driver, wait, store_name, store_id)
                
                if not store_selected:
                    logger.error("Failed to select store for %s - skipping", merchant)
                    stats['errors'] += 1
                    error_stores.append(f"{merchant} - {store_name} (Store not found)")
                    continue
                    
                enter_dates(driver, wait, from_date, to_date)
                submit_form(driver, wait)

                if confirm_submission(driver, wait, original_url):
                    update_from_date_in_db(r["id"], today_str())
                    stats['processed_success'] += 1
                    processed_stores.append(f"{merchant} - {store_name}")
                    logger.info("✅ SUCCESS: %s - %s", merchant, store_name)
                else:
                    stats['no_eligible'] += 1
                    logger.info("ℹ️ No eligible transactions for %s - %s", merchant, store_name)

            except Exception as e:
                logger.error("❌ ERROR: %s / %s → %s", merchant, store_id, e)
                stats['errors'] += 1
                error_stores.append(f"{merchant} - {store_name} ({str(e)[:50]}...)")
                capture_screenshot(driver, f"error_{merchant}_{store_id}")

            finally:
                # Navigate back to settlement page with retry logic
                if not navigate_back_to_settlement_page(driver, wait):
                    logger.critical("Critical navigation failure. Restarting browser...")
                    driver.quit()
                    driver = init_webdriver()
                    wait = WebDriverWait(driver, TIMEOUT)
                    perform_login(driver, wait)

        # ────────────────────────── FINAL REPORT ──────────────────────────
        logger.info("")
        logger.info("=" * 60)
        logger.info("SETTLEMENT PROCESSING REPORT - %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        logger.info("=" * 60)
        logger.info("Total queued for today: %s", stats['total_queued'])
        logger.info("Skipped (from skip.txt): %s", stats['skipped'])
        logger.info("Successfully processed: %s", stats['processed_success'])
        logger.info("No eligible transactions: %s", stats['no_eligible'])
        logger.info("Errors: %s", stats['errors'])
        logger.info("=" * 60)
        
        if processed_stores:
            logger.info("SUCCESSFULLY PROCESSED STORES (%s):", len(processed_stores))
            for store in processed_stores:
                logger.info("  ✅ %s", store)
        
        if skipped_stores:
            logger.info("SKIPPED STORES (%s):", len(skipped_stores))
            for store in skipped_stores[:10]:  # Show first 10 only
                logger.info("  ⏭️ %s", store)
            if len(skipped_stores) > 10:
                logger.info("  ... and %s more", len(skipped_stores) - 10)
        
        if error_stores:
            logger.info("STORES WITH ERRORS (%s):", len(error_stores))
            for store in error_stores[:10]:  # Show first 10 only
                logger.info("  ❌ %s", store)
            if len(error_stores) > 10:
                logger.info("  ... and %s more", len(error_stores) - 10)
        
        logger.info("=" * 60)
        logger.info("Summary: %s/%s processed successfully", 
                   stats['processed_success'], 
                   stats['total_queued'] - stats['skipped'])
        logger.info("=" * 60)

    except Exception as e:
        logger.critical("Fatal error in main execution: %s", e)
        capture_screenshot(driver, "fatal_error")
    finally:
        driver.quit()
        logger.info("Browser closed.")

if __name__ == "__main__":

    main()
