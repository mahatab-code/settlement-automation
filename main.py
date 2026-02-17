#!/usr/bin/env python3

import os
import sys
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pytz

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException


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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("settlement-bot")

# Bangladesh Timezone
BD_TZ = pytz.timezone('Asia/Dhaka')


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
# SELECT MERCHANT FUNCTION
# =========================
def select_merchant(driver, wait, merchant_name):
    """Select merchant with multiple strategies"""
    try:
        logger.info(f"Attempting to select merchant: {merchant_name}")
        
        # Click the merchant dropdown
        wait.until(
            EC.element_to_be_clickable(
                (By.ID, "select2-merchant_id-container")
            )
        ).click()
        time.sleep(1)
        
        # Try different search box selectors
        search_box = None
        search_selectors = [
            (By.CSS_SELECTOR, "input.select2-search__field"),
            (By.CSS_SELECTOR, "input[type='search']"),
            (By.XPATH, "//input[@class='select2-search__field']")
        ]
        
        for selector_type, selector_value in search_selectors:
            try:
                search_box = WebDriverWait(driver, 5).until(
                    EC.visibility_of_element_located((selector_type, selector_value))
                )
                if search_box:
                    logger.info(f"Found search box with selector: {selector_value}")
                    break
            except:
                continue
        
        if not search_box:
            raise Exception("Could not find search box")
        
        # Clear and type merchant name
        search_box.clear()
        search_box.send_keys(merchant_name)
        time.sleep(2)
        
        # Try different result selectors
        result_selectors = [
            (By.XPATH, f"//li[contains(@class,'select2-results__option') and text()='{merchant_name}']"),
            (By.XPATH, f"//li[contains(text(), '{merchant_name}')]"),
            (By.CSS_SELECTOR, "li.select2-results__option")
        ]
        
        for selector_type, selector_value in result_selectors:
            try:
                result = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((selector_type, selector_value))
                )
                if result:
                    logger.info(f"Found merchant with selector: {selector_value}")
                    result.click()
                    time.sleep(1)
                    return True
            except:
                continue
        
        raise Exception(f"Could not find merchant '{merchant_name}' in results")
        
    except Exception as e:
        logger.error(f"Error selecting merchant {merchant_name}: {str(e)}")
        # Take screenshot for debugging
        try:
            driver.save_screenshot(f"error_merchant_{merchant_name.replace(' ', '_')}.png")
            logger.info(f"Screenshot saved for {merchant_name}")
        except:
            pass
        return False


# =========================
# MAIN PROCESS
# =========================
def main():

    driver = init_webdriver()
    wait = WebDriverWait(driver, TIMEOUT)

    try:
        logger.info("Opening login page...")
        driver.get(LOGIN_URL)

        # Enter Email
        wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(EMAIL)

        # Enter Password
        driver.find_element(By.ID, "password-field").send_keys(PASSWORD)

        # Submit
        driver.find_element(By.ID, "password-field").send_keys(Keys.RETURN)

        # Wait until dashboard loads (body loaded)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2)

        logger.info("Login successful")

        # Go to settlement page directly
        driver.get(SETTLEMENT_CREATE_URL)

        wait.until(
            EC.presence_of_element_located(
                (By.ID, "select2-merchant_id-container")
            )
        )

        logger.info("Settlement page loaded")

        # Debug: Show current Bangladesh time and day
        bd_now = get_bd_now()
        bd_today_name = get_bd_today_name()
        logger.info(f"Current Bangladesh Time: {bd_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"Today's weekday in BD: {bd_today_name}")

        df = read_data_from_db()
        
        # Check which merchants are scheduled for today (looking for 1)
        df_today = df[df[bd_today_name].astype(str).str.strip() == '1']

        if df_today.empty:
            logger.info(f"No merchants scheduled for {bd_today_name}")
            return

        logger.info(f"{len(df_today)} merchants to process for {bd_today_name}")
        
        # Process each merchant with individual error handling
        success_count = 0
        error_count = 0

        for index, row in df_today.iterrows():

            try:
                merchant = str(row["merchant_name"]).strip()
                store_id = str(row["store_id"]).strip()
                from_date = pd.to_datetime(row["from_date"]).strftime("%d/%m/%Y")
                to_date = get_bd_yesterday_str("%d/%m/%Y")

                logger.info(f"Processing ({index+1}/{len(df_today)}): {merchant} (Store ID: {store_id})")
                logger.info(f"Date range: {from_date} to {to_date}")

                # Select merchant using the new function
                if not select_merchant(driver, wait, merchant):
                    error_count += 1
                    logger.error(f"Failed to select merchant: {merchant}, skipping...")
                    continue

                # Select store
                try:
                    store_select = Select(wait.until(
                        EC.presence_of_element_located((By.ID, "store_id"))
                    ))
                    store_select.select_by_value(store_id)
                    logger.info(f"Store selected: {store_id}")
                except Exception as e:
                    logger.error(f"Error selecting store {store_id}: {str(e)}")
                    error_count += 1
                    continue

                # Enter dates
                try:
                    # From date
                    from_date_field = wait.until(
                        EC.presence_of_element_located((By.ID, "fromDate"))
                    )
                    from_date_field.clear()
                    from_date_field.send_keys(from_date)
                    
                    # To date
                    to_date_field = driver.find_element(By.ID, "toDate")
                    to_date_field.clear()
                    to_date_field.send_keys(to_date)
                    
                    logger.info("Dates entered successfully")
                except Exception as e:
                    logger.error(f"Error entering dates: {str(e)}")
                    error_count += 1
                    continue

                # Submit
                try:
                    submit_btn = wait.until(
                        EC.element_to_be_clickable((By.ID, "create_settlement"))
                    )
                    submit_btn.click()
                    logger.info("Form submitted")
                except Exception as e:
                    logger.error(f"Error submitting form: {str(e)}")
                    error_count += 1
                    continue

                # Wait for processing
                time.sleep(5)

                # Update database
                update_from_date(row["id"])
                
                success_count += 1
                logger.info(f"✅ Settlement created for {merchant}")
                
                # Small pause between merchants
                time.sleep(2)

            except Exception as e:
                error_count += 1
                logger.error(f"Unexpected error processing {merchant if 'merchant' in locals() else 'unknown'}: {str(e)}")
                # Take screenshot on error
                try:
                    driver.save_screenshot(f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                except:
                    pass
                continue

        logger.info(f"Processing complete - Success: {success_count}, Errors: {error_count}")

    except TimeoutException as e:
        logger.error(f"Timeout occurred during execution: {str(e)}")
        # Take screenshot
        try:
            driver.save_screenshot(f"timeout_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        except:
            pass

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        # Take screenshot
        try:
            driver.save_screenshot(f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        except:
            pass

    finally:
        driver.quit()
        logger.info("Browser closed")


if __name__ == "__main__":
    main()
