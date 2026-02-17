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
from selenium.common.exceptions import TimeoutException


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

        df = read_data_from_db()

        today_name = get_bd_today_name()

        df_today = df[df[today_name] == "✓"]

        if df_today.empty:
            logger.info("No merchants scheduled for today")
            return

        logger.info(f"{len(df_today)} merchants to process")

        for _, row in df_today.iterrows():

            merchant = str(row["merchant_name"]).strip()
            store_id = str(row["store_id"]).strip()
            from_date = pd.to_datetime(row["from_date"]).strftime("%d/%m/%Y")
            to_date = get_bd_yesterday_str("%d/%m/%Y")

            logger.info(f"Processing: {merchant}")

            # Select merchant
            wait.until(
                EC.element_to_be_clickable(
                    (By.ID, "select2-merchant_id-container")
                )
            ).click()

            search_box = wait.until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, "input.select2-search__field")
                )
            )

            search_box.send_keys(merchant)

            wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH,
                     f"//li[contains(@class,'select2-results__option') and text()='{merchant}']")
                )
            ).click()

            # Select store
            Select(wait.until(
                EC.presence_of_element_located((By.ID, "store_id"))
            )).select_by_value(store_id)

            # Enter dates
            wait.until(
                EC.presence_of_element_located((By.ID, "fromDate"))
            ).clear()

            driver.find_element(By.ID, "fromDate").send_keys(from_date)

            driver.find_element(By.ID, "toDate").clear()
            driver.find_element(By.ID, "toDate").send_keys(to_date)

            # Submit
            driver.find_element(By.ID, "create_settlement").click()

            time.sleep(3)

            update_from_date(row["id"])

            logger.info(f"Settlement created for {merchant}")

        logger.info("All settlements processed successfully")

    except TimeoutException:
        logger.error("Timeout occurred during execution")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    finally:
        driver.quit()
        logger.info("Browser closed")


if __name__ == "__main__":
    main()
