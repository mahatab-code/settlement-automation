#!/usr/bin/env python3

import os
import sys
import time
import logging
import re
from datetime import datetime, timedelta
import pytz

import pandas as pd
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, NoAlertPresentException

# =========================
# ENV VARIABLES
# =========================
EMAIL = os.getenv("COMPANY_EMAIL")
PASSWORD = os.getenv("COMPANY_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL")

if not EMAIL or not PASSWORD:
    sys.exit("❌ COMPANY_EMAIL or COMPANY_PASSWORD not set")

if not DATABASE_URL:
    sys.exit("❌ DATABASE_URL not set")

BASE_URL = "https://admin.shurjopayment.com/login"
SETTLEMENT_CREATE_URL = "https://admin.shurjopayment.com/accounts/settlement/create"
TIMEOUT = 120

DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday",
                "Thursday", "Friday", "Saturday", "Sunday"]

TICK_INDICATORS = {"1", "TRUE", "True", "true", "✔", "✓", "x", "X"}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("settlement-bot")

# =========================
# ✅ BDT TIMEZONE FIX ONLY
# =========================
bd_tz = pytz.timezone("Asia/Dhaka")

def get_today_day():
    return datetime.now(bd_tz).strftime("%A")

def get_yesterday_date():
    return (datetime.now(bd_tz) - timedelta(days=1)).strftime("%d/%m/%Y")

def today_str():
    return datetime.now(bd_tz).strftime("%d/%m/%Y")

# =========================
# DB
# =========================
def get_db_engine():
    if DATABASE_URL.startswith("postgres://"):
        db_url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    else:
        db_url = DATABASE_URL
    return create_engine(db_url, pool_pre_ping=True)

def read_data_from_db():
    with get_db_engine().begin() as conn:
        df = pd.read_sql("SELECT * FROM settlement_day", conn)
    return df

def update_from_date_in_db(record_id: int, new_date: str):
    new_dt = datetime.strptime(new_date, "%d/%m/%Y").date()
    with get_db_engine().begin() as conn:
        conn.execute(
            text("UPDATE settlement_day SET from_date=:d WHERE id=:i"),
            {"d": new_dt, "i": record_id}
        )

# =========================
# DRIVER
# =========================
def init_webdriver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

# =========================
# LOGIN (UNCHANGED WORKING VERSION)
# =========================
def perform_login(driver, wait):
    logger.info("Opening login page...")
    driver.get(BASE_URL)

    wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(EMAIL)
    driver.find_element(By.ID, "password-field").send_keys(PASSWORD)
    driver.find_element(By.XPATH, "//button[@type='submit']").click()

    # 🔥 আগের working logic
    wait.until(EC.url_contains("/spadmin"))
    logger.info("Login successful")

    driver.get(SETTLEMENT_CREATE_URL)
    wait.until(EC.presence_of_element_located((By.ID, "select2-merchant_id-container")))
    logger.info("Settlement page loaded")

# =========================
# MAIN
# =========================
def main():

    today_day = get_today_day()
    logger.info(f"Today detected as (BDT): {today_day}")

    driver = init_webdriver()
    wait = WebDriverWait(driver, TIMEOUT)

    try:
        perform_login(driver, wait)

        df = read_data_from_db()

        df_today = df[
            (df[today_day].isin(TICK_INDICATORS)) &
            (~df["from_date"].astype(str).str.contains("2030-01-01"))
        ]

        if df_today.empty:
            logger.info("No merchants scheduled for today")
            return

        logger.info(f"{len(df_today)} merchants scheduled today")

        for _, row in df_today.iterrows():

            merchant = row["merchant_name"]
            store_name = row["store_name"]
            record_id = row["id"]

            from_date = pd.to_datetime(row["from_date"]).strftime("%d/%m/%Y")
            to_date = get_yesterday_date()

            logger.info(f"Processing: {merchant} - {store_name}")

            wait.until(EC.element_to_be_clickable(
                (By.ID, "select2-merchant_id-container"))).click()

            search_box = wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "input.select2-search__field"))
            )
            search_box.send_keys(merchant)

            wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, f"//li[contains(@class,'select2-results__option') and text()='{merchant}']")
                )
            ).click()

            select = Select(wait.until(
                EC.presence_of_element_located((By.ID, "store_id"))
            ))
            select.select_by_visible_text(store_name)

            f = wait.until(EC.presence_of_element_located((By.ID, "fromDate")))
            f.clear()
            f.send_keys(from_date)

            t = driver.find_element(By.ID, "toDate")
            t.clear()
            t.send_keys(to_date)

            wait.until(EC.element_to_be_clickable(
                (By.ID, "create_settlement"))).click()

            time.sleep(2)

            update_from_date_in_db(record_id, today_str())
            logger.info("Settlement created successfully")

            driver.get(SETTLEMENT_CREATE_URL)
            time.sleep(1)

    finally:
        driver.quit()
        logger.info("Browser closed")

if __name__ == "__main__":
    main()
