#!/usr/bin/env python3

import os
import sys
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC


EMAIL = os.environ.get("COMPANY_EMAIL")
PASSWORD = os.environ.get("COMPANY_PASSWORD")
DATABASE_URL = os.environ.get("DATABASE_URL")

if not EMAIL or not PASSWORD or not DATABASE_URL:
    sys.exit("Missing environment variables")

engine = create_engine(DATABASE_URL)

BASE_URL = "https://admin.shurjopayment.com/"
SETTLEMENT_CREATE_URL = f"{BASE_URL}accounts/settlement/create"
TIMEOUT = 120

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("settlement-bot")


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


def read_data_from_db():
    with engine.begin() as conn:
        return pd.read_sql("SELECT * FROM settlement_day", conn)


def update_from_date(record_id):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE settlement_day
            SET from_date=:d
            WHERE id=:i
        """), {
            "d": datetime.now().date(),
            "i": record_id
        })


def main():

    driver = init_webdriver()
    wait = WebDriverWait(driver, TIMEOUT)

    try:
        driver.get(BASE_URL)
        wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(EMAIL)
        driver.find_element(By.ID, "password-field").send_keys(PASSWORD)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()

        wait.until(EC.url_contains("/spadmin"))

        driver.get(SETTLEMENT_CREATE_URL)
        wait.until(EC.presence_of_element_located((By.ID, "select2-merchant_id-container")))

        df = read_data_from_db()

        today = datetime.now().strftime("%A")
        df_today = df[df[today] == "✓"]

        for _, row in df_today.iterrows():

            merchant = row["merchant_name"]
            store_id = row["store_id"]
            from_date = pd.to_datetime(row["from_date"]).strftime("%d/%m/%Y")
            to_date = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

            wait.until(EC.element_to_be_clickable((By.ID, "select2-merchant_id-container"))).click()
            box = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input.select2-search__field")))
            box.send_keys(merchant)
            wait.until(EC.element_to_be_clickable(
                (By.XPATH, f"//li[contains(@class,'select2-results__option') and text()='{merchant}']"))).click()

            Select(wait.until(
                EC.presence_of_element_located((By.ID, "store_id"))
            )).select_by_value(str(store_id))

            wait.until(EC.presence_of_element_located((By.ID, "fromDate"))).send_keys(from_date)
            driver.find_element(By.ID, "toDate").send_keys(to_date)

            driver.find_element(By.ID, "create_settlement").click()
            time.sleep(3)

            update_from_date(row["id"])

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
