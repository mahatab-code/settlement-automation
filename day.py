#!/usr/bin/env python3

import os
import time
import re
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC


# =========================
# ENV VARIABLES
# =========================
EMAIL = os.environ.get("ADMIN_EMAIL")
PASSWORD = os.environ.get("ADMIN_PASSOWORD")
DATABASE_URL = os.environ.get("DATABASE_URL")

if not EMAIL or not PASSWORD or not DATABASE_URL:
    raise Exception("Missing environment variables")

engine = create_engine(DATABASE_URL)

LOGIN_URL = "https://admin.shurjopayment.com/"
SETTLEMENT_DAY_URL = "https://admin.shurjopayment.com/spadmin/merchant/settlement-day"
TRX_REPORT_URL = "https://admin.shurjopayment.com/spadmin/report/merchant-daily-trx"

today_day = datetime.now().strftime("%A")
yesterday = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

DOWNLOAD_DIR = os.path.abspath("downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
    }
    options.add_experimental_option("prefs", prefs)

    return webdriver.Chrome(options=options)


def wait_for_download():
    time.sleep(5)
    while any(f.endswith(".crdownload") for f in os.listdir(DOWNLOAD_DIR)):
        time.sleep(1)


def parse_days(text):
    days = {d: "" for d in
            ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]}

    clean = re.sub(r"\(.*?\)", "", str(text))
    for d in clean.split(","):
        d = d.strip()
        if d in days:
            days[d] = "✓"
    return days


def clear_day_columns():
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE settlement_day
            SET "Monday"='',
                "Tuesday"='',
                "Wednesday"='',
                "Thursday"='',
                "Friday"='',
                "Saturday"='',
                "Sunday"='';
        """))


def update_settlement_csv(file_path):

    df = pd.read_csv(file_path)
    clear_day_columns()

    with engine.begin() as conn:
        for _, row in df.iterrows():

            merchant = str(row.get("Merchant", "")).strip()
            store = str(row.get("Store", "")).strip()
            withdraw = row.get("Withdraw Days", "")

            if not merchant or not store:
                continue

            days = parse_days(withdraw)

            exists = conn.execute(text("""
                SELECT id FROM settlement_day
                WHERE LOWER(merchant_name)=LOWER(:m)
                AND LOWER(store_name)=LOWER(:s)
            """), {"m":merchant, "s":store}).fetchone()

            if exists:
                conn.execute(text("""
                    UPDATE settlement_day
                    SET "Monday"=:mon,
                        "Tuesday"=:tue,
                        "Wednesday"=:wed,
                        "Thursday"=:thu,
                        "Friday"=:fri,
                        "Saturday"=:sat,
                        "Sunday"=:sun,
                        updated_at=NOW()
                    WHERE LOWER(merchant_name)=LOWER(:m)
                    AND LOWER(store_name)=LOWER(:s)
                """), {
                    "m":merchant, "s":store,
                    "mon":days["Monday"],
                    "tue":days["Tuesday"],
                    "wed":days["Wednesday"],
                    "thu":days["Thursday"],
                    "fri":days["Friday"],
                    "sat":days["Saturday"],
                    "sun":days["Sunday"]
                })
            else:
                conn.execute(text("""
                    INSERT INTO settlement_day
                    (merchant_name, store_name, from_date,
                     "Monday","Tuesday","Wednesday",
                     "Thursday","Friday","Saturday","Sunday",
                     is_default_date)
                    VALUES
                    (:m,:s,'2030-01-01',
                     :mon,:tue,:wed,:thu,:fri,:sat,:sun,1)
                """), {
                    "m":merchant, "s":store,
                    "mon":days["Monday"],
                    "tue":days["Tuesday"],
                    "wed":days["Wednesday"],
                    "thu":days["Thursday"],
                    "fri":days["Friday"],
                    "sat":days["Saturday"],
                    "sun":days["Sunday"]
                })


def activate_default_stores(trx_file):

    df = pd.read_csv(trx_file)
    df["Merchant Name"] = df["Merchant Name"].str.lower().str.strip()
    df["Store Name"] = df["Store Name"].str.lower().str.strip()

    with engine.begin() as conn:

        defaults = conn.execute(text("""
            SELECT merchant_name, store_name
            FROM settlement_day
            WHERE from_date='2030-01-01'
            AND is_default_date=1
        """)).fetchall()

        for merchant, store in defaults:

            m = merchant.lower().strip()
            s = store.lower().strip()

            match = df[
                (df["Merchant Name"] == m) &
                (df["Store Name"] == s)
            ]

            if not match.empty:
                trx_date = pd.to_datetime(match.iloc[0]["Date"]).date()

                conn.execute(text("""
                    UPDATE settlement_day
                    SET from_date=:d,
                        is_default_date=0,
                        updated_at=NOW()
                    WHERE LOWER(merchant_name)=LOWER(:m)
                    AND LOWER(store_name)=LOWER(:s)
                    AND from_date='2030-01-01'
                """), {"d":trx_date, "m":merchant, "s":store})


# =========================
# MAIN
# =========================

driver = init_driver()
wait = WebDriverWait(driver, 30)

try:
    driver.get(LOGIN_URL)

    wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(EMAIL)
    driver.find_element(By.ID, "password-field").send_keys(PASSWORD)
    driver.find_element(By.XPATH, "//button[@type='submit']").click()

    wait.until(EC.url_contains("/spadmin"))

    # Settlement CSV
    driver.get(SETTLEMENT_DAY_URL)
    wait.until(EC.element_to_be_clickable((By.ID, "select2-day-container"))).click()
    wait.until(EC.element_to_be_clickable(
        (By.XPATH, f"//li[text()='{today_day}']"))).click()

    Select(wait.until(
        EC.presence_of_element_located((By.NAME, "withdraw_days_table_length"))
    )).select_by_value("-1")

    driver.find_element(By.ID, "filter_search").click()
    time.sleep(8)
    driver.find_element(By.CSS_SELECTOR, "button.buttons-csv").click()
    wait_for_download()

    # TRX CSV
    driver.get(TRX_REPORT_URL)
    wait.until(EC.presence_of_element_located((By.ID, "fromDate"))).send_keys(yesterday)
    driver.find_element(By.ID, "toDate").send_keys(yesterday)
    driver.find_element(By.ID, "filter_search").click()
    time.sleep(8)
    driver.find_element(By.CSS_SELECTOR, "button.buttons-csv").click()
    wait_for_download()

finally:
    driver.quit()


files = sorted(
    [os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR)],
    key=os.path.getctime
)

settlement_file = files[-2]
trx_file = files[-1]

update_settlement_csv(settlement_file)
activate_default_stores(trx_file)

print("Day process completed successfully")
