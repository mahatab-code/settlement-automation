#!/usr/bin/env python3

import os
import time
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
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


# =========================
# URL CONFIG
# =========================
LOGIN_URL = "https://admin.shurjopayment.com/login"
SETTLEMENT_DAY_URL = "https://admin.shurjopayment.com/spadmin/merchant/settlement-day"
TRX_REPORT_URL = "https://admin.shurjopayment.com/spadmin/report/merchant-daily-trx"


# =========================
# DATE CONFIG
# =========================
today_day = datetime.now().strftime("%A")
yesterday = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")


# =========================
# DOWNLOAD DIRECTORY
# =========================
DOWNLOAD_DIR = os.path.abspath("downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# =========================
# CHROME OPTIONS (GitHub Safe)
# =========================
options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")

prefs = {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
}
options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 40)


# =========================
# DB ENGINE
# =========================
engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# =========================
# CLEAR DAY COLUMNS
# =========================
def clear_day_columns():
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE settlement_day SET
            "Monday"='',
            "Tuesday"='',
            "Wednesday"='',
            "Thursday"='',
            "Friday"='',
            "Saturday"='',
            "Sunday"='';
        """))
    print("Day columns cleared")


# =========================
# UPDATE SETTLEMENT CSV
# =========================
def update_settlement_csv(file_path):
    print("Reading settlement CSV...")
    df = pd.read_csv(file_path, dtype=str).fillna("")

    clear_day_columns()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            merchant = row["Merchant"].strip()
            store = row["Store"].strip()
            days = row["Withdraw Days"]

            # check existing
            result = conn.execute(text("""
                SELECT id FROM settlement_day
                WHERE merchant_name=:m AND store_name=:s
            """), {"m": merchant, "s": store}).fetchone()

            day_map = {
                "Monday": "",
                "Tuesday": "",
                "Wednesday": "",
                "Thursday": "",
                "Friday": "",
                "Saturday": "",
                "Sunday": ""
            }

            for d in day_map:
                if d in days:
                    day_map[d] = "1"

            if result:
                conn.execute(text(f"""
                    UPDATE settlement_day SET
                    "Monday"=:Monday,
                    "Tuesday"=:Tuesday,
                    "Wednesday"=:Wednesday,
                    "Thursday"=:Thursday,
                    "Friday"=:Friday,
                    "Saturday"=:Saturday,
                    "Sunday"=:Sunday
                    WHERE merchant_name=:merchant AND store_name=:store
                """), {**day_map, "merchant": merchant, "store": store})
            else:
                conn.execute(text(f"""
                    INSERT INTO settlement_day
                    (merchant_name, store_name, from_date,
                     "Monday","Tuesday","Wednesday","Thursday",
                     "Friday","Saturday","Sunday",
                     created_at, updated_at)
                    VALUES
                    (:merchant,:store,'2030-01-01',
                     :Monday,:Tuesday,:Wednesday,:Thursday,
                     :Friday,:Saturday,:Sunday,
                     NOW(),NOW())
                """), {**day_map, "merchant": merchant, "store": store})

    print("Settlement table updated")


# =========================
# MAIN
# =========================
try:

    print("Opening login page...")
    driver.get(LOGIN_URL)

    wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(EMAIL)
    driver.find_element(By.ID, "password-field").send_keys(PASSWORD)
    driver.find_element(By.ID, "password-field").send_keys(Keys.RETURN)

    # Element-based wait (NO URL WAIT)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(2)

    print("Login successful")

    # =========================
    # SETTLEMENT CSV DOWNLOAD
    # =========================
    driver.get(SETTLEMENT_DAY_URL)

    wait.until(EC.element_to_be_clickable((By.ID, "select2-day-container"))).click()

    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, f"//li[text()='{today_day}']")
        )
    ).click()

    Select(
        wait.until(EC.presence_of_element_located((By.NAME, "withdraw_days_table_length")))
    ).select_by_value("-1")

    driver.find_element(By.ID, "filter_search").click()
    time.sleep(8)

    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(@class,'buttons-csv')]")
        )
    ).click()

    print("Settlement CSV Downloaded")
    time.sleep(5)

    # Find latest CSV
    files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
    settlement_file = os.path.join(DOWNLOAD_DIR, sorted(files)[-1])

    update_settlement_csv(settlement_file)

    print("Process completed successfully")

except Exception as e:
    print("Error:", e)

finally:
    driver.quit()
