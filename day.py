#!/usr/bin/env python3

import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# =========================
# LOAD ENV
# =========================
load_dotenv()

EMAIL = os.getenv("COMPANY_EMAIL")
PASSWORD = os.getenv("COMPANY_PASSWORD")

LOGIN_URL = "https://admin.shurjopayment.com/"
SETTLEMENT_DAY_URL = "https://admin.shurjopayment.com/spadmin/merchant/settlement-day"
TRX_REPORT_URL = "https://admin.shurjopayment.com/spadmin/report/merchant-daily-trx"

DOWNLOAD_DIR = os.path.abspath("downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# =========================
# DATE CONFIG
# =========================
today_day = datetime.now().strftime("%A")
yesterday = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

# =========================
# DRIVER SETUP
# =========================
options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")

prefs = {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
}
options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

wait = WebDriverWait(driver, 30)

try:
    # ============================================================
    # LOGIN
    # ============================================================
    print("Opening login page...")
    driver.get(LOGIN_URL)

    wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(EMAIL)
    driver.find_element(By.ID, "password-field").send_keys(PASSWORD)
    driver.find_element(By.XPATH, "//button[@type='submit']").click()

    wait.until(EC.url_changes(LOGIN_URL))
    print("✅ Login successful")

    # ============================================================
    # 1️⃣ DOWNLOAD SETTLEMENT DAY (CURRENT DAY)
    # ============================================================
    print("Downloading Settlement Day for:", today_day)

    driver.get(SETTLEMENT_DAY_URL)

    # Select day
    wait.until(EC.element_to_be_clickable((By.ID, "select2-day-container"))).click()

    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, f"//li[contains(@class,'select2-results__option') and text()='{today_day}']")
        )
    ).click()

    # Select ALL rows
    select_length = Select(
        wait.until(EC.presence_of_element_located((By.NAME, "withdraw_days_table_length")))
    )
    select_length.select_by_value("-1")

    # Click search
    wait.until(EC.element_to_be_clickable((By.ID, "filter_search"))).click()

    print("Waiting 10 seconds for settlement table load...")
    time.sleep(10)

    # Download Excel
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(@class,'buttons-excel')]")
        )
    ).click()

    print("📥 Settlement Day Excel Downloaded")

    time.sleep(5)

    # ============================================================
    # 2️⃣ DOWNLOAD MERCHANT DAILY TRX (YESTERDAY DATE)
    # ============================================================
    print("Downloading Merchant Daily TRX for:", yesterday)

    driver.get(TRX_REPORT_URL)

    # Fill from date
    from_input = wait.until(EC.presence_of_element_located((By.ID, "fromDate")))
    from_input.clear()
    from_input.send_keys(yesterday)

    # Fill to date
    to_input = driver.find_element(By.ID, "toDate")
    to_input.clear()
    to_input.send_keys(yesterday)

    # Click search
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//input[@id='filter_search']")
        )
    ).click()

    print("Waiting 10 seconds for trx table load...")
    time.sleep(10)

    # Download Excel
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(@class,'buttons-excel')]")
        )
    ).click()

    print("📥 Merchant Daily TRX Excel Downloaded")

    time.sleep(5)

    print("✅ Both downloads completed successfully!")

except Exception as e:
    print("❌ Error occurred:", e)

finally:
    driver.quit()
