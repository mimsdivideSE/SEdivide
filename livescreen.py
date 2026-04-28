import os
import time
import json
import gspread
import pandas as pd
import mysql.connector
from datetime import datetime
import sys

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
STOCK_LIST_GID = 1400370843
SOURCE_TABLE = "wp_live_close"
TARGET_TABLE = "live_screen"
CHANGE_THRESHOLD = 5.0 

# ---------------- HELPERS ---------------- #
def remove_chart_popups(driver):
    """Forcefully removes UI overlays via JavaScript."""
    scrub_script = """
    const popupSelectors = [
        '[class^="overlap-"]', '[class*="dialog-"]', '[class*="modal-"]', 
        '.tv-dialog__close', '.js-dialog__close', '[data-role="toast-container"]',
        '[class*="popup-"]', '#overlap-manager-root'
    ];
    popupSelectors.forEach(selector => {
        document.querySelectorAll(selector).forEach(el => el.remove());
    });
    """
    try:
        driver.execute_script(scrub_script)
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
    except:
        pass

def get_optimized_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

# ---------------- MAIN ---------------- #
def main():
    driver = None
    db_conn = None

    try:
        print(f"🕒 Execution Started at UTC: {datetime.utcnow()}")

        # ---------------- DB CONNECTION ---------------- #
        db_conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            autocommit=True 
        )
        cur = db_conn.cursor(dictionary=True)

        # ---------------- FETCH TOP 50 STOCKS ---------------- #
        print(f"📊 Fetching stocks with change >= {CHANGE_THRESHOLD}%...")
        query = f"""
            SELECT Symbol, real_close, real_change 
            FROM `{SOURCE_TABLE}` 
            WHERE CAST(real_change AS DECIMAL(10,2)) >= %s
            ORDER BY CAST(real_change AS DECIMAL(10,2)) DESC
            LIMIT 50
        """
        cur.execute(query, (CHANGE_THRESHOLD,))
        stocks = cur.fetchall()

        if not stocks:
            print("😴 No signals found.")
            return

        # ---------------- LOAD GOOGLE SHEET ---------------- #
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        df = pd.DataFrame(ws.get_all_values())
        url_map = dict(zip(df.iloc[1:, 0].str.upper().str.strip(), df.iloc[1:, 3]))

        # ---------------- BROWSER ---------------- #
        driver = get_optimized_driver()
        driver.get("https://www.tradingview.com/")
        cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES"))
        for c in cookies:
            driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        driver.refresh()

        success_count = 0

        # ---------------- LOOP ---------------- #
        for stock in stocks:
            symbol = stock["Symbol"].upper().strip()
            url = url_map.get(symbol)
            if not url: continue

            try:
                db_conn.ping(reconnect=True, attempts=3, delay=2)
                
                # STEP 1: REMOVE EXISTING ENTRY FOR THIS SYMBOL FIRST
                # This prevents getting "two of the same symbol" in your UI
                cur.execute(f"DELETE FROM `{TARGET_TABLE}` WHERE symbol = %s", (symbol,))
                
                print(f"📸 [{success_count + 1}] Processing {symbol}...", end=" ", flush=True)

                driver.get(url)
                WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CLASS_NAME, "chart-container")))
                
                # Wait for popups then clear them
                time.sleep(5) 
                remove_chart_popups(driver)
                print("✨ (Popups Cleared)", end=" ", flush=True)
                time.sleep(2)

                img_data = driver.get_screenshot_as_png()

                # STEP 2: INSERT THE FRESH SCREENSHOT
                sql = f"""
                    INSERT INTO `{TARGET_TABLE}` 
                    (symbol, timeframe, real_change, real_close, screenshot, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                cur.execute(sql, (symbol, "day", stock["real_change"], stock["real_close"], img_data, datetime.utcnow()))
                
                print("✅ [Cleaned & Inserted]")
                success_count += 1

            except Exception as e:
                print(f"❌ Error: {str(e)[:50]}")

        print(f"🏁 Done. Total: {success_count}. Duplicates prevented by Delete-Before-Insert logic.")

    except Exception as e:
        print(f"🚨 CRITICAL ERROR: {e}")

    finally:
        if db_conn and db_conn.is_connected():
            cur.close()
            db_conn.close()
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()
