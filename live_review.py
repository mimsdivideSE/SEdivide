import os
import time
import mysql.connector
import sys

from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager


# ================= CONFIG ================= #

FILTER_TABLE = "filter"

TARGET_TABLE = "live_review_screens"


# ================= HELPERS ================= #

def log(message, end="\n"):
    """Ensures instant, unbuffered logs appear in GitHub Actions."""
    print(message, end=end)
    sys.stdout.flush()


def get_clean_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )
    # Strict fallback timeout so a single slow asset never freezes your runner
    driver.set_page_load_timeout(30)
    return driver


# ================= MAIN ================= #

def main():
    driver = None
    db_conn = None

    try:
        log(f"🚀 Execution Started : {datetime.utcnow()}")

        # ================= DATABASE CONNECTION ================= #
        log("🔗 Connecting Database...")
        db_conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            autocommit=True
        )
        cur = db_conn.cursor(dictionary=True)

        # ================= FETCH SYMBOLS ================= #
        log("📊 Fetching BUY & WATCHLIST rows where depriciate is 0...")
        query = f"""
            SELECT id, symbol, timeframe, review_status 
            FROM `{FILTER_TABLE}`
            WHERE review_status IN ('buy', 'watchlist')
              AND (depriciate = 0 OR depriciate IS NULL)
            ORDER BY id DESC
        """
        cur.execute(query)
        stocks = cur.fetchall()

        if not stocks:
            log("😴 No matching symbols found in the database.")
            return

        buy_count = sum(1 for s in stocks if s["review_status"].lower() == "buy")
        watchlist_count = sum(1 for s in stocks if s["review_status"].lower() == "watchlist")
        log(f"✅ Targets Found -> Total: {len(stocks)} [BUY: {buy_count} | WATCHLIST: {watchlist_count}]")

        # ================= INITIALIZE BROWSER ================= #
        log("🌐 Initializing Headless Chrome Instance...")
        driver = get_clean_driver()
        success_count = 0

        # ================= SCREENSHOT LOOP ================= #
        for stock in stocks:
            try:
                symbol = stock["symbol"].upper().strip()
                timeframe = stock["timeframe"]
                status = stock["review_status"]
                filter_id = stock["id"]

                # Directly load the clean ticker chart layout
                chart_url = f"https://www.tradingview.com/chart/?symbol={symbol}"
                log(f"📸 [{success_count + 1}/{len(stocks)}] Processing {symbol} ({status.upper()})...", end=" ")

                try:
                    driver.get(chart_url)
                except Exception:
                    # If page assets take longer than 30s to fully download, proceed with what's loaded
                    pass

                # Give chart elements brief moment to render candles cleanly
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(5)

                # Snap the picture
                img_data = driver.get_screenshot_as_png()

                # Update Destination Table
                cur.execute(
                    f"DELETE FROM `{TARGET_TABLE}` WHERE filter_id = %s", 
                    (filter_id,)
                )

                insert_sql = f"""
                    INSERT INTO `{TARGET_TABLE}`
                    (filter_id, symbol, timeframe, review_status, screenshot, source_url, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cur.execute(
                    insert_sql,
                    (filter_id, symbol, timeframe, status, img_data, chart_url, datetime.utcnow())
                )

                success_count += 1
                log("✅ Saved")

            except Exception as item_error:
                log(f"❌ Failed: {str(item_error)[:100]}")

        log(f"🏁 DONE : Completed {success_count} snapshots successfully.")

    except Exception as critical_error:
        log(f"🚨 CRITICAL ERROR : {critical_error}")

    finally:
        if db_conn and db_conn.is_connected():
            cur.close()
            db_conn.close()
            log("🔌 Database Closed")

        if driver:
            driver.quit()
            log("🛑 Browser Closed")


if __name__ == "__main__":
    main()
