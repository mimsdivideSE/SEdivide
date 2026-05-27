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
    """Ensures instant, unbuffered logs appear cleanly in real-time CI logs."""
    print(message, end=end)
    sys.stdout.flush()


def get_clean_driver():
    """Initializes an optimized, stealthy, resource-light headless Chrome instance."""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    
    # Extra performance & optimization flags
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--mute-audio")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--blink-features=AutomationControlled")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )
    # Strict network safety timeout so frozen asset loading won't lock up pipelines
    driver.set_page_load_timeout(25)
    return driver


def save_screenshot_to_db(filter_id, symbol, timeframe, status, img_data, chart_url):
    """Safely handles the individual transactional lifecycle for saving screenshots."""
    db_conn = None
    try:
        db_conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            autocommit=True
        )
        with db_conn.cursor() as cur:
            # Clear old records matching the target configuration identifier
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
    except mysql.connector.Error as db_err:
        log(f"⚠️ [DB Write Warning]: Failed to update database for {symbol}. Error: {db_err}")
        raise db_err
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()


# ================= MAIN RUNNER ================= #

def main():
    driver = None
    stocks = []

    try:
        log(f"🚀 Execution Started : {datetime.utcnow()}")

        # ================= STEP 1: ISOLATED SYMBOL EXTRACTION ================= #
        log("🔗 Connecting Database to fetch targets...")
        try:
            db_conn = mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
                autocommit=True
            )
            with db_conn.cursor(dictionary=True) as cur:
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
                
                # --- TRUNCATE TARGET TABLE BEFORE NEW PROCESSING ---
                if stocks:
                    log(f"🧹 Truncating target table `{TARGET_TABLE}` before inserting new records...")
                    cur.execute(f"TRUNCATE TABLE `{TARGET_TABLE}`")
                    
        finally:
            if 'db_conn' in locals() and db_conn.is_connected():
                db_conn.close()
                log("🔌 Target fetch connection securely terminated. Data loaded to memory.")

        if not stocks:
            log("😴 No matching symbols found in the database. Process clean shutdown.")
            return

        buy_count = sum(1 for s in stocks if str(s["review_status"]).lower() == "buy")
        watchlist_count = sum(1 for s in stocks if str(s["review_status"]).lower() == "watchlist")
        log(f"✅ Targets Found -> Total: {len(stocks)} [BUY: {buy_count} | WATCHLIST: {watchlist_count}]")

        # ================= STEP 2: ORCHESTRATE SELENIUM ENGINE ================= #
        log("🌐 Initializing Headless Chrome Instance...")
        driver = get_clean_driver()
        success_count = 0

        # ================= STEP 3: HIGH-RELIABILITY SCREENSHOT LOOP ================= #
        for idx, stock in enumerate(stocks, start=1):
            symbol = str(stock["symbol"]).upper().strip()
            timeframe = stock["timeframe"]
            status = stock["review_status"]
            filter_id = stock["id"]

            # Optimize query injection to automatically switch TradingView timeframes natively via URL parameter
            chart_url = f"https://www.tradingview.com/chart/?symbol={symbol}&interval={timeframe}"
            log(f"📸 [{idx}/{len(stocks)}] Processing {symbol} ({status.upper()})...", end=" ")

            try:
                # Direct route load with explicit safety block mitigation
                try:
                    driver.get(chart_url)
                except Exception:
                    # Ignore partial timeout breaks from heavy script metrics assets; work with structural DOM
                    pass

                # Explicitly confirm essential body container exists
                WebDriverWait(driver, 12).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Settle window time for technical indicator updates/rendering
                time.sleep(4)

                # Capture structural view layout directly
                img_data = driver.get_screenshot_as_png()

                # Dispatch atomic transaction write
                save_screenshot_to_db(filter_id, symbol, timeframe, status, img_data, chart_url)

                success_count += 1
                log("✅ Saved")

            except Exception as item_error:
                error_msg = str(item_error)
                log(f"❌ Failed: {error_msg[:80]}")
                
                # Self-healing engine protocol: If the browser instance crashed, dropped out, or froze, recycle it
                if "invalid session id" in error_msg.lower() or "chrome not reachable" in error_msg.lower():
                    log("♻️ Critical browser error encountered. Recycling Chrome instance...")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = get_clean_driver()

        log(f"🏁 DONE : Completed {success_count} snapshots successfully.")

    except Exception as critical_error:
        log(f"🚨 CRITICAL RUNTIME ERROR : {critical_error}")

    finally:
        if driver:
            try:
                driver.quit()
                log("🛑 Browser System Safely Closed")
            except Exception:
                pass


if __name__ == "__main__":
    main()
