import os
import time
import json
import mysql.connector

from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager


# ================= CONFIG ================= #

FILTER_TABLE = "filter"

TARGET_TABLE = "live_review_screens"


# ================= HELPERS ================= #

def remove_chart_popups(driver):

    scrub_script = """
    const popupSelectors = [
        '[class^="overlap-"]',
        '[class*="dialog-"]',
        '[class*="modal-"]',
        '.tv-dialog__close',
        '.js-dialog__close',
        '[data-role="toast-container"]',
        '[class*="popup-"]',
        '#overlap-manager-root',
        '.tp-modal',
        '.tv-ads-banner'
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

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )

    return driver


# ================= MAIN ================= #

def main():

    driver = None
    db_conn = None

    try:

        print(f"🚀 Execution Started : {datetime.utcnow()}")

        # ================= DB ================= #

        print("🔗 Connecting Database...")

        db_conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            autocommit=True
        )

        cur = db_conn.cursor(dictionary=True)

        # ================= FETCH FILTER STOCKS ================= #

        print("📊 Fetching BUY + WATCHLIST symbols...")

        # Relaxed temporarily to find out exactly why your rows aren't processing
        query = f"""
            SELECT
                id,
                symbol,
                timeframe,
                review_status,
                depriciate,
                screenshot_path
            FROM `{FILTER_TABLE}`
            WHERE
                LOWER(TRIM(review_status)) IN ('buy', 'watchlist')
            ORDER BY id DESC
        """

        cur.execute(query)
        all_found_stocks = cur.fetchall()

        # Filter manually in Python to log precisely what is failing
        stocks = []
        for s in all_found_stocks:
            status = str(s["review_status"]).lower().strip()
            dep = s["depriciate"]
            path = s["screenshot_path"]

            # Validate rules explicitly
            is_depriciate_ok = (dep == 0 or dep is None)
            is_path_ok = (path is not None and str(path).strip() != "")

            if is_depriciate_ok and is_path_ok:
                stocks.append(s)
            else:
                # Log exactly why this specific symbol was ignored
                reasons = []
                if not is_depriciate_ok:
                    reasons.append(f"depriciate is {dep} (expected 0/NULL)")
                if not is_path_ok:
                    reasons.append("screenshot_path is empty or NULL")
                print(f"⚠️  Skipped Symbol {s['symbol']} ({status.upper()}) -> Reason: {', '.join(reasons)}")

        if not stocks:
            print("😴 No symbols found matching all criteria after validation.")
            return

        # Separate and count statuses for detailed logging
        buy_count = sum(1 for s in stocks if str(s["review_status"]).lower().strip() == "buy")
        watchlist_count = sum(1 for s in stocks if str(s["review_status"]).lower().strip() == "watchlist")

        print(f"✅ Found total {len(stocks)} valid symbols -> [BUY: {buy_count} processed | WATCHLIST: {watchlist_count} processed]")

        # ================= DRIVER ================= #

        print("🌐 Starting Browser...")

        driver = get_optimized_driver()

        driver.get("https://www.tradingview.com/")

        cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES"))

        for c in cookies:

            driver.add_cookie({
                "name": c["name"],
                "value": c["value"],
                "domain": ".tradingview.com",
                "path": "/"
            })

        driver.refresh()

        success_count = 0

        # ================= LOOP ================= #

        for stock in stocks:

            try:

                symbol = stock["symbol"].upper().strip()

                timeframe = stock["timeframe"]

                review_status = stock["review_status"].upper()

                url = stock["screenshot_path"]

                filter_id = stock["id"]

                print(
                    f"📸 [{success_count + 1}/{len(stocks)}] {symbol} [{timeframe}] ({review_status})",
                    end=" ",
                    flush=True
                )

                # ================= OPEN URL ================= #

                driver.get(url)

                WebDriverWait(driver, 25).until(
                    EC.presence_of_element_located(
                        (By.TAG_NAME, "body")
                    )
                )

                time.sleep(6)

                remove_chart_popups(driver)

                time.sleep(2)

                # ================= SCREENSHOT ================= #

                img_data = driver.get_screenshot_as_png()

                # ================= DELETE OLD ================= #

                cur.execute(
                    f"""
                    DELETE FROM `{TARGET_TABLE}`
                    WHERE filter_id = %s
                    """,
                    (filter_id,)
                )

                # ================= INSERT NEW ================= #

                insert_sql = f"""
                    INSERT INTO `{TARGET_TABLE}`
                    (
                        filter_id,
                        symbol,
                        timeframe,
                        review_status,
                        screenshot,
                        source_url,
                        created_at
                    )
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s)
                """

                cur.execute(
                    insert_sql,
                    (
                        filter_id,
                        symbol,
                        timeframe,
                        stock["review_status"],
                        img_data,
                        url,
                        datetime.utcnow()
                    )
                )

                success_count += 1

                print("✅ Saved")

            except Exception as e:

                print(f"❌ ERROR : {str(e)[:200]}")

        print(f"🏁 DONE : {success_count} screenshots stored.")

    except Exception as e:

        print(f"🚨 CRITICAL ERROR : {e}")

    finally:

        if db_conn and db_conn.is_connected():

            cur.close()

            db_conn.close()

            print("🔌 Database Closed")

        if driver:

            driver.quit()

            print("🛑 Browser Closed")


if __name__ == "__main__":

    main()
