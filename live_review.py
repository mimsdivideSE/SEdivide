import os
import time
import json
import gspread
import pandas as pd
import mysql.connector
import sys
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

# GOOGLE SHEET
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
STOCK_LIST_GID = 1400370843

# ================= HELPERS ================= #

def log(message, end="\n"):
    print(message, end=end)
    sys.stdout.flush()


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


def get_clean_driver():

    opts = Options()

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    # Anti-fingerprint and graphics flags to resolve data connection issues
    opts.add_argument("--use-gl=angle")
    opts.add_argument("--use-angle=swiftshader")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    # PERFORMANCE FLAGS
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--mute-audio")

    opts.add_experimental_option(
        "excludeSwitches",
        ["enable-automation", "enable-logging"]
    )

    opts.add_experimental_option(
        "useAutomationExtension",
        False
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )

    # Core injection script to dynamically mask webdriver attributes
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """
    })

    driver.set_page_load_timeout(35)

    return driver


def save_screenshot_to_db(
    filter_id,
    symbol,
    timeframe,
    status,
    img_data,
    chart_url
):

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

            cur.execute(
                f"""
                DELETE FROM `{TARGET_TABLE}`
                WHERE filter_id = %s
                """,
                (filter_id,)
            )

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
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            cur.execute(
                insert_sql,
                (
                    filter_id,
                    symbol,
                    timeframe,
                    status,
                    img_data,
                    chart_url,
                    datetime.utcnow()
                )
            )

    except mysql.connector.Error as db_err:

        log(
            f"⚠️ [DB ERROR]: {symbol} -> {db_err}"
        )

        raise db_err

    finally:

        if db_conn and db_conn.is_connected():
            db_conn.close()


# ================= MAIN ================= #

def main():

    driver = None
    stocks = []

    try:

        log(f"🚀 Execution Started : {datetime.utcnow()}")

        # ================= DATABASE ================= #

        log("🔗 Connecting Database...")

        try:

            db_conn = mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
                autocommit=True
            )

            with db_conn.cursor(dictionary=True) as cur:

                log(
                    "📊 Fetching BUY & WATCHLIST rows..."
                )

                query = f"""
                    SELECT
                        id,
                        symbol,
                        timeframe,
                        review_status
                    FROM `{FILTER_TABLE}`
                    WHERE review_status IN ('buy', 'watchlist')
                    AND (
                        depriciate = 0
                        OR depriciate IS NULL
                    )
                    ORDER BY id DESC
                """

                cur.execute(query)

                stocks = cur.fetchall()

                if stocks:

                    log(
                        f"🧹 Truncating `{TARGET_TABLE}`..."
                    )

                    cur.execute(
                        f"TRUNCATE TABLE `{TARGET_TABLE}`"
                    )

        finally:

            if (
                'db_conn' in locals()
                and db_conn.is_connected()
            ):

                db_conn.close()

                log(
                    "🔌 Database Connection Closed."
                )

        if not stocks:

            log("😴 No Stocks Found.")

            return

        buy_count = sum(
            1 for s in stocks
            if str(s["review_status"]).lower() == "buy"
        )

        watchlist_count = sum(
            1 for s in stocks
            if str(s["review_status"]).lower() == "watchlist"
        )

        log(
            f"✅ Total: {len(stocks)} "
            f"[BUY: {buy_count} | WATCHLIST: {watchlist_count}]"
        )

        # ================= GOOGLE SHEET ================= #

        log("📄 Loading Google Sheet URLs...")

        creds = json.loads(
            os.getenv("GSPREAD_CREDENTIALS")
        )

        gc = gspread.service_account_from_dict(creds)

        ws = gc.open_by_url(
            STOCK_LIST_URL
        ).get_worksheet_by_id(STOCK_LIST_GID)

        data = ws.get_all_values()

        df = pd.DataFrame(
            data[1:],
            columns=data[0]
        )

        # ================= URL MAP ================= #

        url_map = {}

        for _, row in df.iterrows():

            symbol = row.iloc[0].strip().upper()

            urls = {}

            # DAY URL
            if len(row) > 2 and row.iloc[2]:
                urls["day"] = row.iloc[2]

            # WEEK URL
            if len(row) > 3 and row.iloc[3]:
                urls["week"] = row.iloc[3]

            if urls:
                url_map[symbol] = urls

        # ================= BROWSER INITIALIZATION WITH COOKIES ================= #

        log("🌐 Initializing Chrome...")

        driver = get_clean_driver()

        log("🍪 Injecting TradingView Authentication Session...")
        
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

        for idx, stock in enumerate(stocks, start=1):

            symbol = str(
                stock["symbol"]
            ).upper().strip()

            timeframe = str(
                stock["timeframe"]
            ).lower().strip()

            status = stock["review_status"]

            filter_id = stock["id"]

            # ================= GET GOOGLE SHEET URL ================= #

            urls = url_map.get(symbol)

            if not urls:

                log(
                    f"⚠️ No Google Sheet URL Found for {symbol}"
                )

                continue

            chart_url = urls.get(timeframe)

            if not chart_url:

                log(
                    f"⚠️ No {timeframe} URL for {symbol}"
                )

                continue

            log(
                f"📸 [{idx}/{len(stocks)}] "
                f"{symbol} ({status.upper()}) [{timeframe}]...",
                end=" "
            )

            try:

                try:
                    driver.get(chart_url)
                except Exception:
                    pass

                # Explicitly wait until the layout engine structure loads completely
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located(
                        (By.CLASS_NAME, "chart-container-canvas-layer")
                    )
                )

                # Extended sleep time to confirm candle data stream connection completes
                time.sleep(8)

                remove_chart_popups(driver)

                time.sleep(2)

                img_data = driver
