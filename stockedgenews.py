
# =========================================================
# STOCKEDGE NEWS SCRAPER + MYSQL STORAGE
# USING ENV VARIABLES
# =========================================================

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from webdriver_manager.chrome import ChromeDriverManager

import mysql.connector
from datetime import datetime
import time
import os

# =========================================================
# DATABASE CONFIG FROM ENV
# =========================================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

# =========================================================
# CONNECT MYSQL
# =========================================================

conn = mysql.connector.connect(**DB_CONFIG)

cursor = conn.cursor(dictionary=True)

print("✅ MySQL Connected")

# =========================================================
# GET SYMBOLS
# =========================================================

query = """
SELECT DISTINCT symbol
FROM filter
WHERE review_status IN ('buy', 'watchlist')
AND symbol IS NOT NULL
AND symbol != ''
"""

cursor.execute(query)

stocks = cursor.fetchall()

print(f"✅ Found {len(stocks)} symbols")

# =========================================================
# CHROME SETUP
# =========================================================

chrome_options = Options()

chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--window-size=1920,1080")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)

# =========================================================
# LOOP THROUGH SYMBOLS
# =========================================================

for stock in stocks:

    symbol = stock["symbol"]

    print("\n===================================")
    print(f"🔍 Processing: {symbol}")
    print("===================================")

    try:

        # =================================================
        # OPEN STOCKEDGE SEARCH
        # =================================================

        driver.get("https://search.stockedge.com/")

        time.sleep(3)

        # =================================================
        # SEARCH STOCK
        # =================================================

        search_box = driver.find_element(By.ID, "searchText")

        search_box.clear()

        search_box.send_keys(symbol)

        time.sleep(1)

        search_box.send_keys(Keys.ENTER)

        print(f"Searching: {symbol}")

        time.sleep(5)

        # =================================================
        # GET FIRST RESULT
        # =================================================

        first_result = driver.find_element(
            By.CSS_SELECTOR,
            ".response-table tr td span.entity_name"
        )

        parent_td = first_result.find_element(
            By.XPATH,
            "./ancestor::td"
        )

        stock_link = parent_td.find_element(By.TAG_NAME, "a")

        stock_url = stock_link.get_attribute("href")

        if not stock_url:
            print("❌ No stock URL found")
            continue

        # =================================================
        # OPEN FEEDS PAGE
        # =================================================

        feed_url = stock_url + "?section=feeds"

        print("Opening feeds page...")

        driver.get(feed_url)

        time.sleep(5)

        # =================================================
        # GET FEED ITEMS
        # =================================================

        feed_items = driver.find_elements(By.TAG_NAME, "ion-item")

        print(f"📰 Found {len(feed_items)} feed items")

        # =================================================
        # LOOP FEEDS
        # =================================================

        for item in feed_items:

            try:

                date_text = item.find_element(
                    By.CSS_SELECTOR,
                    "ion-col:nth-child(2) ion-text"
                ).text.strip()

                log_date = datetime.strptime(
                    date_text,
                    "%d-%b-%Y"
                ).date()

            except:
                continue

            try:

                headline = item.find_element(
                    By.TAG_NAME,
                    "p"
                ).text.strip()

            except:
                headline = ""

            if not headline:
                continue

            # =============================================
            # CHECK DUPLICATE
            # =============================================

            check_query = """
            SELECT id
            FROM wp_terminal_news_archive
            WHERE symbol = %s
            AND log_date = %s
            AND news_content = %s
            LIMIT 1
            """

            cursor.execute(
                check_query,
                (
                    symbol,
                    log_date,
                    headline
                )
            )

            exists = cursor.fetchone()

            if exists:
                print("⚠ Already exists")
                continue

            # =============================================
            # INSERT NEWS
            # =============================================

            insert_query = """
            INSERT INTO wp_terminal_news_archive
            (
                symbol,
                log_date,
                news_content
            )
            VALUES (%s, %s, %s)
            """

            cursor.execute(
                insert_query,
                (
                    symbol,
                    log_date,
                    headline
                )
            )

            conn.commit()

            print(f"✅ Saved: {headline[:80]}")

    except Exception as e:

        print(f"❌ Error for {symbol}: {e}")

# =========================================================
# CLOSE EVERYTHING
# =========================================================

driver.quit()

cursor.close()

conn.close()

print("\n===================================")
print("✅ ALL WORK COMPLETED")
print("===================================")

