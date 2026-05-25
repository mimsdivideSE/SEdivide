
# =========================================================
# STOCKEDGE NEWS SCRAPER + MYSQL STORAGE
# PRODUCTION SAFE VERSION
# =========================================================

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from webdriver_manager.chrome import ChromeDriverManager

import mysql.connector
from mysql.connector import Error

from datetime import datetime
import time
import os
import traceback

# =========================================================
# DATABASE CONFIG
# =========================================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),

    # IMPORTANT
    "connection_timeout": 600,
    "autocommit": True
}

# =========================================================
# MYSQL CONNECT FUNCTION
# =========================================================

def connect_mysql():

    try:

        print("\n🔌 Connecting MySQL...")

        conn = mysql.connector.connect(**DB_CONFIG)

        if conn.is_connected():

            print("✅ MySQL Connected Successfully")

            return conn

    except Error as e:

        print(f"❌ MySQL Connection Error: {e}")

        return None

# =========================================================
# CREATE MYSQL CONNECTION
# =========================================================

conn = connect_mysql()

if not conn:
    raise Exception("Database connection failed")

cursor = conn.cursor(dictionary=True)

# =========================================================
# GET SYMBOLS
# =========================================================

print("\n📥 Fetching BUY/WATCHLIST symbols...")

query = """
SELECT DISTINCT symbol
FROM filter
WHERE review_status IN ('buy', 'watchlist')
AND symbol IS NOT NULL
AND symbol != ''
"""

cursor.execute(query)

stocks = cursor.fetchall()

print(f"✅ Total Symbols Found: {len(stocks)}")

# =========================================================
# CHROME SETUP
# =========================================================

print("\n🚀 Launching Chrome Browser...")

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

print("✅ Chrome Started Successfully")

# =========================================================
# PROCESS STOCKS
# =========================================================

total_saved = 0
total_duplicates = 0
total_errors = 0

for index, stock in enumerate(stocks, start=1):

    symbol = stock["symbol"]

    print("\n====================================================")
    print(f"📊 [{index}/{len(stocks)}] Processing: {symbol}")
    print("====================================================")

    try:

        # =================================================
        # KEEP MYSQL CONNECTION ALIVE
        # =================================================

        conn.ping(reconnect=True, attempts=3, delay=5)

        # =================================================
        # OPEN SEARCH PAGE
        # =================================================

        print("🌐 Opening StockEdge Search...")

        driver.get("https://search.stockedge.com/")

        time.sleep(3)

        # =================================================
        # SEARCH STOCK
        # =================================================

        print(f"🔍 Searching Symbol: {symbol}")

        search_box = driver.find_element(By.ID, "searchText")

        search_box.clear()

        search_box.send_keys(symbol)

        time.sleep(1)

        search_box.send_keys(Keys.ENTER)

        time.sleep(5)

        # =================================================
        # GET RESULT
        # =================================================

        print("📄 Extracting stock URL...")

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

        print(f"✅ Stock URL Found")

        # =================================================
        # OPEN FEEDS PAGE
        # =================================================

        feed_url = stock_url + "?section=feeds"

        print("📰 Opening feeds page...")

        driver.get(feed_url)

        time.sleep(5)

        # =================================================
        # GET FEED ITEMS
        # =================================================

        feed_items = driver.find_elements(By.TAG_NAME, "ion-item")

        print(f"📰 Total Feed Items Found: {len(feed_items)}")

        saved_count = 0
        duplicate_count = 0

        # =================================================
        # PROCESS FEEDS
        # =================================================

        for item_index, item in enumerate(feed_items, start=1):

            try:

                # =========================================
                # KEEP MYSQL CONNECTION ALIVE
                # =========================================

                conn.ping(reconnect=True, attempts=3, delay=5)

                # =========================================
                # GET DATE
                # =========================================

                date_text = item.find_element(
                    By.CSS_SELECTOR,
                    "ion-col:nth-child(2) ion-text"
                ).text.strip()

                log_date = datetime.strptime(
                    date_text,
                    "%d-%b-%Y"
                ).date()

                # =========================================
                # GET HEADLINE
                # =========================================

                headline = item.find_element(
                    By.TAG_NAME,
                    "p"
                ).text.strip()

                if not headline:
                    continue

                print(f"\n📝 Feed #{item_index}")
                print(f"📅 Date: {log_date}")
                print(f"📰 Headline: {headline[:120]}")

                # =========================================
                # CHECK DUPLICATE
                # =========================================

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

                    duplicate_count += 1
                    total_duplicates += 1

                    print("⚠ Duplicate News")

                    continue

                # =========================================
                # INSERT NEWS
                # =========================================

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

                saved_count += 1
                total_saved += 1

                print("✅ News Saved Successfully")

            except Exception as feed_error:

                print(f"❌ Feed Processing Error: {feed_error}")

                continue

        # =================================================
        # STOCK SUMMARY
        # =================================================

        print("\n--------------------------------------------")
        print(f"✅ Completed: {symbol}")
        print(f"💾 Saved: {saved_count}")
        print(f"⚠ Duplicates: {duplicate_count}")
        print("--------------------------------------------")

    except Exception as stock_error:

        total_errors += 1

        print(f"\n❌ STOCK ERROR: {symbol}")
        print(f"❌ Error Message: {stock_error}")

        traceback.print_exc()

        continue

# =========================================================
# CLOSE EVERYTHING
# =========================================================

print("\n====================================================")
print("🎯 FINAL SUMMARY")
print("====================================================")

print(f"✅ Total News Saved: {total_saved}")
print(f"⚠ Total Duplicates: {total_duplicates}")
print(f"❌ Total Errors: {total_errors}")

driver.quit()

cursor.close()

conn.close()

print("\n✅ Browser Closed")
print("✅ MySQL Closed")
print("✅ Script Completed")

