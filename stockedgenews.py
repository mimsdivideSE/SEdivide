
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

import mysql.connector
from datetime import datetime
import time
import os
import sys

# =========================================================
# LIVE LOGS FOR GITHUB ACTIONS
# =========================================================

sys.stdout.reconfigure(line_buffering=True)

# =========================================================
# MYSQL CONFIG
# =========================================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "autocommit": True,
    "connection_timeout": 300
}

# =========================================================
# CONNECT MYSQL
# =========================================================

print("🔌 Connecting MySQL...")

conn = mysql.connector.connect(**DB_CONFIG)

cursor = conn.cursor(dictionary=True)

print("✅ MySQL Connected")

# =========================================================
# GET BUY + WATCHLIST SYMBOLS
# =========================================================

print("\n📥 Fetching symbols...")

query = """
SELECT DISTINCT symbol
FROM filter
WHERE review_status IN ('buy', 'watchlist')
AND symbol IS NOT NULL
AND symbol != ''
ORDER BY symbol ASC
"""

cursor.execute(query)

stocks = cursor.fetchall()

print(f"✅ Total Symbols Found: {len(stocks)}")

# =========================================================
# TODAY DATE
# =========================================================

today_date = datetime.now().date()

print(f"📅 Today Date: {today_date}")

# =========================================================
# CHROME OPTIONS
# =========================================================

chrome_options = Options()

chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-infobars")
chrome_options.add_argument("--disable-popup-blocking")

# FASTER PAGE LOADS
chrome_options.page_load_strategy = "eager"

print("\n🚀 Starting Chrome...")

driver = webdriver.Chrome(options=chrome_options)

# PAGE TIMEOUT
driver.set_page_load_timeout(30)

print("✅ Chrome Started")

# =========================================================
# COUNTERS
# =========================================================

total_saved = 0
total_duplicates = 0
total_errors = 0

# =========================================================
# LOOP SYMBOLS
# =========================================================

for index, stock in enumerate(stocks, start=1):

    symbol = stock["symbol"].strip()

    print("\n================================================")
    print(f"📊 [{index}/{len(stocks)}] {symbol}")
    print("================================================")

    today_news_found = False

    try:

        # =================================================
        # KEEP MYSQL CONNECTION ALIVE
        # =================================================

        conn.ping(reconnect=True, attempts=3, delay=2)

        # =================================================
        # OPEN STOCKEDGE SEARCH
        # =================================================

        print("🌐 Opening StockEdge Search...")

        driver.get("https://search.stockedge.com/")

        time.sleep(1)

        # =================================================
        # SEARCH SYMBOL
        # =================================================

        print(f"🔍 Searching: {symbol}")

        search_box = driver.find_element(By.ID, "searchText")

        search_box.clear()

        search_box.send_keys(symbol)

        time.sleep(0.5)

        search_box.send_keys(Keys.ENTER)

        time.sleep(2)

        # =================================================
        # GET FIRST RESULT
        # =================================================

        print("📄 Getting stock URL...")

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

            total_errors += 1

            continue

        print("✅ Stock URL Found")

        # =================================================
        # OPEN FEEDS PAGE
        # =================================================

        feed_url = stock_url + "?section=feeds"

        print("📰 Opening feeds page...")

        driver.get(feed_url)

        time.sleep(2)

        # =================================================
        # GET FEED ITEMS
        # =================================================

        feed_items = driver.find_elements(By.TAG_NAME, "ion-item")

        print(f"📰 Feed Items Found: {len(feed_items)}")

        # =================================================
        # PROCESS FEEDS
        # =================================================

        for item_index, item in enumerate(feed_items, start=1):

            try:

                # =========================================
                # KEEP MYSQL ALIVE
                # =========================================

                conn.ping(reconnect=True, attempts=3, delay=2)

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
                # STOP IF OLDER DATE
                # =========================================

                if log_date != today_date:

                    print(f"⏭ Older News Found: {log_date}")
                    print("🛑 Stopping Feed Processing")

                    break

                # =========================================
                # TODAY NEWS FOUND
                # =========================================

                today_news_found = True

                # =========================================
                # GET HEADLINE
                # =========================================

                headline = item.find_element(
                    By.TAG_NAME,
                    "p"
                ).text.strip()

                if not headline:
                    continue

                print(f"\n📰 Feed #{item_index}")
                print(f"📅 {log_date}")
                print(f"📝 {headline[:150]}")

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

                    total_duplicates += 1

                    print("⚠ Already Exists")

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

                total_saved += 1

                print("✅ Saved Successfully")

            except Exception as feed_error:

                print(f"❌ Feed Error: {feed_error}")

                continue

        # =================================================
        # NO NEWS TODAY
        # =================================================

        if not today_news_found:

            print("📭 No news today")

            no_news_text = "No updates today"

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
                    today_date,
                    no_news_text
                )
            )

            exists = cursor.fetchone()

            if exists:

                total_duplicates += 1

                print("⚠ No-update entry already exists")

            else:

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
                        today_date,
                        no_news_text
                    )
                )

                total_saved += 1

                print("✅ Stored: No updates today")

        print(f"✅ Completed: {symbol}")

    except Exception as stock_error:

        total_errors += 1

        print(f"❌ Stock Error: {stock_error}")

        continue

# =========================================================
# CLOSE EVERYTHING
# =========================================================

print("\n================================================")
print("🎯 FINAL SUMMARY")
print("================================================")

print(f"✅ Total Saved: {total_saved}")
print(f"⚠ Total Duplicates: {total_duplicates}")
print(f"❌ Total Errors: {total_errors}")

driver.quit()

cursor.close()

conn.close()

print("\n✅ Browser Closed")
print("✅ MySQL Closed")
print("✅ Script Completed")

