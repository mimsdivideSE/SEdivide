
# =========================================================
# STOCKEDGE TODAY NEWS SCRAPER
# ONLY BUY + WATCHLIST SYMBOLS
# STORE "No updates today" IF NO NEWS
# =========================================================

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

import mysql.connector
from datetime import datetime
import time
import os

# =========================================================
# MYSQL CONFIG
# =========================================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "autocommit": True
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

print("\n🚀 Starting Chrome...")

driver = webdriver.Chrome(options=chrome_options)

print("✅ Chrome Started")

# =========================================================
# COUNTERS
# =========================================================

total_saved = 0
total_duplicates = 0

# =========================================================
# LOOP SYMBOLS
# =========================================================

for index, stock in enumerate(stocks, start=1):

    symbol = stock["symbol"]

    print("\n================================================")
    print(f"📊 [{index}/{len(stocks)}] {symbol}")
    print("================================================")

    today_news_found = False

    try:

        # =================================================
        # OPEN STOCKEDGE SEARCH
        # =================================================

        driver.get("https://search.stockedge.com/")

        time.sleep(1)

        # =================================================
        # SEARCH SYMBOL
        # =================================================

        search_box = driver.find_element(By.ID, "searchText")

        search_box.clear()

        search_box.send_keys(symbol)

        time.sleep(1)

        search_box.send_keys(Keys.ENTER)

        time.sleep(2)

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

        print("📰 Opening feeds page...")

        driver.get(feed_url)

        time.sleep(2)

        # =================================================
        # GET FEEDS
        # =================================================

        feed_items = driver.find_elements(By.TAG_NAME, "ion-item")

        print(f"📰 Feed Items Found: {len(feed_items)}")

        # =================================================
        # PROCESS TODAY NEWS ONLY
        # =================================================

        for item in feed_items:

            try:

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
                    print("🛑 Stopping")

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

                print(f"\n📰 {headline}")

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

                print("✅ Saved")

            except Exception as feed_error:

                print(f"❌ Feed Error: {feed_error}")

                continue

        # =================================================
        # NO NEWS TODAY
        # =================================================

        if not today_news_found:

            print("📭 No news today")

            no_news_text = "No updates today"

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
                    today_date,
                    no_news_text
                )
            )

            exists = cursor.fetchone()

            if exists:

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

    except Exception as stock_error:

        print(f"❌ Stock Error: {stock_error}")

        continue

# =========================================================
# CLOSE EVERYTHING
# =========================================================

driver.quit()

cursor.close()

conn.close()

print("\n================================================")
print("🎯 FINAL SUMMARY")
print("================================================")

print(f"✅ Total Saved: {total_saved}")
print(f"⚠ Total Duplicates: {total_duplicates}")

print("\n✅ Script Completed")

