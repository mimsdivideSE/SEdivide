import sys
import time
import os
import requests
import pymysql
import urllib.parse
import re
from datetime import datetime
from contextlib import closing

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def extract_video_id(url):
    parsed = urllib.parse.urlparse(url)
    if parsed.hostname == "youtu.be":
        return parsed.path[1:]
    if "youtube.com" in parsed.hostname:
        query = urllib.parse.parse_qs(parsed.query)
        return query.get("v", [None])[0]
    return None

def get_latest_videos(channel_url, count=3):
    video_links = []
    try:
        clean_url = channel_url.split('?')[0].rstrip('/')
        if not clean_url.endswith('/videos'):
            clean_url += '/videos'

        log(f"📺 Fetching videos from: {clean_url}")
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(clean_url, headers=headers)

        video_ids = re.findall(r'"videoId":"([^"]+)"', response.text)

        unique_ids = []
        for vid in video_ids:
            if vid not in unique_ids:
                unique_ids.append(vid)

        for vid in unique_ids[:count]:
            video_links.append(f"https://www.youtube.com/watch?v={vid}")

        log(f"✅ Found {len(video_links)} videos.")
    except Exception as e:
        log(f"❌ Error fetching videos: {e}")

    return video_links

# ---------------- DRIVER ---------------- #
def create_driver():
    options = Options()

    # ✅ SHOW BROWSER (IMPORTANT CHANGE)
    # options.add_argument("--headless=new")  ❌ removed

    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": DOWNLOAD_DIR})

    return driver

# ---------------- MAIN LOGIC ---------------- #
def get_video_data(youtube_url):
    driver = create_driver()
    wait = WebDriverWait(driver, 30)

    try:
        downsub_url = f"https://downsub.com/?url={urllib.parse.quote(youtube_url)}"
        driver.get(downsub_url)

        log("🌐 Opened DownSub")

        # -------- TITLE --------
        try:
            title_el = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".card-header b")))
            video_title = title_el.text.replace("Download Subtitles", "").strip()
        except:
            video_title = driver.title

        # -------- CLEAR OLD FILES --------
        for f in os.listdir(DOWNLOAD_DIR):
            try:
                os.remove(os.path.join(DOWNLOAD_DIR, f))
            except:
                pass

        # ================= STEP 1: EDIT =================
        edit_btn = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//button"
        )))
        driver.execute_script("arguments[0].click();", edit_btn)
        log("✏️ Clicked Edit")

        # ✅ WAIT FOR NEW PAGE
        wait.until(EC.presence_of_element_located((By.ID, "__nuxt")))
        log("✅ Editor page loaded")
        time.sleep(3)

        # ================= STEP 2: TRANSLATE =================
        translate_btn = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//button[contains(., 'Translate')]"
        )))
        driver.execute_script("arguments[0].click();", translate_btn)
        log("🌐 Clicked Translate")
        time.sleep(3)

        # ================= STEP 3: SELECT ENGLISH =================
        try:
            eng = wait.until(EC.element_to_be_clickable((
                By.XPATH, "//li[contains(., 'English')]"
            )))
            eng.click()
            log("🇬🇧 Selected English")
        except:
            log("⚠️ English auto-selected")

        time.sleep(4)

        # ================= STEP 4: DOWNLOAD =================
        download_btn = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//button[contains(., 'Download')]"
        )))
        driver.execute_script("arguments[0].click();", download_btn)
        log("⬇️ Opened download popup")

        time.sleep(3)

        # ================= STEP 5: POPUP =================
        try:
            original = wait.until(EC.element_to_be_clickable((
                By.XPATH, "//div[contains(., 'Original')]"
            )))
            original.click()
            log("☑️ Unchecked original")
        except:
            pass

        try:
            translated = wait.until(EC.element_to_be_clickable((
                By.XPATH, "//div[contains(., 'Translation')]"
            )))
            translated.click()
            log("☑️ Checked translation")
        except:
            pass

        time.sleep(2)

        # ================= STEP 6: FINAL DOWNLOAD =================
        final_btn = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//button[contains(., 'Download')]"
        )))
        final_btn.click()
        log("📥 Download started")

        # -------- WAIT FILE --------
        timeout = 30
        start = time.time()

        while time.time() - start < timeout:
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".txt")]
            if files:
                file_path = os.path.join(DOWNLOAD_DIR, files[0])
                with open(file_path, "r", encoding="utf-8") as f:
                    text = f.read()
                return video_title, text
            time.sleep(2)

        return video_title, None

    except Exception as e:
        log(f"❌ ERROR: {e}")
        driver.save_screenshot("debug.png")  # 👈 VERY IMPORTANT
        return None, None

    finally:
        driver.quit()

# ---------------- DB ---------------- #
def save_to_db(video_id, url, title, content):
    if not DB_CONFIG['host'] or not content:
        return
    try:
        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO wp_transcript (video_id, video_url, title, content)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        title = VALUES(title),
                        content = VALUES(content)
                """, (video_id, url, title, content))
            conn.commit()
        log(f"✅ Saved: {title[:50]}")
    except Exception as e:
        log(f"❌ DB Error: {e}")

# ---------------- RUN ---------------- #
if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/@stockmarketcommando"

    urls = get_latest_videos(target, 3)

    for url in urls:
        log(f"🎬 Processing: {url}")
        vid = extract_video_id(url)

        title, text = get_video_data(url)

        if text:
            save_to_db(vid, url, title, text)

        time.sleep(2)
