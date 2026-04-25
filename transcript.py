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

# ✅ NEW (translation)
from googletrans import Translator

translator = Translator()

# ---------------- CONFIG ---------------- #
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ---------------- TRANSLATION ---------------- #
def translate_to_english(text):
    try:
        # ⚠️ GoogleTrans limit → split text
        chunks = [text[i:i+3000] for i in range(0, len(text), 3000)]
        translated = ""

        for chunk in chunks:
            res = translator.translate(chunk, src='hi', dest='en')
            translated += res.text + "\n"

        return translated.strip()
    except Exception as e:
        log(f"❌ Translation error: {e}")
        return text

# ---------------- HELPERS ---------------- #
def extract_video_id(url):
    parsed = urllib.parse.urlparse(url)
    if parsed.hostname == "youtu.be":
        return parsed.path[1:]
    if "youtube.com" in parsed.hostname:
        return urllib.parse.parse_qs(parsed.query).get("v", [None])[0]
    return None

def get_latest_videos(channel_url, count=3):
    video_links = []
    try:
        clean_url = channel_url.split('?')[0].rstrip('/')
        if not clean_url.endswith('/videos'):
            clean_url += '/videos'

        log(f"📺 Fetching videos from: {clean_url}")
        response = requests.get(clean_url, headers={"User-Agent": "Mozilla/5.0"})
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

    # ✅ Auto detect GitHub Actions
    if os.getenv("CI") == "true":
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        log("🤖 Headless mode")
    else:
        options.add_argument("--start-maximized")
        log("👀 Visible mode")

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": DOWNLOAD_DIR})

    return driver

# ---------------- SCRAPER ---------------- #
def get_video_data(youtube_url):
    driver = create_driver()
    video_title = "Unknown Title"
    transcript_text = None

    try:
        downsub_url = f"https://downsub.com/?url={urllib.parse.quote(youtube_url)}"
        driver.get(downsub_url)
        wait = WebDriverWait(driver, 30)

        # TITLE
        try:
            title_el = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".card-header b")))
            video_title = title_el.text.replace("Download Subtitles", "").strip()
        except:
            video_title = driver.title

        # CLEAR OLD FILES
        for f in os.listdir(DOWNLOAD_DIR):
            try:
                os.remove(os.path.join(DOWNLOAD_DIR, f))
            except:
                pass

        # DOWNLOAD TXT (Hindi)
        txt_button = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(., 'TXT')]")
        ))
        txt_button.click()

        # WAIT FILE
        start = time.time()
        while time.time() - start < 25:
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.txt')]
            if files:
                path = os.path.join(DOWNLOAD_DIR, files[0])
                with open(path, "r", encoding="utf-8") as f:
                    transcript_text = f.read()
                break
            time.sleep(2)

        return video_title, transcript_text

    except Exception as e:
        log(f"❌ Error: {e}")
        driver.save_screenshot("debug.png")
        return video_title, None

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
        log(f"✅ Saved: {title[:50]}...")
    except Exception as e:
        log(f"❌ DB Error: {e}")

# ---------------- RUN ---------------- #
if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/@stockmarketcommando"

    urls = get_latest_videos(target, count=3)

    if not urls:
        log("❌ No videos found.")
        sys.exit(1)

    for video_url in urls:
        log(f"🎬 Processing: {video_url}")

        vid_id = extract_video_id(video_url)
        title, text = get_video_data(video_url)

        if text:
            log("🌐 Translating to English...")
            text_en = translate_to_english(text)   # ✅ NEW STEP

            save_to_db(vid_id, video_url, title, text_en)

        time.sleep(2)
