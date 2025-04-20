# -*- coding: utf-8 -*-
import time
import datetime
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException, NoSuchWindowException
)
# Import PageLoadStrategy (Requires Selenium 4+)
from selenium.webdriver.common.page_load_strategy import PageLoadStrategy
# Using webdriver-manager is generally preferred for handling driver versions
from webdriver_manager.chrome import ChromeDriverManager
import logging
import io
import traceback
from pathlib import Path
import matplotlib
# Set backend *before* importing pyplot for headless environments
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.table import Table
import matplotlib.font_manager as fm
import retrying
import json
import re
import os

# --- Timezone Handling ---
# Use standard library zoneinfo (Python 3.9+) or fallback to pytz
try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
    logging.info("Using zoneinfo for KST.")
except ImportError:
    try:
        import pytz
        KST = pytz.timezone("Asia/Seoul")
        logging.info("Using pytz for KST.")
    except ImportError:
        logging.error("Neither zoneinfo nor pytz found. Timezone might be incorrect. Please install pytz.")
        # Fallback to UTC or raise error? For KST-specific report, raising might be better.
        # For now, log error and continue with system time potentially being UTC.
        KST = None # Indicate timezone couldn't be set

# --- Configuration ---
# Read credentials from environment variables (Set these in Render Cron Job Environment)
WEBMAIL_USERNAME = os.environ.get("WEBMAIL_USERNAME")
WEBMAIL_PASSWORD = os.environ.get("WEBMAIL_PASSWORD")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# --- Other Settings ---
# Configure logging level from environment or default to INFO
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info(f"Logging level set to: {LOG_LEVEL}")


WEBMAIL_LOGIN_URL = "http://gw.ktmos.co.kr/mail2/loginPage.do"
WEBMAIL_ID_FIELD_ID = "userEmail"
WEBMAIL_PW_FIELD_ID = "userPw"

# --- Target Date Calculation (using KST if possible) ---
if KST:
    TARGET_DATETIME_KST = datetime.datetime.now(KST)
else:
    TARGET_DATETIME_KST = datetime.datetime.now() # Use system time (likely UTC on Render)
    logging.warning("KST timezone not available, using system time for date calculation.")

TARGET_DATE = TARGET_DATETIME_KST.date()
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")
CURRENT_HOUR_KST = TARGET_DATETIME_KST.hour # Used for evening check

logging.info(f"Script running for target date: {TARGET_DATE_STR} (Derived from KST if possible)")
logging.info(f"Current KST hour (for logic): {CURRENT_HOUR_KST}")


REPORT_DOWNLOAD_URL_TEMPLATE = "http://gw.ktmos.co.kr/owattend/rest/dclz/report/CompositeStatus/sumr/user/days/excel?startDate={date}&endDate={date}&deptSeq=1231&erpNumDisplayYn=Y"
REPORT_URL = REPORT_DOWNLOAD_URL_TEMPLATE.format(date=TARGET_DATE_STR)

EXCEL_SHEET_NAME = "세부현황_B"
STANDARD_START_TIME_STR = "09:00:00"
STANDARD_END_TIME_STR = "18:00:00"
EVENING_RUN_THRESHOLD_HOUR = 18 # KST hour (e.g., 18 for 6 PM KST)

# --- Constants for Leave Types ---
FULL_DAY_LEAVE_REASONS = {"연차", "보건휴가", "출산휴가", "출산전후휴가", "청원휴가", "가족돌봄휴가", "특별휴가", "공가", "공상", "예비군훈련", "민방위훈련", "공로휴가", "병가", "보상휴가"}
FULL_DAY_LEAVE_TYPES = {"법정휴가", "병가/휴직", "보상휴가", "공가"}
MORNING_HALF_LEAVE = "오전반차"
AFTERNOON_HALF_LEAVE = "오후반차"
ATTENDANCE_TYPE = "출퇴근"

# --- Helper Functions ---

def escape_markdown(text):
    """Escapes characters for Telegram MarkdownV2"""
    if text is None: return ''
    text = str(text); escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def send_telegram_message_basic(bot_token, chat_id, text):
    """Basic message sending for early critical errors, attempts MarkdownV2 then plain."""
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    logging.info(f"Attempting to send basic message (first 50 chars): {text[:50]}")
    # Try MarkdownV2 first
    payload = {'chat_id': chat_id, 'text': escape_markdown(text), 'parse_mode': 'MarkdownV2'}
    try:
        response = requests.post(api_url, data=payload, timeout=10)
        response.raise_for_status()
        if response.json().get("ok"):
             logging.info(f"Basic TG message sent successfully (MarkdownV2).")
             return True
        else: # MarkdownV2 failed at API level
             logging.warning(f"Basic TG API Error (MarkdownV2): {response.json().get('description')}. Retrying plain.")
    except Exception as e_md: # Catch network errors or status errors for Markdown attempt
        logging.warning(f"Basic TG Send Error (MarkdownV2): {e_md}. Retrying plain.")

    # Retry Plain Text
    payload['parse_mode'] = None
    payload['text'] = text # Use original unescaped text for plain
    try:
        response = requests.post(api_url, data=payload, timeout=10)
        response.raise_for_status()
        if response.json().get("ok"):
             logging.info("Basic TG message sent successfully (plain text fallback).")
             return True
        else:
             logging.error(f"Basic TG API Error (plain fallback): {response.json().get('description')}")
             return False
    except Exception as e_plain:
        logging.error(f"Basic TG Send Error (plain fallback): {e_plain}")
        return False

# --- Credential Check ---
# Check credentials after basic functions are defined
missing_secrets = []
if not WEBMAIL_USERNAME: missing_secrets.append("WEBMAIL_USERNAME")
if not WEBMAIL_PASSWORD: missing_secrets.append("WEBMAIL_PASSWORD")
if not TELEGRAM_BOT_TOKEN: missing_secrets.append("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_CHAT_ID: missing_secrets.append("TELEGRAM_CHAT_ID")

if missing_secrets:
     error_message_raw = f"!!! CRITICAL ERROR: Missing required environment variables: {', '.join(missing_secrets)} !!! Ensure they are set in the Render Service Environment."
     logging.critical(error_message_raw)
     # Attempt to send notification if token/chat_id are available
     if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
         send_telegram_message_basic(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_message_raw)
     exit(1) # Exit script if secrets are missing

# --- Full Helper Functions ---

def setup_driver():
    """Sets up the Selenium WebDriver for headless execution."""
    logging.info("Setting up ChromeDriver...")
    options = webdriver.ChromeOptions()
    # Essential Options for Headless/Docker/Render
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    # Other Options
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")

    # Set Page Load Strategy (important for potentially slow pages)
    options.page_load_strategy = PageLoadStrategy.eager # Wait for DOM ready
    logging.info(f"Set page load strategy to: {options.page_load_strategy}")

    driver = None
    try:
        # Attempt using webdriver-manager first (requires Chrome installed in environment)
        try:
            logging.info("Attempting driver setup using webdriver-manager...")
            # Set cache path to a writable directory in Render if needed
            # cache_path = "/path/to/writable/dir/.wdm" # e.g., within /tmp or mounted disk
            # service = Service(ChromeDriverManager(path=cache_path).install())
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            logging.info("ChromeDriver setup using webdriver-manager successful.")
        except Exception as wdm_error:
            logging.warning(f"Webdriver-manager failed ({wdm_error}). Check Chrome installation or network access for driver download.")
            logging.warning("Falling back to assuming chromedriver is in PATH (requires manual setup in environment/Dockerfile)...")
            # Fallback: Assume chromedriver is in PATH
            # Requires chromedriver binary matching the Chrome version to be installed and in PATH
            service = Service() # Uses chromedriver from PATH
            driver = webdriver.Chrome(service=service, options=options)
            logging.info("ChromeDriver setup using system PATH successful.")

        # Set Timeouts (after driver object is created)
        page_load_timeout_sec = 180; implicit_wait_sec = 20; script_timeout_sec = 60
        driver.set_page_load_timeout(page_load_timeout_sec)
        driver.implicitly_wait(implicit_wait_sec)
        driver.set_script_timeout(script_timeout_sec)
        logging.info(f"Set timeouts: Page Load={page_load_timeout_sec}s, Implicit Wait={implicit_wait_sec}s, Script={script_timeout_sec}s")
        logging.info("ChromeDriver setup complete.")
        return driver

    except WebDriverException as e:
        logging.error(f"WebDriver setup error: {e}", exc_info=True)
        if "executable needs to be in PATH" in str(e).lower():
             logging.error("Ensure chromedriver is installed and in the system PATH, or that webdriver-manager can run.")
        elif "net::ERR_CONNECTION_REFUSED" in str(e).lower():
             logging.error("Connection to ChromeDriver refused. Ensure ChromeDriver process started correctly.")
        raise
    except Exception as e:
        logging.error(f"Unexpected ChromeDriver setup error: {e}", exc_info=True)
        raise


@retrying.retry(stop_max_attempt_number=2, wait_fixed=3000, retry_on_exception=lambda e: isinstance(e, (TimeoutException, NoSuchElementException)))
def login_and_get_cookies(driver, url, username_id, password_id, username, password):
    """Logs into the webmail and extracts session cookies."""
    logging.info(f"Attempting login to: {url}")
    try:
        page_load_timeout = driver.timeouts.page_load # Get configured timeout
        logging.info(f"Navigating to login page (timeout={page_load_timeout}ms)...") # Note: Selenium uses ms
        driver.get(url)
        logging.info(f"Navigation command issued. Current URL after get: {driver.current_url}")

        wait = WebDriverWait(driver, 30) # Explicit wait for elements
        logging.info(f"Waiting for username field: ID='{username_id}'")
        user_field = wait.until(EC.visibility_of_element_located((By.ID, username_id)))
        logging.info(f"Waiting for password field: ID='{password_id}'")
        pw_field = wait.until(EC.visibility_of_element_located((By.ID, password_id)))
        logging.info("Login fields located.")

        logging.info("Entering credentials...")
        user_field.clear(); time.sleep(0.2); user_field.send_keys(username); time.sleep(0.5)
        pw_field.clear(); time.sleep(0.2); pw_field.send_keys(password); time.sleep(0.5)

        logging.info(f"Submitting login form via RETURN key...")
        pw_field.send_keys(Keys.RETURN)

        post_login_locator = (By.XPATH, "//a[contains(@href, 'logout')] | //*[contains(text(),'로그아웃')] | //div[@id='main_container'] | //span[@class='username']")
        logging.info(f"Waiting up to 30s for login success indication...")
        wait.until(EC.presence_of_element_located(post_login_locator))
        logging.info("Post-login element found. Login appears successful.")

        time.sleep(2); logging.info("Extracting cookies...")
        cookies = {c['name']: c['value'] for c in driver.get_cookies()}
        if not cookies: raise Exception("쿠키 추출 실패 (로그인 후 쿠키 없음)")
        logging.info(f"Extracted {len(cookies)} cookies.")
        return cookies

    except TimeoutException as e:
        current_url = "N/A"; page_source_snippet = "N/A"; screenshot_path = f"login_timeout_screenshot_{int(time.time())}.png"
        try: current_url = driver.current_url
        except Exception: pass
        try: page_source_snippet = driver.page_source[:1000]
        except Exception: pass
        try: driver.save_screenshot(screenshot_path); logging.info(f"Saved timeout screenshot: {screenshot_path}")
        except Exception as ss_err: logging.warning(f"Failed to save timeout screenshot: {ss_err}")
        logging.error(f"TimeoutException during login: {e}", exc_info=True) # Log full traceback for timeout
        logging.error(f"URL at timeout: {current_url}")
        if "page load" in str(e).lower() or "timed out receiving message from renderer" in str(e).lower():
             raise Exception(f"로그인 페이지 로드 시간 초과 ({url}). 사이트 접속/응답 확인 필요.") from e
        else: raise Exception("로그인 실패: 페이지 요소 대기 시간 초과.") from e

    except Exception as e:
        logging.error(f"Unexpected error during login: {e}", exc_info=True)
        screenshot_path = f"login_error_screenshot_{int(time.time())}.png"
        try: driver.save_screenshot(screenshot_path); logging.info(f"Saved error screenshot: {screenshot_path}")
        except Exception as ss_err: logging.warning(f"Failed to save error screenshot: {ss_err}")
        if isinstance(e, WebDriverException) and "Read timed out" in str(e):
             raise Exception(f"로그인 페이지 접속 실패 (Read Timeout).") from e
        raise


@retrying.retry(stop_max_attempt_number=3, wait_fixed=10000, retry_on_exception=lambda e: isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)))
def download_excel_report(report_url, cookies):
    """Downloads the Excel report using session cookies."""
    logging.info(f"Downloading report from: {report_url}")
    session = requests.Session(); session.cookies.update(cookies)
    headers = { 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36', 'Referer': WEBMAIL_LOGIN_URL.split('/mail2')[0] + '/', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Encoding': 'gzip, deflate', 'Accept-Language': 'ko-KR,ko;q=0.9', 'Connection': 'keep-alive', 'Upgrade-Insecure-Requests': '1' }
    try:
        response = session.get(report_url, headers=headers, stream=True, timeout=120)
        logging.info(f"Download response status: {response.status_code}")
        response.raise_for_status()
        content_type = response.headers.get('Content-Type', '').lower()
        content_disposition = response.headers.get('Content-Disposition', '')
        logging.info(f"Response Headers - CT: '{content_type}', CD: '{content_disposition}'")
        is_excel = any(mime in content_type for mime in ['excel', 'spreadsheetml', 'vnd.ms-excel', 'octet-stream']) or any(ext in content_disposition for ext in ['.xlsx', '.xls'])
        if is_excel:
            excel_data = io.BytesIO(response.content); file_size = excel_data.getbuffer().nbytes
            logging.info(f"Excel download OK ({file_size} bytes).")
            if file_size < 2048:
                logging.warning(f"Small file ({file_size} bytes). Checking content...")
                try:
                    preview = excel_data.getvalue()[:500].decode('utf-8', errors='ignore')
                    if any(tag in preview.lower() for tag in ['<html', 'login', '로그인', 'error', 'session']):
                        logging.error(f"Small file looks like error page: {preview}")
                        raise Exception("다운로드된 파일이 작고 오류 페이지로 보입니다.")
                finally: excel_data.seek(0)
            return excel_data
        else:
            logging.error(f"Downloaded content not identified as Excel.")
            try: preview = response.content[:500].decode('utf-8', errors='ignore'); logging.error(f"Content preview:\n{preview}")
            except Exception: logging.error("Could not decode preview.")
            raise Exception(f"다운로드 파일 형식이 엑셀이 아닙니다 (CT: {content_type}).")
    except requests.exceptions.Timeout as e: raise Exception(f"보고서 다운로드 시간 초과: {report_url}") from e
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error downloading: {e.response.status_code}", exc_info=True)
        try: logging.error(f"HTTP Error Response:\n{e.response.content[:500].decode('utf-8', errors='ignore')}")
        except Exception: pass
        if e.response.status_code in [401, 403]: raise Exception(f"보고서 다운로드 권한 오류 ({e.response.status_code}).") from e
        raise Exception(f"보고서 다운로드 HTTP 오류: {e.response.status_code}") from e
    except requests.exceptions.RequestException as e: raise Exception(f"보고서 다운로드 네트워크 오류: {e}") from e
    except Exception as e: logging.error(f"Unexpected download error: {e}", exc_info=True); raise


def parse_time_robust(time_str):
    """Robustly parses time strings from various formats."""
    if pd.isna(time_str) or time_str == '': return None
    if isinstance(time_str, datetime.time): return time_str
    if isinstance(time_str, datetime.datetime): return time_str.time()
    time_str_orig = time_str; time_str = str(time_str).strip()
    if not time_str: return None
    if isinstance(time_str_orig, (float, int)) and 0 <= time_str_orig < 1:
        try:
            total_seconds = int(round(time_str_orig * 86400)); total_seconds = min(total_seconds, 86399)
            h, rem = divmod(total_seconds, 3600); m, s = divmod(rem, 60); return datetime.time(h, m, s)
        except (ValueError, TypeError): pass
    if ' ' in time_str and ':' in time_str:
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S'):
            try: return datetime.datetime.strptime(time_str, fmt).time()
            except ValueError: pass
    for fmt in ('%H:%M:%S', '%H:%M', '%I:%M:%S %p', '%I:%M %p'):
        try: return datetime.datetime.strptime(time_str, fmt).time()
        except ValueError: continue
    time_str_numeric = ''.join(filter(str.isdigit, time_str))
    if len(time_str_numeric) == 6:
        try: return datetime.datetime.strptime(time_str_numeric, '%H%M%S').time()
        except ValueError: pass
    elif len(time_str_numeric) == 4:
        try: return datetime.datetime.strptime(time_str_numeric, '%H%M').time()
        except ValueError: pass
    logging.warning(f"Could not parse time string: '{time_str_orig}'")
    return None


def parse_date_robust(date_str):
    """Robustly parses date strings from various formats."""
    if pd.isna(date_str): return None
    if isinstance(date_str, datetime.date): return date_str
    if isinstance(date_str, datetime.datetime): return date_str.date()
    date_str_orig = date_str; date_str = str(date_str).strip();
    if not date_str: return None
    date_part = date_str.split(' ')[0]
    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%y-%m-%d', '%y/%m/%d', '%Y%m%d', '%m/%d/%Y', '%m/%d/%y'):
        try: return datetime.datetime.strptime(date_part, fmt).date()
        except ValueError: continue
    logging.warning(f"Could not parse date string: '{date_str_orig}'")
    return None


def combine_date_time(date_val, time_val):
    """Combines date and time into datetime."""
    if isinstance(date_val, datetime.date) and isinstance(time_val, datetime.time):
        return datetime.datetime.combine(date_val, time_val)
    return None


def find_korean_font():
    """Finds a usable Korean font (Nanum preferred). Requires fonts installed in environment."""
    common_font_files = ["NanumGothic.ttf", "NanumBarunGothic.ttf", "malgun.ttf"]
    # Check standard Linux font paths (adjust if using different base image)
    linux_font_paths = ["/usr/share/fonts/truetype/nanum/", "/usr/share/fonts/opentype/nanum/"]
    for path in linux_font_paths:
        try:
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    if filename in common_font_files:
                        found = os.path.join(path, filename)
                        try: fm.FontProperties(fname=found); logging.info(f"Found verified font: {found}"); return found
                        except Exception: logging.warning(f"Found {found} but failed verification."); continue
        except OSError: continue
    # Fallback to matplotlib font list
    logging.info("Korean font not in common paths, searching matplotlib font list...")
    try:
        for f_path in fm.findSystemFonts(fontpaths=None, fontext='ttf'):
             if any(name in Path(f_path).name for name in common_font_files):
                 try: fm.FontProperties(fname=f_path); logging.info(f"Found verified font via system search: {f_path}"); return f_path
                 except Exception: logging.warning(f"Found potential {f_path} but failed verification."); continue
    except Exception as e: logging.warning(f"Error searching system fonts: {e}")
    logging.error("CRITICAL: Korean font not found. Ensure 'fonts-nanum*' is installed via Dockerfile or buildpack.")
    return None


def create_table_image(df, title, output_path="table_image.png"):
    """Creates a PNG image from a Pandas DataFrame."""
    logging.info("Creating table image...")
    if df.empty: logging.warning("DataFrame empty, skipping image."); return None

    font_path = find_korean_font() # Attempt to find font
    if font_path:
        try: prop = fm.FontProperties(fname=font_path, size=10); plt.rcParams['font.family'] = prop.get_name(); plt.rcParams['axes.unicode_minus'] = False; logging.info(f"Using font: {font_path}")
        except Exception as font_err: logging.error(f"Failed set font {font_path}: {font_err}. Falling back.", exc_info=True); plt.rcParams['font.family'] = 'sans-serif'
    else: plt.rcParams['font.family'] = 'sans-serif' # Fallback

    nr, nc = df.shape; base_w, incr_w = 6, 0.9; base_h, incr_h = 2, 0.35; max_w, max_h = 30, 45
    fw = min(max(base_w, base_w + nc * incr_w), max_w); fh = min(max(base_h, base_h + nr * incr_h), max_h)
    logging.info(f"Table: {nr}r, {nc}c. Figure:({fw:.1f}, {fh:.1f})")
    fig, ax = plt.subplots(figsize=(fw, fh)); ax.axis('off')
    try:
        tab = Table(ax, bbox=[0, 0, 1, 1])
        for j, col in enumerate(df.columns): # Header
            cell = tab.add_cell(0, j, 1, 1, text=str(col), loc='center', facecolor='#E0E0E0', width=1.0/nc if nc > 0 else 1); cell.set_text_props(weight='bold')
        for i in range(nr): # Rows
            for j in range(nc):
                txt = str(df.iloc[i, j]); max_len = 45; txt = txt[:max_len-3]+'...' if len(txt)>max_len else txt
                cell_color = 'white' if i % 2 == 0 else '#F5F5F5'
                tab.add_cell(i + 1, j, 1, 1, text=txt, loc='center', facecolor=cell_color, width=1.0/nc if nc > 0 else 1)
        tab.auto_set_font_size(False); tab.set_fontsize(9); ax.add_table(tab)
        plt.title(title, fontsize=13, weight='bold', pad=20); plt.tight_layout(pad=1.5)
        plt.savefig(output_path, bbox_inches='tight', dpi=110); plt.close(fig)
        logging.info(f"Table image saved: {output_path}")
        size_bytes = Path(output_path).stat().st_size; size_mb = size_bytes / (1024*1024); logging.info(f"Image size: {size_mb:.2f} MB")
        if size_mb > 9.5: logging.warning(f"Image size may exceed Telegram limit.")
        if size_bytes < 100: logging.error("Generated image size < 100 bytes."); return None
        return output_path
    except Exception as e: logging.error(f"Failed create/save table image: {e}", exc_info=True); plt.close(fig); return None


@retrying.retry(stop_max_attempt_number=3, wait_fixed=5000, retry_on_exception=lambda e: isinstance(e, requests.exceptions.RequestException))
def send_telegram_photo(bot_token, chat_id, photo_path, caption):
    """Sends photo to Telegram with caption."""
    api_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"; path_obj = Path(photo_path)
    if not path_obj.exists(): logging.error(f"Photo not found: {photo_path}"); return False
    file_size = path_obj.stat().st_size
    if file_size == 0: logging.error(f"Photo size is 0: {photo_path}"); return False
    size_mb = file_size / (1024*1024); logging.info(f"Sending photo {photo_path} ({size_mb:.2f} MB)...")
    if size_mb > 10: logging.warning(f"Photo size {size_mb:.2f}MB > 10MB limit.")
    try:
        with open(photo_path, 'rb') as photo_file:
            max_caption_len = 1024; caption = caption[:max_caption_len-4]+"..." if len(caption)>max_caption_len else caption
            files = {'photo': (path_obj.name, photo_file)}; payload = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'MarkdownV2'}
            response = requests.post(api_url, data=payload, files=files, timeout=90)
            try: rd = response.json()
            except json.JSONDecodeError: logging.error(f"TG JSON decode fail (sendPhoto). Status:{response.status_code}"); response.raise_for_status()
            if response.status_code == 200 and rd.get("ok"): logging.info("Telegram photo sent."); return True
            else:
                err_desc = rd.get('description', 'N/A'); err_code = rd.get('error_code', 'N/A'); logging.error(f"TG API Error (sendPhoto): {err_desc} (Code: {err_code})")
                if 400 <= response.status_code < 500: raise requests.exceptions.HTTPError(f"TG Client Error {response.status_code}: {err_desc}", response=response)
                else: response.raise_for_status() # Allow retry for 5xx
                return False # Should not be reached if exception raised
    except requests.exceptions.HTTPError as e:
         if 400 <= e.response.status_code < 500: # Don't retry client errors
              logging.error(f"HTTP Client Error sending photo (no retry): {e}", exc_info=True)
              error_text = f"*{escape_markdown(TARGET_DATE_STR)} 이미지 전송 실패* \\(HTTP {e.response.status_code}\\): {escape_markdown(e.response.json().get('description','N/A'))}"
              send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_text) # Notify
         else: logging.error(f"HTTP Error sending photo: {e}"); raise # Allow retry for other HTTP errors
         return False
    except requests.exceptions.RequestException as e: logging.error(f"Network error sending photo: {e}. Retrying."); raise
    except FileNotFoundError: logging.error(f"File not found during photo send: {photo_path}"); return False
    except Exception as e: logging.error(f"Unexpected error sending photo: {e}", exc_info=True); return False


# Corrected send_telegram_message function
def send_telegram_message(bot_token, chat_id, text):
    """Sends a text message to Telegram, splitting if too long, with fallback."""
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"; max_len = 4096; messages_to_send = []
    if not text: logging.warning("Attempted send empty message."); return True
    if len(text) > max_len: # Split logic
        logging.info(f"Message > {max_len} chars, splitting."); start = 0
        while start < len(text):
            end = text.rfind('\n', start, start + max_len); end = start + max_len if end == -1 or end <= start else end
            chunk = text[start:end].strip();
            if chunk: messages_to_send.append(chunk)
            start = end
    else: messages_to_send.append(text)
    logging.info(f"Sending {len(messages_to_send)} message part(s)."); all_parts_sent = True
    for i, part in enumerate(messages_to_send):
        if not part: logging.warning(f"Skipping empty part {i+1}."); continue
        payload = {'chat_id': chat_id, 'text': escape_markdown(part), 'parse_mode': 'MarkdownV2'} # Escape for MDv2
        part_sent = False; attempt = 0; max_attempts = 2
        while not part_sent and attempt < max_attempts:
            attempt += 1; mode = payload.get('parse_mode', 'Plain Text') if payload.get('parse_mode') else 'Plain Text'
            logging.info(f"Sending part {i+1}/{len(messages_to_send)} (Mode: {mode}, Attempt {attempt})")
            try:
                response = requests.post(api_url, data=payload, timeout=30); rd = response.json()
                if response.status_code == 200 and rd.get("ok"): logging.info(f"TG Part {i+1} sent OK (Mode: {mode})."); part_sent = True
                else:
                    err_desc = rd.get('description', 'N/A'); err_code = rd.get('error_code', 'N/A'); logging.error(f"TG API Error (Part {i+1}, Mode: {mode}): {err_desc} ({err_code})"); logging.error(f"Failed content sample: {part[:200]}")
                    if payload.get('parse_mode') == 'MarkdownV2' and attempt < max_attempts: logging.warning("MDv2 failed, retrying plain."); payload['parse_mode'] = None; payload['text'] = part # Use original for plain
                    else: all_parts_sent = False; break # Stop trying this part
            except requests.exceptions.Timeout:
                logging.error(f"Timeout sending TG Part {i+1} (Mode: {mode})."); time.sleep(5)
                if attempt == max_attempts: all_parts_sent = False # Mark failure if last attempt times out
            except requests.exceptions.RequestException as e:
                logging.error(f"Network error sending TG Part {i+1} (Mode: {mode}): {e}"); time.sleep(5)
                if attempt == max_attempts: all_parts_sent = False # Mark failure if last attempt fails
            except json.JSONDecodeError:
                 logging.error(f"JSON decode fail (TG Send Part {i+1}). Status: {response.status_code}"); time.sleep(5)
                 if attempt == max_attempts: all_parts_sent = False # Mark failure
            except Exception as e:
                logging.error(f"Unexpected error sending TG Part {i+1} (Mode: {mode}): {e}", exc_info=True)
                if attempt == max_attempts: all_parts_sent = False # Mark failure
                break # Don't retry unexpected
        if not part_sent: all_parts_sent = False # Mark overall as failed if any part fails
    return all_parts_sent


def analyze_attendance(excel_data, sheet_name):
    """Analyzes the downloaded Excel data for attendance status."""
    logging.info(f"Analyzing attendance from sheet: '{sheet_name}'.")
    analysis_result = { "notifications": [], "detailed_status": [], "summary": { "total_employees": 0, "target": 0, "excluded": 0, "clocked_in": 0, "missing_in": 0, "clocked_out": 0, "missing_out": 0 }, "excluded_employees": [], "df_processed": None }
    processed_data_for_image = []
    try:
        try: df = pd.read_excel(excel_data, sheet_name=sheet_name, skiprows=2, dtype=str, keep_default_na=False)
        except ValueError as sheet_error:
             if "Worksheet named" in str(sheet_error): raise ValueError(f"'{sheet_name}' 시트 없음.") from sheet_error
             else: raise
        logging.info(f"Loaded {len(df)} rows from '{sheet_name}'.");
        if df.empty: logging.warning("Excel sheet empty."); return analysis_result
        df.columns = [str(col).strip() for col in df.columns]; logging.debug(f"Columns: {df.columns.tolist()}")
        col_map = { '서무원': '이름', '출퇴근': '유형', '정상': '구분', 'Unnamed: 11': '출근시간_raw', 'Unnamed: 13': '퇴근시간_raw', 'Unnamed: 16': '휴가시작시간_raw', 'Unnamed: 18': '휴가종료시간_raw' }
        date_col = next((c for c in df.columns if re.match(r'^\d{4}-\d{2}-\d{2}$', str(c).strip())), None)
        if not date_col: date_col = TARGET_DATE_STR if TARGET_DATE_STR in df.columns else (df.columns[5] if len(df.columns)>5 and parse_date_robust(df.columns[5]) else None)
        if not date_col: raise KeyError("날짜 컬럼 찾기 실패.")
        col_map[date_col] = '일자'; logging.info(f"Date column: '{date_col}'")
        missing = [c for c in col_map if c not in df.columns]
        if missing: raise KeyError(f"필수 컬럼 누락: {', '.join(missing)}")
        df_proc = df[list(col_map.keys())].copy(); df_proc.rename(columns=col_map, inplace=True)
        df_proc['일자_dt'] = df_proc['일자'].apply(parse_date_robust)
        df_proc['출근시간_dt'] = df_proc['출근시간_raw'].apply(parse_time_robust)
        df_proc['퇴근시간_dt'] = df_proc['퇴근시간_raw'].apply(parse_time_robust)
        df_proc['휴가시작시간_dt'] = df_proc['휴가시작시간_raw'].apply(parse_time_robust)
        df_proc['휴가종료시간_dt'] = df_proc['휴가종료시간_raw'].apply(parse_time_robust)
        df_filt = df_proc[df_proc['일자_dt'] == TARGET_DATE].copy()
        if df_filt.empty: logging.warning(f"No data for target date {TARGET_DATE_STR}."); return analysis_result
        logging.info(f"Processing {len(df_filt)} rows for {TARGET_DATE_STR}.")
        std_start_t = datetime.datetime.strptime(STANDARD_START_TIME_STR,'%H:%M:%S').time(); std_end_t = datetime.datetime.strptime(STANDARD_END_TIME_STR,'%H:%M:%S').time()
        std_start_dt = datetime.datetime.combine(TARGET_DATE, std_start_t); std_end_dt = datetime.datetime.combine(TARGET_DATE, std_end_t)
        lunch_start_t = datetime.time(12,0,0); lunch_end_t = datetime.time(13,0,0); noon_start_t = lunch_end_t
        grouped = df_filt.groupby('이름'); analysis_result["summary"]["total_employees"] = len(grouped)
        for name, group in grouped:
            name = str(name).strip();
            if not name: continue
            esc_name = escape_markdown(name); logging.debug(f"--- Processing: {name} ---")
            excluded = False; excl_reason = ""; leaves = []; attd = None
            for _, r in group.iterrows():
                r_type=str(r.get('유형','')).strip(); r_cat=str(r.get('구분','')).strip(); ls=r['휴가시작시간_dt']; le=r['휴가종료시간_dt']
                is_leave = r_type in FULL_DAY_LEAVE_TYPES or r_cat in FULL_DAY_LEAVE_REASONS or r_cat in [MORNING_HALF_LEAVE, AFTERNOON_HALF_LEAVE]
                if is_leave:
                    lname=r_cat if r_cat else r_type;
                    if lname: leaves.append({'type':lname,'start':ls,'end':le}); logging.debug(f"{name}: Leave={lname}({ls}-{le})")
                    if ls and le and ls<=std_start_t and le>=std_end_t: excluded=True; excl_reason=f"{lname} ({ls.strftime('%H:%M')}-{le.strftime('%H:%M')})"; break
                elif r_type==ATTENDANCE_TYPE: attd={'in':r['출근시간_dt'],'out':r['퇴근시간_dt'],'in_raw':str(r['출근시간_raw']).strip(),'out_raw':str(r['퇴근시간_raw']).strip()}; logging.debug(f"{name}: Attd={attd}")
            if excluded: logging.info(f"{name}: Excluded (Single: {excl_reason})"); analysis_result["excluded_employees"].append(f"{name}: {excl_reason}"); analysis_result["summary"]["excluded"]+=1; processed_data_for_image.append({'이름':name,'일자':TARGET_DATE_STR,'유형':'휴가/제외','구분':excl_reason,'출근시간':'-','퇴근시간':'-'}); continue
            if not excluded and leaves:
                m_cov=False; a_cov=False; min_s=None; max_e=None
                for l in leaves: ls,le,lt = l['start'],l['end'],l['type']
                if ls and (min_s is None or ls<min_s): min_s=ls
                if le and (max_e is None or le>max_e): max_e=le
                if lt==MORNING_HALF_LEAVE or (ls and le and ls<=std_start_t and le>=lunch_start_t): m_cov=True
                if lt==AFTERNOON_HALF_LEAVE or (ls and le and ls<=lunch_end_t and le>=std_end_t): a_cov=True
                if m_cov and a_cov:
                    excluded=True; c_types=" + ".join(sorted(list(set(l['type'] for l in leaves if l['type'])))); t_range=f"{min_s.strftime('%H:%M') if min_s else '?'} - {max_e.strftime('%H:%M') if max_e else '?'}"; excl_reason=f"{c_types} ({t_range})"
                    logging.info(f"{name}: Excluded (Combined: {excl_reason})"); analysis_result["excluded_employees"].append(f"{name}: {excl_reason}"); analysis_result["summary"]["excluded"]+=1; processed_data_for_image.append({'이름':name,'일자':TARGET_DATE_STR,'유형':'휴가/제외','구분':excl_reason,'출근시간':'-','퇴근시간':'-'}); continue
            analysis_result["summary"]["target"]+=1; logging.debug(f"{name}: Analyzing attendance.")
            in_dt,out_dt,in_raw,out_raw = (attd['in'],attd['out'],attd['in_raw'],attd['out_raw']) if attd else (None,None,'','')
            act_start=combine_date_time(TARGET_DATE,in_dt); act_end=combine_date_time(TARGET_DATE,out_dt); has_in=(act_start is not None); has_out=(act_end is not None)
            has_m_leave=False; has_a_leave=False; disp_type='출퇴근'; disp_cat='정상'; m_types=[]; a_types=[]
            if leaves:
                for l in leaves: ls,le,lt=l['start'],l['end'],l['type']
                if lt==MORNING_HALF_LEAVE or (ls and le and ls<=std_start_t and le>=lunch_start_t): has_m_leave=True; m_types.append(lt)
                if lt==AFTERNOON_HALF_LEAVE or (ls and le and ls<=lunch_end_t and le>=std_end_t): has_a_leave=True; a_types.append(lt)
                if has_m_leave: disp_type=MORNING_HALF_LEAVE; disp_cat=" + ".join(sorted(list(set(m_types)))) or '오전반차'
                if has_a_leave: disp_type=AFTERNOON_HALF_LEAVE if disp_type=='출퇴근' else disp_type+"+"+AFTERNOON_HALF_LEAVE; disp_cat=(" + ".join(sorted(list(set(a_types)))) or '오후반차') if not has_m_leave else "오전+오후반차"
            exp_start=datetime.datetime.combine(TARGET_DATE,noon_start_t) if has_m_leave else std_start_dt; exp_end=datetime.datetime.combine(TARGET_DATE,lunch_start_t) if has_a_leave else std_end_dt
            issues=[]
            if has_in: analysis_result["summary"]["clocked_in"]+=1;
            if not has_m_leave and act_start>std_start_dt: issues.append(f"지각: {escape_markdown(in_dt.strftime('%H:%M:%S'))}")
            elif has_m_leave and act_start>exp_start: issues.append(f"오전반차 후 지각: {escape_markdown(in_dt.strftime('%H:%M:%S'))}")
            else: analysis_result["summary"]["missing_in"]+=1;
            if not has_m_leave: issues.append("출근 기록 없음")
            if has_out: analysis_result["summary"]["clocked_out"]+=1;
            if not has_a_leave and act_end<std_end_dt: issues.append(f"조퇴: {escape_markdown(out_dt.strftime('%H:%M:%S'))}")
            elif has_a_leave and act_end<exp_end: issues.append(f"오후반차 전 조퇴: {escape_markdown(out_dt.strftime('%H:%M:%S'))}")
            elif has_in: analysis_result["summary"]["missing_out"]+=1;
            if not has_a_leave: issues.append("퇴근 기록 없음")
            if issues: issue_str=", ".join(issues); analysis_result["notifications"].append(f"*{esc_name}*: {issue_str}"); logging.info(f"{name}: Issues - {issue_str}")
            in_stat=in_dt.strftime('%H:%M:%S') if has_in else "기록없음"; out_stat=out_dt.strftime('%H:%M:%S') if has_out else ("기록없음" if has_in else "출근없음")
            analysis_result["detailed_status"].append({'name':name,'in_status':in_stat,'out_status':out_stat})
            processed_data_for_image.append({'이름':name,'일자':TARGET_DATE_STR,'유형':disp_type,'구분':disp_cat,'출근시간':in_raw if in_raw else ('-' if has_in else '기록없음'),'퇴근시간':out_raw if out_raw else ('-' if has_out else '기록없음')})
        # Finalize
        if processed_data_for_image: analysis_result["df_processed"] = pd.DataFrame(processed_data_for_image, columns=['이름','일자','유형','구분','출근시간','퇴근시간']); logging.info(f"Created image DF ({len(analysis_result['df_processed'])} rows).")
        else: logging.warning("No data for image DF."); analysis_result["df_processed"] = pd.DataFrame(columns=['이름','일자','유형','구분','출근시간','퇴근시간'])
        # Sanity checks
        s=analysis_result["summary"]; ct=s["target"]+s["excluded"]; tc=s["clocked_in"]+s["missing_in"]; oc=s["clocked_out"]+s["missing_out"]
        if ct!=s["total_employees"]: logging.warning(f"Summ Count Err: Tot={s['total_employees']}, Calc={ct}")
        if s["target"]!=tc: logging.warning(f"Target Count Err: Targ={s['target']}, In/Miss={tc}")
        if s["clocked_in"]!=oc: logging.warning(f"ClockIn Count Err: In={s['clocked_in']}, Out/Miss={oc}")
        logging.info(f"Analysis complete. Summary: {s}"); return analysis_result
    except KeyError as e: logging.error(f"Analysis KeyError: {e}.", exc_info=True); err=f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류* 컬럼({escape_markdown(str(e))}) 없음."; send_telegram_message(TELEGRAM_BOT_TOKEN,TELEGRAM_CHAT_ID,err); analysis_result["summary"]["total_employees"]=-1; return analysis_result
    except ValueError as e: logging.error(f"Analysis ValueError: {e}.", exc_info=True); err=f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류* 값 오류: {escape_markdown(str(e))}."; send_telegram_message(TELEGRAM_BOT_TOKEN,TELEGRAM_CHAT_ID,err); analysis_result["summary"]["total_employees"]=-1; return analysis_result
    except Exception as e: logging.error(f"Unexpected analysis error: {e}", exc_info=True); err=f"*{escape_markdown(TARGET_DATE_STR)} 분석 예외* 오류: {escape_markdown(str(e))}"; send_telegram_message(TELEGRAM_BOT_TOKEN,TELEGRAM_CHAT_ID,err); analysis_result["summary"]["total_employees"]=-1; return analysis_result


# --- Main Execution Logic ---
if __name__ == "__main__":
    script_start_time = time.time()
    logging.info(f"--- Attendance Bot Script started for date: {TARGET_DATE_STR} (Using KST: {KST is not None}) ---")

    driver = None; excel_data = None; error_occurred = False; analysis_result = {}
    image_path = None # Ensure image_path is defined

    # Phase 1: Setup, Login, Download
    try:
        driver = setup_driver()
        cookies = login_and_get_cookies(driver, WEBMAIL_LOGIN_URL, WEBMAIL_ID_FIELD_ID, WEBMAIL_PW_FIELD_ID, WEBMAIL_USERNAME, WEBMAIL_PASSWORD)
        excel_data = download_excel_report(REPORT_URL, cookies)
        logging.info("Setup, login, download successful.")
    except Exception as setup_err:
        logging.error(f"Critical error during setup/login/download: {setup_err}", exc_info=True)
        error_occurred = True
        err_msg = f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 초기화 오류*\n단계: 설정/로그인/다운로드\n오류: {escape_markdown(str(setup_err))}"
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, err_msg) # Use full sender now
    finally:
        if driver:
            try: driver.quit(); logging.info("WebDriver closed.")
            except Exception as e: logging.error(f"Error closing WebDriver: {e}", exc_info=True)

    # Phase 2: Analysis and Reporting
    if excel_data and not error_occurred:
        logging.info("Proceeding with analysis and reporting...")
        try:
            analysis_result = analyze_attendance(excel_data, EXCEL_SHEET_NAME)
            if not isinstance(analysis_result, dict) or analysis_result.get("summary", {}).get("total_employees", -1) == -1:
                logging.error("Analysis function indicated failure.")
                error_occurred = True # Analysis itself failed
            else:
                # --- Reporting Logic ---
                logging.info("Analysis successful. Preparing reports...")
                is_evening = CURRENT_HOUR_KST >= EVENING_RUN_THRESHOLD_HOUR
                logging.info(f"Current KST hour {CURRENT_HOUR_KST}. Is evening run? {is_evening}")

                summary = analysis_result.get("summary", {})
                df_image = analysis_result.get("df_processed")

                # 1. Create & Send Image
                if df_image is not None and not df_image.empty:
                    img_title = f"{TARGET_DATE_STR} 근태 ({summary.get('target',0)}명 확인, {summary.get('excluded',0)}명 제외)"
                    img_fname = f"attendance_{TARGET_DATE_STR}.png"
                    image_path = create_table_image(df_image, img_title, img_fname)
                    if image_path:
                        caption = f"*{escape_markdown(TARGET_DATE_STR)} 근태 상세 현황*"
                        if not send_telegram_photo(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, image_path, caption):
                            logging.error("Failed to send Telegram photo."); error_occurred = True; send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 이미지 전송 실패*")
                        else: logging.info("Telegram photo sent.")
                    else: logging.error("Failed create table image."); error_occurred=True; send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 이미지 생성 실패*")
                elif df_image is None: logging.warning("DF for image is None (analysis error?).")
                else: logging.info("DF for image is empty. Skipping image.")

                # Cleanup image file
                if image_path and Path(image_path).exists():
                    try: Path(image_path).unlink(); logging.info(f"Deleted image: {image_path}")
                    except Exception as del_e: logging.warning(f"Failed delete image {image_path}: {del_e}")

                # 2. Detailed Report (Issues or Status)
                report_lines = []; title = ""
                date_esc = escape_markdown(TARGET_DATE_STR)
                if is_evening:
                    title = f"🌙 *{date_esc} 퇴근 근태 현황*"
                    statuses = analysis_result.get("detailed_status", [])
                    if statuses: report_lines = [f"{i+1}\\. *{escape_markdown(s['name'])}*: {escape_markdown(s['in_status'])} \\| {escape_markdown(s['out_status'])}" for i, s in enumerate(statuses)]
                    else: report_lines = ["_확인 대상 없음_" if summary.get('target')==0 else "_데이터 없음_"]
                else: # Morning
                    title = f"☀️ *{date_esc} 출근 근태 확인 필요*"
                    issues = analysis_result.get("notifications", [])
                    if issues: report_lines = [f"{i+1}\\. {issue}" for i, issue in enumerate(issues)] # Already escaped
                    else: report_lines = ["_특이사항 없음_"]
                if report_lines:
                    msg = f"{title}\n{escape_markdown('-'*20)}\n" + "\n".join(report_lines)
                    logging.info(f"Sending detailed report ('{title}')...")
                    if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, msg): logging.error("Failed send detailed report."); error_occurred=True
                else: logging.warning("No content for detailed report.")

                # 3. Summary Report
                logging.info("Generating summary report...")
                excluded = analysis_result.get("excluded_employees", [])
                prefix = "🌙" if is_evening else "☀️"
                sum_title = f"{prefix} *{date_esc} {'퇴근' if is_evening else '출근'} 현황 요약*"
                sum_lines = [f"\\- 전체: {summary.get('total_employees',0)}명", f"\\- 대상: {summary.get('target',0)}명 \\(제외: {summary.get('excluded',0)}명\\)"]
                if is_evening: sum_lines.extend([f"\\- 출근: {summary.get('clocked_in',0)}명 \\(미기록: {summary.get('missing_in',0)}명\\)", f"\\- 퇴근: {summary.get('clocked_out',0)}명 \\(미기록: {summary.get('missing_out',0)}명\\)"])
                else: sum_lines.extend([f"\\- 출근: {summary.get('clocked_in',0)}명", f"\\- 출근 미기록: {summary.get('missing_in',0)}명"])
                sum_details = "\n".join(sum_lines)
                if excluded: sum_details += f"\n\n*제외 인원 ({summary.get('excluded',0)}명)*:\n  " + "\n  ".join([f"\\- {item}" for item in excluded]) # Items already escaped
                sum_msg = f"{sum_title}\n{escape_markdown('-'*20)}\n{sum_details}"
                logging.info("Sending summary report...")
                if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, sum_msg): logging.error("Failed send summary report."); error_occurred=True

        except Exception as report_err:
            logging.error(f"Error during analysis/reporting phase: {report_err}", exc_info=True)
            error_occurred = True
            err_msg = f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(분석/보고 단계\\)\n오류: {escape_markdown(str(report_err))}"
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, err_msg)

    elif not excel_data and not error_occurred: # Handle case where download failed silently
        logging.error("Excel data missing, but no initial error flagged.")
        error_occurred = True; send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류*\n엑셀 데이터 누락됨\\.")

    # Phase 3: Final Completion Message
    script_end_time = time.time(); time_taken = script_end_time - script_start_time
    logging.info(f"--- Script finished in {time_taken:.2f} seconds ---")
    status = "오류 발생" if error_occurred else "정상 완료"; emoji = "❌" if error_occurred else "✅"
    final_msg = f"{emoji} *{escape_markdown(TARGET_DATE_STR)} 근태 확인*: {escape_markdown(status)} \\({escape_markdown(f'{time_taken:.1f}')}초\\)"
    if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, final_msg): logging.error("Failed send final status message.")
    else: logging.info("Final status message sent.")
    exit_code = 1 if error_occurred else 0; logging.info(f"Exiting with code: {exit_code}"); exit(exit_code)
