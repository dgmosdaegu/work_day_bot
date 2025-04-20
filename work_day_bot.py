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
# Import PageLoadStrategy
from selenium.webdriver.common.page_load_strategy import PageLoadStrategy
from webdriver_manager.chrome import ChromeDriverManager # Use webdriver-manager for Render too
import logging
import io
import traceback
from pathlib import Path
import matplotlib
# Set backend *before* importing pyplot when running headlessly
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.table import Table
import matplotlib.font_manager as fm
import retrying
import json
import re
import os

# --- Configuration ---
# Read credentials from environment variables (Ensure these are set in Render Service Env Vars)
WEBMAIL_USERNAME = os.environ.get("WEBMAIL_USERNAME")
WEBMAIL_PASSWORD = os.environ.get("WEBMAIL_PASSWORD")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# --- Other Settings ---
logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO').upper(), format='%(asctime)s - %(levelname)s - %(message)s')

WEBMAIL_LOGIN_URL = "http://gw.ktmos.co.kr/mail2/loginPage.do"
WEBMAIL_ID_FIELD_ID = "userEmail"
WEBMAIL_PW_FIELD_ID = "userPw"

# Use UTC time within the script and adjust logic if necessary, or set TZ env var in Render
# TARGET_DATE = datetime.date.today() # This will be UTC date on Render unless TZ is set
# It's often better to get KST explicitly if the report *must* be for KST today
try:
    from zoneinfo import ZoneInfo # Python 3.9+
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    # Fallback for older Python if needed, though Render likely has 3.9+
    import pytz
    KST = pytz.timezone("Asia/Seoul")

TARGET_DATETIME_KST = datetime.datetime.now(KST)
TARGET_DATE = TARGET_DATETIME_KST.date()
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")
CURRENT_HOUR_KST = TARGET_DATETIME_KST.hour


REPORT_DOWNLOAD_URL_TEMPLATE = "http://gw.ktmos.co.kr/owattend/rest/dclz/report/CompositeStatus/sumr/user/days/excel?startDate={date}&endDate={date}&deptSeq=1231&erpNumDisplayYn=Y"
REPORT_URL = REPORT_DOWNLOAD_URL_TEMPLATE.format(date=TARGET_DATE_STR)

EXCEL_SHEET_NAME = "세부현황_B"
STANDARD_START_TIME_STR = "09:00:00"
STANDARD_END_TIME_STR = "18:00:00"
EVENING_RUN_THRESHOLD_HOUR = 18 # KST hour (e.g., 18 for 6 PM)

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
    text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def send_telegram_message_basic(bot_token, chat_id, text):
    """Basic message sending, used for early critical errors."""
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'MarkdownV2'}
    try:
        # Escape text just before sending in this basic function
        escaped_text = escape_markdown(text)
        payload['text'] = escaped_text
        response = requests.post(api_url, data=payload, timeout=10)
        response.raise_for_status()
        if response.json().get("ok"):
             logging.info(f"Basic TG message sent successfully.")
             return True
        else:
             logging.error(f"Basic TG API Error: {response.json().get('description')}")
             # Try sending plain if Markdown failed
             payload['parse_mode'] = None
             payload['text'] = text # Use original text for plain mode
             response = requests.post(api_url, data=payload, timeout=10)
             if response.json().get("ok"):
                  logging.info("Basic TG message sent successfully (plain text fallback).")
                  return True
             else:
                  logging.error(f"Basic TG API Error (plain fallback): {response.json().get('description')}")
                  return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error sending basic TG message: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error sending basic TG message: {e}", exc_info=True)
        return False

# --- Credential Check ---
missing_secrets = []
if not WEBMAIL_USERNAME: missing_secrets.append("WEBMAIL_USERNAME")
if not WEBMAIL_PASSWORD: missing_secrets.append("WEBMAIL_PASSWORD")
if not TELEGRAM_BOT_TOKEN: missing_secrets.append("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_CHAT_ID: missing_secrets.append("TELEGRAM_CHAT_ID")

if missing_secrets:
     # Construct error message first
     error_message_raw = f"!!! CRITICAL ERROR: Missing required environment variables: {', '.join(missing_secrets)} !!! Ensure they are set in the Render Service Environment."
     logging.critical(error_message_raw)
     # Attempt to send notification if possible
     if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
         # Use the basic sender as the full one might rely on things not yet defined
         send_telegram_message_basic(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_message_raw)
     exit(1) # Exit immediately

# --- Full Helper Functions ---

def setup_driver():
    """Sets up the Selenium WebDriver, preferably using webdriver-manager."""
    logging.info("Setting up ChromeDriver...")
    options = webdriver.ChromeOptions()

    # --- Essential Options for Headless/Docker/Render ---
    options.add_argument("--headless=new") # Use the modern headless mode
    options.add_argument("--no-sandbox") # REQUIRED in containerized environments like Render/Docker
    options.add_argument("--disable-dev-shm-usage") # Avoids issues with limited shared memory in containers
    options.add_argument("--disable-gpu") # Often recommended for headless, may reduce resource usage

    # --- Other Useful Options ---
    # Use a realistic user agent
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36") # Example Linux UA
    options.add_argument("--window-size=1920,1080") # Set a reasonable window size
    options.add_experimental_option("excludeSwitches", ["enable-logging"]) # Quieter console output
    options.add_argument("--disable-extensions") # Improve startup time/reduce complexity
    options.add_argument("--disable-popup-blocking")

    # Set Page Load Strategy
    options.page_load_strategy = PageLoadStrategy.eager # Wait for DOM ready, not full load
    logging.info(f"Set page load strategy to: {options.page_load_strategy}")

    try:
        # --- Using webdriver-manager (Recommended) ---
        # Checks for installed Chrome version and downloads matching ChromeDriver
        # Assumes Chrome is installed in the Render environment (e.g., via Dockerfile)
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            logging.info("ChromeDriver setup using webdriver-manager successful.")
        except Exception as wdm_error:
             logging.warning(f"webdriver-manager failed ({wdm_error}). Falling back to assuming chromedriver is in PATH...")
             # --- Fallback: Assume chromedriver is in PATH (Less reliable) ---
             # This requires chromedriver binary to be installed and accessible in the Render environment's PATH
             service = Service() # Uses chromedriver from PATH by default
             driver = webdriver.Chrome(service=service, options=options)
             logging.info("ChromeDriver setup using system PATH successful (ensure chromedriver is installed and compatible).")


        # Set Timeouts (after driver is initialized)
        page_load_timeout_sec = 180 # Increased timeout for page load
        implicit_wait_sec = 20     # Default wait for find_element(s)
        script_timeout_sec = 60      # Timeout for async JS

        driver.set_page_load_timeout(page_load_timeout_sec)
        driver.implicitly_wait(implicit_wait_sec)
        driver.set_script_timeout(script_timeout_sec)
        logging.info(f"Set timeouts: Page Load={page_load_timeout_sec}s, Implicit Wait={implicit_wait_sec}s, Script={script_timeout_sec}s")

        logging.info("ChromeDriver setup complete (running headless).")
        return driver

    except WebDriverException as e:
        # Provide more specific feedback for common issues
        if "chrome not reachable" in str(e).lower():
             logging.error("WebDriver setup error: Chrome browser seems unreachable. Ensure Chrome is installed correctly in the environment.")
        elif "executable needs to be in PATH" in str(e).lower():
             logging.error("WebDriver setup error: ChromeDriver executable not found in PATH. Use webdriver-manager or ensure chromedriver is installed and in PATH.")
        else:
            logging.error(f"WebDriver setup error: {e}", exc_info=True)
        raise # Re-raise the original exception
    except Exception as e:
        logging.error(f"Unexpected ChromeDriver setup error: {e}", exc_info=True)
        raise


# Retrying logic for transient issues like elements not appearing immediately
@retrying.retry(stop_max_attempt_number=2, wait_fixed=3000, retry_on_exception=lambda e: isinstance(e, (TimeoutException, NoSuchElementException)))
def login_and_get_cookies(driver, url, username_id, password_id, username, password):
    """Logs into the webmail and extracts session cookies."""
    logging.info(f"Attempting login to: {url}")
    try:
        logging.info(f"Navigating to login page (timeout={driver.get_page_load_timeout()}s)...")
        driver.get(url)
        logging.info(f"Navigation command issued. Current URL after get: {driver.current_url}")

        # Use explicit waits for critical elements after navigation
        wait = WebDriverWait(driver, 30) # Wait up to 30 seconds

        logging.info(f"Waiting for username field: ID='{username_id}'")
        user_field = wait.until(EC.visibility_of_element_located((By.ID, username_id)))
        logging.info(f"Waiting for password field: ID='{password_id}'")
        pw_field = wait.until(EC.visibility_of_element_located((By.ID, password_id)))
        logging.info("Login fields located.")

        logging.info("Entering credentials...")
        user_field.clear(); time.sleep(0.2); user_field.send_keys(username); time.sleep(0.5)
        pw_field.clear(); time.sleep(0.2); pw_field.send_keys(password); time.sleep(0.5)

        logging.info(f"Submitting login form by sending RETURN to PW field ({password_id})...")
        pw_field.send_keys(Keys.RETURN)

        # Wait for a reliable post-login element
        post_login_locator = (By.XPATH, "//a[contains(@href, 'logout')] | //*[contains(text(),'로그아웃')] | //div[@id='main_container'] | //span[@class='username']") # Adjust if needed
        logging.info(f"Waiting up to 30s for login success indication (e.g., {post_login_locator})...")
        wait.until(EC.presence_of_element_located(post_login_locator))
        logging.info("Post-login element found. Login appears successful.")

        time.sleep(2) # Brief pause for any final redirects or AJAX
        logging.info("Extracting cookies...")
        cookies = {c['name']: c['value'] for c in driver.get_cookies()}
        if not cookies:
            # Should not happen if post-login element was found, but check anyway
            logging.warning("Login seemed successful, but no cookies were extracted.")
            raise Exception("쿠키 추출 실패 (로그인 후 쿠키 없음)")
        logging.info(f"Extracted {len(cookies)} cookies.")
        return cookies

    except TimeoutException as e:
        current_url = "N/A (Error getting URL)"
        page_source_snippet = "N/A (Error getting source)"
        screenshot_path = f"login_timeout_screenshot_{int(time.time())}.png"
        try: current_url = driver.current_url
        except Exception: pass
        try: page_source_snippet = driver.page_source[:1500] # Get more source
        except Exception: pass
        try: driver.save_screenshot(screenshot_path); logging.info(f"Saved timeout screenshot to {screenshot_path}")
        except Exception as ss_err: logging.warning(f"Failed to save timeout screenshot: {ss_err}")

        logging.error(f"TimeoutException during login process. Error details: {e}")
        logging.error(f"URL at time of timeout (if available): {current_url}")

        # Check if the error message indicates page load timeout specifically
        if "page load" in str(e).lower() or "timed out receiving message from renderer" in str(e).lower():
             error_detail = "Page load timeout during initial navigation."
             logging.error(error_detail + " Target site might be down/slow/blocking.")
             raise Exception(f"로그인 페이지 로드 시간 초과 ({url}). 사이트 접속 불가 또는 차단 확인 필요.") from e
        else:
            error_detail = "Timeout occurred waiting for an element or condition after navigation."
            logging.error(error_detail)
            logging.error(f"Page source snippet (if available):\n{page_source_snippet}")
            # Check if still on login page
            login_page_check_url = url.split('?')[0]
            if login_page_check_url in current_url:
                 logging.error("Still on login page. Check credentials or page structure changes.")
                 raise Exception("로그인 실패: 페이지 요소 대기 시간 초과 (로그인 페이지에 머무름).") from e
            else:
                 logging.error("Redirected away from login page but timed out waiting for post-login element.")
                 raise Exception("로그인 실패: 로그인 후 페이지 요소 대기 시간 초과.") from e

    except Exception as e:
        logging.error(f"An unexpected error occurred during login: {e}", exc_info=True)
        screenshot_path = f"login_error_screenshot_{int(time.time())}.png"
        try: driver.save_screenshot(screenshot_path); logging.info(f"Saved error screenshot to {screenshot_path}")
        except Exception as ss_err: logging.warning(f"Failed to save error screenshot: {ss_err}")
        # Check for specific WebDriver errors like ReadTimeout
        if isinstance(e, WebDriverException) and "Read timed out" in str(e):
             raise Exception(f"로그인 페이지 접속 실패 (Read Timeout). 사이트 접속 불가 또는 네트워크 문제 확인 필요.") from e
        raise # Re-raise other unexpected exceptions


@retrying.retry(stop_max_attempt_number=3, wait_fixed=10000, retry_on_exception=lambda e: isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)))
def download_excel_report(report_url, cookies):
    """Downloads the Excel report using the session cookies."""
    logging.info(f"Downloading report from: {report_url}")
    session = requests.Session()
    session.cookies.update(cookies)
    # Use a standard UA, ensure Referer is plausible
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Referer': WEBMAIL_LOGIN_URL.split('/mail2')[0] + '/', # Base domain as referer
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    try:
        response = session.get(report_url, headers=headers, stream=True, timeout=120) # Generous timeout for download
        logging.info(f"Download response status code: {response.status_code}")
        response.raise_for_status() # Check for 4xx/5xx errors

        content_type = response.headers.get('Content-Type', '').lower()
        content_disposition = response.headers.get('Content-Disposition', '')
        logging.info(f"Response Headers - Content-Type: '{content_type}', Content-Disposition: '{content_disposition}'")

        # Check for indicators of an Excel file
        is_excel_type = any(mime in content_type for mime in ['excel', 'spreadsheetml', 'vnd.ms-excel', 'octet-stream'])
        is_excel_disposition = '.xlsx' in content_disposition or '.xls' in content_disposition

        if is_excel_type or is_excel_disposition:
            excel_data = io.BytesIO(response.content)
            file_size = excel_data.getbuffer().nbytes
            logging.info(f"Excel download successful ({file_size} bytes).")

            # Sanity check for potentially empty/error files disguised as Excel
            if file_size < 2048: # Adjust threshold if needed
                logging.warning(f"Downloaded file is very small ({file_size} bytes). Checking content...")
                # Check if content looks like HTML (error page)
                try:
                    preview = excel_data.getvalue()[:500].decode('utf-8', errors='ignore')
                    if any(tag in preview.lower() for tag in ['<html', '<head', '<body', 'login', '로그인', 'error', 'session', '권한']):
                        logging.error(f"Small file content suggests an error or login page:\n{preview}")
                        raise Exception("다운로드된 파일이 작고 오류 페이지로 보입니다.")
                    else:
                        logging.warning("Small file content doesn't immediately look like HTML error. Proceeding.")
                except Exception as parse_err:
                    logging.warning(f"Could not decode/check small file content: {parse_err}. Assuming valid.")
                finally:
                    excel_data.seek(0) # IMPORTANT: Rewind buffer after reading preview

            return excel_data
        else:
            # Log the received content if it's not Excel
            logging.error(f"Downloaded content not identified as Excel.")
            try: preview = response.content[:500].decode('utf-8', errors='ignore'); logging.error(f"Content preview:\n{preview}")
            except Exception: logging.error("Could not decode content preview.")
            raise Exception(f"다운로드된 파일 형식이 엑셀이 아닙니다 (Content-Type: {content_type}).")

    except requests.exceptions.Timeout as e:
        logging.error(f"Timeout occurred while downloading report from {report_url}")
        raise Exception(f"보고서 다운로드 시간 초과: {report_url}") from e
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error during download: {e.response.status_code} - {e.response.reason}")
        try: logging.error(f"HTTP Error Response Content:\n{e.response.content[:500].decode('utf-8', errors='ignore')}")
        except Exception: pass
        # Check for common auth errors
        if e.response.status_code in [401, 403]:
             raise Exception(f"보고서 다운로드 권한 오류 ({e.response.status_code}). 쿠키가 만료되었거나 권한이 부족할 수 있습니다.") from e
        raise Exception(f"보고서 다운로드 중 HTTP 오류 발생: {e.response.status_code}") from e
    except requests.exceptions.RequestException as e:
        logging.error(f"Network or request error during download: {e}")
        raise Exception(f"보고서 다운로드 중 네트워크 오류 발생: {e}") from e
    except Exception as e:
        logging.error(f"An unexpected error occurred during report download: {e}", exc_info=True)
        raise


def parse_time_robust(time_str):
    """Robustly parses time strings from various formats."""
    if pd.isna(time_str) or time_str == '': return None
    if isinstance(time_str, datetime.time): return time_str
    if isinstance(time_str, datetime.datetime): return time_str.time()

    time_str_orig = time_str
    time_str = str(time_str).strip()
    if not time_str: return None

    # Handle numeric/Excel time (fraction of a day)
    if isinstance(time_str_orig, (float, int)) and 0 <= time_str_orig < 1:
        try:
            total_seconds = int(round(time_str_orig * 86400)) # 24*60*60
            total_seconds = min(total_seconds, 86399) # Cap at 23:59:59
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return datetime.time(hours, minutes, seconds)
        except (ValueError, TypeError): pass # Fall through to string parsing

    # Handle string formats
    if ' ' in time_str and ':' in time_str: # Datetime string
        try: return datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').time()
        except ValueError: pass
        try: return datetime.datetime.strptime(time_str, '%Y/%m/%d %H:%M:%S').time()
        except ValueError: pass
        # Add other datetime formats if necessary

    # Common time formats
    for fmt in ('%H:%M:%S', '%H:%M', '%I:%M:%S %p', '%I:%M %p'):
        try: return datetime.datetime.strptime(time_str, fmt).time()
        except ValueError: continue

    # Handle HHMMSS or HHMM if purely numeric
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

    date_str_orig = date_str
    date_str = str(date_str).strip()
    if not date_str: return None

    # Extract date part if time is included
    date_part = date_str.split(' ')[0]

    # Common date formats
    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%y-%m-%d', '%y/%m/%d', '%Y%m%d', '%m/%d/%Y', '%m/%d/%y'):
        try: return datetime.datetime.strptime(date_part, fmt).date()
        except ValueError: continue

    logging.warning(f"Could not parse date string: '{date_str_orig}'")
    return None


def combine_date_time(date_val, time_val):
    """Combines a date and time object into a datetime object."""
    if isinstance(date_val, datetime.date) and isinstance(time_val, datetime.time):
        return datetime.datetime.combine(date_val, time_val)
    return None


def find_korean_font():
    """Finds a usable Korean font, especially in Linux/Render environments."""
    # Prioritize Nanum fonts as they are commonly installed via apt
    common_font_files = ["NanumGothic.ttf", "NanumBarunGothic.ttf", "malgun.ttf", "AppleGothic.ttf", "gulim.ttc"]
    linux_font_paths = [ "/usr/share/fonts/truetype/nanum/", "/usr/share/fonts/opentype/nanum/" ]

    # 1. Check known Linux paths for Nanum fonts
    for path in linux_font_paths:
        try:
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    if filename in common_font_files:
                        found_path = os.path.join(path, filename)
                        # Verify font can be loaded by matplotlib
                        try: fm.FontProperties(fname=found_path); logging.info(f"Found and verified Korean font: {found_path}"); return found_path
                        except Exception as load_err: logging.warning(f"Found font {found_path} but failed verification: {load_err}"); continue
        except OSError: continue

    # 2. If not found, search all system fonts using font_manager
    logging.info("Korean font not found in common paths, searching all system fonts...")
    try:
        system_fonts = fm.findSystemFonts(fontpaths=None, fontext='ttf')
        for f_path in system_fonts:
            font_name = Path(f_path).name
            if any(common_name in font_name for common_name in common_font_files):
                try: fm.FontProperties(fname=f_path); logging.info(f"Found and verified Korean font via system search: {f_path}"); return f_path
                except Exception as load_err: logging.warning(f"Found potential font {f_path} but failed verification: {load_err}"); continue
    except Exception as e:
        logging.warning(f"Error searching system fonts with font_manager: {e}")

    logging.error("CRITICAL: Korean font not found. Install 'fonts-nanum*' or required fonts in the environment. Table image will likely have broken text.")
    return None # Indicate failure


def create_table_image(df, title, output_path="table_image.png"):
    """Creates a PNG image from a Pandas DataFrame."""
    logging.info("Attempting to create table image...")
    if df.empty:
        logging.warning("DataFrame is empty, cannot generate table image.")
        return None

    # Backend already set to Agg globally if needed

    font_path = find_korean_font()
    if font_path:
        try:
            # Explicitly rebuild cache if issues persist (can be slow)
            # fm._load_fontmanager(try_read_cache=False)
            prop = fm.FontProperties(fname=font_path, size=10)
            plt.rcParams['font.family'] = prop.get_name()
            plt.rcParams['axes.unicode_minus'] = False
            logging.info(f"Using font: {font_path} (Family: {prop.get_name()})")
        except Exception as font_err:
             logging.error(f"Failed to set font properties for {font_path}: {font_err}", exc_info=True)
             logging.warning("Falling back to default sans-serif font.")
             plt.rcParams['font.family'] = 'sans-serif'
    else:
        # Error logged in find_korean_font function
        plt.rcParams['font.family'] = 'sans-serif' # Use default

    nr, nc = df.shape
    # Dynamic figsize with reasonable limits
    base_w, incr_w = 6, 0.9; base_h, incr_h = 2, 0.35; max_w, max_h = 30, 45
    fw = min(max(base_w, base_w + nc * incr_w), max_w)
    fh = min(max(base_h, base_h + nr * incr_h), max_h)
    logging.info(f"Table: {nr} rows, {nc} cols. Figure size: ({fw:.1f}, {fh:.1f})")

    fig, ax = plt.subplots(figsize=(fw, fh))
    ax.axis('off')

    try:
        tab = Table(ax, bbox=[0, 0, 1, 1])

        # Header styling
        for j, col in enumerate(df.columns):
            cell = tab.add_cell(0, j, 1, 1, text=str(col), loc='center', facecolor='#E0E0E0', width=1.0/nc if nc > 0 else 1)
            cell.set_text_props(weight='bold')

        # Row styling
        for i in range(nr):
            for j in range(nc):
                txt = str(df.iloc[i, j]); max_len = 45
                if len(txt) > max_len: txt = txt[:max_len - 3] + '...'
                cell_color = 'white' if i % 2 == 0 else '#F5F5F5' # Alternate row colors
                tab.add_cell(i + 1, j, 1, 1, text=txt, loc='center', facecolor=cell_color, width=1.0/nc if nc > 0 else 1)

        tab.auto_set_font_size(False)
        tab.set_fontsize(9)
        ax.add_table(tab)

        plt.title(title, fontsize=13, weight='bold', pad=20)
        plt.tight_layout(pad=1.5) # Add padding around table/title

        plt.savefig(output_path, bbox_inches='tight', dpi=110) # Adjust DPI as needed
        plt.close(fig) # Close figure to release memory

        logging.info(f"Table image saved successfully: {output_path}")
        size_bytes = Path(output_path).stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        logging.info(f"Image file size: {size_mb:.2f} MB")

        if size_mb > 9.5: logging.warning(f"Image size ({size_mb:.2f} MB) may exceed Telegram limit.")
        if size_bytes < 100: logging.error("Generated image file size is suspiciously small (<100 bytes)."); return None

        return output_path

    except Exception as e:
        logging.error(f"Failed to create or save table image: {e}", exc_info=True)
        plt.close(fig) # Ensure cleanup on error
        return None


@retrying.retry(stop_max_attempt_number=3, wait_fixed=5000, retry_on_exception=lambda e: isinstance(e, requests.exceptions.RequestException))
def send_telegram_photo(bot_token, chat_id, photo_path, caption):
    """Sends a photo to Telegram with caption, handling potential errors."""
    api_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    path_obj = Path(photo_path) # Work with Path object

    if not path_obj.exists():
        logging.error(f"Cannot send photo, file not found: {photo_path}")
        return False
    file_size = path_obj.stat().st_size
    if file_size == 0:
        logging.error(f"Cannot send photo, file size is 0 bytes: {photo_path}")
        return False
    # Check Telegram's typical size limit (10MB photo, 50MB document)
    if file_size > 10 * 1024 * 1024:
         logging.warning(f"Photo size ({file_size / (1024*1024):.2f} MB) exceeds 10MB limit. Sending may fail.")
         # Consider sending as document if > 10MB and < 50MB? For now, just warn.

    logging.info(f"Sending photo {photo_path} ({file_size / (1024*1024):.2f} MB)...")
    try:
        with open(photo_path, 'rb') as photo_file:
            # Ensure caption length is safe (1024 chars)
            max_caption_len = 1024
            if len(caption) > max_caption_len:
                logging.warning(f"Caption length truncated to {max_caption_len} chars.")
                caption = caption[:max_caption_len - 4] + "..." # Leave room for ellipsis

            files = {'photo': (path_obj.name, photo_file)}
            payload = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'MarkdownV2'}
            response = requests.post(api_url, data=payload, files=files, timeout=90) # Increased timeout for upload

            rd = {}
            try: rd = response.json()
            except json.JSONDecodeError:
                logging.error(f"TG API JSON decode fail (sendPhoto). Status:{response.status_code}, Content:{response.text[:500]}")
                response.raise_for_status() # Raise error based on status code

            if response.status_code == 200 and rd.get("ok"):
                logging.info("Telegram photo sent successfully.")
                return True
            else:
                err_desc = rd.get('description', 'N/A')
                err_code = rd.get('error_code', 'N/A')
                logging.error(f"TG API Error (sendPhoto): {err_desc} (Code: {err_code})")
                # Raise exception for client errors (4xx) to prevent retry
                if 400 <= response.status_code < 500:
                    raise requests.exceptions.HTTPError(f"TG Client Error {response.status_code}: {err_desc}", response=response)
                else: response.raise_for_status() # Let retry handle server errors (5xx)
                return False # Should be unreachable

    except requests.exceptions.HTTPError as e:
         # Log specific client errors that shouldn't be retried
         if 400 <= e.response.status_code < 500:
              logging.error(f"HTTP Client Error sending photo (will not retry): {e}", exc_info=True)
              error_text = f"*{escape_markdown(TARGET_DATE_STR)} 이미지 전송 실패*\n텔레그램 API 오류 \\(HTTP {e.response.status_code}\\): {escape_markdown(e.response.json().get('description','N/A'))}"
              send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_text) # Notify about failure
         else: logging.error(f"HTTP Server/Network Error sending photo: {e}"); raise # Allow retry
         return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error sending photo: {e}. Retrying allowed.")
        raise # Allow retry
    except FileNotFoundError:
         logging.error(f"File not found error during photo sending: {photo_path}")
         return False
    except Exception as e:
        logging.error(f"Unexpected error sending Telegram photo: {e}", exc_info=True)
        # Optional: Raise specific exception or just log
        # raise Exception(f"Unexpected photo send error: {e}")
        return False


def send_telegram_message(bot_token, chat_id, text):
    """Sends a text message to Telegram, splitting if too long, with fallback."""
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    max_len = 4096
    messages_to_send = []

    if not text: logging.warning("Attempted to send an empty message."); return True

    # Split message logic (same as before)
    if len(text) > max_len:
        logging.info(f"Message length ({len(text)}) exceeds {max_len}, splitting...")
        start = 0
        while start < len(text):
            end = text.rfind('\n', start, start + max_len)
            if end == -1 or end <= start: end = start + max_len
            chunk = text[start:end].strip()
            if chunk: messages_to_send.append(chunk)
            start = end
    else:
        messages_to_send.append(text)

    logging.info(f"Attempting to send {len(messages_to_send)} message part(s) to Telegram.")
    all_parts_sent_successfully = True

    for i, part in enumerate(messages_to_send):
        if not part: logging.warning(f"Skipping empty message part {i+1}."); continue

        payload = {'chat_id': chat_id, 'text': part, 'parse_mode': 'MarkdownV2'}
        part_sent = False
        attempt = 0
        max_attempts = 2 # Try MarkdownV2, then plain text

        while not part_sent and attempt < max_attempts:
            attempt += 1
            mode = payload.get('parse_mode', 'Plain Text') # Default to Plain if None
            if payload.get('parse_mode') is None: mode = 'Plain Text'

            logging.info(f"Sending part {i+1}/{len(messages_to_send)} using mode: {mode} (Attempt {attempt})")
            try:
                response = requests.post(api_url, data=payload, timeout=30)
                rd = response.json()

                if response.status_code == 200 and rd.get("ok"):
                    logging.info(f"Telegram message part {i+1} sent successfully using {mode}.")
                    part_sent = True
                else:
                    err_desc = rd.get('description', 'N/A')
                    err_code = rd.get('error_code', 'N/A')
                    logging.error(f"TG API Error (Part {i+1}, Mode: {mode}): {err_desc} (Code: {err_code})")
                    # Log problematic content snippet only on error
                    logging.error(f"Failed content sample (first 300 chars): {part[:300]}")

                    # If MarkdownV2 failed, retry as plain text on the next attempt
                    if payload.get('parse_mode') == 'MarkdownV2' and attempt < max_attempts:
                        logging.warning("MarkdownV2 failed, will retry as plain text.")
                        payload['parse_mode'] = None # Remove parse_mode for plain text
                        payload['text'] = part # Ensure original text is used for plain
                    else:
                        # If plain text fails, or it was the last attempt, mark as failed
                        all_parts_sent_successfully = False
                        break # Stop trying for this part

            except requests.exceptions.Timeout:
                logging.error(f"Timeout sending Telegram message part {i+1} (Mode: {mode}).")
                time.sleep(5)
                # *** CORRECTED LINE ***
                if attempt == max_attempts:
                    all_parts_sent_successfully = False
            except requests.exceptions.RequestException as e:
                logging.error(f"Network error sending Telegram message part {i+1} (Mode: {mode}): {e}")
                time.sleep(5)
                # *** CORRECTED LINE ***
                if attempt == max_attempts:
                    all_parts_sent_successfully = False
            except json.JSONDecodeError:
                 logging.error(f"JSON decode fail (TG Send Part {i+1}). Status: {response.status_code}, Content: {response.text[:500]}")
                 time.sleep(5)
                 # *** CORRECTED LINE ***
                 if attempt == max_attempts:
                     all_parts_sent_successfully = False
            except Exception as e:
                logging.error(f"Unexpected error sending Telegram message part {i+1} (Mode: {mode}): {e}", exc_info=True)
                # *** CORRECTED LINE (Indentation only) ***
                if attempt == max_attempts:
                    all_parts_sent_successfully = False
                break # Don't retry on unexpected error

        if not part_sent:
             all_parts_sent_successfully = False # Mark overall failure if any part fails

    return all_parts_sent_successfully


def analyze_attendance(excel_data, sheet_name):
    """Analyzes the downloaded Excel data for attendance status."""
    logging.info(f"Analyzing attendance data from sheet: '{sheet_name}'.")
    analysis_result = {
        "notifications": [], # For morning issues report
        "detailed_status": [], # For evening detailed report
        "summary": { "total_employees": 0, "target": 0, "excluded": 0, "clocked_in": 0, "missing_in": 0, "clocked_out": 0, "missing_out": 0 },
        "excluded_employees": [], # For summary report exclusion list
        "df_processed": None # For generating the table image
    }
    processed_data_for_image = []

    try:
        # Read Excel, handle potential issues
        try:
            df = pd.read_excel(excel_data, sheet_name=sheet_name, skiprows=2, dtype=str, keep_default_na=False)
        except ValueError as sheet_error:
             if "Worksheet named" in str(sheet_error) and sheet_name in str(sheet_error):
                  logging.error(f"FATAL: Worksheet named '{sheet_name}' not found in the Excel file.")
                  raise ValueError(f"'{sheet_name}' 시트를 찾을 수 없습니다.") from sheet_error
             else: raise # Re-raise other ValueErrors

        logging.info(f"Loaded {len(df)} rows from sheet '{sheet_name}'.")
        if df.empty: logging.warning(f"Excel sheet '{sheet_name}' is empty."); return analysis_result

        # --- Column Handling ---
        df.columns = [str(col).strip() for col in df.columns]
        logging.debug(f"Initial columns: {df.columns.tolist()}")

        # Define mapping based on observed structure (verify if Excel changes)
        actual_to_desired_mapping = { '서무원': '이름', '출퇴근': '유형', '정상': '구분', 'Unnamed: 11': '출근시간_raw', 'Unnamed: 13': '퇴근시간_raw', 'Unnamed: 16': '휴가시작시간_raw', 'Unnamed: 18': '휴가종료시간_raw' }

        # Dynamically find date column (more robust)
        date_col_actual_name = None
        for col in df.columns:
            if re.match(r'^\d{4}-\d{2}-\d{2}$', str(col).strip()): date_col_actual_name = col; break

        if not date_col_actual_name:
             # Fallback using target date string if present as column name
             if TARGET_DATE_STR in df.columns: date_col_actual_name = TARGET_DATE_STR; logging.warning(f"Using fallback date column: '{date_col_actual_name}'")
             else:
                # Last resort: Guess based on position (Fragile!)
                potential_date_col_index = 5
                if len(df.columns) > potential_date_col_index and parse_date_robust(df.columns[potential_date_col_index]):
                     date_col_actual_name = df.columns[potential_date_col_index]
                     logging.warning(f"Using highly speculative date column at index {potential_date_col_index}: '{date_col_actual_name}'")
                else:
                     logging.error("FATAL: Cannot find date column. Columns: %s", df.columns.tolist()); raise KeyError("엑셀 보고서에서 날짜 컬럼을 찾을 수 없습니다.")

        actual_to_desired_mapping[date_col_actual_name] = '일자'
        logging.info(f"Identified date column as '{date_col_actual_name}'")

        # Validate all required columns exist
        required_source_cols = list(actual_to_desired_mapping.keys())
        missing_source_cols = [c for c in required_source_cols if c not in df.columns]
        if missing_source_cols:
            logging.error(f"FATAL: Missing required source columns: {missing_source_cols}. Available: {df.columns.tolist()}")
            raise KeyError(f"필수 원본 컬럼 누락: {', '.join(missing_source_cols)}")

        # Select and rename
        df_processed = df[required_source_cols].copy(); df_processed.rename(columns=actual_to_desired_mapping, inplace=True)
        logging.info(f"Columns after rename: {df_processed.columns.tolist()}")

        # --- Data Parsing ---
        df_processed['일자_dt'] = df_processed['일자'].apply(parse_date_robust)
        df_processed['출근시간_dt'] = df_processed['출근시간_raw'].apply(parse_time_robust)
        df_processed['퇴근시간_dt'] = df_processed['퇴근시간_raw'].apply(parse_time_robust)
        df_processed['휴가시작시간_dt'] = df_processed['휴가시작시간_raw'].apply(parse_time_robust)
        df_processed['휴가종료시간_dt'] = df_processed['휴가종료시간_raw'].apply(parse_time_robust)

        # --- Filtering & Standard Times ---
        df_filtered = df_processed[df_processed['일자_dt'] == TARGET_DATE].copy()
        if df_filtered.empty: logging.warning(f"No data found for target date {TARGET_DATE_STR}."); return analysis_result
        logging.info(f"Processing {len(df_filtered)} rows for target date {TARGET_DATE_STR}.")

        try:
            standard_start_time = datetime.datetime.strptime(STANDARD_START_TIME_STR, '%H:%M:%S').time(); standard_end_time = datetime.datetime.strptime(STANDARD_END_TIME_STR, '%H:%M:%S').time()
            standard_start_dt = datetime.datetime.combine(TARGET_DATE, standard_start_time); standard_end_dt = datetime.datetime.combine(TARGET_DATE, standard_end_time)
            lunch_start_time = datetime.time(12, 0, 0); lunch_end_time = datetime.time(13, 0, 0); afternoon_start_time = lunch_end_time
        except ValueError as time_parse_err: logging.error(f"FATAL: Invalid standard time format: {time_parse_err}"); raise ValueError("표준 근무 시간 형식 오류")

        # --- Process per Employee ---
        grouped = df_filtered.groupby('이름')
        analysis_result["summary"]["total_employees"] = len(grouped)
        logging.info(f"Found {len(grouped)} unique employees for {TARGET_DATE_STR}.")

        for name_raw, group_df in grouped:
            name_trimmed = str(name_raw).strip();
            if not name_trimmed: logging.warning("Skipping entry with empty name."); continue
            name_escaped = escape_markdown(name_trimmed); logging.debug(f"--- Processing: {name_trimmed} ---")

            is_fully_excluded = False; exclusion_reason = ""; collected_leaves = []; attendance_data = None

            # Collect all leaves and latest attendance record for the employee
            for _, row in group_df.iterrows():
                row_type = str(row.get('유형', '')).strip(); row_category = str(row.get('구분', '')).strip()
                start_time = row['휴가시작시간_dt']; end_time = row['휴가종료시간_dt']
                is_leave = row_type in FULL_DAY_LEAVE_TYPES or row_category in FULL_DAY_LEAVE_REASONS or row_category in [MORNING_HALF_LEAVE, AFTERNOON_HALF_LEAVE]

                if is_leave:
                    leave_name = row_category if row_category else row_type
                    if leave_name:
                        collected_leaves.append({'type': leave_name, 'start': start_time, 'end': end_time})
                        logging.debug(f"{name_trimmed}: Found leave: {leave_name} ({start_time}-{end_time})")
                        # Check for single full-day leave immediately
                        if start_time and end_time and start_time <= standard_start_time and end_time >= standard_end_time:
                            is_fully_excluded = True; exclusion_reason = f"{leave_name} ({start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')})"; break
                elif row_type == ATTENDANCE_TYPE:
                    attendance_data = {'in_dt': row['출근시간_dt'], 'out_dt': row['퇴근시간_dt'], 'in_raw': str(row['출근시간_raw']).strip(), 'out_raw': str(row['퇴근시간_raw']).strip()}
                    logging.debug(f"{name_trimmed}: Found attendance: In={attendance_data['in_dt']}, Out={attendance_data['out_dt']}")

            if is_fully_excluded: # Already excluded by single leave
                 logging.info(f"{name_trimmed}: Excluded (Single Full Day Leave: {exclusion_reason})")
                 analysis_result["excluded_employees"].append(f"{name_trimmed}: {exclusion_reason}")
                 analysis_result["summary"]["excluded"] += 1
                 processed_data_for_image.append({'이름': name_trimmed, '일자': TARGET_DATE_STR, '유형': '휴가/제외', '구분': exclusion_reason, '출근시간': '-', '퇴근시간': '-'})
                 continue # Next employee

            # Check combined leaves if not already excluded
            if not is_fully_excluded and collected_leaves:
                covers_morning = False; covers_afternoon = False; min_start = None; max_end = None
                for leave in collected_leaves:
                    ls, le, lt = leave['start'], leave['end'], leave['type']
                    if ls and (min_start is None or ls < min_start): min_start = ls
                    if le and (max_end is None or le > max_end): max_end = le
                    if lt == MORNING_HALF_LEAVE or (ls and le and ls <= standard_start_time and le >= lunch_start_time): covers_morning = True
                    if lt == AFTERNOON_HALF_LEAVE or (ls and le and ls <= lunch_end_time and le >= standard_end_time): covers_afternoon = True
                if covers_morning and covers_afternoon:
                     is_fully_excluded = True
                     combined_types = " + ".join(sorted(list(set(l['type'] for l in collected_leaves if l['type']))))
                     time_range = f"{min_start.strftime('%H:%M') if min_start else '?'} - {max_end.strftime('%H:%M') if max_end else '?'}"
                     exclusion_reason = f"{combined_types} ({time_range})"
                     logging.info(f"{name_trimmed}: Excluded (Combined Leaves: {exclusion_reason})")
                     analysis_result["excluded_employees"].append(f"{name_trimmed}: {exclusion_reason}")
                     analysis_result["summary"]["excluded"] += 1
                     processed_data_for_image.append({'이름': name_trimmed, '일자': TARGET_DATE_STR, '유형': '휴가/제외', '구분': exclusion_reason, '출근시간': '-', '퇴근시간': '-'})
                     continue # Next employee

            # --- Analyze Attendance ---
            analysis_result["summary"]["target"] += 1; logging.debug(f"{name_trimmed}: Analyzing attendance.")
            clock_in_dt, clock_out_dt = None, None; clock_in_raw, clock_out_raw = '', ''
            if attendance_data: clock_in_dt, clock_out_dt, clock_in_raw, clock_out_raw = attendance_data['in_dt'], attendance_data['out_dt'], attendance_data['in_raw'], attendance_data['out_raw']

            actual_start_dt = combine_date_time(TARGET_DATE, clock_in_dt) if clock_in_dt else None
            actual_end_dt = combine_date_time(TARGET_DATE, clock_out_dt) if clock_out_dt else None
            has_clock_in = actual_start_dt is not None; has_clock_out = actual_end_dt is not None

            # Determine effective leaves for this employee
            has_morning_leave = False; has_afternoon_leave = False; leave_display_type = '출퇴근'; leave_display_category = '정상'
            morning_types = []; afternoon_types = []
            if collected_leaves:
                for leave in collected_leaves:
                    ls, le, lt = leave['start'], leave['end'], leave['type']
                    if lt == MORNING_HALF_LEAVE or (ls and le and ls <= standard_start_time and le >= lunch_start_time): has_morning_leave = True; morning_types.append(lt)
                    if lt == AFTERNOON_HALF_LEAVE or (ls and le and ls <= lunch_end_time and le >= standard_end_time): has_afternoon_leave = True; afternoon_types.append(lt)
                if has_morning_leave: leave_display_type = MORNING_HALF_LEAVE; leave_display_category = " + ".join(sorted(list(set(morning_types)))) or '오전반차'
                if has_afternoon_leave: leave_display_type = AFTERNOON_HALF_LEAVE if leave_display_type == '출퇴근' else leave_display_type+"+"+AFTERNOON_HALF_LEAVE; leave_display_category = " + ".join(sorted(list(set(afternoon_types)))) or '오후반차'
                if has_morning_leave and has_afternoon_leave: leave_display_category = "오전+오후반차" # Should have been excluded, but handle display

            expected_start_dt = datetime.datetime.combine(TARGET_DATE, afternoon_start_time) if has_morning_leave else standard_start_dt
            expected_end_dt = datetime.datetime.combine(TARGET_DATE, lunch_start_time) if has_afternoon_leave else standard_end_dt

            # --- Check Issues ---
            issues = []
            # Clock In
            if has_clock_in:
                analysis_result["summary"]["clocked_in"] += 1
                if not has_morning_leave and actual_start_dt > standard_start_dt: issues.append(f"지각: {escape_markdown(clock_in_dt.strftime('%H:%M:%S'))}")
                elif has_morning_leave and actual_start_dt > expected_start_dt: issues.append(f"오전반차 후 지각: {escape_markdown(clock_in_dt.strftime('%H:%M:%S'))}")
            else: # Missing Clock In
                analysis_result["summary"]["missing_in"] += 1
                if not has_morning_leave: issues.append("출근 기록 없음")

            # Clock Out
            if has_clock_out:
                analysis_result["summary"]["clocked_out"] += 1
                if not has_afternoon_leave and actual_end_dt < standard_end_dt: issues.append(f"조퇴: {escape_markdown(clock_out_dt.strftime('%H:%M:%S'))}")
                elif has_afternoon_leave and actual_end_dt < expected_end_dt: issues.append(f"오후반차 전 조퇴: {escape_markdown(clock_out_dt.strftime('%H:%M:%S'))}")
            elif has_clock_in: # Missing Clock Out (only if clocked in)
                 analysis_result["summary"]["missing_out"] += 1
                 if not has_afternoon_leave: issues.append("퇴근 기록 없음")

            # --- Compile Reports ---
            if issues:
                issue_string = ", ".join(issues)
                analysis_result["notifications"].append(f"*{name_escaped}*: {issue_string}")
                logging.info(f"{name_trimmed}: Issues - {issue_string}")

            in_status = clock_in_dt.strftime('%H:%M:%S') if has_clock_in else "기록없음"
            out_status = clock_out_dt.strftime('%H:%M:%S') if has_clock_out else ("기록없음" if has_clock_in else "출근없음")
            analysis_result["detailed_status"].append({'name': name_trimmed, 'in_status': in_status, 'out_status': out_status})

            processed_data_for_image.append({'이름': name_trimmed, '일자': TARGET_DATE_STR, '유형': leave_display_type, '구분': leave_display_category, '출근시간': clock_in_raw if clock_in_raw else ('-' if has_clock_in else '기록없음'), '퇴근시간': clock_out_raw if clock_out_raw else ('-' if has_clock_out else '기록없음')})


        # --- Finalize Analysis ---
        if processed_data_for_image:
             image_df_cols = ['이름', '일자', '유형', '구분', '출근시간', '퇴근시간']
             analysis_result["df_processed"] = pd.DataFrame(processed_data_for_image, columns=image_df_cols)
             logging.info(f"Created image DF with {len(analysis_result['df_processed'])} rows.")
        else:
             logging.warning("No data processed for image DF."); analysis_result["df_processed"] = pd.DataFrame(columns=['이름', '일자', '유형', '구분', '출근시간', '퇴근시간'])

        # Sanity checks
        summary = analysis_result["summary"]; calc_total = summary["target"] + summary["excluded"]
        if calc_total != summary["total_employees"]: logging.warning(f"Summary Count Mismatch: Total={summary['total_employees']}, Calc={calc_total}")
        target_check = summary["clocked_in"] + summary["missing_in"]; out_check = summary["clocked_out"] + summary["missing_out"]
        if summary["target"] != target_check: logging.warning(f"Target Count Mismatch: Target={summary['target']}, In/Miss={target_check}")
        if summary["clocked_in"] != out_check: logging.warning(f"ClockIn Count Mismatch: ClockedIn={summary['clocked_in']}, Out/Miss={out_check}")

        logging.info(f"Analysis complete. Summary: {summary}")
        return analysis_result

    except KeyError as e:
        logging.error(f"Analysis KeyError: {e}. Check Excel column names/structure.", exc_info=True)
        error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n엑셀 컬럼 오류 \\({escape_markdown(str(e))}\\)\\."
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)
        analysis_result["summary"]["total_employees"] = -1; return analysis_result
    except ValueError as e: # Catch specific errors like invalid time formats
         logging.error(f"Analysis ValueError: {e}.", exc_info=True)
         error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n데이터 값 또는 형식 오류: {escape_markdown(str(e))}\\."; send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg); analysis_result["summary"]["total_employees"] = -1; return analysis_result
    except Exception as e:
        logging.error(f"Unexpected error during analysis: {e}", exc_info=True)
        error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 분석 중 예외 발생*\n오류: {escape_markdown(str(e))}"; send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg); analysis_result["summary"]["total_employees"] = -1; return analysis_result


# --- Main Execution Logic ---
if __name__ == "__main__":
    script_start_time = time.time()
    logging.info(f"--- Attendance Bot Script started for date: {TARGET_DATE_STR} (KST) ---")

    driver = None
    excel_file_data = None
    error_occurred = False
    analysis_result = {}

    # Phase 1: Setup, Login, Download
    try:
        driver = setup_driver()
        # Ensure connectivity check passes in workflow or handle potential blocks
        cookies = login_and_get_cookies(driver, WEBMAIL_LOGIN_URL, WEBMAIL_ID_FIELD_ID, WEBMAIL_PW_FIELD_ID, WEBMAIL_USERNAME, WEBMAIL_PASSWORD)
        excel_file_data = download_excel_report(REPORT_URL, cookies)
        logging.info("Setup, login, and download successful.")
    except Exception as setup_err:
        logging.error(f"Critical error during setup/login/download: {setup_err}", exc_info=True)
        error_occurred = True
        # Escape the error message for Telegram
        error_msg_escaped = escape_markdown(f"{TARGET_DATE_STR} 스크립트 초기화 오류\n단계: 설정/로그인/다운로드\n오류: {str(setup_err)}")
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{error_msg_escaped}*")
    finally:
        if driver:
            try: driver.quit(); logging.info("WebDriver closed.")
            except Exception as e: logging.error(f"Error closing WebDriver: {e}", exc_info=True)

    # Phase 2: Analysis and Reporting
    if excel_file_data and not error_occurred:
        logging.info("Proceeding with analysis and reporting...")
        try:
            analysis_result = analyze_attendance(excel_file_data, EXCEL_SHEET_NAME)
            if not isinstance(analysis_result, dict) or analysis_result.get("summary", {}).get("total_employees", -1) == -1:
                logging.error("Analysis function indicated failure. Reporting may be incomplete.")
                error_occurred = True # Mark as error if analysis fails internally
            else:
                # --- Reporting Logic ---
                logging.info("Analysis successful. Preparing reports...")
                is_evening = CURRENT_HOUR_KST >= EVENING_RUN_THRESHOLD_HOUR
                logging.info(f"Current KST hour {CURRENT_HOUR_KST}, Evening run threshold {EVENING_RUN_THRESHOLD_HOUR}. Is evening? {is_evening}")

                attendance_issues = analysis_result.get("notifications", [])
                detailed_statuses = analysis_result.get("detailed_status", [])
                analysis_summary = analysis_result.get("summary", {})
                excluded_employees = analysis_result.get("excluded_employees", [])
                df_for_image = analysis_result.get("df_processed")
                image_path = None # Initialize image path

                # 1. Create & Send Table Image
                if df_for_image is not None and not df_for_image.empty:
                    image_title = f"{TARGET_DATE_STR} 근태 현황 ({analysis_summary.get('target', 0)}명 확인, {analysis_summary.get('excluded', 0)}명 제외)"
                    image_filename = f"attendance_report_{TARGET_DATE_STR}.png"
                    # Create image in a writable directory (Render's /tmp is usually safe)
                    # output_dir = "/tmp" # Or use "." if current dir is writable
                    # image_path_full = os.path.join(output_dir, image_filename)
                    image_path = create_table_image(df_for_image, image_title, image_filename) # Use relative path first

                    if image_path:
                        logging.info(f"Attempting to send image: {image_path}")
                        caption = f"*{escape_markdown(TARGET_DATE_STR)} 근태 상세 현황*"
                        if not send_telegram_photo(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, image_path, caption):
                             logging.error("Failed to send Telegram photo."); error_occurred = True
                             # Notify about send failure
                             send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 이미지 전송 실패*")
                        else: logging.info("Telegram photo sent.")
                    else: # Image creation failed
                        logging.error("Failed to create table image."); error_occurred = True
                        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 이미지 생성 실패*")
                elif df_for_image is None: logging.warning("Analysis error prevented DF creation for image.")
                else: logging.info("No data for image generation (empty DF).")

                # Clean up image file regardless of send success if path exists
                if image_path and Path(image_path).exists():
                    try: Path(image_path).unlink(); logging.info(f"Deleted image file: {image_path}")
                    except Exception as del_err: logging.warning(f"Could not delete image {image_path}: {del_err}")

                # 2. Send Detailed Report
                escaped_date_header = escape_markdown(TARGET_DATE_STR)
                report_lines = []; report_title = ""
                if is_evening:
                    report_title = f"🌙 *{escaped_date_header} 퇴근 근태 현황*"
                    logging.info("Generating evening status report.")
                    if detailed_statuses:
                        for idx, status in enumerate(detailed_statuses): report_lines.append(f"{idx + 1}\\. *{escape_markdown(status['name'])}*: {escape_markdown(status['in_status'])} \\| {escape_markdown(status['out_status'])}")
                    else: report_lines.append("_확인 대상 인원 없음_" if analysis_summary.get('target', 0) == 0 else "_처리된 데이터 없음_")
                else: # Morning
                    report_title = f"☀️ *{escaped_date_header} 출근 근태 확인 필요*"
                    logging.info("Generating morning issue report.")
                    if attendance_issues:
                        for idx, issue_msg in enumerate(attendance_issues): report_lines.append(f"{idx + 1}\\. {issue_msg}") # Already escaped
                    else: report_lines.append("_특이사항 없음_")

                if report_lines:
                    full_detailed_msg = f"{report_title}\n{escape_markdown('-'*20)}\n" + "\n".join(report_lines)
                    logging.info(f"Sending detailed report ('{report_title}')...")
                    if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, full_detailed_msg):
                        logging.error("Failed send detailed report."); error_occurred = True
                else: logging.warning("No content for detailed report.")

                # 3. Send Summary Report
                logging.info("Generating summary report..."); summary_title = ""; summary_details = ""
                total, target, excluded_count = analysis_summary.get("total_employees", 0), analysis_summary.get("target", 0), analysis_summary.get("excluded", 0)
                clock_in, miss_in = analysis_summary.get("clocked_in", 0), analysis_summary.get("missing_in", 0)
                clock_out, miss_out = analysis_summary.get("clocked_out", 0), analysis_summary.get("missing_out", 0)
                title_prefix = "🌙" if is_evening else "☀️"
                summary_title = f"{title_prefix} *{escaped_date_header} {'퇴근' if is_evening else '출근'} 현황 요약*"
                summary_lines = [f"\\- 전체 인원: {total}명", f"\\- 확인 대상: {target}명 \\(제외: {excluded_count}명\\)"]
                if is_evening:
                    summary_lines.extend([f"\\- 출근 기록: {clock_in}명 \\(미기록: {miss_in}명\\)", f"\\- 퇴근 기록: {clock_out}명 \\(미기록: {miss_out}명\\)"])
                else:
                    summary_lines.extend([f"\\- 출근 기록: {clock_in}명", f"\\- 출근 미기록: {miss_in}명"])
                summary_details = "\n".join(summary_lines)
                if excluded_employees:
                    excluded_items = "\n  ".join([f"\\- {item}" for item in excluded_employees]) # Items already escaped
                    summary_details += f"\n\n*제외 인원 ({excluded_count}명)*:\n  {excluded_items}"
                elif excluded_count > 0: summary_details += f"\n\n*제외 인원 ({excluded_count}명)*: _(상세 목록 없음)_"

                full_summary_msg = f"{summary_title}\n{escape_markdown('-'*20)}\n{summary_details}"
                logging.info("Sending summary report...")
                if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, full_summary_msg):
                    logging.error("Failed send summary report."); error_occurred = True

        except Exception as analysis_report_err:
            logging.error(f"Error during analysis or reporting phase: {analysis_report_err}", exc_info=True)
            error_occurred = True
            error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(분석/보고 단계\\)\n오류: {escape_markdown(str(analysis_report_err))}"
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)

    elif not excel_file_data and not error_occurred:
        logging.error("Excel data missing, but no initial error flagged. Download might have silently failed.")
        error_occurred = True
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류*\n엑셀 데이터 누락됨\\.")

    # Phase 3: Final Completion Message
    script_end_time = time.time(); time_taken = script_end_time - script_start_time
    logging.info(f"--- Script finished in {time_taken:.2f} seconds ---")
    completion_status = "오류 발생" if error_occurred else "정상 완료"; status_emoji = "❌" if error_occurred else "✅"
    escaped_final_date = escape_markdown(TARGET_DATE_STR); escaped_final_status = escape_markdown(completion_status); escaped_final_time = escape_markdown(f"{time_taken:.1f}")
    final_message = f"{status_emoji} *{escaped_final_date} 근태 확인 스크립트*: {escaped_final_status} \\(소요시간: {escaped_final_time}초\\)"

    if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, final_message):
        logging.error("Failed to send the final completion status message.")
    else:
        logging.info("Final completion status message sent.")

    exit_code = 1 if error_occurred else 0; logging.info(f"Exiting script with code: {exit_code}")
    exit(exit_code)
