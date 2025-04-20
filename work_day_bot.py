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
from webdriver_manager.chrome import ChromeDriverManager
import logging
import io
import traceback
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.table import Table
import matplotlib.font_manager as fm
import retrying
import json
import re
import os

# --- Configuration ---
# Read credentials from environment variables
WEBMAIL_USERNAME = os.environ.get("WEBMAIL_USERNAME")
WEBMAIL_PASSWORD = os.environ.get("WEBMAIL_PASSWORD")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# --- Other Settings ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

WEBMAIL_LOGIN_URL = "http://gw.ktmos.co.kr/mail2/loginPage.do"
WEBMAIL_ID_FIELD_ID = "userEmail"
WEBMAIL_PW_FIELD_ID = "userPw"

TARGET_DATE = datetime.date.today()
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")

REPORT_DOWNLOAD_URL_TEMPLATE = "http://gw.ktmos.co.kr/owattend/rest/dclz/report/CompositeStatus/sumr/user/days/excel?startDate={date}&endDate={date}&deptSeq=1231&erpNumDisplayYn=Y"
REPORT_URL = REPORT_DOWNLOAD_URL_TEMPLATE.format(date=TARGET_DATE_STR)

EXCEL_SHEET_NAME = "세부현황_B"
STANDARD_START_TIME_STR = "09:00:00"
STANDARD_END_TIME_STR = "18:00:00"
EVENING_RUN_THRESHOLD_HOUR = 18 # KST

# --- Credential Check (Moved after basic send function) ---
# ... (Keep this section as before) ...

# --- Constants for Leave Types ---
# ... (Keep this section as before) ...

# --- Helper Functions ---

def escape_markdown(text):
    # ... (Keep this function as before) ...
    if text is None: return ''
    text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def send_telegram_message_basic(bot_token, chat_id, text):
    # ... (Keep this function as before) ...
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'MarkdownV2'}
    try:
        response = requests.post(api_url, data=payload, timeout=30)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        if response.json().get("ok"):
             logging.info(f"Basic TG message sent successfully.")
             return True
        else:
             logging.error(f"Basic TG API Error: {response.json().get('description')}")
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
     error_message = f"!!! CRITICAL ERROR: Missing required environment variables: {', '.join(missing_secrets)} !!! Ensure they are set as GitHub Secrets and mapped correctly in the workflow file."
     logging.critical(error_message)
     if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
         send_telegram_message_basic(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, escape_markdown(error_message))
     exit(1) # Exit immediately


# --- Full Helper Functions ---

def setup_driver():
    logging.info("Setting up ChromeDriver...")
    options = webdriver.ChromeOptions()
    # Essential for GitHub Actions Linux runners:
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])

    # *** NEW: Set Page Load Strategy to 'eager' ***
    # normal: waits for the load event fire (default)
    # eager: waits for DOMContentLoaded event fire
    # none: returns immediately after initial HTML download
    options.page_load_strategy = PageLoadStrategy.eager
    logging.info(f"Set page load strategy to: {options.page_load_strategy}")

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        # *** NEW: Explicitly set timeouts ***
        # Page load timeout: How long to wait for driver.get() to complete
        driver.set_page_load_timeout(180) # Increased to 180 seconds
        # Implicit wait: Default time to wait for elements if not immediately found
        driver.implicitly_wait(20) # Increased slightly to 20 seconds
        # Script timeout: How long async JS can run
        driver.set_script_timeout(60)

        logging.info(f"Set timeouts: Page Load=180s, Implicit Wait=20s, Script=60s")
        logging.info("ChromeDriver setup complete (running headless).")
        return driver
    except WebDriverException as e:
        logging.error(f"WebDriver setup error: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"Unexpected ChromeDriver setup error: {e}", exc_info=True)
        raise

# Retrying logic remains useful for transient element visibility issues
@retrying.retry(stop_max_attempt_number=2, wait_fixed=3000, retry_on_exception=lambda e: isinstance(e, (TimeoutException, NoSuchElementException)))
def login_and_get_cookies(driver, url, username_id, password_id, username, password):
    logging.info(f"Attempting login to: {url}")
    try:
        # The driver.get() call now uses the 180s timeout set in setup_driver
        logging.info(f"Navigating to login page (timeout=180s)...")
        driver.get(url)
        logging.info(f"Navigation to {url} command issued. Current URL: {driver.current_url}") # Log after get returns

        # Wait explicitly for login fields, using the increased implicit wait (20s) or explicit wait
        wait = WebDriverWait(driver, 30) # Explicit wait up to 30 seconds for critical elements

        logging.info(f"Waiting for username field: {username_id}")
        user_field = wait.until(EC.visibility_of_element_located((By.ID, username_id)))
        logging.info(f"Waiting for password field: {password_id}")
        pw_field = wait.until(EC.visibility_of_element_located((By.ID, password_id)))

        logging.info("Entering credentials...")
        user_field.clear(); time.sleep(0.2); user_field.send_keys(username); time.sleep(0.5)
        pw_field.clear(); time.sleep(0.2); pw_field.send_keys(password); time.sleep(0.5)

        logging.info(f"Submitting login form by sending RETURN to PW field ({password_id})...")
        pw_field.send_keys(Keys.RETURN)

        # Wait for post-login indication (use a generous wait here too)
        post_login_locator = (By.XPATH, "//a[contains(@href, 'logout')] | //*[contains(text(),'로그아웃')] | //div[@id='main_container'] | //span[@class='username']")
        logging.info(f"Waiting up to 30s for login success indication (e.g., {post_login_locator})...")
        wait.until(EC.presence_of_element_located(post_login_locator))

        logging.info("Login appears successful.")
        time.sleep(2) # Allow things to settle
        logging.info("Extracting cookies...")
        cookies = {c['name']: c['value'] for c in driver.get_cookies()}
        if not cookies:
            logging.warning("No cookies extracted after login.")
            raise Exception("쿠키 추출 실패 (로그인 후 쿠키 없음)")
        logging.info(f"Extracted {len(cookies)} cookies.")
        return cookies

    # Catch the timeout specifically from driver.get() if possible (though it might manifest as timeout on wait later)
    except TimeoutException as e:
        current_url = "N/A"
        page_source_snippet = "N/A"
        try:
            current_url = driver.current_url
            page_source_snippet = driver.page_source[:1000]
        except Exception as inner_err:
             logging.warning(f"Could not get URL or page source after timeout: {inner_err}")

        logging.error(f"TimeoutException during login process. Error: {e}")
        logging.error(f"Current URL (if available): {current_url}")
        # Check if the timeout occurred during the initial navigation or later waits
        if "page load" in str(e).lower() or "timed out receiving message from renderer" in str(e).lower():
             logging.error("Timeout likely occurred during initial page load (driver.get). Target site might be down/slow/blocking.")
             raise Exception(f"로그인 페이지 로드 시간 초과 ({url}). 사이트 접속 불가 또는 차단 확인 필요.") from e
        else:
             logging.error(f"Timeout occurred waiting for element or page change after navigation.")
             logging.error(f"Page source snippet (if available):\n{page_source_snippet}")
             screenshot_path = "login_timeout_screenshot.png"
             try: driver.save_screenshot(screenshot_path); logging.info(f"Saved screenshot to {screenshot_path}")
             except Exception as ss_err: logging.warning(f"Failed to save screenshot: {ss_err}")
             raise Exception("로그인 실패: 페이지 요소 대기 시간 초과.") from e

    except Exception as e:
        logging.error(f"An unexpected error occurred during login: {e}", exc_info=True)
        try: driver.save_screenshot("login_error_screenshot.png"); logging.info("Saved error screenshot.")
        except Exception as ss_err: logging.warning(f"Failed to save error screenshot: {ss_err}")
        # Check if it's the ReadTimeoutError from the initial connection attempt
        if isinstance(e, WebDriverException) and "Read timed out" in str(e):
             raise Exception(f"로그인 페이지 접속 실패 (Read Timeout). 사이트 접속 불가 또는 네트워크 문제 확인 필요.") from e
        raise # Re-raise other unexpected exceptions


# --- download_excel_report, parse_time_robust, parse_date_robust, combine_date_time, find_korean_font, create_table_image, send_telegram_photo, send_telegram_message, analyze_attendance ---
# No changes needed in these functions for this specific error. Keep them as they were in the previous complete version.
# ... (Paste the rest of your helper functions and analyze_attendance function here) ...
@retrying.retry(stop_max_attempt_number=3, wait_fixed=10000, retry_on_exception=lambda e: isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)))
def download_excel_report(report_url, cookies):
    logging.info(f"Downloading report from: {report_url}")
    session = requests.Session()
    session.cookies.update(cookies)
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'Referer': WEBMAIL_LOGIN_URL.split('/mail2')[0], # Base domain as referer
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    try:
        response = session.get(report_url, headers=headers, stream=True, timeout=120) # Increased timeout
        logging.info(f"Download response status code: {response.status_code}")
        response.raise_for_status() # Raises HTTPError for 4xx/5xx responses
        content_type = response.headers.get('Content-Type', '').lower()
        logging.info(f"Response Content-Type: {content_type}")
        is_excel = any(mime in content_type for mime in ['excel', 'spreadsheetml', 'vnd.ms-excel', 'octet-stream'])
        content_disposition = response.headers.get('Content-Disposition', '')
        is_excel_disposition = '.xlsx' in content_disposition or '.xls' in content_disposition
        if is_excel or is_excel_disposition:
            excel_data = io.BytesIO(response.content)
            file_size = excel_data.getbuffer().nbytes
            logging.info(f"Excel download successful ({file_size} bytes).")
            if file_size < 2048: # Basic sanity check
                logging.warning(f"Downloaded file is very small ({file_size} bytes). Checking content...")
                try:
                    preview = excel_data.getvalue()[:500].decode('utf-8', errors='ignore')
                    if any(k in preview.lower() for k in ['login', '로그인', 'error', '오류', 'session', '세션', '권한', '<html>', '<head>']):
                        logging.error(f"Downloaded file content suggests an error or login page: {preview}")
                        raise Exception("다운로드된 파일이 엑셀이 아닌 오류 페이지일 수 있습니다.")
                    else:
                        logging.warning("Small file content doesn't immediately look like an error page.")
                except Exception as parse_err:
                    logging.warning(f"Could not decode or fully check small file content: {parse_err}.")
                finally:
                    excel_data.seek(0) # Rewind
            return excel_data
        else:
            logging.error(f"Downloaded content is not identified as Excel. Content-Type: '{content_type}', Disposition: '{content_disposition}'")
            try: preview = response.content[:500].decode('utf-8', errors='ignore'); logging.error(f"Content preview:\n{preview}")
            except Exception: logging.error("Could not decode content preview.")
            raise Exception("다운로드된 파일 형식이 엑셀이 아닙니다.")
    except requests.exceptions.Timeout:
        logging.error(f"Timeout downloading report from {report_url}")
        raise Exception(f"보고서 다운로드 시간 초과: {report_url}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error during download: {e.response.status_code} - {e.response.reason}")
        try: error_content = e.response.content[:500].decode('utf-8', errors='ignore'); logging.error(f"HTTP Error Response Content:\n{error_content}")
        except Exception: logging.error("Could not decode HTTP error response content.")
        raise Exception(f"보고서 다운로드 중 HTTP 오류 발생: {e.response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error during download: {e}")
        raise Exception(f"보고서 다운로드 중 네트워크 오류 발생: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during report download: {e}", exc_info=True)
        raise

def parse_time_robust(time_str):
    # ... (Keep this function as before) ...
    if pd.isna(time_str) or time_str == '': return None
    if isinstance(time_str, datetime.time): return time_str
    if isinstance(time_str, datetime.datetime): return time_str.time()
    time_str = str(time_str).strip()
    if not time_str: return None
    if isinstance(time_str, (float, int)):
        try:
            total_seconds = int(time_str * 24 * 60 * 60)
            total_seconds = min(total_seconds, 24 * 60 * 60 - 1)
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return datetime.time(hours, minutes, seconds)
        except (ValueError, TypeError):
            logging.warning(f"Could not parse numeric time value: '{time_str}'")
            return None
    if ' ' in time_str and ':' in time_str:
        try: return datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').time()
        except ValueError: pass
        try: return datetime.datetime.strptime(time_str, '%Y/%m/%d %H:%M:%S').time()
        except ValueError: pass
    for fmt in ('%H:%M:%S', '%H:%M', '%I:%M:%S %p', '%I:%M %p'):
        try: return datetime.datetime.strptime(time_str, fmt).time()
        except ValueError: continue
    time_str_numeric = ''.join(filter(str.isdigit, time_str))
    if len(time_str_numeric) == 4: # HHMM
        try: return datetime.datetime.strptime(time_str_numeric, '%H%M').time()
        except ValueError: pass
    elif len(time_str_numeric) == 6: # HHMMSS
        try: return datetime.datetime.strptime(time_str_numeric, '%H%M%S').time()
        except ValueError: pass
    if not re.match(r'^[\d:\sAMP]+$', time_str, re.IGNORECASE):
        logging.debug(f"Ignoring likely non-time string: '{time_str}'")
        return None
    else:
        logging.warning(f"Could not parse time string with known formats: '{time_str}'")
        return None

def parse_date_robust(date_str):
    # ... (Keep this function as before) ...
    if pd.isna(date_str): return None
    if isinstance(date_str, datetime.date): return date_str
    if isinstance(date_str, datetime.datetime): return date_str.date()
    date_str = str(date_str).strip()
    if not date_str: return None
    try:
        date_part = date_str.split(' ')[0]
        for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%Y%m%d', '%m/%d/%Y'):
            try: return datetime.datetime.strptime(date_part, fmt).date()
            except ValueError: continue
        logging.warning(f"Could not parse date string with known formats: '{date_str}'")
        return None
    except Exception as e:
        logging.warning(f"Error parsing date string '{date_str}': {e}")
        return None

def combine_date_time(date_val, time_val):
    # ... (Keep this function as before) ...
    if isinstance(date_val, datetime.date) and isinstance(time_val, datetime.time):
        return datetime.datetime.combine(date_val, time_val)
    return None

def find_korean_font():
    # ... (Keep this function as before) ...
    common_font_files = ["NanumGothic.ttf", "malgun.ttf", "AppleGothic.ttf", "gulim.ttc", "NanumBarunGothic.ttf"]
    linux_font_paths = [ "/usr/share/fonts/truetype/nanum/", "/usr/share/fonts/opentype/nanum/", "/usr/share/fonts/truetype/google/", os.path.expanduser("~/.fonts/") ]
    for path in linux_font_paths:
        try:
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    if filename in common_font_files:
                        found_path = os.path.join(path, filename)
                        logging.info(f"Found Korean font in specific path: {found_path}")
                        return found_path
        except OSError: continue
    logging.info("Korean font not found in common Linux paths, searching with font_manager...")
    try:
        system_fonts = fm.findSystemFonts(fontpaths=None, fontext='ttf')
        for f in system_fonts:
            font_name = Path(f).name
            if any(common_name in font_name for common_name in common_font_files):
                logging.info(f"Found potential Korean font via font_manager: {f}")
                try: fm.FontProperties(fname=f); logging.info(f"Successfully loaded font properties for {f}. Using this font."); return f
                except Exception as load_err: logging.warning(f"Found font {f} but failed to load properties: {load_err}. Skipping."); continue
    except Exception as e: logging.warning(f"Error searching system fonts with font_manager: {e}")
    logging.warning("Korean font not found after checking common paths and system search.")
    return None

def create_table_image(df, title, output_path="table_image.png"):
    # ... (Keep this function as before) ...
    logging.info("Attempting to create table image...")
    if df.empty: logging.warning("DataFrame is empty, cannot generate table image."); return None
    plt.switch_backend('Agg')
    try:
        font_path = find_korean_font()
        if font_path:
            try:
                fm._load_fontmanager(try_read_cache=False)
                prop = fm.FontProperties(fname=font_path, size=10)
                plt.rcParams['font.family'] = prop.get_name()
                plt.rcParams['axes.unicode_minus'] = False
                logging.info(f"Successfully set font: {font_path} using family name {prop.get_name()}")
            except Exception as font_prop_err:
                 logging.error(f"Failed to set font properties for {font_path}: {font_prop_err}", exc_info=True)
                 logging.warning("Proceeding without specific Korean font.")
                 plt.rcParams['font.family'] = 'sans-serif'
        else:
            logging.warning("Korean font not found. Table image might have broken characters.")
            plt.rcParams['font.family'] = 'sans-serif'
    except Exception as e:
        logging.error(f"Error during font setup: {e}.", exc_info=True)
        plt.rcParams['font.family'] = 'sans-serif'
    nr, nc = df.shape
    base_w, incr_w = 6, 0.8; base_h, incr_h = 2, 0.3; max_w, max_h = 25, 40
    fw = min(max(base_w, base_w + nc * incr_w), max_w)
    fh = min(max(base_h, base_h + nr * incr_h), max_h)
    logging.info(f"Table dimensions: {nr} rows, {nc} columns. Calculated Figure size: ({fw:.1f}, {fh:.1f})")
    fig, ax = plt.subplots(figsize=(fw, fh)); ax.axis('off')
    try:
        tab = Table(ax, bbox=[0, 0, 1, 1])
        for j, col in enumerate(df.columns): tab.add_cell(0, j, 1, 1, text=str(col), loc='center', facecolor='lightgray', width=1.0/nc if nc > 0 else 1)
        for i in range(nr):
            for j in range(nc):
                txt = str(df.iloc[i, j]); max_len = 40
                if len(txt) > max_len: txt = txt[:max_len - 3] + '...'
                cell_color = 'white'
                tab.add_cell(i + 1, j, 1, 1, text=txt, loc='center', facecolor=cell_color, width=1.0/nc if nc > 0 else 1)
        tab.auto_set_font_size(False); tab.set_fontsize(9); ax.add_table(tab)
        plt.title(title, fontsize=12, pad=15); plt.tight_layout(pad=1.0)
        plt.savefig(output_path, bbox_inches='tight', dpi=120); plt.close(fig)
        logging.info(f"Table image saved successfully: {output_path}")
        size_bytes = Path(output_path).stat().st_size; size_mb = size_bytes / (1024 * 1024)
        logging.info(f"Image file size: {size_mb:.2f} MB")
        if size_mb > 9.5: logging.warning(f"Generated image size ({size_mb:.2f} MB) might exceed Telegram's limit.")
        elif size_bytes == 0: logging.error("Generated image file size is 0 bytes."); return None
        return output_path
    except Exception as e:
        logging.error(f"Failed to create or save table image: {e}", exc_info=True)
        plt.close(fig); return None

@retrying.retry(stop_max_attempt_number=3, wait_fixed=5000, retry_on_exception=lambda e: isinstance(e, requests.exceptions.RequestException))
def send_telegram_photo(bot_token, chat_id, photo_path, caption):
    # ... (Keep this function as before) ...
    api_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    if not Path(photo_path).exists(): logging.error(f"Cannot send photo, file not found: {photo_path}"); return False
    if Path(photo_path).stat().st_size == 0: logging.error(f"Cannot send photo, file size is 0 bytes: {photo_path}"); return False
    try:
        with open(photo_path, 'rb') as photo:
            max_caption_len = 1024
            if len(caption) > max_caption_len: logging.warning(f"Caption length ({len(caption)}) exceeds limit ({max_caption_len}). Truncating."); caption = caption[:max_caption_len - 3] + "..."
            files = {'photo': (Path(photo_path).name, photo)}; payload = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'MarkdownV2'}
            response = requests.post(api_url, data=payload, files=files, timeout=60)
            rd = {};
            try: rd = response.json()
            except json.JSONDecodeError: logging.error(f"Failed to decode JSON response from Telegram API. Status: {response.status_code}, Content: {response.text[:500]}"); response.raise_for_status()
            if response.status_code == 200 and rd.get("ok"): logging.info("Telegram photo sent successfully."); return True
            else:
                err_desc = rd.get('description', 'N/A'); err_code = rd.get('error_code', 'N/A')
                logging.error(f"Telegram API Error (sendPhoto): {err_desc} (Code: {err_code})"); logging.error(f"Payload sent (excluding file): {payload}")
                if 400 <= response.status_code < 500: raise requests.exceptions.HTTPError(f"Telegram Client Error {response.status_code}: {err_desc}", response=response)
                else: response.raise_for_status()
                return False
    except requests.exceptions.HTTPError as e:
         if 400 <= e.response.status_code < 500:
              logging.error(f"HTTP Client Error sending photo (will not retry): {e}", exc_info=True)
              error_text = f"*{escape_markdown(TARGET_DATE_STR)} 이미지 전송 실패*\n텔레그램 API 오류 \\(HTTP {e.response.status_code}\\): {escape_markdown(e.response.json().get('description','N/A'))}"
              send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_text)
         else: logging.error(f"HTTP Server/Network Error sending photo: {e}"); raise
         return False
    except requests.exceptions.RequestException as e: logging.error(f"Network error sending photo: {e}. Retrying allowed."); raise
    except FileNotFoundError: logging.error(f"File not found error during photo sending: {photo_path}"); return False
    except Exception as e: logging.error(f"Unexpected error sending Telegram photo: {e}", exc_info=True); raise Exception(f"Unexpected photo send error: {e}")

def send_telegram_message(bot_token, chat_id, text):
    # ... (Keep this function as before) ...
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"; max_len = 4096; messages_to_send = []
    if not text: logging.warning("Attempted to send an empty message."); return True
    if len(text) > max_len:
        logging.info(f"Message length ({len(text)}) exceeds {max_len}, splitting..."); start = 0
        while start < len(text):
            end = text.rfind('\n', start, start + max_len)
            if end == -1 or end <= start: end = start + max_len
            chunk = text[start:end].strip();
            if chunk: messages_to_send.append(chunk)
            start = end
    else: messages_to_send.append(text)
    logging.info(f"Sending {len(messages_to_send)} message part(s) to Telegram."); all_parts_sent_successfully = True
    for i, part in enumerate(messages_to_send):
        if not part: logging.warning(f"Skipping empty message part {i+1}."); continue
        payload = {'chat_id': chat_id, 'text': part, 'parse_mode': 'MarkdownV2'}; part_sent = False; attempt = 0; max_attempts = 2
        while not part_sent and attempt < max_attempts:
            attempt += 1; mode = payload.get('parse_mode', 'Plain'); logging.info(f"Sending part {i+1}/{len(messages_to_send)} using mode: {mode} (Attempt {attempt})")
            try:
                response = requests.post(api_url, data=payload, timeout=30); rd = response.json()
                if response.status_code == 200 and rd.get("ok"): logging.info(f"Telegram message part {i+1} sent successfully using {mode}."); part_sent = True
                else:
                    err_desc = rd.get('description', 'N/A'); err_code = rd.get('error_code', 'N/A')
                    logging.error(f"Telegram API Error (sendMessage Part {i+1}, Mode: {mode}): {err_desc} (Code: {err_code})"); logging.error(f"Failed content preview (first 500 chars): {part[:500]}")
                    if mode == 'MarkdownV2' and attempt < max_attempts: logging.warning("MarkdownV2 failed, retrying as plain text."); payload['parse_mode'] = None; payload['text'] = part
                    else: all_parts_sent_successfully = False; break
            except requests.exceptions.Timeout: logging.error(f"Timeout sending Telegram message part {i+1} (Mode: {mode})."); time.sleep(5); if attempt == max_attempts: all_parts_sent_successfully = False
            except requests.exceptions.RequestException as e: logging.error(f"Network error sending Telegram message part {i+1} (Mode: {mode}): {e}"); time.sleep(5); if attempt == max_attempts: all_parts_sent_successfully = False
            except json.JSONDecodeError: logging.error(f"Failed to decode JSON response from Telegram API sending part {i+1}. Status: {response.status_code}, Content: {response.text[:500]}"); time.sleep(5); if attempt == max_attempts: all_parts_sent_successfully = False
            except Exception as e: logging.error(f"Unexpected error sending Telegram message part {i+1} (Mode: {mode}): {e}", exc_info=True); if attempt == max_attempts: all_parts_sent_successfully = False; break
        if not part_sent: all_parts_sent_successfully = False
    return all_parts_sent_successfully

def analyze_attendance(excel_data, sheet_name):
    # ... (Keep this function as before - contains detailed logic) ...
    logging.info(f"Analyzing attendance data from sheet: '{sheet_name}'.")
    analysis_result = { "notifications": [], "detailed_status": [], "summary": { "total_employees": 0, "target": 0, "excluded": 0, "clocked_in": 0, "missing_in": 0, "clocked_out": 0, "missing_out": 0 }, "excluded_employees": [], "df_processed": None }
    processed_data_for_image = []
    try:
        df = pd.read_excel(excel_data, sheet_name=sheet_name, skiprows=2, dtype=str, keep_default_na=False)
        logging.info(f"Loaded {len(df)} rows from sheet '{sheet_name}'.")
        if df.empty: logging.warning(f"Excel sheet '{sheet_name}' is empty."); return analysis_result
        df.columns = [str(col).strip() for col in df.columns]; logging.info(f"Cleaned columns: {df.columns.tolist()}")
        actual_to_desired_mapping = { '서무원': '이름', '출퇴근': '유형', '정상': '구분', 'Unnamed: 11': '출근시간_raw', 'Unnamed: 13': '퇴근시간_raw', 'Unnamed: 16': '휴가시작시간_raw', 'Unnamed: 18': '휴가종료시간_raw' }
        date_col_actual_name = None
        for col in df.columns:
            if re.match(r'^\d{4}-\d{2}-\d{2}$', str(col).strip()): date_col_actual_name = col; logging.info(f"Dynamically identified date column: '{date_col_actual_name}'"); break
        if not date_col_actual_name:
            if TARGET_DATE_STR in df.columns: date_col_actual_name = TARGET_DATE_STR; logging.warning(f"Using fallback date column name: '{date_col_actual_name}'")
            else:
                 potential_date_col_index = 5
                 if len(df.columns) > potential_date_col_index:
                     guessed_col = df.columns[potential_date_col_index]
                     if parse_date_robust(guessed_col): date_col_actual_name = guessed_col; logging.warning(f"Using highly speculative date column at index {potential_date_col_index}: '{date_col_actual_name}'")
                     else: logging.error("FATAL: Cannot find date column. Columns: %s", df.columns.tolist()); raise KeyError("엑셀 보고서에서 날짜 컬럼을 찾을 수 없습니다.")
                 else: logging.error("FATAL: Cannot find date column (not enough cols). Columns: %s", df.columns.tolist()); raise KeyError("엑셀 보고서에서 날짜 컬럼을 찾을 수 없습니다 (컬럼 부족).")
        actual_to_desired_mapping[date_col_actual_name] = '일자'
        required_source_cols = list(actual_to_desired_mapping.keys()); missing_source_cols = [c for c in required_source_cols if c not in df.columns]
        if missing_source_cols: logging.error(f"FATAL: Missing source columns: {missing_source_cols}"); raise KeyError(f"필수 원본 컬럼 누락: {', '.join(missing_source_cols)}")
        df_processed = df[required_source_cols].copy(); df_processed.rename(columns=actual_to_desired_mapping, inplace=True)
        logging.info(f"Columns after rename: {df_processed.columns.tolist()}")
        df_processed['일자_dt'] = df_processed['일자'].apply(parse_date_robust)
        df_processed['출근시간_dt'] = df_processed['출근시간_raw'].apply(parse_time_robust)
        df_processed['퇴근시간_dt'] = df_processed['퇴근시간_raw'].apply(parse_time_robust)
        df_processed['휴가시작시간_dt'] = df_processed['휴가시작시간_raw'].apply(parse_time_robust)
        df_processed['휴가종료시간_dt'] = df_processed['휴가종료시간_raw'].apply(parse_time_robust)
        logging.info(f"Parsed Dates: {df_processed['일자_dt'].notna().sum()}. Parsed Times - In: {df_processed['출근시간_dt'].notna().sum()}, Out: {df_processed['퇴근시간_dt'].notna().sum()}, LeaveStart: {df_processed['휴가시작시간_dt'].notna().sum()}, LeaveEnd: {df_processed['휴가종료시간_dt'].notna().sum()}")
        df_filtered = df_processed[df_processed['일자_dt'] == TARGET_DATE].copy()
        if df_filtered.empty: logging.warning(f"No data found for target date {TARGET_DATE_STR}."); return analysis_result
        logging.info(f"Found {len(df_filtered)} rows for target date {TARGET_DATE_STR}.")
        try:
            standard_start_time = datetime.datetime.strptime(STANDARD_START_TIME_STR, '%H:%M:%S').time(); standard_end_time = datetime.datetime.strptime(STANDARD_END_TIME_STR, '%H:%M:%S').time()
            standard_start_dt = datetime.datetime.combine(TARGET_DATE, standard_start_time); standard_end_dt = datetime.datetime.combine(TARGET_DATE, standard_end_time)
            lunch_start_time = datetime.time(12, 0, 0); lunch_end_time = datetime.time(13, 0, 0); afternoon_start_time = lunch_end_time
        except ValueError as time_parse_err: logging.error(f"FATAL: Could not parse standard time strings: {time_parse_err}"); raise ValueError("표준 근무 시간 또는 점심 시간 형식이 잘못되었습니다.")
        grouped = df_filtered.groupby('이름'); analysis_result["summary"]["total_employees"] = len(grouped)
        logging.info(f"Processing {len(grouped)} unique employees for date {TARGET_DATE_STR}.")
        for name_raw, group_df in grouped:
            name_trimmed = str(name_raw).strip();
            if not name_trimmed: logging.warning("Skipping empty name."); continue
            name_escaped = escape_markdown(name_trimmed); logging.debug(f"--- Processing employee: {name_trimmed} ---")
            is_fully_excluded = False; exclusion_reason_formatted = ""; collected_leaves = []; attendance_row_data = None
            for _, row in group_df.iterrows():
                leave_type = str(row.get('유형', '')).strip(); leave_category = str(row.get('구분', '')).strip()
                leave_start_time_dt = row['휴가시작시간_dt']; leave_end_time_dt = row['휴가종료시간_dt']
                is_leave_row = leave_type in FULL_DAY_LEAVE_TYPES or leave_category in FULL_DAY_LEAVE_REASONS or leave_category in [MORNING_HALF_LEAVE, AFTERNOON_HALF_LEAVE]
                if is_leave_row:
                    current_leave_name = leave_category if leave_category else leave_type
                    if current_leave_name:
                        collected_leaves.append({'type': current_leave_name, 'start': leave_start_time_dt, 'end': leave_end_time_dt})
                        logging.debug(f"{name_trimmed}: Recorded leave - Type='{current_leave_name}', Start={leave_start_time_dt}, End={leave_end_time_dt}")
                        if leave_start_time_dt and leave_end_time_dt:
                            if leave_start_time_dt <= standard_start_time and leave_end_time_dt >= standard_end_time:
                                logging.info(f"{name_trimmed}: Excluded by single leave: {current_leave_name} ({leave_start_time_dt.strftime('%H:%M')} - {leave_end_time_dt.strftime('%H:%M')})")
                                is_fully_excluded = True; exclusion_reason_formatted = f"{name_trimmed}: {current_leave_name} ({leave_start_time_dt.strftime('%H:%M')} \\- {leave_end_time_dt.strftime('%H:%M')})"; break
                elif leave_type == ATTENDANCE_TYPE:
                    attendance_row_data = {'출근시간_dt': row['출근시간_dt'], '퇴근시간_dt': row['퇴근시간_dt'], '출근시간_raw': str(row['출근시간_raw']).strip(), '퇴근시간_raw': str(row['퇴근시간_raw']).strip()}
                    logging.debug(f"{name_trimmed}: Found attendance data - In={row['출근시간_dt']}, Out={row['퇴근시간_dt']}")
            if not is_fully_excluded and collected_leaves:
                logging.debug(f"{name_trimmed}: Checking combined leaves ({len(collected_leaves)} entries).")
                covers_morning = False; covers_afternoon = False; overall_min_start = standard_end_time; overall_max_end = standard_start_time
                for leave in collected_leaves:
                    ls = leave['start']; le = leave['end']; lt = leave['type']
                    if ls and (overall_min_start is None or ls < overall_min_start): overall_min_start = ls
                    if le and (overall_max_end is None or le > overall_max_end): overall_max_end = le
                    if lt == MORNING_HALF_LEAVE or (ls and le and ls <= standard_start_time and le >= lunch_start_time): covers_morning = True
                    if lt == AFTERNOON_HALF_LEAVE or (ls and le and ls <= lunch_end_time and le >= standard_end_time): covers_afternoon = True
                    logging.debug(f"{name_trimmed}: Leave '{lt}' ({ls}-{le}) -> Morning={covers_morning}, Afternoon={covers_afternoon}")
                if covers_morning and covers_afternoon:
                     logging.info(f"{name_trimmed}: Excluded by COMBINED leaves."); is_fully_excluded = True
                     combined_types = " + ".join(sorted(list(set(l['type'] for l in collected_leaves if l['type']))))
                     time_range_str = f"{overall_min_start.strftime('%H:%M') if overall_min_start else '?'} \\- {overall_max_end.strftime('%H:%M') if overall_max_end else '?'}"
                     exclusion_reason_formatted = f"{name_trimmed}: {combined_types} ({time_range_str})"
            if is_fully_excluded:
                analysis_result["excluded_employees"].append(escape_markdown(exclusion_reason_formatted)); analysis_result["summary"]["excluded"] += 1
                processed_data_for_image.append({'이름': name_trimmed, '일자': TARGET_DATE_STR, '유형': '휴가/제외', '구분': escape_markdown(exclusion_reason_formatted.split(': ')[-1]), '출근시간': '-', '퇴근시간': '-'})
                logging.debug(f"{name_trimmed}: Final status - Excluded."); continue
            analysis_result["summary"]["target"] += 1; logging.debug(f"{name_trimmed}: Final status - Target.")
            clock_in_dt, clock_out_dt = None, None; clock_in_raw, clock_out_raw = '', ''
            if attendance_row_data: clock_in_dt = attendance_row_data['출근시간_dt']; clock_out_dt = attendance_row_data['퇴근시간_dt']; clock_in_raw = attendance_row_data['출근시간_raw']; clock_out_raw = attendance_row_data['퇴근시간_raw']
            actual_start_dt = combine_date_time(TARGET_DATE, clock_in_dt) if clock_in_dt else None; actual_end_dt = combine_date_time(TARGET_DATE, clock_out_dt) if clock_out_dt else None
            has_clock_in = actual_start_dt is not None; has_clock_out = actual_end_dt is not None
            current_has_morning_leave = False; current_has_afternoon_leave = False; leave_display_type = '출퇴근'; leave_display_category = '정상'
            if collected_leaves:
                 morning_leave_types = []; afternoon_leave_types = []
                 for leave in collected_leaves:
                     ls = leave['start']; le = leave['end']; lt = leave['type']
                     if lt == MORNING_HALF_LEAVE or (ls and le and ls <= standard_start_time and le >= lunch_start_time): current_has_morning_leave = True; morning_leave_types.append(lt)
                     if lt == AFTERNOON_HALF_LEAVE or (ls and le and ls <= lunch_end_time and le >= standard_end_time): current_has_afternoon_leave = True; afternoon_leave_types.append(lt)
                 if current_has_morning_leave and not current_has_afternoon_leave: leave_display_type = MORNING_HALF_LEAVE; leave_display_category = " + ".join(sorted(list(set(morning_leave_types)))) or '오전반차'
                 elif not current_has_morning_leave and current_has_afternoon_leave: leave_display_type = AFTERNOON_HALF_LEAVE; leave_display_category = " + ".join(sorted(list(set(afternoon_leave_types)))) or '오후반차'
            expected_start_dt = datetime.datetime.combine(TARGET_DATE, afternoon_start_time) if current_has_morning_leave else standard_start_dt
            expected_end_dt = datetime.datetime.combine(TARGET_DATE, lunch_start_time) if current_has_afternoon_leave else standard_end_dt
            issues = []
            if has_clock_in:
                analysis_result["summary"]["clocked_in"] += 1; logging.debug(f"{name_trimmed}: IN at {clock_in_dt.strftime('%H:%M:%S')}. Expected >= {expected_start_dt.strftime('%H:%M:%S')}.")
                if not current_has_morning_leave and actual_start_dt > standard_start_dt: late_duration = actual_start_dt - standard_start_dt; issues.append(f"지각: {escape_markdown(clock_in_dt.strftime('%H:%M:%S'))} \\({late_duration}\\)")
                elif current_has_morning_leave and actual_start_dt > expected_start_dt: late_duration = actual_start_dt - expected_start_dt; issues.append(f"오전반차 후 지각: {escape_markdown(clock_in_dt.strftime('%H:%M:%S'))} \\(예상 {expected_start_dt.strftime('%H:%M')}, {late_duration} 늦음\\)")
            else:
                analysis_result["summary"]["missing_in"] += 1
                if not current_has_morning_leave: issues.append("출근 기록 없음"); logging.debug(f"{name_trimmed}: Missing IN (expected by {expected_start_dt.strftime('%H:%M:%S')}).")
            if has_clock_out:
                analysis_result["summary"]["clocked_out"] += 1; logging.debug(f"{name_trimmed}: OUT at {clock_out_dt.strftime('%H:%M:%S')}. Expected <= {expected_end_dt.strftime('%H:%M:%S')}.")
                if not current_has_afternoon_leave and actual_end_dt < standard_end_dt: early_duration = standard_end_dt - actual_end_dt; issues.append(f"조퇴: {escape_markdown(clock_out_dt.strftime('%H:%M:%S'))} \\({early_duration} 일찍\\)")
                elif current_has_afternoon_leave and actual_end_dt < expected_end_dt: early_duration = expected_end_dt - actual_end_dt; issues.append(f"오후반차 전 조퇴: {escape_markdown(clock_out_dt.strftime('%H:%M:%S'))} \\(예상 {expected_end_dt.strftime('%H:%M')}, {early_duration} 일찍\\)")
            elif has_clock_in:
                 analysis_result["summary"]["missing_out"] += 1
                 if not current_has_afternoon_leave: issues.append("퇴근 기록 없음"); logging.debug(f"{name_trimmed}: Missing OUT (expected by {expected_end_dt.strftime('%H:%M:%S')}).")
            if issues: issue_string = ", ".join(issues); msg = f"*{name_escaped}*: {issue_string}"; analysis_result["notifications"].append(msg); logging.info(f"{name_trimmed}: Issues detected - {issue_string}")
            clock_in_status_str = clock_in_dt.strftime('%H:%M:%S') if has_clock_in else "기록없음"
            clock_out_status_str = clock_out_dt.strftime('%H:%M:%S') if has_clock_out else ("기록없음" if has_clock_in else "출근기록없음")
            analysis_result["detailed_status"].append({'name': name_trimmed, 'in_status': clock_in_status_str, 'out_status': clock_out_status_str})
            processed_data_for_image.append({'이름': name_trimmed, '일자': TARGET_DATE_STR, '유형': leave_display_type, '구분': leave_display_category, '출근시간': clock_in_raw if clock_in_raw else ('-' if has_clock_in else '기록없음'), '퇴근시간': clock_out_raw if clock_out_raw else ('-' if has_clock_out else '기록없음')})
        if processed_data_for_image:
             image_df_cols = ['이름', '일자', '유형', '구분', '출근시간', '퇴근시간']
             analysis_result["df_processed"] = pd.DataFrame(processed_data_for_image, columns=image_df_cols)
             logging.info(f"Created final DataFrame for image generation with {len(analysis_result['df_processed'])} rows.")
        else:
             logging.warning("No data processed for image DataFrame."); analysis_result["df_processed"] = pd.DataFrame(columns=['이름', '일자', '유형', '구분', '출근시간', '퇴근시간'])
        summary = analysis_result["summary"]; calc_total = summary["target"] + summary["excluded"]
        if calc_total != summary["total_employees"]: logging.warning(f"Summary count mismatch! Total Parsed ({summary['total_employees']}) != Target ({summary['target']}) + Excluded ({summary['excluded']}). Calc={calc_total}")
        target_check = summary["clocked_in"] + summary["missing_in"]
        if summary["target"] != target_check: logging.warning(f"Target count mismatch! Target ({summary['target']}) != Clocked In ({summary['clocked_in']}) + Missing In ({summary['missing_in']}). Sum={target_check}")
        else: logging.debug("Target count validation OK.")
        out_check = summary["clocked_out"] + summary["missing_out"]
        if summary["clocked_in"] != out_check: logging.warning(f"Clocked-In count mismatch! Clocked In ({summary['clocked_in']}) != Clocked Out ({summary['clocked_out']}) + Missing Out ({summary['missing_out']}). Sum={out_check}")
        else: logging.debug("Clocked-In count validation OK.")
        logging.info(f"Attendance analysis complete. Summary: {analysis_result['summary']}")
        return analysis_result
    except KeyError as e:
        logging.error(f"Analysis failed due to KeyError: {e}.", exc_info=True)
        error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n엑셀 컬럼 오류 \\({escape_markdown(str(e))}\\)\\. 구조 확인 필요\\."
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg); analysis_result["summary"]["total_employees"] = -1; return analysis_result
    except ValueError as e:
         logging.error(f"Analysis failed due to ValueError: {e}.", exc_info=True)
         error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n데이터 값 오류: {escape_markdown(str(e))}\\."; send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg); analysis_result["summary"]["total_employees"] = -1; return analysis_result
    except Exception as e:
        logging.error(f"Unexpected error during analysis: {e}", exc_info=True)
        error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 분석 중 예외 발생*\n오류: {escape_markdown(str(e))}"; send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg); analysis_result["summary"]["total_employees"] = -1; return analysis_result


# --- Main Execution Logic ---
if __name__ == "__main__":
    script_start_time = time.time()
    logging.info(f"--- Attendance Bot Script started for date: {TARGET_DATE_STR} ---")
    driver = None
    excel_file_data = None
    error_occurred = False
    analysis_result = {}

    # Phase 1: Setup, Login, Download
    try:
        driver = setup_driver()
        cookies = login_and_get_cookies(driver, WEBMAIL_LOGIN_URL, WEBMAIL_ID_FIELD_ID, WEBMAIL_PW_FIELD_ID, WEBMAIL_USERNAME, WEBMAIL_PASSWORD)
        excel_file_data = download_excel_report(REPORT_URL, cookies)
        logging.info("Successfully logged in and downloaded the Excel report.")
    except Exception as setup_err:
        # Catch exceptions from setup, login, or download
        logging.error(f"Critical error during setup/login/download phase: {setup_err}", exc_info=True)
        error_occurred = True
        error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 초기화 오류*\n단계: 설정/로그인/다운로드\n오류: {escape_markdown(str(setup_err))}"
        # Use basic send in case full function isn't working yet, though it should be
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)
    finally:
        if driver:
            try: driver.quit(); logging.info("WebDriver closed successfully.")
            except Exception as e: logging.error(f"Error closing WebDriver: {e}", exc_info=True) # Log any close error

    # Phase 2: Analysis and Reporting
    if excel_file_data and not error_occurred:
        logging.info("Proceeding with analysis and reporting...")
        try:
            analysis_result = analyze_attendance(excel_file_data, EXCEL_SHEET_NAME)
            if not isinstance(analysis_result, dict) or analysis_result.get("summary", {}).get("total_employees", -1) == -1:
                logging.error("Analysis function indicated failure. Reporting may be skipped or incomplete.")
                error_occurred = True # Mark script as failed overall
            else:
                # --- Proceed with reporting using analysis_result ---
                # ... (Reporting logic: image, detailed report, summary report - KEEP AS IS) ...
                logging.info("Analysis successful. Preparing reports...")
                now_local_dt = datetime.datetime.now(); is_evening = now_local_dt.hour >= EVENING_RUN_THRESHOLD_HOUR
                logging.info(f"Current hour {now_local_dt.hour}, Evening run? {is_evening}")
                attendance_issues = analysis_result.get("notifications", []); detailed_statuses = analysis_result.get("detailed_status", [])
                analysis_summary = analysis_result.get("summary", {}); excluded_employees = analysis_result.get("excluded_employees", [])
                df_for_image = analysis_result.get("df_processed")

                # --- 1. Send Table Image ---
                if df_for_image is not None and not df_for_image.empty:
                    image_title = f"{TARGET_DATE_STR} 근태 현황 ({analysis_summary.get('target', 0)}명 확인, {analysis_summary.get('excluded', 0)}명 제외)"
                    image_filename = f"attendance_report_{TARGET_DATE_STR}.png"; image_path = create_table_image(df_for_image, image_title, image_filename)
                    if image_path:
                        logging.info(f"Attempting to send image: {image_path}"); caption = f"*{escape_markdown(TARGET_DATE_STR)} 근태 상세 현황*"
                        photo_sent = send_telegram_photo(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, image_path, caption)
                        if not photo_sent:
                             logging.error("Failed to send Telegram photo."); img_fail_msg = f"*{escape_markdown(TARGET_DATE_STR)} 이미지 전송 실패*\\n텔레그램 전송 오류 발생\\."; send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, img_fail_msg); error_occurred = True
                        else: logging.info("Telegram photo sent successfully.")
                        try: Path(image_path).unlink(missing_ok=True); logging.info(f"Deleted image file: {image_path}")
                        except Exception as del_err: logging.warning(f"Could not delete image file {image_path}: {del_err}")
                    else:
                        logging.error("Failed to create table image."); img_fail_msg = f"*{escape_markdown(TARGET_DATE_STR)} 이미지 생성 실패*\\n표 이미지 생성 불가\\."; send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, img_fail_msg); error_occurred = True
                elif df_for_image is None: logging.warning("Analysis did not produce DataFrame for image.")
                else: logging.info("DataFrame for image is empty. Skipping image.")

                # --- 2. Send Detailed Report ---
                escaped_date_header = escape_markdown(TARGET_DATE_STR); report_lines = []; report_title = ""
                if is_evening:
                    report_title = f"🌙 *{escaped_date_header} 퇴근 근태 현황*"; logging.info("Generating evening detailed status report.")
                    if detailed_statuses:
                        for idx, status in enumerate(detailed_statuses): line = f"{idx + 1}\\. *{escape_markdown(status['name'])}*: {escape_markdown(status['in_status'])} \\| {escape_markdown(status['out_status'])}"; report_lines.append(line)
                        logging.info(f"Prepared {len(report_lines)} evening status entries.")
                    else: logging.info("No targets for evening status."); report_lines.append("_확인 대상 인원 없음_")
                else: # Morning
                    report_title = f"☀️ *{escaped_date_header} 출근 근태 확인 필요*"; logging.info("Generating morning issue report.")
                    if attendance_issues:
                        for idx, issue_msg in enumerate(attendance_issues): report_lines.append(f"{idx + 1}\\. {issue_msg}")
                        logging.info(f"Prepared {len(report_lines)} morning issue entries.")
                    else: logging.info("No morning issues."); report_lines.append("_특이사항 없음_")
                if report_lines:
                    msg_header = f"{report_title}\n{escape_markdown('-'*20)}\n"; msg_body = "\n".join(report_lines); full_detailed_msg = msg_header + msg_body
                    logging.info(f"Sending detailed report ('{report_title}')...")
                    if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, full_detailed_msg): logging.error("Failed send detailed report."); error_occurred = True
                else: logging.warning("No content for detailed report.")

                # --- 3. Send Summary Report ---
                logging.info("Generating summary report..."); summary_title = ""; summary_details = ""
                total = analysis_summary.get("total_employees", 0); target = analysis_summary.get("target", 0); excluded_count = analysis_summary.get("excluded", 0)
                clock_in = analysis_summary.get("clocked_in", 0); miss_in = analysis_summary.get("missing_in", 0)
                clock_out = analysis_summary.get("clocked_out", 0); miss_out = analysis_summary.get("missing_out", 0)
                if is_evening:
                    summary_title = f"🌙 *{escaped_date_header} 퇴근 현황 요약*"; summary_details = ( f"\\- 전체: {total}명\n\\- 대상: {target}명 \\(제외: {excluded_count}명\\)\n\\- 출근: {clock_in}명 \\(미기록: {miss_in}명\\)\n\\- 퇴근: {clock_out}명 \\(미기록: {miss_out}명\\)" )
                else: # Morning
                    summary_title = f"☀️ *{escaped_date_header} 출근 현황 요약*"; summary_details = ( f"\\- 전체: {total}명\n\\- 대상: {target}명 \\(제외: {excluded_count}명\\)\n\\- 출근: {clock_in}명 \\(미기록: {miss_in}명\\)" )
                if excluded_employees: excluded_items = "\n  ".join([f"\\- {item}" for item in excluded_employees]); summary_details += f"\n\n*제외 인원 ({excluded_count}명)*:\n  {excluded_items}"
                elif excluded_count > 0: summary_details += f"\n\n*제외 인원 ({excluded_count}명)*: _(상세 목록 없음)_"
                full_summary_msg = f"{summary_title}\n{escape_markdown('-'*20)}\n{summary_details}"
                logging.info("Sending summary report...")
                if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, full_summary_msg): logging.error("Failed send summary report."); error_occurred = True

        except Exception as analysis_report_err:
            logging.error(f"Error during analysis or reporting phase: {analysis_report_err}", exc_info=True)
            error_occurred = True
            error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(분석/보고 단계\\)\n오류: {escape_markdown(str(analysis_report_err))}"
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)
    elif not excel_file_data and not error_occurred:
        logging.error("Excel data missing, but no initial error flagged.")
        error_occurred = True; error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류*\n엑셀 데이터 누락됨\\."; send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)

    # Phase 3: Final Completion Message
    script_end_time = time.time(); time_taken = script_end_time - script_start_time
    logging.info(f"--- Script finished in {time_taken:.2f} seconds ---")
    completion_status = "오류 발생" if error_occurred else "정상 완료"; status_emoji = "❌" if error_occurred else "✅"
    escaped_final_date = escape_markdown(TARGET_DATE_STR); escaped_final_status = escape_markdown(completion_status); escaped_final_time = escape_markdown(f"{time_taken:.1f}")
    final_message = f"{status_emoji} *{escaped_final_date} 근태 확인 스크립트*: {escaped_final_status} \\(소요시간: {escaped_final_time}초\\)"
    if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, final_message): logging.error("Failed to send final completion status message.")
    else: logging.info("Final completion status message sent.")
    exit_code = 1 if error_occurred else 0; logging.info(f"Exiting script with code: {exit_code}")
    exit(exit_code)
