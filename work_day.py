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
import os # <-- Import os module

# --- Configuration ---
# Load from environment variables, provide None as default to check later
WEBMAIL_USERNAME = os.environ.get("WEBMAIL_USER")
WEBMAIL_PASSWORD = os.environ.get("WEBMAIL_PASS")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT")

# --- Other Settings ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

WEBMAIL_LOGIN_URL = "http://gw.ktmos.co.kr/mail2/loginPage.do"
WEBMAIL_ID_FIELD_ID = "userEmail"
WEBMAIL_PW_FIELD_ID = "userPw"

# Use runner's date (likely UTC). Adjust logic if KST-specific date is absolutely required.
TARGET_DATE = datetime.date.today()
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")

REPORT_DOWNLOAD_URL_TEMPLATE = "http://gw.ktmos.co.kr/owattend/rest/dclz/report/CompositeStatus/sumr/user/days/excel?startDate={date}&endDate={date}&deptSeq=1231&erpNumDisplayYn=Y"
REPORT_URL = REPORT_DOWNLOAD_URL_TEMPLATE.format(date=TARGET_DATE_STR)

EXCEL_SHEET_NAME = "세부현황_B"
STANDARD_START_TIME_STR = "09:00:00"
STANDARD_END_TIME_STR = "18:00:00"
# Adjust threshold based on UTC if running on GitHub Actions (e.g., 6 PM KST = 9 AM UTC)
EVENING_RUN_THRESHOLD_HOUR = 9 # Example: 9 corresponds to 6 PM KST

# --- Credential Check ---
# Check if environment variables are set
missing_secrets = []
if not WEBMAIL_USERNAME: missing_secrets.append("WEBMAIL_USER")
if not WEBMAIL_PASSWORD: missing_secrets.append("WEBMAIL_PASS")
if not TELEGRAM_BOT_TOKEN: missing_secrets.append("TELEGRAM_TOKEN")
if not TELEGRAM_CHAT_ID: missing_secrets.append("TELEGRAM_CHAT")

if missing_secrets:
    logging.critical(f"!!! CRITICAL ERROR: Missing required environment variables (GitHub Secrets): {', '.join(missing_secrets)} !!!")
    # Optional: Send a basic Telegram message if possible
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
         try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", data={'chat_id':TELEGRAM_CHAT_ID, 'text':f"CRITICAL: GitHub Actions failed. Missing secrets: {', '.join(missing_secrets)}"})
         except: pass
    exit(1) # Exit with error code

# --- Constants for Leave Types ---
FULL_DAY_LEAVE_REASONS = {"연차", "보건휴가", "출산휴가", "출산전후휴가", "청원휴가", "가족돌봄휴가", "특별휴가", "공가", "공상", "예비군훈련", "민방위훈련", "공로휴가", "병가", "보상휴가"}
FULL_DAY_LEAVE_TYPES = {"법정휴가", "병가/휴직", "보상휴가", "공가"}
MORNING_HALF_LEAVE = "오전반차"
AFTERNOON_HALF_LEAVE = "오후반차"
ATTENDANCE_TYPE = "출퇴근"

# --- Helper Function Definitions ---

def setup_driver():
    """Sets up the Selenium ChromeDriver for headless execution in GitHub Actions."""
    logging.info("Setting up ChromeDriver...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new") # Run headless - ESSENTIAL FOR GITHUB ACTIONS
    options.add_argument("--disable-gpu") # Often needed in headless/linux
    options.add_argument("--no-sandbox") # Often needed in CI/Docker environments
    options.add_argument("--disable-dev-shm-usage") # Overcomes limited resource problems
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36")
    options.add_argument("--window-size=1920,1080") # Specify window size
    options.add_experimental_option("excludeSwitches", ["enable-logging"]) # Suppress some logs
    try:
        # Use webdriver-manager to automatically handle chromedriver installation/path
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(10) # Default wait time for finding elements

        # *** INCREASE PAGE LOAD TIMEOUT ***
        # Set how long driver.get() should wait for the page load to complete
        page_load_timeout_seconds = 180 # Try 3 minutes initially, adjust if needed
        driver.set_page_load_timeout(page_load_timeout_seconds)
        logging.info(f"Set page load timeout to {page_load_timeout_seconds} seconds.")

        logging.info("ChromeDriver setup complete (running headless).")
        return driver
    except Exception as e:
        logging.error(f"ChromeDriver setup error: {e}")
        logging.error(traceback.format_exc()) # Log full traceback for debugging
        raise # Re-raise the exception to stop the script

def login_and_get_cookies(driver, url, username_id, password_id, username, password):
    """Logs into the webmail using Selenium and extracts session cookies."""
    logging.info(f"Navigating to login page: {url}")
    # The driver.get() call below will now use the page_load_timeout set in setup_driver()
    driver.get(url)
    wait = WebDriverWait(driver, 30) # Wait for elements after page starts loading
    time.sleep(2) # Static wait can sometimes help with dynamic page loads

    try:
        # Wait for username and password fields to be visible
        user_field = wait.until(EC.visibility_of_element_located((By.ID, username_id)))
        pw_field = wait.until(EC.visibility_of_element_located((By.ID, password_id)))

        logging.info("Entering credentials...")
        user_field.clear(); time.sleep(0.1); user_field.send_keys(username); time.sleep(0.3)
        pw_field.clear(); time.sleep(0.1); pw_field.send_keys(password); time.sleep(0.3)

        logging.info(f"Submitting login form by sending RETURN to PW field ({password_id})...")
        pw_field.send_keys(Keys.RETURN)

        # Wait for an element that indicates successful login (e.g., logout link, main container)
        post_login_locator = (By.XPATH, "//a[contains(@href, 'logout')] | //*[contains(text(),'로그아웃')] | //div[@id='main_container'] | //span[@class='username']")
        logging.info(f"Waiting for login success indication using locator: {post_login_locator}...")
        wait.until(EC.presence_of_element_located(post_login_locator))

        logging.info("Login successful (post-login element found).")
        time.sleep(1) # Short pause after login seems complete

        logging.info("Extracting cookies...")
        cookies = {c['name']: c['value'] for c in driver.get_cookies()}
        logging.info(f"Extracted {len(cookies)} cookies.")
        return cookies

    except TimeoutException:
        current_url = driver.current_url
        logging.warning(f"TimeoutException waiting for post-login element OR page load (check previous logs). Current URL: {current_url}")
        login_page_check_url = url.split('?')[0] # Base login URL

        # Check if still on login page AFTER the page load timeout might have occurred
        if login_page_check_url in current_url:
            logging.info("Still on the login page. Checking for error messages...")
            found_error = None
            try:
                # Look for common error message elements
                error_elements = driver.find_elements(By.CSS_SELECTOR, ".login_box .error, .error_msg, #errormsg, .warning, .alert, [class*='error'], [id*='error']")
                for err_el in error_elements:
                     if err_el.is_displayed() and err_el.text.strip():
                         found_error = err_el.text.strip()
                         logging.error(f"Detected login failure message: '{found_error}'")
                         break
            except Exception as find_err:
                logging.warning(f"Exception while searching for error messages: {find_err}")

            if found_error:
                raise Exception(f"로그인 실패: {found_error}") # Raise specific error
            else:
                try:
                    screenshot_path = "login_timeout_screenshot.png"
                    driver.save_screenshot(screenshot_path)
                    logging.info(f"Saved screenshot to {screenshot_path} (login page timeout/element not found)")
                except Exception as ss_err:
                    logging.warning(f"Failed to save screenshot: {ss_err}")
                # Distinguish timeout reason if possible
                raise Exception("로그인 실패: 페이지 로드 또는 로그인 후 요소 확인 시간 초과.")
        else:
             # Page changed, but element confirmation failed. Assume login likely worked.
             logging.warning("Redirected away from login page, but post-login element confirmation timed out. Attempting to extract cookies anyway.")
             try:
                 cookies = {c['name']: c['value'] for c in driver.get_cookies()}
                 logging.warning(f"Successfully extracted {len(cookies)} cookies after timeout.")
                 return cookies
             except Exception as cookie_err:
                 raise Exception(f"로그인 상태 확인 실패: 페이지는 변경되었으나 쿠키 추출 중 오류 발생 ({cookie_err})")

    except Exception as e:
        # Catch other errors like the initial page load timeout from driver.get()
        logging.error(f"An unexpected error occurred during login process: {e}")
        logging.error(traceback.format_exc())
        try:
            screenshot_path = "login_error_screenshot.png"
            driver.save_screenshot(screenshot_path)
            logging.info(f"Saved error screenshot to {screenshot_path}")
        except Exception as ss_err:
            logging.warning(f"Failed to save error screenshot: {ss_err}")
        raise # Re-raise the original exception

def download_excel_report(report_url, cookies):
    """Downloads the Excel report using the session cookies."""
    logging.info(f"Attempting to download Excel report from: {report_url}")
    session = requests.Session()
    session.cookies.update(cookies) # Load cookies obtained from Selenium

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
        'Referer': WEBMAIL_LOGIN_URL.split('/mail2')[0], # Set Referer based on login URL base
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9,ko;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    try:
        response = session.get(report_url, headers=headers, stream=True, timeout=90) # Increased timeout
        logging.info(f"Download request status code: {response.status_code}")
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        content_type = response.headers.get('Content-Type', '').lower()
        logging.info(f"Response Content-Type: {content_type}")

        excel_mimes = ['application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/octet-stream', 'application/zip']

        if any(mime in content_type for mime in excel_mimes):
            excel_data = io.BytesIO(response.content)
            file_size = excel_data.getbuffer().nbytes
            if file_size < 1024: # Check for suspiciously small files
                 logging.warning(f"Downloaded file is very small ({file_size} bytes). Checking content for errors...")
                 try:
                     preview = excel_data.getvalue()[:500].decode('utf-8', errors='ignore')
                     if any(keyword in preview.lower() for keyword in ['error', '오류', '로그인', '권한', '세션 만료', 'session expired']):
                         logging.error(f"Small file content suggests an error page: {preview}")
                         return None # Indicate download failure
                 except Exception as preview_err:
                     logging.warning(f"Could not decode preview of small file: {preview_err}")
                 excel_data.seek(0) # Reset stream position

            logging.info(f"Excel file downloaded successfully ({file_size} bytes).")
            return excel_data
        else:
            logging.error(f"Downloaded content type '{content_type}' does not appear to be an Excel file.")
            try:
                preview = response.content[:500].decode('utf-8', errors='ignore')
                logging.error(f"Content preview (first 500 bytes): {preview}")
            except Exception as decode_err:
                logging.error(f"Could not decode content preview: {decode_err}")
            return None # Indicate failure

    except requests.exceptions.Timeout:
        logging.error(f"Timeout occurred while downloading the report from {report_url}")
        return None
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error during download: {e} - Status code: {e.response.status_code}")
        logging.error(f"Response content preview: {e.response.text[:500] if e.response else 'N/A'}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"A network or request error occurred during download: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during Excel download: {e}")
        logging.error(traceback.format_exc())
        return None

def parse_time_robust(time_str):
    """Parses various time string formats into a datetime.time object."""
    if pd.isna(time_str): return None
    if isinstance(time_str, datetime.time): return time_str
    if isinstance(time_str, datetime.datetime): return time_str.time()

    time_str = str(time_str).strip()
    if not time_str: return None

    # Handle 'YYYY-MM-DD HH:MM:SS' format
    if ' ' in time_str and ':' in time_str:
         try:
             dt_obj = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
             return dt_obj.time()
         except ValueError:
             pass # Try other formats

    # Try common time formats
    for fmt in ('%H:%M:%S', '%H:%M'):
        try:
            return datetime.datetime.strptime(time_str, fmt).time()
        except ValueError:
            continue

    if not any(c.isdigit() for c in time_str):
        logging.debug(f"Ignoring likely non-time string: '{time_str}'")
        return None
    else:
        logging.warning(f"Could not parse time string '{time_str}' into known formats (HH:MM:SS or HH:MM).")
        return None

def parse_date_robust(date_str):
    """Parses various date string formats into a datetime.date object."""
    if pd.isna(date_str): return None
    if isinstance(date_str, datetime.date): return date_str
    if isinstance(date_str, datetime.datetime): return date_str.date()

    date_str = str(date_str).strip()
    if not date_str: return None

    try:
        # Assume 'YYYY-MM-DD' potentially followed by time
        date_part = date_str.split(' ')[0]
        return datetime.datetime.strptime(date_part, '%Y-%m-%d').date()
    except ValueError:
        logging.warning(f"Could not parse date string '{date_str}' using format YYYY-MM-DD.")
        return None

def combine_date_time(date_val, time_val):
    """Combines date and time objects into a datetime object."""
    if isinstance(date_val, datetime.date) and isinstance(time_val, datetime.time):
        return datetime.datetime.combine(date_val, time_val)
    return None

def escape_markdown(text):
    """Escapes special characters for Telegram MarkdownV2."""
    if text is None: return ''
    text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def create_table_image(df, title, output_path="table_image.png"):
    """Creates an image representation of a Pandas DataFrame using Matplotlib."""
    logging.info("Creating table image...")
    if df.empty:
        logging.warning("DataFrame is empty, cannot create table image.")
        return None

    try:
        # Attempt to find and set a Korean font suitable for GitHub Actions runner
        font_path = None
        # Prioritize Nanum fonts often found on Ubuntu/Debian
        possible_fonts = ["NanumGothic.ttf", "NanumBarunGothic.ttf", "malgun.ttf", "AppleGothic.ttf"]
        # Check standard system font directories and common locations in Actions runners
        font_dirs = fm.findSystemFonts(fontpaths=None, fontext='ttf')
        action_font_paths = ["/usr/share/fonts/truetype/nanum/"]
        for p in action_font_paths:
             if Path(p).is_dir():
                 font_dirs.extend([str(f) for f in Path(p).glob("*.ttf")])

        logging.debug(f"Searching for fonts in {len(font_dirs)} locations.")
        found_korean_font = False
        for f_path in font_dirs:
            f_name = Path(f_path).name
            if any(pf.lower() in f_name.lower() for pf in possible_fonts):
                font_path = f_path
                logging.info(f"Using detected Korean font: {font_path}")
                found_korean_font = True
                break

        if found_korean_font:
             prop = fm.FontProperties(fname=font_path, size=10)
             plt.rcParams['font.family'] = prop.get_name()
             plt.rcParams['axes.unicode_minus'] = False # Important for displaying minus signs correctly
             logging.info(f"Set Matplotlib font family to: {prop.get_name()}")
        else:
            logging.warning("Korean font (e.g., NanumGothic) not found. Text in image might render incorrectly. Ensure 'fonts-nanum*' is installed in the runner.")
            # Fallback to default sans-serif
            plt.rcParams['font.family'] = 'sans-serif'
            plt.rcParams['axes.unicode_minus'] = False

    except Exception as e:
        logging.warning(f"Error during font setup: {e}. Text rendering may be affected.")

    # Dynamically adjust figure size based on DataFrame dimensions
    nr, nc = df.shape
    fw = min(max(8, nc * 1.2), 25) # Figure width: base 8, scale with cols, max 25
    fh = min(max(4, nr * 0.4 + 1.5), 50) # Figure height: base 4, scale with rows, max 50
    logging.info(f"Table dimensions: {nr} rows, {nc} cols. Figure size: ({fw:.1f}, {fh:.1f}) inches.")

    fig, ax = plt.subplots(figsize=(fw, fh))
    ax.axis('off') # Hide axes

    tab = Table(ax, bbox=[0, 0, 1, 1]) # Create table spanning the entire axis

    # Add header row
    for j, col in enumerate(df.columns):
        cell = tab.add_cell(0, j, 1, 1, text=str(col), loc='center', facecolor='lightgray')
        cell.set_fontsize(9)
        cell.set_text_props(weight='bold')

    # Add data rows
    for i in range(nr):
        for j in range(nc):
            txt = str(df.iloc[i, j])
            max_len = 60 # Truncate long cell text
            if len(txt) > max_len:
                txt = txt[:max_len-3] + '...'
            cell = tab.add_cell(i + 1, j, 1, 1, text=txt, loc='center', facecolor='white')
            cell.set_fontsize(8)

    ax.add_table(tab)
    plt.title(title, fontsize=12, pad=20) # Add title with padding
    plt.tight_layout(pad=1.0) # Adjust layout

    try:
        plt.savefig(output_path, bbox_inches='tight', dpi=120) # Save with tight bounding box and reasonable DPI
        plt.close(fig) # Close the figure to free memory
        logging.info(f"Table image saved successfully to: {output_path}")
        try:
            size_mb = Path(output_path).stat().st_size / (1024 * 1024)
            logging.info(f"Image file size: {size_mb:.2f} MB")
            if size_mb > 10: # Warn if image gets too large for Telegram
                logging.warning(f"Generated image size ({size_mb:.2f} MB) is large (>10MB).")
        except Exception as size_err:
            logging.warning(f"Could not get image file size: {size_err}")
        return output_path
    except Exception as e:
        logging.error(f"Failed to save table image: {e}")
        logging.error(traceback.format_exc())
        plt.close(fig) # Ensure figure is closed even on error
        return None

@retrying.retry(stop_max_attempt_number=3, wait_fixed=2000, retry_on_exception=lambda e: isinstance(e, requests.exceptions.RequestException))
def send_telegram_photo(bot_token, chat_id, photo_path, caption):
    """Sends a photo to Telegram with a caption, with retry logic for network errors."""
    api_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    if not Path(photo_path).exists():
        logging.error(f"Photo file not found: {photo_path}")
        return False # Indicate failure

    try:
        with open(photo_path, 'rb') as photo:
            # Telegram caption limit is 1024 chars
            if len(caption) > 1024:
                logging.warning(f"Caption length ({len(caption)}) exceeds 1024 chars, truncating.")
                caption = caption[:1021] + '...'

            files = {'photo': photo}
            payload = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'MarkdownV2'}

            response = requests.post(api_url, data=payload, files=files, timeout=60) # Timeout for file upload
            response_data = {}

            try:
                response_data = response.json()
            except json.JSONDecodeError:
                 logging.error(f"Failed to decode JSON response from Telegram API (sendPhoto). Status: {response.status_code}, Content: {response.text[:500]}")
                 response.raise_for_status() # Raise error based on status

            if response.status_code == 200 and response_data.get("ok"):
                logging.info("Telegram photo sent successfully.")
                return True
            else:
                err_desc = response_data.get('description', 'No description')
                err_code = response_data.get('error_code', 'N/A')
                logging.error(f"Telegram API Error (sendPhoto): {err_desc} (Code: {err_code})")
                logging.error(f"Failed caption (first 100 chars): {caption[:100]}...")
                # Raise specific exception for client errors (4xx) to prevent retries on bad requests
                if 400 <= response.status_code < 500:
                     raise requests.exceptions.HTTPError(f"Telegram client error {response.status_code}: {err_desc}", response=response)
                else:
                     response.raise_for_status() # Raise for other errors (e.g., 5xx)
                return False # Should not be reached if exception is raised, but good practice

    except requests.exceptions.HTTPError as e:
         logging.error(f"HTTP error sending photo: {e}")
         # Do not re-raise if it's a 4xx error (already handled), otherwise re-raise for retries
         if not (400 <= e.response.status_code < 500):
             raise
         return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error sending photo: {e}")
        raise # Re-raise to trigger retry logic
    except Exception as e:
        logging.error(f"Unexpected error sending Telegram photo: {e}")
        logging.error(traceback.format_exc())
        return False # Indicate failure on unexpected error

def send_telegram_message(bot_token, chat_id, text):
    """Sends a text message to Telegram, splitting if necessary, with MarkdownV2/plaintext fallback."""
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    max_length = 4096
    messages = []

    if not text or not str(text).strip():
        logging.warning("Attempted to send an empty Telegram message.")
        return True # Nothing to send, considered successful

    text = str(text)

    # Split message if too long
    if len(text) > max_length:
        logging.info(f"Message length ({len(text)}) > {max_length}. Splitting.")
        start = 0
        while start < len(text):
            end = text.rfind('\n', start, start + max_length)
            if end == -1 or end <= start:
                end = start + max_length
            chunk = text[start:end].strip()
            if chunk: messages.append(chunk)
            start = end
    else:
        messages.append(text)

    logging.info(f"Sending {len(messages)} message part(s).")
    all_sent_ok = True

    for i, part in enumerate(messages):
        payload_md = {'chat_id': chat_id, 'text': part, 'parse_mode': 'MarkdownV2'}
        sent_successfully = False
        try:
            # --- Try sending with MarkdownV2 ---
            response = requests.post(api_url, data=payload_md, timeout=30)
            response_data = {}
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                logging.error(f"Failed JSON decode (Part {i+1}, MDv2). Status: {response.status_code}, Content: {response.text[:500]}")
                response.raise_for_status()

            if response.status_code == 200 and response_data.get("ok"):
                logging.info(f"TG message part {i+1}/{len(messages)} sent successfully (MarkdownV2).")
                sent_successfully = True
            else:
                err_desc = response_data.get('description', 'N/A')
                err_code = response_data.get('error_code', 'N/A')
                logging.error(f"TG API Error (Part {i+1}, MDv2): {err_desc} ({err_code})")
                logging.error(f"Failed MDv2 content (excerpt): {part[:200]}...")

                # --- Fallback: Try sending as Plain Text ---
                if not sent_successfully: # Only try fallback if MDv2 failed
                    logging.info(f"Attempting Part {i+1} as plain text fallback.")
                    payload_plain = {'chat_id': chat_id, 'text': part}
                    try:
                        plain_response = requests.post(api_url, data=payload_plain, timeout=30)
                        plain_response_data = plain_response.json()
                        if plain_response.status_code == 200 and plain_response_data.get("ok"):
                            logging.info(f"TG message part {i+1}/{len(messages)} sent successfully (Plain Text Fallback).")
                            sent_successfully = True
                        else:
                            plain_err_desc = plain_response_data.get('description', 'N/A')
                            plain_err_code = plain_response_data.get('error_code', 'N/A')
                            logging.error(f"Plain text fallback failed (Part {i+1}): {plain_err_desc} ({plain_err_code})")
                    except Exception as plain_err:
                        logging.error(f"Exception during plain text fallback (Part {i+1}): {plain_err}")

        except requests.exceptions.Timeout:
            logging.error(f"Timeout sending TG message part {i+1}.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error sending TG message part {i+1}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error sending TG message part {i+1}: {e}")
            logging.error(traceback.format_exc())

        if not sent_successfully:
            all_sent_ok = False # Mark overall failure if any part fails

    return all_sent_ok


# --- Main Attendance Analysis Function ---

def analyze_attendance(excel_data, sheet_name):
    """Analyzes the attendance data from the downloaded Excel report."""
    logging.info(f"Analyzing attendance from sheet: {sheet_name}.")
    analysis_result = {
        "notifications": [],         # Morning report: Issues (late, absent)
        "detailed_status": [],       # Evening report: Clock in/out status for everyone
        "summary": {                 # Summary report data
            "total_employees": 0,
            "target": 0,             # Employees expected to have attendance records
            "excluded": 0,           # Employees on full-day leave/excluded
            "clocked_in": 0,
            "missing_in": 0,
            "clocked_out": 0,
            "missing_out": 0         # Among those who clocked in
        },
        "excluded_employees": [],    # List of excluded employees + reason/time
        "df_processed": None         # DataFrame ready for image generation
    }
    processed_data_for_image = [] # Temp list to build the image DataFrame

    try:
        # Read the specific sheet, skipping header rows, treat all as string initially
        df = pd.read_excel(excel_data, sheet_name=sheet_name, skiprows=2, dtype=str, keep_default_na=False)
        logging.info(f"Loaded {len(df)} rows from sheet '{sheet_name}'.")
        if df.empty:
            logging.warning("Excel sheet is empty or has no data after skipping rows.")
            return analysis_result # Return default empty result

        # Clean column names (remove leading/trailing whitespace)
        df.columns = [str(col).strip() for col in df.columns]
        logging.info(f"Cleaned columns found: {df.columns.tolist()}")

        # Define mapping from actual (potentially changing) column names to desired names
        actual_to_desired_mapping = {
            '서무원': '이름',          # Employee Name
            '출퇴근': '유형',          # Type (e.g., 출퇴근, 법정휴가)
            '정상': '구분',            # Category/Reason (e.g., 정상, 연차, 오전반차)
            'Unnamed: 11': '출근시간_raw', # Raw clock-in time string
            'Unnamed: 13': '퇴근시간_raw', # Raw clock-out time string
            'Unnamed: 16': '휴가시작시간_raw',# Raw leave start time
            'Unnamed: 18': '휴가종료시간_raw' # Raw leave end time
        }

        # Dynamically find the date column (looks like 'YYYY-MM-DD')
        date_col_actual_name = None
        for col in df.columns:
            if re.match(r'\d{4}-\d{2}-\d{2}', str(col).strip()):
                date_col_actual_name = col
                logging.info(f"Found date column: '{date_col_actual_name}'")
                break
        # Fallback if regex fails (less reliable)
        if not date_col_actual_name and len(df.columns) > 5 and df.columns[5] == TARGET_DATE_STR:
             date_col_actual_name = df.columns[5]
             logging.warning(f"Using fallback date column at index 5: '{date_col_actual_name}'")

        if not date_col_actual_name:
             logging.error("FATAL: Could not find the date column in the Excel sheet.")
             # Send critical error message
             send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n엑셀 파일에서 날짜 컬럼을 찾을 수 없습니다\\.")
             analysis_result["summary"]["total_employees"] = -1 # Indicate critical failure
             return analysis_result
        else:
             # Add the found date column to the mapping
             actual_to_desired_mapping[date_col_actual_name] = '일자'

        # Check if all required source columns exist in the DataFrame
        required_source_cols = list(actual_to_desired_mapping.keys())
        all_available_cols = df.columns.tolist()
        missing_source_cols = [c for c in required_source_cols if c not in all_available_cols]

        if missing_source_cols:
            logging.error(f"FATAL: Missing required source columns in Excel: {missing_source_cols}")
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n필수 엑셀 원본 컬럼 없음\\: `{escape_markdown(', '.join(missing_source_cols))}`")
            analysis_result["summary"]["total_employees"] = -1
            return analysis_result

        # Select and rename columns
        df_processed = df[required_source_cols].rename(columns=actual_to_desired_mapping)
        logging.info(f"Columns after selection and rename: {df_processed.columns.tolist()}")

        # --- Data Parsing ---
        df_processed['일자_dt'] = df_processed['일자'].apply(parse_date_robust)
        df_processed['출근시간_dt'] = df_processed['출근시간_raw'].apply(parse_time_robust)
        df_processed['퇴근시간_dt'] = df_processed['퇴근시간_raw'].apply(parse_time_robust)
        df_processed['휴가시작시간_dt'] = df_processed['휴가시작시간_raw'].apply(parse_time_robust)
        df_processed['휴가종료시간_dt'] = df_processed['휴가종료시간_raw'].apply(parse_time_robust)
        logging.info(f"Parsed times - In:{df_processed['출근시간_dt'].notna().sum()}, Out:{df_processed['퇴근시간_dt'].notna().sum()}, LeaveStart:{df_processed['휴가시작시간_dt'].notna().sum()}, LeaveEnd:{df_processed['휴가종료시간_dt'].notna().sum()}")

        # Define standard work times and lunch break times
        standard_start_time = parse_time_robust(STANDARD_START_TIME_STR) # 09:00
        standard_end_time = parse_time_robust(STANDARD_END_TIME_STR)     # 18:00
        noon_time = parse_time_robust("12:00:00") # Lunch start
        afternoon_start_time = parse_time_robust("13:00:00") # Work resumes after lunch

        if not all([standard_start_time, standard_end_time, noon_time, afternoon_start_time]):
             logging.error("FATAL: Could not parse standard work/lunch time strings.")
             send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n표준 근무/점심 시간 설정값 파싱 실패\\.")
             analysis_result["summary"]["total_employees"] = -1
             return analysis_result

        standard_start_dt = datetime.datetime.combine(TARGET_DATE, standard_start_time)
        standard_end_dt = datetime.datetime.combine(TARGET_DATE, standard_end_time)
        lunch_start_dt = datetime.datetime.combine(TARGET_DATE, noon_time)
        lunch_end_dt = datetime.datetime.combine(TARGET_DATE, afternoon_start_time)


        # Filter data for the target date
        df_filtered = df_processed[df_processed['일자_dt'] == TARGET_DATE].copy() # Use .copy() to avoid SettingWithCopyWarning
        if df_filtered.empty:
            logging.warning(f"No attendance data found for the target date {TARGET_DATE_STR}.")
            return analysis_result # No data for the day

        # Group data by employee name
        grouped = df_filtered.groupby('이름')
        analysis_result["summary"]["total_employees"] = len(grouped)
        logging.info(f"Processing {len(grouped)} unique employees for date {TARGET_DATE_STR}.")

        # --- Iterate through each employee's records for the day ---
        for name_raw, group_df in grouped:
            if not name_raw or str(name_raw).strip() == "":
                logging.warning("Skipping entry with missing employee name.")
                continue

            name_escaped = escape_markdown(name_raw) # Escape name for Telegram messages

            is_fully_excluded = False       # Flag if employee has full day off
            exclusion_reason_formatted = "" # Formatted reason for exclusion message
            collected_leaves = []           # Store {'type': str, 'start': time, 'end': time} for the employee
            attendance_row_data = None      # Store clock-in/out times if found

            # --- Collect all leave and attendance data for the employee ---
            for _, row in group_df.iterrows():
                leave_type = str(row.get('유형', '')).strip()      # e.g., 법정휴가, 출퇴근
                leave_category = str(row.get('구분', '')).strip()  # e.g., 연차, 오전반차, 정상
                leave_start_time = row['휴가시작시간_dt']         # Parsed time object or None
                leave_end_time = row['휴가종료시간_dt']           # Parsed time object or None

                # Determine if this row represents a leave entry
                is_leave_row = (leave_type in FULL_DAY_LEAVE_TYPES or
                                leave_category in FULL_DAY_LEAVE_REASONS or
                                leave_category in [MORNING_HALF_LEAVE, AFTERNOON_HALF_LEAVE])

                if is_leave_row:
                    # Use '구분' if available, otherwise '유형' as the description
                    current_leave_desc = leave_category if leave_category else leave_type
                    if current_leave_desc: # Only process if we have a valid leave description
                        collected_leaves.append({
                            'type': current_leave_desc,
                            'start': leave_start_time,
                            'end': leave_end_time
                        })
                        # Check for a single leave entry spanning the entire standard workday
                        if leave_start_time and leave_end_time:
                            if leave_start_time <= standard_start_time and leave_end_time >= standard_end_time:
                                logging.debug(f"Employee '{name_raw}' fully excluded by single leave: {current_leave_desc} ({leave_start_time.strftime('%H:%M')} - {leave_end_time.strftime('%H:%M')})")
                                is_fully_excluded = True
                                # Format reason for summary report
                                exclusion_reason_formatted = f"{name_raw}: {current_leave_desc} ({leave_start_time.strftime('%H:%M')} \\- {leave_end_time.strftime('%H:%M')})"
                                break # Found full exclusion, no need to check other rows for this employee

                # Check if this row contains attendance clock-in/out times
                if leave_type == ATTENDANCE_TYPE:
                     # Store the latest attendance data found (in case of duplicates)
                     attendance_row_data = {
                         '출근시간_dt': row['출근시간_dt'],
                         '퇴근시간_dt': row['퇴근시간_dt'],
                         '출근시간_raw': row['출근시간_raw'], # Keep raw string for image
                         '퇴근시간_raw': row['퇴근시간_raw']  # Keep raw string for image
                     }

            # --- Process collected leaves AFTER iterating all rows for the employee ---
            # Check for combined leaves covering the full day if not already excluded by a single entry
            if not is_fully_excluded and collected_leaves:
                covers_morning = False
                covers_afternoon = False
                min_leave_start = standard_end_time # Initialize to find earliest start
                max_leave_end = standard_start_time   # Initialize to find latest end

                for leave in collected_leaves:
                    ls = leave['start']
                    le = leave['end']
                    lt = leave['type']

                    # Update overall start/end times from VALID leave times
                    if ls and ls < min_leave_start: min_leave_start = ls
                    if le and le > max_leave_end: max_leave_end = le

                    # Check if leave covers the morning working hours (before lunch)
                    if lt == MORNING_HALF_LEAVE or (ls and le and ls <= standard_start_time and le >= noon_time):
                         covers_morning = True

                    # Check if leave covers the afternoon working hours (after lunch)
                    if lt == AFTERNOON_HALF_LEAVE or (ls and le and ls <= afternoon_start_time and le >= standard_end_time):
                         covers_afternoon = True

                # If both morning and afternoon are covered by *any combination* of leaves
                if covers_morning and covers_afternoon:
                     logging.debug(f"Employee '{name_raw}' fully excluded by combined leaves.")
                     is_fully_excluded = True
                     # Format combined reason: list unique types and overall time range
                     combined_types = " \\+ ".join(sorted(list(set(l['type'] for l in collected_leaves if l['type']))))
                     # Use min/max times only if they were updated (i.e., valid times found)
                     time_range_str = ""
                     if min_leave_start < standard_end_time and max_leave_end > standard_start_time:
                         time_range_str = f" ({min_leave_start.strftime('%H:%M')} \\- {max_leave_end.strftime('%H:%M')})"

                     exclusion_reason_formatted = f"{name_raw}: {combined_types}{time_range_str}"

            # --- Final Decision: Exclude or Analyze Attendance ---
            if is_fully_excluded:
                analysis_result["summary"]["excluded"] += 1
                # Add formatted reason to the list for the summary report
                analysis_result["excluded_employees"].append(escape_markdown(exclusion_reason_formatted)) # Already includes name
                # Add entry for image table indicating exclusion
                image_reason = exclusion_reason_formatted.split(': ', 1)[-1] # Get reason part after name
                processed_data_for_image.append({
                    '이름': name_raw,
                    '일자': TARGET_DATE_STR,
                    '유형': '휴가/제외',
                    '구분': image_reason, # Show combined reason
                    '출근시간': '-',
                    '퇴근시간': '-'
                })
                continue # Move to the next employee

            # --- Analyze Attendance for Non-Excluded Employees ---
            analysis_result["summary"]["target"] += 1 # Count as target for analysis

            # Extract clock-in/out times from the stored attendance data (if any)
            clock_in_dt, clock_out_dt = None, None
            clock_in_raw, clock_out_raw = '', ''
            if attendance_row_data:
                clock_in_dt = attendance_row_data['출근시간_dt']
                clock_out_dt = attendance_row_data['퇴근시간_dt']
                clock_in_raw = str(attendance_row_data['출근시간_raw']) if attendance_row_data['출근시간_raw'] else ''
                clock_out_raw = str(attendance_row_data['퇴근시간_raw']) if attendance_row_data['퇴근시간_raw'] else ''

            # Convert to datetime objects for comparison
            actual_start_dt = combine_date_time(TARGET_DATE, clock_in_dt) if clock_in_dt else None
            actual_end_dt = combine_date_time(TARGET_DATE, clock_out_dt) if clock_out_dt else None
            has_clock_in = actual_start_dt is not None
            has_clock_out = actual_end_dt is not None

            # Determine if employee has morning/afternoon leave based on collected leaves
            current_has_morning_leave = any(
                l['type'] == MORNING_HALF_LEAVE or
                (l['start'] and l['end'] and l['start'] <= standard_start_time and l['end'] >= noon_time)
                for l in collected_leaves
            )
            current_has_afternoon_leave = any(
                l['type'] == AFTERNOON_HALF_LEAVE or
                (l['start'] and l['end'] and l['start'] <= afternoon_start_time and l['end'] >= standard_end_time)
                for l in collected_leaves
            )

            # Determine expected start/end times based on leaves
            expected_start_dt = lunch_end_dt if current_has_morning_leave else standard_start_dt
            expected_end_dt = lunch_start_dt if current_has_afternoon_leave else standard_end_dt

            # --- Check for Issues and Populate Reports ---
            issues = [] # Collect issues for the morning report notification

            # Check Clock-In
            if has_clock_in:
                analysis_result["summary"]["clocked_in"] += 1
                # Check for lateness (strict comparison)
                if actual_start_dt > expected_start_dt:
                    late_reason = '오전반차 후 지각' if current_has_morning_leave else '지각'
                    issues.append(f"{late_reason}: {escape_markdown(clock_in_dt.strftime('%H:%M:%S'))}")
            else: # No clock-in record
                analysis_result["summary"]["missing_in"] += 1
                # Report missing clock-in only if they weren't expected to be absent all morning
                if not current_has_morning_leave:
                    issues.append("출근 기록 없음")

            # Check Clock-Out
            if has_clock_out:
                analysis_result["summary"]["clocked_out"] += 1
                # Check for early departure (strict comparison)
                if actual_end_dt < expected_end_dt:
                    # Report early departure only if they weren't expected to leave early (afternoon leave)
                    if not current_has_afternoon_leave:
                        issues.append(f"조퇴: {escape_markdown(clock_out_dt.strftime('%H:%M:%S'))}")
            elif has_clock_in: # Only check missing clock-out if they actually clocked in
                 analysis_result["summary"]["missing_out"] += 1
                 # Report missing clock-out only if they weren't expected to be absent all afternoon
                 if not current_has_afternoon_leave:
                    issues.append("퇴근 기록 없음")

            # Add collected issues to the morning notifications list
            if issues:
                msg = f"*{name_escaped}*: {', '.join(issues)}"
                analysis_result["notifications"].append(msg)

            # --- Populate detailed status for Evening Report ---
            clock_in_status_str = clock_in_dt.strftime('%H:%M:%S') if has_clock_in else "기록없음"
            clock_out_status_str = clock_out_dt.strftime('%H:%M:%S') if has_clock_out else ("미퇴근" if has_clock_in else "기록없음") # Distinguish no record vs not clocked out yet
            analysis_result["detailed_status"].append({
                'name': name_raw,
                'in_status': clock_in_status_str,
                'out_status': clock_out_status_str
            })

            # --- Populate Data for Image Table ---
            img_type = ATTENDANCE_TYPE
            img_category = '정상'
            leave_descs = [l['type'] for l in collected_leaves if l['type']]
            if MORNING_HALF_LEAVE in leave_descs:
                img_type = MORNING_HALF_LEAVE; img_category = '반차'
            if AFTERNOON_HALF_LEAVE in leave_descs:
                img_type = AFTERNOON_HALF_LEAVE; img_category = '반차' # Overwrite if both exist for simplicity
            # If only specific half-day leaves, set type explicitly
            if MORNING_HALF_LEAVE in leave_descs and AFTERNOON_HALF_LEAVE not in leave_descs:
                 img_type = MORNING_HALF_LEAVE
            elif AFTERNOON_HALF_LEAVE in leave_descs and MORNING_HALF_LEAVE not in leave_descs:
                 img_type = AFTERNOON_HALF_LEAVE


            # Use raw times for display, indicate missing records
            img_clock_in = clock_in_raw if clock_in_raw else ('기록없음' if not current_has_morning_leave else '-') # Show raw time or indication
            img_clock_out = clock_out_raw if clock_out_raw else ('미퇴근' if has_clock_in and not current_has_afternoon_leave else ('기록없음' if not has_clock_in else '-'))

            processed_data_for_image.append({
                '이름': name_raw,
                '일자': TARGET_DATE_STR,
                '유형': img_type,
                '구분': img_category,
                '출근시간': img_clock_in,
                '퇴근시간': img_clock_out
            })

        # --- Final Steps After Processing All Employees ---
        if processed_data_for_image:
             # Define columns for the image DataFrame in desired order
             image_df_cols = ['이름', '일자', '유형', '구분', '출근시간', '퇴근시간']
             analysis_result["df_processed"] = pd.DataFrame(processed_data_for_image, columns=image_df_cols)
             logging.info(f"Created DataFrame for image generation with {len(analysis_result['df_processed'])} rows.")
        else:
             logging.warning("No data rows were processed to create the image DataFrame.")

        # Sanity checks for summary counts
        calc_total = analysis_result["summary"]["target"] + analysis_result["summary"]["excluded"]
        if calc_total != analysis_result["summary"]["total_employees"]:
            logging.warning(f"Summary count mismatch! Total Employees ({analysis_result['summary']['total_employees']}) != Target ({analysis_result['summary']['target']}) + Excluded ({analysis_result['summary']['excluded']})")

        target_check = analysis_result["summary"]["clocked_in"] + analysis_result["summary"]["missing_in"]
        if analysis_result["summary"]["target"] != target_check:
            logging.warning(f"Target count mismatch! Target ({analysis_result['summary']['target']}) != ClockedIn ({analysis_result['summary']['clocked_in']}) + MissingIn ({analysis_result['summary']['missing_in']})")

        logging.info(f"Analysis complete. Summary: {analysis_result['summary']}")
        return analysis_result

    except KeyError as e:
        logging.error(f"Analysis failed due to KeyError (likely missing/unexpected column): {e}")
        logging.error(traceback.format_exc())
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n엑셀 컬럼 이름 오류 \\(KeyError\\): `{escape_markdown(str(e))}`")
        analysis_result["summary"]["total_employees"] = -1 # Indicate critical failure
        return analysis_result
    except Exception as e:
        logging.error(f"An unexpected error occurred during attendance analysis: {e}")
        logging.error(traceback.format_exc())
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n분석 중 예상치 못한 오류 발생: {escape_markdown(str(e))}")
        analysis_result["summary"]["total_employees"] = -1 # Indicate critical failure
        return analysis_result


# --- Main Execution Block ---
if __name__ == "__main__":
    script_start_time = time.time()
    # Log the target date being used (important for verifying timezone effects)
    logging.info(f"--- Script started for date (runner's perspective): {TARGET_DATE_STR} ---")
    driver = None
    excel_file_data = None
    error_occurred = False
    analysis_result = {} # Initialize analysis_result

    # --- Phase 1: Setup, Login, Download ---
    try:
        driver = setup_driver() # This now sets the page load timeout
        cookies = login_and_get_cookies(driver, WEBMAIL_LOGIN_URL, WEBMAIL_ID_FIELD_ID, WEBMAIL_PW_FIELD_ID, WEBMAIL_USERNAME, WEBMAIL_PASSWORD)
        excel_file_data = download_excel_report(REPORT_URL, cookies)

        if excel_file_data is None:
            logging.error("Excel report download failed, stopping execution.")
            error_occurred = True
            # Attempt to send Telegram notification about download failure
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(초기 단계\\):\n엑셀 보고서 다운로드 실패\\.")

    except Exception as e:
        logging.error(f"Critical error during setup/login/download phase: {e}")
        logging.error(traceback.format_exc()) # Log full traceback
        error_occurred = True
        # Attempt to send Telegram notification about the critical error
        # Make sure escape_markdown is defined before this point (it is)
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(초기 단계\\):\n{escape_markdown(str(e))}")
    finally:
        # Ensure WebDriver is closed even if errors occurred
        if driver:
            try:
                driver.quit()
                logging.info("WebDriver closed successfully.")
            except (WebDriverException, NoSuchWindowException) as e:
                logging.warning(f"Non-critical error closing WebDriver (might have crashed): {e}")
            except Exception as e:
                logging.error(f"Unexpected error closing WebDriver: {e}")

    # --- Phase 2: Analysis and Reporting ---
    # Proceed only if download was successful and no critical errors occurred in Phase 1
    if excel_file_data and not error_occurred:
        try:
            analysis_result = analyze_attendance(excel_file_data, EXCEL_SHEET_NAME)

            # Check if analysis itself indicated a failure
            if not analysis_result or analysis_result.get("summary", {}).get("total_employees", 0) == -1:
                 logging.error("Attendance analysis failed or returned invalid result. Skipping report generation.")
                 error_occurred = True
                 # Analysis function should have sent its own error message
            else:
                # Determine if it's morning or evening run based on UTC time
                now_utc_time = datetime.datetime.utcnow().time()
                is_evening = now_utc_time >= datetime.time(EVENING_RUN_THRESHOLD_HOUR, 0)
                logging.info(f"Current UTC time {now_utc_time.strftime('%H:%M:%S')}. Reporting as {'Evening' if is_evening else 'Morning'}.")

                # Extract results from analysis
                attendance_issues = analysis_result.get("notifications", []) # For morning report
                detailed_statuses = analysis_result.get("detailed_status", []) # For evening report
                analysis_summary = analysis_result.get("summary", {})
                excluded_employees = analysis_result.get("excluded_employees", []) # Already formatted & escaped
                df_for_image = analysis_result.get("df_processed")

                # --- Send Table Image ---
                if df_for_image is not None and not df_for_image.empty:
                    img_title = f"{TARGET_DATE_STR} 근태 현황 (처리: {analysis_summary.get('total_employees', 0)}명)" # Use total from summary
                    image_path = create_table_image(df_for_image, img_title, "attendance_table.png")
                    if image_path:
                        caption = f"*{escape_markdown(TARGET_DATE_STR)} 근태 상세 현황*"
                        try:
                            if not send_telegram_photo(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, image_path, caption):
                                 logging.error("Failed to send Telegram photo (send_telegram_photo returned False).")
                                 # Decide if photo failure is critical enough to set error_occurred = True
                                 # error_occurred = True
                        except Exception as photo_e:
                             logging.error(f"Exception occurred while sending Telegram photo: {photo_e}")
                             # error_occurred = True
                        finally:
                            # Clean up the generated image file
                            try:
                                Path(image_path).unlink(missing_ok=True)
                                logging.info(f"Deleted temporary image file: {image_path}")
                            except Exception as del_err:
                                logging.warning(f"Could not delete image file {image_path}: {del_err}")
                    else:
                        logging.error("Failed to create the table image file.")
                        # error_occurred = True # Decide if image creation failure is critical
                elif df_for_image is None:
                     logging.warning("Analysis did not produce a DataFrame for the image. Skipping image.")
                else: # df_for_image is empty
                     logging.info("DataFrame for image is empty. Skipping image generation.")

                # --- Send Detailed Text Report (Morning Issues or Evening Status) ---
                message_lines = []
                report_title = ""
                escaped_date_header = escape_markdown(TARGET_DATE_STR)

                if is_evening:
                    # Evening Report: Show status for all non-excluded employees
                    logging.info("Generating evening detailed status report.")
                    report_title = f"*{escaped_date_header} 퇴근 현황 상세*"
                    if detailed_statuses:
                        for idx, status in enumerate(detailed_statuses):
                             name_esc = escape_markdown(status.get('name', 'N/A'))
                             in_esc = escape_markdown(status.get('in_status', 'N/A'))
                             out_esc = escape_markdown(status.get('out_status', 'N/A'))
                             line = f"{idx + 1}\\. *{name_esc}*: 출근[{in_esc}] / 퇴근[{out_esc}]"
                             message_lines.append(line)
                    else:
                         logging.info("No detailed status entries found for evening report.")
                         if analysis_summary.get("target", 0) > 0:
                            message_lines.append(escape_markdown("분석 대상 인원에 대한 상세 상태 데이터가 없습니다."))
                         else:
                            message_lines.append(escape_markdown("분석 대상 인원이 없습니다."))

                else:
                    # Morning Report: Show only issues (late, missing clock-in)
                    logging.info("Generating morning issue report.")
                    report_title = f"*{escaped_date_header} 출근 확인 필요*"
                    if attendance_issues:
                         # Issues are already formatted with Markdown escapes in analyze_attendance
                        for idx, issue in enumerate(attendance_issues):
                            line = f"{idx + 1}\\. {issue}"
                            message_lines.append(line)
                    else:
                        logging.info("No specific morning attendance issues found.")
                        if analysis_summary.get("target", 0) > 0:
                            message_lines.append(escape_markdown("모든 분석 대상 인원의 출근 기록이 정상입니다 (지각/미기록 없음)."))
                        else:
                            message_lines.append(escape_markdown("분석 대상 인원이 없습니다."))

                # Send the composed detailed report if it has content
                if message_lines:
                     msg_header = f"{report_title}\n{escape_markdown('-'*20)}\n\n"
                     msg_body = "\n".join(message_lines)
                     full_msg = msg_header + msg_body
                     logging.info(f"Sending detailed {'evening status' if is_evening else 'morning issue'} report.")
                     if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, full_msg):
                         # Don't necessarily set error_occurred=True just because Telegram failed,
                         # but log it prominently.
                         logging.error("Failed to send detailed report message to Telegram.")
                else:
                     logging.warning("No content generated for the detailed report message.")

        except Exception as e:
            # Catch errors during the analysis or reporting generation phase
            logging.error(f"Error during analysis/reporting generation phase: {e}")
            logging.error(traceback.format_exc())
            error_occurred = True # Mark as error if analysis/report generation fails
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(결과 처리/알림 생성 중\\):\n{escape_markdown(str(e))}")

    # --- Phase 3: Send Summary Report ---
    # Send summary regardless of image/detail success, unless analysis itself failed critically
    if analysis_result and analysis_result.get("summary", {}).get("total_employees", -1) != -1:
        try:
            analysis_summary = analysis_result.get("summary", {})
            # Determine is_evening again based on current time for accuracy
            now_utc_time = datetime.datetime.utcnow().time()
            is_evening = now_utc_time >= datetime.time(EVENING_RUN_THRESHOLD_HOUR, 0)

            total = analysis_summary.get("total_employees", 0)
            target = analysis_summary.get("target", 0)
            excluded = analysis_summary.get("excluded", 0)
            clock_in = analysis_summary.get("clocked_in", 0)
            miss_in = analysis_summary.get("missing_in", 0)
            clock_out = analysis_summary.get("clocked_out", 0)
            miss_out = analysis_summary.get("missing_out", 0)
            excluded_list = analysis_result.get("excluded_employees", []) # Already formatted & escaped

            escaped_date_summary = escape_markdown(TARGET_DATE_STR)
            summary_msg = ""
            summary_details = ""

            if not is_evening:
                summary_title = f"☀️ {escaped_date_summary} 출근 현황 요약"
                summary_details = (
                    f"\\- 전체 인원: {total}명\n"
                    f"\\- 확인 대상: {target}명 \\(제외: {excluded}명\\)\n"
                    f"\\- 출근 기록 확인: {clock_in}명\n"
                    f"\\- *출근 기록 없음*: {miss_in}명" # Highlight missing
                )
            else: # Evening
                summary_title = f"🌙 {escaped_date_summary} 퇴근 현황 요약"
                summary_details = (
                    f"\\- 전체 인원: {total}명\n"
                    f"\\- 확인 대상: {target}명 \\(제외: {excluded}명\\)\n"
                    f"\\- 출근 기록자: {clock_in}명 \\(미기록: {miss_in}명\\)\n"
                    f"\\- 퇴근 기록 확인: {clock_out}명\n"
                    f"\\- *퇴근 기록 없음* \\(출근자 중\\): {miss_out}명" # Highlight missing
                )

            # Append excluded list if not empty
            if excluded_list:
                # Excluded items are already escaped markdown from analysis function
                excluded_items = "\n  ".join([f"\\- {item}" for item in excluded_list])
                summary_details += f"\n\n*제외 인원 상세* ({len(excluded_list)}명):\n  {excluded_items}"

            summary_msg = f"{summary_title}\n{escape_markdown('-'*20)}\n{summary_details}"
            logging.info("Sending summary report.")
            if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, summary_msg):
                # Log failure but don't necessarily mark script as failed overall
                logging.error("Failed to send summary report message to Telegram.")

        except Exception as summary_err:
            logging.error(f"Error generating or sending summary report: {summary_err}")
            logging.error(traceback.format_exc())
            error_occurred = True # Mark as error if summary generation fails
            # Try to send a fallback error message for summary failure
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류*\n요약 보고서 생성/전송 중 오류 발생\\: {escape_markdown(str(summary_err))}")
    elif not error_occurred:
         # This case means analysis failed early (e.g., total_employees = -1) or excel download failed
         logging.warning("Skipping summary report because analysis result was invalid or indicated earlier failure.")


    # --- Phase 4: Final Completion Message ---
    script_end_time = time.time()
    time_taken = script_end_time - script_start_time
    logging.info(f"--- Script finished in {time_taken:.2f} seconds ---")

    completion_status = "오류 발생" if error_occurred else "정상 완료"
    escaped_final_date = escape_markdown(TARGET_DATE_STR)
    escaped_final_status = escape_markdown(completion_status)
    time_taken_str = f"{time_taken:.1f}" # Format time taken
    escaped_final_time = escape_markdown(time_taken_str)

    final_message = f"*{escaped_final_date} 근태 확인 스크립트*: {escaped_final_status} \\(소요시간: {escaped_final_time}초\\)"
    # Try sending final status, but don't mark 'error_occurred' if only this final message fails
    send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, final_message)

    # --- Exit with appropriate code for GitHub Actions ---
    logging.info(f"Exiting with code {1 if error_occurred else 0}")
    exit(1 if error_occurred else 0)
