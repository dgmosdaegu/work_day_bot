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
# Read credentials from environment variables
# These MUST match the names used in the GitHub Secrets and the workflow file's env block
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

# --- Credential Check ---
# Check if environment variables are set AT THE START
missing_secrets = []
if not WEBMAIL_USERNAME: missing_secrets.append("WEBMAIL_USERNAME")
if not WEBMAIL_PASSWORD: missing_secrets.append("WEBMAIL_PASSWORD")
if not TELEGRAM_BOT_TOKEN: missing_secrets.append("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_CHAT_ID: missing_secrets.append("TELEGRAM_CHAT_ID")

if missing_secrets:
    error_message = f"!!! CRITICAL ERROR: Missing required environment variables: {', '.join(missing_secrets)} !!! Ensure they are set as GitHub Secrets and mapped correctly in the workflow file."
    logging.critical(error_message)
    # Attempt to send a Telegram message ONLY IF token/chat_id ARE available
    # (This part might not run if the Telegram creds themselves are missing)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            # Need a basic function here since the main one might not be defined yet
            def send_startup_error(token, chatid, msg):
                 url = f"https://api.telegram.org/bot{token}/sendMessage"
                 payload = {'chat_id': chatid, 'text': msg, 'parse_mode': 'MarkdownV2'}
                 try: requests.post(url, data=payload, timeout=10)
                 except Exception: pass # Ignore errors here, just best effort
            # Need to escape the message for MarkdownV2
            escaped_error = re.sub(f'([{re.escape("_*[]()~`>#+-=|{}.!")}])', r'\\\1', error_message)
            send_startup_error(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, escaped_error)
        except Exception as e:
             logging.error(f"Could not send startup Telegram error notification: {e}")
    exit(1) # Exit immediately if secrets are missing

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
    # Escape characters: _ * [ ] ( ) ~ ` > # + - = | { } . !
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    # Use re.sub to escape the characters
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

# Basic send message function for early errors if needed
def send_telegram_message_basic(bot_token, chat_id, text):
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

# Ensure credential check happens after defining basic send function
if missing_secrets:
     error_message = f"!!! CRITICAL ERROR: Missing required environment variables: {', '.join(missing_secrets)} !!! Ensure they are set as GitHub Secrets and mapped correctly in the workflow file."
     logging.critical(error_message)
     if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
         send_telegram_message_basic(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, escape_markdown(error_message))
     exit(1)


# --- Full Helper Functions ---

def setup_driver():
    logging.info("Setting up ChromeDriver...")
    options = webdriver.ChromeOptions()
    # Essential for GitHub Actions Linux runners:
    options.add_argument("--headless=new") # Use the new headless mode
    options.add_argument("--no-sandbox") # Bypass OS security model, REQUIRED for running as root/in docker/actions
    options.add_argument("--disable-dev-shm-usage") # overcome limited resource problems in docker/actions

    options.add_argument("--disable-gpu") # Often recommended for headless
    # Use a realistic user agent
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36")
    options.add_argument("--window-size=1920,1080") # Specify window size
    options.add_experimental_option("excludeSwitches", ["enable-logging"]) # Suppress DevTools listening message
    try:
        # webdriver-manager handles downloading the correct driver for the installed Chrome
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(15) # Slightly longer implicit wait
        logging.info("ChromeDriver setup complete (running headless).")
        return driver
    except WebDriverException as e:
        logging.error(f"WebDriver setup error: {e}")
        # Check if it's a Chrome version mismatch issue
        if "session not created" in str(e) and "This version of ChromeDriver" in str(e):
            logging.error("Potential ChromeDriver/Chrome version mismatch. Check runner's Chrome version and webdriver-manager.")
        else:
            logging.error(traceback.format_exc())
        raise
    except Exception as e:
        logging.error(f"Unexpected ChromeDriver setup error: {e}")
        logging.error(traceback.format_exc())
        raise

@retrying.retry(stop_max_attempt_number=3, wait_fixed=5000, retry_on_exception=lambda e: isinstance(e, (TimeoutException, NoSuchElementException)))
def login_and_get_cookies(driver, url, username_id, password_id, username, password):
    logging.info(f"Attempting login to: {url}")
    driver.get(url)
    wait = WebDriverWait(driver, 20) # Wait up to 20 seconds for elements

    try:
        logging.info(f"Waiting for username field: {username_id}")
        user_field = wait.until(EC.visibility_of_element_located((By.ID, username_id)))
        logging.info(f"Waiting for password field: {password_id}")
        pw_field = wait.until(EC.visibility_of_element_located((By.ID, password_id)))

        logging.info("Entering credentials...")
        user_field.clear(); time.sleep(0.2); user_field.send_keys(username); time.sleep(0.5)
        pw_field.clear(); time.sleep(0.2); pw_field.send_keys(password); time.sleep(0.5)

        logging.info(f"Submitting login form by sending RETURN to PW field ({password_id})...")
        pw_field.send_keys(Keys.RETURN)

        # Wait for a post-login element OR a redirect away from login page
        # Example: Look for a logout link OR main content area OR username display
        post_login_locator = (By.XPATH, "//a[contains(@href, 'logout')] | //*[contains(text(),'로그아웃')] | //div[@id='main_container'] | //span[@class='username']") # Adjust as needed
        logging.info(f"Waiting for login success indication (e.g., {post_login_locator})...")
        wait.until(EC.presence_of_element_located(post_login_locator))

        logging.info("Login appears successful.")
        time.sleep(2) # Allow potential redirects or AJAX calls to finish
        logging.info("Extracting cookies...")
        cookies = {c['name']: c['value'] for c in driver.get_cookies()}
        if not cookies:
            logging.warning("No cookies extracted after login.")
            raise Exception("쿠키 추출 실패 (로그인 후 쿠키 없음)")
        logging.info(f"Extracted {len(cookies)} cookies.")
        return cookies

    except TimeoutException as e:
        current_url = driver.current_url
        logging.error(f"Timeout occurred during login process. Current URL: {current_url}")
        page_source_snippet = driver.page_source[:1000] # Get beginning of source
        logging.error(f"Page source snippet:\n{page_source_snippet}")
        # Save screenshot for debugging in Actions (will be an artifact)
        screenshot_path = "login_timeout_screenshot.png"
        try:
             driver.save_screenshot(screenshot_path)
             logging.info(f"Saved screenshot to {screenshot_path}")
        except Exception as ss_err:
             logging.warning(f"Failed to save screenshot: {ss_err}")

        login_page_check_url = url.split('?')[0] # Base URL check
        if login_page_check_url in current_url:
             logging.error("Still on login page after timeout. Checking for error messages.")
             found_error = None
             try: # Try finding common error message patterns
                  error_elements = driver.find_elements(By.CSS_SELECTOR, ".login_box .error, .error_msg, #errormsg, .warning, .alert, [class*='error'], [id*='error']")
                  for err_el in error_elements:
                       if err_el.is_displayed() and err_el.text.strip():
                            found_error = err_el.text.strip()
                            logging.error(f"Detected login error message: '{found_error}'")
                            break
             except Exception as find_err:
                  logging.warning(f"Could not search for error messages: {find_err}")

             if found_error:
                  raise Exception(f"로그인 실패: {found_error}")
             else:
                  raise Exception("로그인 실패: 페이지 변경 안됨 (타임아웃), 특정 에러 메시지 없음")
        else:
             logging.warning("Redirected away from login page, but timed out waiting for post-login element. Attempting to extract cookies anyway.")
             try:
                  cookies = {c['name']: c['value'] for c in driver.get_cookies()}
                  if cookies:
                       logging.warning(f"Extracted {len(cookies)} cookies despite timeout. Proceeding cautiously.")
                       return cookies
                  else:
                       raise Exception("로그인 확인 실패: 리다이렉트 되었으나 쿠키 추출 불가")
             except Exception as cookie_err:
                  raise Exception(f"로그인 확인 실패: 포스트 로그인 요소 타임아웃 및 쿠키 추출 오류 ({cookie_err})")

    except Exception as e:
        logging.error(f"An unexpected error occurred during login: {e}", exc_info=True)
        try:
            driver.save_screenshot("login_error_screenshot.png")
            logging.info("Saved error screenshot.")
        except Exception as ss_err:
            logging.warning(f"Failed to save error screenshot: {ss_err}")
        raise # Re-raise the exception


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

        # Check for non-successful status codes
        response.raise_for_status() # Raises HTTPError for 4xx/5xx responses

        content_type = response.headers.get('Content-Type', '').lower()
        logging.info(f"Response Content-Type: {content_type}")

        # Check if the content type indicates an Excel file
        is_excel = any(mime in content_type for mime in ['excel', 'spreadsheetml', 'vnd.ms-excel', 'octet-stream'])
        # Also check content disposition header if available
        content_disposition = response.headers.get('Content-Disposition', '')
        is_excel_disposition = '.xlsx' in content_disposition or '.xls' in content_disposition

        if is_excel or is_excel_disposition:
            excel_data = io.BytesIO(response.content)
            file_size = excel_data.getbuffer().nbytes
            logging.info(f"Excel download successful ({file_size} bytes).")

            # Basic sanity check for very small files which might be error pages
            if file_size < 2048: # Adjust threshold if necessary
                logging.warning(f"Downloaded file is very small ({file_size} bytes). Might be an error page or empty report. Checking content...")
                try:
                    # Try reading as text to detect HTML error messages (like login prompts)
                    preview = excel_data.getvalue()[:500].decode('utf-8', errors='ignore')
                    if any(k in preview.lower() for k in ['login', '로그인', 'error', '오류', 'session', '세션', '권한', '<html>', '<head>']):
                        logging.error(f"Downloaded file content suggests an error or login page: {preview}")
                        raise Exception("다운로드된 파일이 엑셀이 아닌 오류 페이지일 수 있습니다.")
                    else:
                        logging.warning("Small file content doesn't immediately look like an error page, but proceed with caution.")
                except Exception as parse_err:
                    logging.warning(f"Could not decode or fully check small file content: {parse_err}. Assuming it might be valid but small.")
                finally:
                    excel_data.seek(0) # Rewind the stream buffer

            return excel_data
        else:
            # If not excel, log the content received for debugging
            logging.error(f"Downloaded content is not identified as Excel. Content-Type: '{content_type}', Disposition: '{content_disposition}'")
            try:
                preview = response.content[:500].decode('utf-8', errors='ignore')
                logging.error(f"Content preview (first 500 bytes):\n{preview}")
            except Exception:
                logging.error("Could not decode content preview.")
            raise Exception("다운로드된 파일 형식이 엑셀이 아닙니다.")

    except requests.exceptions.Timeout:
        logging.error(f"Timeout occurred while downloading report from {report_url}")
        raise Exception(f"보고서 다운로드 시간 초과: {report_url}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error during download: {e.response.status_code} - {e.response.reason}")
        try:
            error_content = e.response.content[:500].decode('utf-8', errors='ignore')
            logging.error(f"HTTP Error Response Content (preview):\n{error_content}")
        except Exception:
             logging.error("Could not decode HTTP error response content.")
        raise Exception(f"보고서 다운로드 중 HTTP 오류 발생: {e.response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network or request error during download: {e}")
        raise Exception(f"보고서 다운로드 중 네트워크 오류 발생: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during report download: {e}", exc_info=True)
        raise # Re-raise the exception


def parse_time_robust(time_str):
    if pd.isna(time_str) or time_str == '': return None
    if isinstance(time_str, datetime.time): return time_str
    if isinstance(time_str, datetime.datetime): return time_str.time()

    time_str = str(time_str).strip()
    if not time_str: return None

    # Handle common Excel time formats (float representation)
    if isinstance(time_str, (float, int)):
        try:
            # Excel stores time as fraction of a day
            total_seconds = int(time_str * 24 * 60 * 60)
            # Handle potential rounding issues near midnight
            total_seconds = min(total_seconds, 24 * 60 * 60 - 1)
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return datetime.time(hours, minutes, seconds)
        except (ValueError, TypeError):
            logging.warning(f"Could not parse numeric time value: '{time_str}'")
            return None

    # Handle time strings
    # Check for full datetime string first
    if ' ' in time_str and ':' in time_str:
        try: return datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').time()
        except ValueError: pass
        try: return datetime.datetime.strptime(time_str, '%Y/%m/%d %H:%M:%S').time()
        except ValueError: pass
        # Add other potential datetime formats if needed

    # Try common time formats
    for fmt in ('%H:%M:%S', '%H:%M', '%I:%M:%S %p', '%I:%M %p'):
        try: return datetime.datetime.strptime(time_str, fmt).time()
        except ValueError: continue

    # Handle non-standard formats (e.g., no colon, but makes sense as time)
    time_str_numeric = ''.join(filter(str.isdigit, time_str))
    if len(time_str_numeric) == 4: # HHMM
        try: return datetime.datetime.strptime(time_str_numeric, '%H%M').time()
        except ValueError: pass
    elif len(time_str_numeric) == 6: # HHMMSS
        try: return datetime.datetime.strptime(time_str_numeric, '%H%M%S').time()
        except ValueError: pass

    # If it contains non-digit characters beyond separators, it's likely not a time
    if not re.match(r'^[\d:\sAMP]+$', time_str, re.IGNORECASE):
        logging.debug(f"Ignoring likely non-time string: '{time_str}'")
        return None
    else:
        logging.warning(f"Could not parse time string with known formats: '{time_str}'")
        return None


def parse_date_robust(date_str):
    if pd.isna(date_str): return None
    if isinstance(date_str, datetime.date): return date_str
    if isinstance(date_str, datetime.datetime): return date_str.date()
    date_str = str(date_str).strip()
    if not date_str: return None
    try:
        # Extract only the date part if time is included
        date_part = date_str.split(' ')[0]
        # Try common date formats
        for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%Y%m%d', '%m/%d/%Y'):
            try: return datetime.datetime.strptime(date_part, fmt).date()
            except ValueError: continue
        logging.warning(f"Could not parse date string with known formats: '{date_str}'")
        return None
    except Exception as e:
        logging.warning(f"Error parsing date string '{date_str}': {e}")
        return None

def combine_date_time(date_val, time_val):
    if isinstance(date_val, datetime.date) and isinstance(time_val, datetime.time):
        return datetime.datetime.combine(date_val, time_val)
    return None

# Function to find Korean fonts, especially important in Linux environments like GitHub Actions
def find_korean_font():
    common_font_files = ["NanumGothic.ttf", "malgun.ttf", "AppleGothic.ttf", "gulim.ttc", "NanumBarunGothic.ttf"]
    # Common paths in Linux (like GitHub Actions runners after installing fonts-nanum*)
    linux_font_paths = [
        "/usr/share/fonts/truetype/nanum/",
        "/usr/share/fonts/opentype/nanum/",
        "/usr/share/fonts/truetype/google/", # Might contain Noto Sans KR
        os.path.expanduser("~/.fonts/") # User fonts if any
    ]

    # 1. Check specific known paths first (more reliable on Linux)
    for path in linux_font_paths:
        try:
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    if filename in common_font_files:
                        found_path = os.path.join(path, filename)
                        logging.info(f"Found Korean font in specific path: {found_path}")
                        return found_path
        except OSError: # Path might not exist or permissions issue
            continue

    # 2. If not found, use matplotlib's font manager search
    logging.info("Korean font not found in common Linux paths, searching with font_manager...")
    try:
        system_fonts = fm.findSystemFonts(fontpaths=None, fontext='ttf')
        for f in system_fonts:
            font_name = Path(f).name
            if any(common_name in font_name for common_name in common_font_files):
                logging.info(f"Found potential Korean font via font_manager: {f}")
                # Basic check if font can be loaded (optional but good)
                try:
                    fm.FontProperties(fname=f)
                    logging.info(f"Successfully loaded font properties for {f}. Using this font.")
                    return f
                except Exception as load_err:
                    logging.warning(f"Found font {f} but failed to load properties: {load_err}. Skipping.")
                    continue
    except Exception as e:
        logging.warning(f"Error searching system fonts with font_manager: {e}")

    logging.warning("Korean font not found after checking common paths and system search.")
    return None


def create_table_image(df, title, output_path="table_image.png"):
    logging.info("Attempting to create table image...")
    if df.empty:
        logging.warning("DataFrame is empty, cannot generate table image.")
        return None

    # Configure Matplotlib backend suitable for non-GUI environments
    plt.switch_backend('Agg')

    try:
        # Use the helper function to find the font
        font_path = find_korean_font()
        if font_path:
            try:
                # Clear font cache before setting new parameters - sometimes helps in Actions
                fm._load_fontmanager(try_read_cache=False)
                prop = fm.FontProperties(fname=font_path, size=10)
                plt.rcParams['font.family'] = prop.get_name()
                plt.rcParams['axes.unicode_minus'] = False # Allow minus sign display
                logging.info(f"Successfully set font: {font_path} using family name {prop.get_name()}")
            except Exception as font_prop_err:
                 logging.error(f"Failed to set font properties for {font_path}: {font_prop_err}", exc_info=True)
                 logging.warning("Proceeding without specific Korean font. Text may be broken.")
                 # Fallback or just let matplotlib handle it
                 plt.rcParams['font.family'] = 'sans-serif' # Generic fallback
        else:
            logging.warning("Korean font not found. Table image might have broken characters.")
            plt.rcParams['font.family'] = 'sans-serif' # Generic fallback

    except Exception as e:
        logging.error(f"Error during font setup: {e}.", exc_info=True)
        # Attempt to continue without custom font
        plt.rcParams['font.family'] = 'sans-serif'

    nr, nc = df.shape
    # Adjust figsize dynamically but keep it reasonable
    # Base size + increments per row/col, with max limits
    base_w, incr_w = 6, 0.8
    base_h, incr_h = 2, 0.3
    max_w, max_h = 25, 40 # Max dimensions to prevent huge images

    fw = min(max(base_w, base_w + nc * incr_w), max_w)
    fh = min(max(base_h, base_h + nr * incr_h), max_h)
    logging.info(f"Table dimensions: {nr} rows, {nc} columns. Calculated Figure size: ({fw:.1f}, {fh:.1f})")

    fig, ax = plt.subplots(figsize=(fw, fh))
    ax.axis('off') # Hide axes

    try:
        tab = Table(ax, bbox=[0, 0, 1, 1]) # Use full axes bounding box

        # Add Header
        for j, col in enumerate(df.columns):
            tab.add_cell(0, j, 1, 1, text=str(col), loc='center', facecolor='lightgray', width=1.0/nc if nc > 0 else 1)

        # Add Rows
        for i in range(nr):
            for j in range(nc):
                txt = str(df.iloc[i, j])
                max_len = 40 # Limit cell text length
                if len(txt) > max_len: txt = txt[:max_len - 3] + '...'
                cell_color = 'white' # Simpler background
                tab.add_cell(i + 1, j, 1, 1, text=txt, loc='center', facecolor=cell_color, width=1.0/nc if nc > 0 else 1)

        tab.auto_set_font_size(False)
        tab.set_fontsize(9) # Adjust font size if needed
        ax.add_table(tab)

        plt.title(title, fontsize=12, pad=15) # Add padding to title
        plt.tight_layout(pad=1.0) # Adjust padding

        plt.savefig(output_path, bbox_inches='tight', dpi=120) # Increase DPI slightly for clarity
        plt.close(fig) # Close the figure to free memory

        logging.info(f"Table image saved successfully: {output_path}")
        size_bytes = Path(output_path).stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        logging.info(f"Image file size: {size_mb:.2f} MB")

        # Check Telegram's photo size limit (around 10MB usually)
        if size_mb > 9.5:
            logging.warning(f"Generated image size ({size_mb:.2f} MB) is close to or exceeds Telegram's typical limit (10MB). Sending might fail.")
        elif size_bytes == 0:
             logging.error("Generated image file size is 0 bytes. Image creation likely failed silently.")
             return None

        return output_path

    except Exception as e:
        logging.error(f"Failed to create or save table image: {e}", exc_info=True)
        plt.close(fig) # Ensure figure is closed on error
        return None


@retrying.retry(stop_max_attempt_number=3, wait_fixed=5000, retry_on_exception=lambda e: isinstance(e, requests.exceptions.RequestException))
def send_telegram_photo(bot_token, chat_id, photo_path, caption):
    api_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    if not Path(photo_path).exists():
        logging.error(f"Cannot send photo, file not found: {photo_path}")
        return False
    if Path(photo_path).stat().st_size == 0:
        logging.error(f"Cannot send photo, file size is 0 bytes: {photo_path}")
        return False

    try:
        with open(photo_path, 'rb') as photo:
            # Ensure caption length is within limits (Telegram limits vary, 1024 is generally safe)
            max_caption_len = 1024
            if len(caption) > max_caption_len:
                logging.warning(f"Caption length ({len(caption)}) exceeds limit ({max_caption_len}). Truncating.")
                caption = caption[:max_caption_len - 3] + "..."

            files = {'photo': (Path(photo_path).name, photo)} # Provide filename explicitly
            payload = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'MarkdownV2'}
            response = requests.post(api_url, data=payload, files=files, timeout=60) # Timeout for upload

            rd = {} # Response dictionary
            try:
                rd = response.json()
            except json.JSONDecodeError:
                logging.error(f"Failed to decode JSON response from Telegram API. Status: {response.status_code}, Content: {response.text[:500]}")
                response.raise_for_status() # Raise HTTPError based on status code

            if response.status_code == 200 and rd.get("ok"):
                logging.info("Telegram photo sent successfully.")
                return True
            else:
                err_desc = rd.get('description', 'N/A')
                err_code = rd.get('error_code', 'N/A')
                logging.error(f"Telegram API Error (sendPhoto): {err_desc} (Code: {err_code})")
                logging.error(f"Payload sent (excluding file): {payload}")
                # Raise an exception for client errors (like bad request due to formatting) to prevent retry
                if 400 <= response.status_code < 500:
                    raise requests.exceptions.HTTPError(f"Telegram Client Error {response.status_code}: {err_desc}", response=response)
                else: # For server errors or others, let retrying handle it
                    response.raise_for_status()
                return False # Should be unreachable if exception is raised

    except requests.exceptions.HTTPError as e:
         # Log specific client errors that shouldn't be retried
         if 400 <= e.response.status_code < 500:
              logging.error(f"HTTP Client Error sending photo (will not retry): {e}", exc_info=True)
              # Optionally send a text message about the failure
              error_text = f"*{escape_markdown(TARGET_DATE_STR)} 이미지 전송 실패*\n텔레그램 API 오류 \\(HTTP {e.response.status_code}\\): {escape_markdown(e.response.json().get('description','N/A'))}"
              send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_text)
         else:
              logging.error(f"HTTP Server/Network Error sending photo: {e}")
              raise # Re-raise to allow retry for server/network issues
         return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error sending photo: {e}. Retrying allowed.")
        raise # Re-raise to allow retry
    except FileNotFoundError:
         logging.error(f"File not found error during photo sending: {photo_path}")
         return False # No point retrying if file vanished
    except Exception as e:
        logging.error(f"Unexpected error sending Telegram photo: {e}", exc_info=True)
        # Raise exception to potentially allow retry if it was transient
        raise Exception(f"Unexpected photo send error: {e}")


def send_telegram_message(bot_token, chat_id, text):
    """Sends a text message to Telegram, splitting if too long."""
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    max_len = 4096 # Telegram's message length limit
    messages_to_send = []

    if not text:
        logging.warning("Attempted to send an empty message.")
        return True # Consider empty message as success (nothing to send)

    # Split message if it exceeds the limit
    if len(text) > max_len:
        logging.info(f"Message length ({len(text)}) exceeds {max_len}, splitting...")
        start = 0
        while start < len(text):
            # Find the last newline character within the limit
            end = text.rfind('\n', start, start + max_len)
            if end == -1 or end <= start:
                # If no newline found, just split at max_len
                end = start + max_len
            chunk = text[start:end].strip()
            if chunk: # Avoid sending empty chunks
                 messages_to_send.append(chunk)
            start = end
    else:
        messages_to_send.append(text)

    logging.info(f"Sending {len(messages_to_send)} message part(s) to Telegram.")
    all_parts_sent_successfully = True

    for i, part in enumerate(messages_to_send):
        if not part:
            logging.warning(f"Skipping empty message part {i+1}.")
            continue

        payload = {'chat_id': chat_id, 'text': part, 'parse_mode': 'MarkdownV2'}
        part_sent = False
        attempt = 0
        max_attempts = 2 # Try MarkdownV2, then plain text

        while not part_sent and attempt < max_attempts:
            attempt += 1
            mode = payload.get('parse_mode', 'Plain')
            logging.info(f"Sending part {i+1}/{len(messages_to_send)} using mode: {mode} (Attempt {attempt})")
            try:
                response = requests.post(api_url, data=payload, timeout=30)
                rd = response.json() # Try decoding JSON regardless of status

                if response.status_code == 200 and rd.get("ok"):
                    logging.info(f"Telegram message part {i+1} sent successfully using {mode}.")
                    part_sent = True
                else:
                    err_desc = rd.get('description', 'N/A')
                    err_code = rd.get('error_code', 'N/A')
                    logging.error(f"Telegram API Error (sendMessage Part {i+1}, Mode: {mode}): {err_desc} (Code: {err_code})")
                    logging.error(f"Failed content preview (first 500 chars): {part[:500]}")

                    # If MarkdownV2 failed, try again with plain text
                    if mode == 'MarkdownV2' and attempt < max_attempts:
                        logging.warning("MarkdownV2 failed, retrying as plain text.")
                        payload['parse_mode'] = None # Remove parse_mode for plain text
                        payload['text'] = part # Ensure original text is used
                    else:
                        # If plain text also fails, or it was the first try with plain text, mark as failed
                        all_parts_sent_successfully = False
                        break # Stop trying for this part

            except requests.exceptions.Timeout:
                logging.error(f"Timeout sending Telegram message part {i+1} (Mode: {mode}).")
                if attempt == max_attempts: all_parts_sent_successfully = False
                time.sleep(5) # Wait before next attempt/part
            except requests.exceptions.RequestException as e:
                logging.error(f"Network error sending Telegram message part {i+1} (Mode: {mode}): {e}")
                if attempt == max_attempts: all_parts_sent_successfully = False
                time.sleep(5) # Wait before next attempt/part
            except json.JSONDecodeError:
                 logging.error(f"Failed to decode JSON response from Telegram API sending part {i+1}. Status: {response.status_code}, Content: {response.text[:500]}")
                 if attempt == max_attempts: all_parts_sent_successfully = False
                 time.sleep(5)
            except Exception as e:
                logging.error(f"Unexpected error sending Telegram message part {i+1} (Mode: {mode}): {e}", exc_info=True)
                if attempt == max_attempts: all_parts_sent_successfully = False
                # Don't retry on unexpected error immediately, break the loop for this part
                break

        if not part_sent:
             all_parts_sent_successfully = False # Mark overall failure if any part fails

    return all_parts_sent_successfully


def analyze_attendance(excel_data, sheet_name):
    logging.info(f"Analyzing attendance data from sheet: '{sheet_name}'.")
    analysis_result = {
        "notifications": [], # Morning issues (late, absent)
        "detailed_status": [], # Evening status (in/out times for everyone)
        "summary": { "total_employees": 0, "target": 0, "excluded": 0, "clocked_in": 0, "missing_in": 0, "clocked_out": 0, "missing_out": 0 },
        "excluded_employees": [], # List of names and reasons for exclusion
        "df_processed": None # DataFrame for image generation
    }
    processed_data_for_image = [] # Temp list to build the image DataFrame

    try:
        # Read Excel, skip header rows, ensure strings are read, keep empty strings as ''
        df = pd.read_excel(excel_data, sheet_name=sheet_name, skiprows=2, dtype=str, keep_default_na=False)
        logging.info(f"Loaded {len(df)} rows from sheet '{sheet_name}'.")

        if df.empty:
            logging.warning(f"Excel sheet '{sheet_name}' is empty or has no data after header rows.")
            # Don't return error, just indicate zero employees
            return analysis_result

        # Clean column names (remove leading/trailing spaces, handle potential multi-index)
        df.columns = [str(col).strip() for col in df.columns]
        logging.info(f"Cleaned columns: {df.columns.tolist()}")

        # --- Column Mapping ---
        # Define expected column names and their raw counterparts found by inspection
        # It seems the raw names 'Unnamed: X' can be unstable. Using text content might be better if possible.
        # But for now, stick to the indices observed previously. Re-verify if structure changes.
        actual_to_desired_mapping = {
            '서무원': '이름',       # Employee Name (Assuming this column name is stable)
            '출퇴근': '유형',       # Type (e.g., 출퇴근, 법정휴가)
            '정상': '구분',         # Category (e.g., 정상, 연차, 오전반차, 오후반차)
            # Raw time columns - THESE INDICES ARE CRITICAL and may change
            'Unnamed: 11': '출근시간_raw', # Clock-in time (raw string)
            'Unnamed: 13': '퇴근시간_raw', # Clock-out time (raw string)
            'Unnamed: 16': '휴가시작시간_raw', # Leave start time (raw string)
            'Unnamed: 18': '휴가종료시간_raw'  # Leave end time (raw string)
            # 'Unnamed: 19': '사유' # Reason (Optional, if needed later)
        }

        # Find the date column dynamically (looks for YYYY-MM-DD format)
        date_col_actual_name = None
        for col in df.columns:
            if re.match(r'^\d{4}-\d{2}-\d{2}$', str(col).strip()):
                date_col_actual_name = col
                logging.info(f"Dynamically identified date column: '{date_col_actual_name}'")
                break

        if not date_col_actual_name:
            # Fallback: Check if the expected date string is present as a column name (less robust)
            if TARGET_DATE_STR in df.columns:
                 date_col_actual_name = TARGET_DATE_STR
                 logging.warning(f"Using fallback date column name: '{date_col_actual_name}'")
            else:
                 # Fallback based on typical position (e.g., index 5) - VERY FRAGILE
                 potential_date_col_index = 5
                 if len(df.columns) > potential_date_col_index:
                     guessed_col = df.columns[potential_date_col_index]
                     if parse_date_robust(guessed_col): # Check if it looks like a date
                         date_col_actual_name = guessed_col
                         logging.warning(f"Using highly speculative date column at index {potential_date_col_index}: '{date_col_actual_name}'")
                     else:
                        logging.error("FATAL: Cannot find date column (dynamic or fallback failed). Columns found: %s", df.columns.tolist())
                        raise KeyError("엑셀 보고서에서 날짜 컬럼을 찾을 수 없습니다.")
                 else:
                      logging.error("FATAL: Cannot find date column and not enough columns for fallback index. Columns found: %s", df.columns.tolist())
                      raise KeyError("엑셀 보고서에서 날짜 컬럼을 찾을 수 없습니다 (컬럼 부족).")

        actual_to_desired_mapping[date_col_actual_name] = '일자' # Add the found date column to mapping

        # --- Validate Required Columns ---
        required_source_cols = list(actual_to_desired_mapping.keys())
        missing_source_cols = [c for c in required_source_cols if c not in df.columns]
        if missing_source_cols:
            logging.error(f"FATAL: Required source columns are missing from the Excel sheet: {missing_source_cols}")
            logging.error(f"Available columns: {df.columns.tolist()}")
            raise KeyError(f"필수 원본 컬럼 누락: {', '.join(missing_source_cols)}")

        # --- Select and Rename Columns ---
        df_processed = df[required_source_cols].copy() # Use copy to avoid SettingWithCopyWarning
        df_processed.rename(columns=actual_to_desired_mapping, inplace=True)
        logging.info(f"Columns after selection and renaming: {df_processed.columns.tolist()}")

        # --- Parse Dates and Times ---
        df_processed['일자_dt'] = df_processed['일자'].apply(parse_date_robust)
        # Apply robust time parsing
        df_processed['출근시간_dt'] = df_processed['출근시간_raw'].apply(parse_time_robust)
        df_processed['퇴근시간_dt'] = df_processed['퇴근시간_raw'].apply(parse_time_robust)
        df_processed['휴가시작시간_dt'] = df_processed['휴가시작시간_raw'].apply(parse_time_robust)
        df_processed['휴가종료시간_dt'] = df_processed['휴가종료시간_raw'].apply(parse_time_robust)

        # Log parsing results summary
        logging.info(f"Parsed Dates: {df_processed['일자_dt'].notna().sum()} valid dates found.")
        logging.info(f"Parsed Times - In: {df_processed['출근시간_dt'].notna().sum()}, Out: {df_processed['퇴근시간_dt'].notna().sum()}, LeaveStart: {df_processed['휴가시작시간_dt'].notna().sum()}, LeaveEnd: {df_processed['휴가종료시간_dt'].notna().sum()}")

        # Filter for the target date AFTER parsing
        df_filtered = df_processed[df_processed['일자_dt'] == TARGET_DATE].copy()
        if df_filtered.empty:
            logging.warning(f"No data found for the target date {TARGET_DATE_STR} after filtering.")
            # Return empty result set, not an error
            return analysis_result
        logging.info(f"Found {len(df_filtered)} rows for target date {TARGET_DATE_STR}.")

        # --- Define Standard Times ---
        try:
            standard_start_time = datetime.datetime.strptime(STANDARD_START_TIME_STR, '%H:%M:%S').time()
            standard_end_time = datetime.datetime.strptime(STANDARD_END_TIME_STR, '%H:%M:%S').time()
            standard_start_dt = datetime.datetime.combine(TARGET_DATE, standard_start_time)
            standard_end_dt = datetime.datetime.combine(TARGET_DATE, standard_end_time)
            # Define lunch break boundaries explicitly
            lunch_start_time = datetime.time(12, 0, 0)
            lunch_end_time = datetime.time(13, 0, 0)
            afternoon_start_time = lunch_end_time # Work resumes after lunch
        except ValueError as time_parse_err:
            logging.error(f"FATAL: Could not parse standard time strings: {time_parse_err}")
            raise ValueError("표준 근무 시간 또는 점심 시간 형식이 잘못되었습니다.")

        # --- Process Each Employee ---
        # Group by employee name
        grouped = df_filtered.groupby('이름')
        analysis_result["summary"]["total_employees"] = len(grouped)
        logging.info(f"Processing {len(grouped)} unique employees for date {TARGET_DATE_STR}.")

        for name_raw, group_df in grouped:
            name_trimmed = str(name_raw).strip()
            if not name_trimmed:
                logging.warning("Skipping entry with empty employee name.")
                continue
            name_escaped = escape_markdown(name_trimmed) # Escape name for Telegram output

            logging.debug(f"--- Processing employee: {name_trimmed} ---")
            is_fully_excluded = False
            exclusion_reason_formatted = ""
            collected_leaves = [] # Stores {'type': str, 'start': time | None, 'end': time | None}
            attendance_row_data = None # Stores {'출근시간_dt': ..., '퇴근시간_dt': ..., '출근시간_raw': ..., '퇴근시간_raw': ...}

            # Iterate through all rows for this employee on this date
            for _, row in group_df.iterrows():
                leave_type = str(row.get('유형', '')).strip() # e.g., 법정휴가
                leave_category = str(row.get('구분', '')).strip() # e.g., 연차, 오전반차
                leave_start_time_dt = row['휴가시작시간_dt']
                leave_end_time_dt = row['휴가종료시간_dt']

                # Determine if this row represents a leave/absence
                is_leave_row = leave_type in FULL_DAY_LEAVE_TYPES or \
                               leave_category in FULL_DAY_LEAVE_REASONS or \
                               leave_category in [MORNING_HALF_LEAVE, AFTERNOON_HALF_LEAVE]

                if is_leave_row:
                    # Prefer '구분' (Category) for the leave name if available, else use '유형' (Type)
                    current_leave_name = leave_category if leave_category else leave_type
                    if current_leave_name: # Only record if we have a name
                        collected_leaves.append({
                            'type': current_leave_name,
                            'start': leave_start_time_dt,
                            'end': leave_end_time_dt
                        })
                        logging.debug(f"{name_trimmed}: Recorded leave - Type='{current_leave_name}', Start={leave_start_time_dt}, End={leave_end_time_dt}")

                        # --- Initial Check for Single Full-Day Exclusion ---
                        # Check if this single leave entry covers the entire standard workday
                        if leave_start_time_dt and leave_end_time_dt:
                            if leave_start_time_dt <= standard_start_time and leave_end_time_dt >= standard_end_time:
                                logging.info(f"{name_trimmed}: Identified as fully excluded by single leave entry: {current_leave_name} ({leave_start_time_dt.strftime('%H:%M')} - {leave_end_time_dt.strftime('%H:%M')})")
                                is_fully_excluded = True
                                # Format reason including times
                                exclusion_reason_formatted = f"{name_trimmed}: {current_leave_name} ({leave_start_time_dt.strftime('%H:%M')} \\- {leave_end_time_dt.strftime('%H:%M')})"
                                break # Found a full exclusion, no need to check other rows for this person

                # Check if this row contains attendance data
                elif leave_type == ATTENDANCE_TYPE:
                    # Store the latest attendance data found (in case of multiple rows, though unlikely)
                    attendance_row_data = {
                        '출근시간_dt': row['출근시간_dt'],
                        '퇴근시간_dt': row['퇴근시간_dt'],
                        '출근시간_raw': str(row['출근시간_raw']).strip(),
                        '퇴근시간_raw': str(row['퇴근시간_raw']).strip()
                    }
                    logging.debug(f"{name_trimmed}: Found attendance data - In={row['출근시간_dt']}, Out={row['퇴근시간_dt']}")


            # --- Combine Leave Information (AFTER checking all rows for the employee) ---
            # Check if combined leaves result in full day exclusion only if not already excluded by a single entry
            if not is_fully_excluded and collected_leaves:
                logging.debug(f"{name_trimmed}: Checking combined leaves ({len(collected_leaves)} entries).")
                covers_morning = False
                covers_afternoon = False
                overall_min_start = standard_end_time # Initialize to ensure any valid time is earlier
                overall_max_end = standard_start_time # Initialize to ensure any valid time is later

                for leave in collected_leaves:
                    ls = leave['start']
                    le = leave['end']
                    lt = leave['type']

                    # Update overall time range using valid leave times
                    if ls and (overall_min_start is None or ls < overall_min_start): overall_min_start = ls
                    if le and (overall_max_end is None or le > overall_max_end): overall_max_end = le

                    # Check Morning Coverage (Standard Start Time to Lunch Start Time)
                    # Covers morning if:
                    # 1. It's explicitly "오전반차"
                    # 2. Or it's another leave type that starts at/before standard start and ends at/after lunch start
                    if lt == MORNING_HALF_LEAVE:
                        covers_morning = True
                    elif ls and le and ls <= standard_start_time and le >= lunch_start_time:
                        covers_morning = True

                    # Check Afternoon Coverage (Lunch End Time to Standard End Time)
                    # Covers afternoon if:
                    # 1. It's explicitly "오후반차"
                    # 2. Or it's another leave type that starts at/before lunch end and ends at/after standard end
                    if lt == AFTERNOON_HALF_LEAVE:
                        covers_afternoon = True
                    elif ls and le and ls <= lunch_end_time and le >= standard_end_time:
                        covers_afternoon = True

                    logging.debug(f"{name_trimmed}: Leave '{lt}' ({ls}-{le}) -> Morning={covers_morning}, Afternoon={covers_afternoon}")

                # If both morning and afternoon work periods are covered by leaves
                if covers_morning and covers_afternoon:
                    logging.info(f"{name_trimmed}: Identified as fully excluded by COMBINED leave entries.")
                    is_fully_excluded = True
                    # Create a combined reason string
                    combined_types = " + ".join(sorted(list(set(l['type'] for l in collected_leaves if l['type']))))
                    # Format time range using the earliest start and latest end from all leaves
                    time_range_str = f"{overall_min_start.strftime('%H:%M') if overall_min_start else '?'} \\- {overall_max_end.strftime('%H:%M') if overall_max_end else '?'}"
                    exclusion_reason_formatted = f"{name_trimmed}: {combined_types} ({time_range_str})"


            # --- Final Decision: Exclude or Analyze Attendance ---
            if is_fully_excluded:
                analysis_result["excluded_employees"].append(escape_markdown(exclusion_reason_formatted)) # Use the detailed reason
                analysis_result["summary"]["excluded"] += 1
                # Add to image data as excluded
                processed_data_for_image.append({
                    '이름': name_trimmed,
                    '일자': TARGET_DATE_STR,
                    '유형': '휴가/제외', # General type for excluded
                    '구분': escape_markdown(exclusion_reason_formatted.split(': ')[-1]), # Get the reason part
                    '출근시간': '-',
                    '퇴근시간': '-'
                })
                logging.debug(f"{name_trimmed}: Final status - Excluded.")
                continue # Move to the next employee


            # --- Analyze Attendance for Non-Excluded Employees ---
            analysis_result["summary"]["target"] += 1 # Count as target for attendance check
            logging.debug(f"{name_trimmed}: Final status - Target for attendance check.")

            clock_in_dt, clock_out_dt = None, None
            clock_in_raw, clock_out_raw = '', ''
            if attendance_row_data:
                clock_in_dt = attendance_row_data['출근시간_dt']
                clock_out_dt = attendance_row_data['퇴근시간_dt']
                clock_in_raw = attendance_row_data['출근시간_raw']
                clock_out_raw = attendance_row_data['퇴근시간_raw']

            actual_start_dt = combine_date_time(TARGET_DATE, clock_in_dt) if clock_in_dt else None
            actual_end_dt = combine_date_time(TARGET_DATE, clock_out_dt) if clock_out_dt else None
            has_clock_in = actual_start_dt is not None
            has_clock_out = actual_end_dt is not None

            # Determine effective leaves for *this* employee based on collected leaves
            # Re-evaluate morning/afternoon coverage based on *all* leaves collected for them
            current_has_morning_leave = False
            current_has_afternoon_leave = False
            leave_display_type = '출퇴근' # Default if no half-day leave
            leave_display_category = '정상' # Default

            if collected_leaves:
                 morning_leave_types = []
                 afternoon_leave_types = []
                 for leave in collected_leaves:
                     ls = leave['start']
                     le = leave['end']
                     lt = leave['type']
                     if lt == MORNING_HALF_LEAVE or (ls and le and ls <= standard_start_time and le >= lunch_start_time):
                         current_has_morning_leave = True
                         if lt: morning_leave_types.append(lt)
                     if lt == AFTERNOON_HALF_LEAVE or (ls and le and ls <= lunch_end_time and le >= standard_end_time):
                         current_has_afternoon_leave = True
                         if lt: afternoon_leave_types.append(lt)

                 # Set display type/category based on half-day leaves
                 if current_has_morning_leave and not current_has_afternoon_leave:
                      leave_display_type = MORNING_HALF_LEAVE
                      leave_display_category = " + ".join(sorted(list(set(morning_leave_types)))) or '오전반차'
                 elif not current_has_morning_leave and current_has_afternoon_leave:
                      leave_display_type = AFTERNOON_HALF_LEAVE
                      leave_display_category = " + ".join(sorted(list(set(afternoon_leave_types)))) or '오후반차'
                 # If both, technically excluded, but maybe handle as combined? Keep simple for now.
                 # If other leaves exist but don't cover half days, keep default '출퇴근'/'정상'

            # --- Determine Expected Times Based on Leaves ---
            expected_start_dt = datetime.datetime.combine(TARGET_DATE, afternoon_start_time) if current_has_morning_leave else standard_start_dt
            expected_end_dt = datetime.datetime.combine(TARGET_DATE, lunch_start_time) if current_has_afternoon_leave else standard_end_dt # Should end work before lunch if afternoon off

            # --- Check for Issues (Lateness, Early Leave, Missing Records) ---
            issues = []
            # Clock In Check
            if has_clock_in:
                analysis_result["summary"]["clocked_in"] += 1
                logging.debug(f"{name_trimmed}: Clocked IN at {clock_in_dt.strftime('%H:%M:%S')}. Expected start >= {expected_start_dt.strftime('%H:%M:%S')}.")
                # Check for lateness ONLY IF they don't have morning leave
                if not current_has_morning_leave and actual_start_dt > standard_start_dt:
                    late_duration = actual_start_dt - standard_start_dt
                    issues.append(f"지각: {escape_markdown(clock_in_dt.strftime('%H:%M:%S'))} \\({late_duration}\\)")
                # Check if they came in late AFTER morning leave
                elif current_has_morning_leave and actual_start_dt > expected_start_dt:
                     late_duration = actual_start_dt - expected_start_dt
                     issues.append(f"오전반차 후 지각: {escape_markdown(clock_in_dt.strftime('%H:%M:%S'))} \\(예상 {expected_start_dt.strftime('%H:%M')}, {late_duration} 늦음\\)")
            else:
                analysis_result["summary"]["missing_in"] += 1
                # Report missing clock-in ONLY IF they are not on morning leave
                if not current_has_morning_leave:
                    issues.append("출근 기록 없음")
                    logging.debug(f"{name_trimmed}: Missing clock IN (expected by {expected_start_dt.strftime('%H:%M:%S')}).")

            # Clock Out Check
            if has_clock_out:
                analysis_result["summary"]["clocked_out"] += 1
                logging.debug(f"{name_trimmed}: Clocked OUT at {clock_out_dt.strftime('%H:%M:%S')}. Expected end <= {expected_end_dt.strftime('%H:%M:%S')}.")
                # Check for early departure ONLY IF they don't have afternoon leave
                if not current_has_afternoon_leave and actual_end_dt < standard_end_dt:
                    early_duration = standard_end_dt - actual_end_dt
                    issues.append(f"조퇴: {escape_markdown(clock_out_dt.strftime('%H:%M:%S'))} \\({early_duration} 일찍\\)")
                # Check if they left early BEFORE afternoon leave started (less common check)
                elif current_has_afternoon_leave and actual_end_dt < expected_end_dt:
                     early_duration = expected_end_dt - actual_end_dt
                     issues.append(f"오후반차 전 조퇴: {escape_markdown(clock_out_dt.strftime('%H:%M:%S'))} \\(예상 {expected_end_dt.strftime('%H:%M')}, {early_duration} 일찍\\)")

            elif has_clock_in: # Only check missing clock-out if they actually clocked in
                 analysis_result["summary"]["missing_out"] += 1
                 # Report missing clock-out ONLY IF they are not on afternoon leave
                 if not current_has_afternoon_leave:
                     issues.append("퇴근 기록 없음")
                     logging.debug(f"{name_trimmed}: Missing clock OUT (expected by {expected_end_dt.strftime('%H:%M:%S')}).")
            # If not clocked in, missing clock out is implied, don't double-report


            # --- Compile Reports ---
            # Morning Report (Issues)
            if issues:
                # Join issues with commas, ensure proper escaping for the final message
                issue_string = ", ".join(issues)
                msg = f"*{name_escaped}*: {issue_string}"
                analysis_result["notifications"].append(msg)
                logging.info(f"{name_trimmed}: Issues detected - {issue_string}")

            # Evening Report (Detailed Status) - Always add non-excluded employees
            clock_in_status_str = clock_in_dt.strftime('%H:%M:%S') if has_clock_in else "기록없음"
            clock_out_status_str = clock_out_dt.strftime('%H:%M:%S') if has_clock_out else ("기록없음" if has_clock_in else "출근기록없음") # More specific status
            analysis_result["detailed_status"].append({
                'name': name_trimmed,
                'in_status': clock_in_status_str,
                'out_status': clock_out_status_str
            })

            # --- Prepare Data for Image ---
            processed_data_for_image.append({
                '이름': name_trimmed,
                '일자': TARGET_DATE_STR,
                '유형': leave_display_type, # '오전반차', '오후반차', or '출퇴근'
                '구분': leave_display_category, # Reason for half-day or '정상'
                '출근시간': clock_in_raw if clock_in_raw else ('-' if has_clock_in else '기록없음'), # Show raw time or status
                '퇴근시간': clock_out_raw if clock_out_raw else ('-' if has_clock_out else '기록없음')
            })

        # --- Finalize Analysis ---
        if processed_data_for_image:
             # Define columns for the output DataFrame, ensuring consistency
             image_df_cols = ['이름', '일자', '유형', '구분', '출근시간', '퇴근시간']
             analysis_result["df_processed"] = pd.DataFrame(processed_data_for_image, columns=image_df_cols)
             logging.info(f"Created final DataFrame for image generation with {len(analysis_result['df_processed'])} rows.")
        else:
             logging.warning("No data was processed to generate the image DataFrame (e.g., all employees excluded or filtered out).")
             analysis_result["df_processed"] = pd.DataFrame(columns=['이름', '일자', '유형', '구분', '출근시간', '퇴근시간']) # Create empty DF with cols

        # --- Sanity Checks on Summary Counts ---
        summary = analysis_result["summary"]
        calc_total = summary["target"] + summary["excluded"]
        if calc_total != summary["total_employees"]:
            logging.warning(f"Summary count mismatch! Total Parsed Employees ({summary['total_employees']}) != Target ({summary['target']}) + Excluded ({summary['excluded']}). Calculated Total={calc_total}")

        # Check target vs in/missing_in
        target_check = summary["clocked_in"] + summary["missing_in"]
        if summary["target"] != target_check:
             logging.warning(f"Target employee count mismatch! Target ({summary['target']}) != Clocked In ({summary['clocked_in']}) + Missing In ({summary['missing_in']}). Sum={target_check}")
        else:
             logging.debug("Target count validation (Target == ClockedIn + MissingIn) OK.")

        # Check clock_in vs out/missing_out
        # Note: Missing Out only counts if they Clocked In first.
        out_check = summary["clocked_out"] + summary["missing_out"]
        if summary["clocked_in"] != out_check:
             logging.warning(f"Clocked-In count mismatch! Clocked In ({summary['clocked_in']}) != Clocked Out ({summary['clocked_out']}) + Missing Out ({summary['missing_out']}). Sum={out_check}")
        else:
             logging.debug("Clocked-In count validation (ClockedIn == ClockedOut + MissingOut) OK.")


        logging.info(f"Attendance analysis complete. Summary: {analysis_result['summary']}")
        return analysis_result

    except KeyError as e:
        logging.error(f"Analysis failed due to KeyError: {e}. This often indicates a missing or renamed column in the Excel file.", exc_info=True)
        # Send error message immediately if possible
        error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n엑셀 파일 처리 중 필요한 컬럼\\({escape_markdown(str(e))}\\)을 찾을 수 없습니다\\. 엑셀 구조를 확인하세요\\."
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)
        # Indicate failure in result
        analysis_result["summary"]["total_employees"] = -1 # Special value indicates error
        return analysis_result
    except ValueError as e:
         logging.error(f"Analysis failed due to ValueError: {e}. This might be due to unexpected data formats (e.g., time).", exc_info=True)
         error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*\n데이터 처리 중 값 오류 발생: {escape_markdown(str(e))}\\."
         send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)
         analysis_result["summary"]["total_employees"] = -1
         return analysis_result
    except Exception as e:
        logging.error(f"An unexpected error occurred during attendance analysis: {e}", exc_info=True)
        error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 분석 중 예외 발생*\n오류: {escape_markdown(str(e))}"
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)
        analysis_result["summary"]["total_employees"] = -1
        return analysis_result


# --- Main Execution Logic ---
if __name__ == "__main__":
    script_start_time = time.time()
    logging.info(f"--- Attendance Bot Script started for date: {TARGET_DATE_STR} ---")

    # Environment variables are checked at the top now

    driver = None
    excel_file_data = None
    error_occurred = False # Track if any significant error happened
    analysis_result = {} # Store results from analysis function

    # Phase 1: Setup, Login, Download
    try:
        driver = setup_driver()
        cookies = login_and_get_cookies(driver, WEBMAIL_LOGIN_URL, WEBMAIL_ID_FIELD_ID, WEBMAIL_PW_FIELD_ID, WEBMAIL_USERNAME, WEBMAIL_PASSWORD)
        # No need to check cookies here, login function raises exception on failure

        excel_file_data = download_excel_report(REPORT_URL, cookies)
        # No need to check excel_file_data here, download function raises exception on failure

        logging.info("Successfully logged in and downloaded the Excel report.")

    except Exception as setup_err:
        logging.error(f"Critical error during setup/login/download phase: {setup_err}", exc_info=True)
        error_occurred = True
        # Send error message to Telegram
        error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 초기화 오류*\n단계: 설정/로그인/다운로드 중 오류 발생\n오류: {escape_markdown(str(setup_err))}"
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)
        # analysis_result will remain empty or default

    finally:
        # Ensure WebDriver is closed even if errors occurred
        if driver:
            try:
                driver.quit()
                logging.info("WebDriver closed successfully.")
            except (WebDriverException, NoSuchWindowException) as e:
                logging.warning(f"Non-critical error closing WebDriver: {e}")
            except Exception as e:
                logging.error(f"Unexpected error closing WebDriver: {e}", exc_info=True)

    # Phase 2: Analysis and Reporting (only if download succeeded)
    if excel_file_data and not error_occurred:
        logging.info("Proceeding with analysis and reporting...")
        try:
            analysis_result = analyze_attendance(excel_file_data, EXCEL_SHEET_NAME)

            # Check if analysis itself reported an error
            if not isinstance(analysis_result, dict) or analysis_result.get("summary", {}).get("total_employees", -1) == -1:
                logging.error("Analysis function indicated failure. Reporting may be skipped or incomplete.")
                # Error message should have been sent from within analyze_attendance
                error_occurred = True # Mark script as failed overall
            else:
                logging.info("Analysis successful. Preparing reports...")
                # --- Proceed with reporting using analysis_result ---
                now_local_dt = datetime.datetime.now() # Get current local time for evening check
                # Assuming the server/runner time zone is KST or configured correctly.
                # If running in UTC, need adjustment: EVENING_RUN_THRESHOLD_HOUR_UTC = 9 (for 6 PM KST)
                is_evening = now_local_dt.hour >= EVENING_RUN_THRESHOLD_HOUR
                logging.info(f"Current hour {now_local_dt.hour}, Evening run threshold {EVENING_RUN_THRESHOLD_HOUR}. Is evening? {is_evening}")

                attendance_issues = analysis_result.get("notifications", [])
                detailed_statuses = analysis_result.get("detailed_status", [])
                analysis_summary = analysis_result.get("summary", {})
                excluded_employees = analysis_result.get("excluded_employees", [])
                df_for_image = analysis_result.get("df_processed")

                # --- 1. Send Table Image ---
                if df_for_image is not None and not df_for_image.empty:
                    image_title = f"{TARGET_DATE_STR} 근태 현황 ({analysis_summary.get('target', 0)}명 확인, {analysis_summary.get('excluded', 0)}명 제외)"
                    image_filename = f"attendance_report_{TARGET_DATE_STR}.png" # Date-specific filename
                    image_path = create_table_image(df_for_image, image_title, image_filename)

                    if image_path:
                        logging.info(f"Attempting to send image: {image_path}")
                        caption = f"*{escape_markdown(TARGET_DATE_STR)} 근태 상세 현황*"
                        photo_sent = send_telegram_photo(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, image_path, caption)
                        if not photo_sent:
                             logging.error("Failed to send Telegram photo after successful creation.")
                             # Send text message indicating image failure
                             img_fail_msg = f"*{escape_markdown(TARGET_DATE_STR)} 이미지 전송 실패*\n표 이미지 생성은 성공했으나, 텔레그램 전송 중 오류가 발생했습니다\\."
                             send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, img_fail_msg)
                             error_occurred = True # Mark failure if photo send fails
                        else:
                             logging.info("Telegram photo sent successfully.")
                        # Clean up the image file
                        try:
                            Path(image_path).unlink(missing_ok=True)
                            logging.info(f"Deleted temporary image file: {image_path}")
                        except Exception as del_err:
                            logging.warning(f"Could not delete temporary image file {image_path}: {del_err}")
                    else:
                        logging.error("Failed to create table image. No image sent.")
                        # Send text message indicating image creation failure
                        img_fail_msg = f"*{escape_markdown(TARGET_DATE_STR)} 이미지 생성 실패*\n근태 현황 표 이미지를 생성할 수 없습니다\\."
                        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, img_fail_msg)
                        error_occurred = True # Mark failure if image creation fails
                elif df_for_image is None:
                    logging.warning("Analysis did not produce a DataFrame for image generation (possibly due to an error).")
                else: # df_for_image is empty
                    logging.info("DataFrame for image is empty (e.g., no target employees or all excluded). Skipping image generation.")
                    # Optionally send a message that there's no data for the image
                    # no_img_data_msg = f"*{escape_markdown(TARGET_DATE_STR)} 현황*:\n표 이미지 생성 대상 데이터 없음 \\(확인 대상 {analysis_summary.get('target', 0)}명\\)\\."
                    # send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, no_img_data_msg)


                # --- 2. Send Detailed Report (Morning Issues or Evening Status) ---
                escaped_date_header = escape_markdown(TARGET_DATE_STR)
                report_lines = []
                report_title = ""

                if is_evening:
                    report_title = f"🌙 *{escaped_date_header} 퇴근 근태 현황*"
                    logging.info("Generating evening detailed status report.")
                    if detailed_statuses:
                        for idx, status in enumerate(detailed_statuses):
                            # Format: 1. Name: In Status | Out Status
                            line = f"{idx + 1}\\. *{escape_markdown(status['name'])}*: {escape_markdown(status['in_status'])} \\| {escape_markdown(status['out_status'])}"
                            report_lines.append(line)
                        logging.info(f"Prepared {len(report_lines)} entries for evening status report.")
                    else:
                        logging.info("No non-excluded employees found for evening detailed status report.")
                        report_lines.append("_확인 대상 인원 없음_") # Indicate no targets
                else: # Morning run
                    report_title = f"☀️ *{escaped_date_header} 출근 근태 확인 필요*"
                    logging.info("Generating morning issue report.")
                    if attendance_issues:
                        # Issues are already formatted with name and escaped message from analysis function
                        for idx, issue_msg in enumerate(attendance_issues):
                             report_lines.append(f"{idx + 1}\\. {issue_msg}") # Issue msg is already escaped
                        logging.info(f"Prepared {len(report_lines)} entries for morning issue report.")
                    else:
                        logging.info("No specific morning attendance issues detected.")
                        report_lines.append("_특이사항 없음_") # Indicate no issues

                # Send the detailed report if there's content
                if report_lines:
                    msg_header = f"{report_title}\n{escape_markdown('-'*20)}\n"
                    msg_body = "\n".join(report_lines)
                    full_detailed_msg = msg_header + msg_body
                    logging.info(f"Sending detailed report ('{report_title}')...")
                    if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, full_detailed_msg):
                        logging.error("Failed to send detailed report message.")
                        error_occurred = True
                else:
                     # This case should ideally not happen due to the fallback messages above
                     logging.warning("No content generated for the detailed report (this shouldn't happen).")


                # --- 3. Send Summary Report ---
                logging.info("Generating summary report...")
                summary_title = ""
                summary_details = ""
                total = analysis_summary.get("total_employees", 0)
                target = analysis_summary.get("target", 0)
                excluded_count = analysis_summary.get("excluded", 0)
                clock_in = analysis_summary.get("clocked_in", 0)
                miss_in = analysis_summary.get("missing_in", 0)
                clock_out = analysis_summary.get("clocked_out", 0)
                miss_out = analysis_summary.get("missing_out", 0)

                if is_evening:
                    summary_title = f"🌙 *{escaped_date_header} 퇴근 현황 요약*"
                    summary_details = (
                        f"\\- 전체 인원: {total}명\n"
                        f"\\- 확인 대상: {target}명 \\(제외: {excluded_count}명\\)\n"
                        f"\\- 출근 기록: {clock_in}명 \\(미기록: {miss_in}명\\)\n"
                        f"\\- 퇴근 기록: {clock_out}명\n"
                        f"\\- 퇴근 미기록 \\(출근자 중\\): {miss_out}명"
                    )
                else: # Morning run
                    summary_title = f"☀️ *{escaped_date_header} 출근 현황 요약*"
                    summary_details = (
                        f"\\- 전체 인원: {total}명\n"
                        f"\\- 확인 대상: {target}명 \\(제외: {excluded_count}명\\)\n"
                        f"\\- 출근 기록: {clock_in}명\n"
                        f"\\- 출근 미기록: {miss_in}명"
                    )

                # Add excluded employee list to summary if available
                if excluded_employees:
                    # Excluded employee strings are already escaped and formatted in analysis function
                    excluded_items = "\n  ".join([f"\\- {item}" for item in excluded_employees])
                    summary_details += f"\n\n*제외 인원 ({excluded_count}명)*:\n  {excluded_items}"
                elif excluded_count > 0:
                     summary_details += f"\n\n*제외 인원 ({excluded_count}명)*: _(상세 목록 없음)_" # Should not happen if analysis worked

                full_summary_msg = f"{summary_title}\n{escape_markdown('-'*20)}\n{summary_details}"
                logging.info("Sending summary report...")
                if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, full_summary_msg):
                    logging.error("Failed to send summary report message.")
                    error_occurred = True


        except Exception as analysis_report_err:
            # Catch errors during the analysis/reporting *phase* (after successful download)
            logging.error(f"Error during analysis or reporting phase: {analysis_report_err}", exc_info=True)
            error_occurred = True
            error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(분석/보고 단계\\)\n오류: {escape_markdown(str(analysis_report_err))}"
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)

    elif not excel_file_data and not error_occurred:
        # Case where download function returned None but didn't raise an Exception (should not happen with current download logic)
        logging.error("Excel data is missing, but no initial error was flagged. This indicates an unexpected issue.")
        error_occurred = True
        error_msg = f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류*\n엑셀 데이터가 알 수 없는 이유로 비어있습니다\\. 로그를 확인하세요\\."
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)


    # Phase 3: Final Completion Message
    script_end_time = time.time()
    time_taken = script_end_time - script_start_time
    logging.info(f"--- Script finished in {time_taken:.2f} seconds ---")

    completion_status = "오류 발생" if error_occurred else "정상 완료"
    status_emoji = "❌" if error_occurred else "✅"
    escaped_final_date = escape_markdown(TARGET_DATE_STR)
    escaped_final_status = escape_markdown(completion_status)
    escaped_final_time = escape_markdown(f"{time_taken:.1f}")

    final_message = f"{status_emoji} *{escaped_final_date} 근태 확인 스크립트*: {escaped_final_status} \\(소요시간: {escaped_final_time}초\\)"

    # Try sending final status regardless of previous errors
    if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, final_message):
        logging.error("Failed to send the final completion status message.")
        # Exit code still depends on whether an error occurred during the main process
    else:
        logging.info("Final completion status message sent.")

    # Exit with appropriate code for GitHub Actions
    exit_code = 1 if error_occurred else 0
    logging.info(f"Exiting script with code: {exit_code}")
    exit(exit_code)
