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
# from selenium.webdriver.common.action_chains import ActionChains # 현재 직접 사용 안 함
from webdriver_manager.chrome import ChromeDriverManager
import logging
import io
import traceback
from pathlib import Path
import json
import re
import numpy as np
import sys
import os
import subprocess

# --- Configuration ---
DEFAULT_CONFIG = {
    "WEBMAIL_USERNAME": "", "WEBMAIL_PASSWORD": "",
    "TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": "",
    "SENDER_NAME": "근태 확인 봇",
}

# --- 경로 설정 강화 (기존 로직 유지) ---
if hasattr(sys, '_MEIPASS'):
    APP_ROOT_PATH = sys._MEIPASS
    PROGRAM_EXECUTABLE_NAME = Path(sys.executable).stem
else:
    APP_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    PROGRAM_EXECUTABLE_NAME = Path(__file__).stem

_USER_DATA_PATH_ENV_VAR = "KTMOS_BOT_USER_DATA_PATH"
_user_data_path_from_env = os.getenv(_USER_DATA_PATH_ENV_VAR)
_final_user_data_path = ""
_path_source_info_lines = []

if _user_data_path_from_env:
    _final_user_data_path = _user_data_path_from_env
    _path_source_info_lines.append(f"INFO: 환경 변수 '{_USER_DATA_PATH_ENV_VAR}'에서 사용자 데이터 경로 사용: {_final_user_data_path}")
else:
    try:
        user_home_documents = os.path.join(Path.home(), "Documents")
        if not os.path.isdir(user_home_documents):
            _path_source_info_lines.append(f"경고: 표준 '내 문서' 폴더를 찾을 수 없습니다 ('{user_home_documents}').")
            _final_user_data_path = ""
        else:
            program_specific_folder = os.path.join(user_home_documents, "ktMOS", PROGRAM_EXECUTABLE_NAME)
            _final_user_data_path = program_specific_folder
            _path_source_info_lines.append(f"INFO: 기본 사용자 데이터 경로를 '내 문서\\ktMOS\\{PROGRAM_EXECUTABLE_NAME}' 폴더로 설정: {_final_user_data_path}")
    except Exception as e:
        _path_source_info_lines.append(f"경고: 기본 '내 문서' 경로를 결정할 수 없습니다. 오류: {e}.")
        _final_user_data_path = ""

_path_set_and_validated = False
if _final_user_data_path:
    try:
        if not os.path.exists(_final_user_data_path):
            os.makedirs(_final_user_data_path, exist_ok=True)
            _path_source_info_lines.append(f"INFO: 사용자 데이터 디렉토리 생성: {_final_user_data_path}")
        elif not os.path.isdir(_final_user_data_path):
            _path_source_info_lines.append(f"오류: 지정된 사용자 데이터 경로 '{_final_user_data_path}'가 존재하지만 디렉토리가 아닙니다.")
            _final_user_data_path = ""

        if _final_user_data_path and os.path.isdir(_final_user_data_path):
             _path_set_and_validated = True
    except Exception as e:
        _path_source_info_lines.append(f"경고: 사용자 데이터 디렉토리 '{_final_user_data_path}'를 생성하거나 접근할 수 없습니다. 오류: {e}.")
        _final_user_data_path = ""

if not _path_set_and_validated:
    fallback_base_dir = ""
    if hasattr(sys, '_MEIPASS'):
        fallback_base_dir = os.path.dirname(sys.executable)
    elif os.getenv('GITHUB_WORKSPACE'):
        fallback_base_dir = os.getenv('GITHUB_WORKSPACE')
    else:
        fallback_base_dir = os.path.dirname(os.path.abspath(__file__))

    _final_user_data_path = fallback_base_dir
    _path_source_info_lines.append(f"INFO: 대체 경로 사용. 사용자 데이터는 다음 위치에 저장 시도: {_final_user_data_path}")
    try:
        if not os.path.exists(_final_user_data_path):
            os.makedirs(_final_user_data_path, exist_ok=True)
            _path_source_info_lines.append(f"INFO: 대체 사용자 데이터 디렉토리 생성: {_final_user_data_path}")
    except Exception as e_fallback_create:
        _final_user_data_path = os.getcwd()
        _path_source_info_lines.append(f"심각: 대체 디렉토리를 생성할 수 없습니다. 오류: {e_fallback_create}. 현재 작업 디렉토리를 사용합니다: {_final_user_data_path}")

USER_DATA_PATH = _final_user_data_path
CONFIG_FILE = os.path.join(USER_DATA_PATH, "work_day_config_headless.json")
LOG_FILE = os.path.join(USER_DATA_PATH, 'attendance_bot_headless.log')
DRIVERS_DIR = os.path.join(APP_ROOT_PATH, 'drivers')
ICON_FILE = os.path.join(APP_ROOT_PATH, 'work_day.ico')

# --- 로깅 설정 ---
logging.basicConfig(level=logging.INFO,
                    handlers=[
                        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
                        logging.StreamHandler(sys.stdout)
                    ],
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- 상수 ---
WEBMAIL_LOGIN_URL = "http://gw.ktmos.co.kr/mail2/loginPage.do"
WEBMAIL_ID_FIELD_ID = "userEmail"; WEBMAIL_PW_FIELD_ID = "userPw"
REPORT_DOWNLOAD_URL_TEMPLATE = "http://gw.ktmos.co.kr/owattend/rest/dclz/report/CompositeStatus/sumr/user/days/excel?startDate={date}&endDate={date}&deptSeq=1231&erpNumDisplayYn=Y"
EXCEL_SHEET_NAME = "세부현황_B"
STANDARD_START_TIME_STR = "09:00:00"; STANDARD_END_TIME_STR = "18:00:00"
EVENING_RUN_THRESHOLD_HOUR = 18
LEAVE_ACTIVITY_TYPES = {"법정휴가", "보상휴가", "출장", "교육", "공가", "병가", "경조휴가", "특별휴가"}
FULL_DAY_REASONS = {"연차", "출산휴가", "출산전후휴가", "청원휴가", "가족돌봄휴가", "특별휴가", "공가", "공상", "예비군훈련", "민방위훈련", "공로휴가", "병가"}
MORNING_HALF_LEAVE_REASON = "오전반차"; AFTERNOON_HALF_LEAVE_REASON = "오후반차"
NORMAL_WORK_TYPE = "출퇴근"; NORMAL_WORK_CATEGORY = "정상"
STD_WORK_START_TIME = datetime.time(9, 0); STD_WORK_END_TIME = datetime.time(18, 0)
STD_LUNCH_START_TIME = datetime.time(12, 0); STD_LUNCH_END_TIME = datetime.time(13, 0)
STD_MORNING_LEAVE_WORK_START = datetime.time(14, 0)
STD_AFTERNOON_LEAVE_WORK_END = datetime.time(14, 0)

# --- Helper Functions ---
def log_message(message, level="INFO"):
    # timestamp = datetime.datetime.now().strftime("%H:%M:%S") # 로깅 프레임워크가 시간 자동 추가
    # formatted_message = f"[{timestamp} {level}] {message}"
    if level == "ERROR":
        logging.error(message)
    elif level == "WARNING":
        logging.warning(message)
    elif level == "DEBUG":
        logging.debug(message)
    else:
        logging.info(message)

def get_chrome_version():
    try:
        if sys.platform == "win32":
            program_files_x86 = os.environ.get('ProgramFiles(x86)')
            program_files = os.environ.get('ProgramFiles')
            chrome_path_x86 = os.path.join(program_files_x86, 'Google', 'Chrome', 'Application', 'chrome.exe') if program_files_x86 else None
            chrome_path_pf = os.path.join(program_files, 'Google', 'Chrome', 'Application', 'chrome.exe') if program_files else None
            chrome_exe_path = None
            if chrome_path_x86 and os.path.exists(chrome_path_x86): chrome_exe_path = chrome_path_x86
            elif chrome_path_pf and os.path.exists(chrome_path_pf): chrome_exe_path = chrome_path_pf
            else: log_message("Chrome not found in standard Windows locations.", "WARNING"); return None
            cmd = f'powershell -command "(Get-Item \\"{chrome_exe_path}\\").VersionInfo.ProductVersion"'
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=False, encoding='utf-8')
        elif sys.platform.startswith("linux"):
            cmd = 'google-chrome --version'
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=False)
        else:
            log_message(f"Chrome version check not implemented for {sys.platform}", "WARNING")
            return None

        if result.returncode != 0 or not result.stdout:
            log_message(f"Chrome version command failed or returned empty. stderr: {result.stderr}", "ERROR")
            return None

        version_str = result.stdout.strip()
        if "Google Chrome" in version_str:
            version_str = version_str.split("Google Chrome ")[-1]

        log_message(f"Chrome version string: {version_str}")
        major_version = version_str.split('.')[0]
        if major_version.isdigit():
            log_message(f"Chrome major version: {major_version}")
            return int(major_version)
        else:
            log_message(f"Cannot parse version: {version_str}", "ERROR"); return None
    except FileNotFoundError: log_message("Version check command (powershell/google-chrome) not found.", "ERROR"); return None
    except Exception as e: log_message(f"Get version error: {e}", "ERROR"); logging.exception("Get Version Error"); return None

# --- Selenium/Requests/Parsing/Report Functions ---
def setup_driver():
    log_message("Setting up ChromeDriver...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions") # 추가된 옵션
    # User-Agent는 고정하거나, get_chrome_version()의 결과를 신뢰할 수 있을 때 동적으로 설정
    # 현재는 안정성을 위해 고정된 최신 버전대 User-Agent 사용
    options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36") # 예시 최신 UA
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    prefs = {"credentials_enable_service": False, "profile.password_manager_enabled": False}
    options.add_experimental_option("prefs", prefs)

    # ChromeDriver 로그 활성화 (디버깅용)
    # service_args = ['--verbose', f'--log-path={os.path.join(USER_DATA_PATH, "chromedriver.log")}'] # USER_DATA_PATH 사용
    service_args = [] # 기본값은 로그 없음

    service = None
    try:
        log_message("Attempting to install/setup ChromeDriver using webdriver-manager...")
        try:
            service = Service(ChromeDriverManager().install(), service_args=service_args)
        except Exception as wdm_error:
            log_message(f"webdriver-manager failed: {wdm_error}", "ERROR")
            log_message(f"Attempting to use ChromeDriverManager with a generic version.", "WARNING")
            try:
                service = Service(ChromeDriverManager().install(), service_args=service_args)
            except Exception as wdm_fallback_error:
                log_message(f"webdriver-manager fallback also failed: {wdm_fallback_error}", "ERROR")
                raise Exception(f"webdriver-manager failed to provide a ChromeDriver: {wdm_fallback_error}")

        log_message(f"Initializing WebDriver with service path: {service.path if service else 'N/A'}")
        driver = webdriver.Chrome(service=service, options=options)
        
        # === 페이지 로드 타임아웃 설정 ===
        driver.set_page_load_timeout(180) # 180초 (3분)으로 설정
        # ===============================
        
        driver.implicitly_wait(20) # 암시적 대기 시간 약간 증가 (기존 15초)
        log_message("ChromeDriver and WebDriver setup complete.")
        return driver
    except WebDriverException as e:
        log_message(f"WebDriverException on init: {e}", "ERROR")
        if "version mismatch" in str(e).lower(): log_message("Version mismatch likely.", "ERROR")
        logging.exception("WebDriver Init Error")
        raise
    except Exception as e:
        log_message(f"Unexpected WebDriver init error: {e}", "ERROR")
        logging.exception("WebDriver Init Error")
        raise

def login_and_get_cookies(driver, url, username_id, password_id, username, password):
    log_message(f"Navigating to login page: {url}")
    try:
        driver.get(url) # 페이지 로드 타임아웃은 setup_driver에서 설정됨
    except TimeoutException as e: # driver.get() 에서 페이지 로드 타임아웃 발생 시
        log_message(f"Page load timeout for {url}: {e}", "ERROR")
        # 스크린샷 저장 (GitHub Actions에서는 아티팩트로 저장해야 확인 가능)
        screenshot_path = os.path.join(USER_DATA_PATH, f"pageload_timeout_screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        try:
            driver.save_screenshot(screenshot_path)
            log_message(f"Screenshot saved to {screenshot_path} due to page load timeout.", "INFO")
        except Exception as scr_err:
            log_message(f"Failed to save screenshot on page load timeout: {scr_err}", "WARNING")
        raise Exception(f"페이지 로드 타임아웃 ({driver.get_timeouts()['pageLoad'] / 1000}초 초과): {url}") from e # 원본 예외 포함하여 다시 발생


    wait = WebDriverWait(driver, 60) # 요소 대기 시간 기존 45에서 60으로 증가
    time.sleep(5) # 페이지 렌더링 및 JS 실행 대기 시간 기존 3에서 5로 증가
    try:
        user_field = wait.until(EC.visibility_of_element_located((By.ID, username_id)));
        pw_field = wait.until(EC.visibility_of_element_located((By.ID, password_id)))
        user_field.clear(); time.sleep(0.2); user_field.send_keys(username); time.sleep(0.5)
        pw_field.clear(); time.sleep(0.2); pw_field.send_keys(password); time.sleep(0.5)
        pw_field.send_keys(Keys.RETURN); log_message(f"Submitted login.")
        post_login_locator = (By.ID, "btnWrite")
        wait.until(EC.presence_of_element_located((post_login_locator))); log_message("Login successful (Mail page loaded)."); time.sleep(2)
        cookies = {c['name']: c['value'] for c in driver.get_cookies()}; log_message(f"Extracted {len(cookies)} cookies."); return cookies
    except TimeoutException: # 요소 대기 타임아웃
        current_url = driver.current_url; log_message(f"Timeout waiting for post-login element ({post_login_locator[1]}). URL: {current_url}", "WARNING")
        screenshot_path = os.path.join(USER_DATA_PATH, f"login_element_timeout_screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        try:
            driver.save_screenshot(screenshot_path)
            log_message(f"Screenshot saved to {screenshot_path}", "INFO")
        except Exception as scr_err:
            log_message(f"Failed to save screenshot: {scr_err}", "WARNING")

        login_page_check_url = url.split('?')[0]
        if login_page_check_url in current_url:
            found_error = None;
            try:
                err_elements = driver.find_elements(By.CSS_SELECTOR, ".login_box .error, .error_msg, #errormsg, .warning, .alert, [class*='error'], [id*='error']")
                for el in err_elements:
                    if el.is_displayed() and el.text.strip(): found_error = el.text.strip(); log_message(f"Login failure message on page: '{found_error}'", "ERROR"); break
            except Exception as find_err: log_message(f"Could not search for login errors: {find_err}", "WARNING")
            if found_error: raise Exception(f"로그인 실패: {found_error}")
            else: raise Exception("로그인 실패: 타임아웃 (메일 페이지 로딩 실패 또는 로그인 정보 불일치)")
        else:
            log_message("Redirected away from login page, but expected element not found. Assuming login issue.", "WARNING");
            raise Exception(f"로그인 확인 실패: 메일 페이지의 예상 요소({post_login_locator[1]})를 찾을 수 없습니다.")
    except Exception as e:
        log_message(f"Unexpected login error: {e}", "ERROR"); logging.exception("Login error:"); raise

def download_excel_report(report_url, cookies):
    log_message(f"Downloading report: {report_url}"); session = requests.Session(); session.cookies.update(cookies)
    user_agent_string = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' # setup_driver와 일치 권장
    headers = { 'User-Agent': user_agent_string, 'Referer': WEBMAIL_LOGIN_URL.split('/mail2')[0]};
    log_message(f"Using User-Agent for download: {user_agent_string}", "DEBUG")
    try:
        response = session.get(report_url, headers=headers, stream=True, timeout=120);
        log_message(f"Download HTTP status: {response.status_code}"); response.raise_for_status()
        content_type = response.headers.get('Content-Type', '').lower();
        is_excel = any(m in content_type for m in ['excel', 'spreadsheetml', 'vnd.ms-excel', 'octet-stream'])
        if is_excel:
            excel_data = io.BytesIO(response.content); file_size = excel_data.getbuffer().nbytes; log_message(f"Downloaded Excel data ({file_size} bytes).")
            if file_size < 1024:
                log_message(f"Small file ({file_size} bytes). Checking content for potential errors.", "WARNING");
                try:
                    preview = excel_data.getvalue()[:500].decode('utf-8', errors='ignore')
                    if any(kw in preview.lower() for kw in ['error', '오류', '로그인', '권한', '세션', 'login', 'session', 'invalid']):
                        log_message(f"Small file content suggests error: {preview}", "ERROR"); return None
                except Exception as prev_err: log_message(f"Small file preview check failed: {prev_err}", "WARNING");
                excel_data.seek(0)
            return excel_data
        else:
            log_message(f"Downloaded content type is not Excel. Type: {content_type}", "ERROR")
            try:
                error_content = response.text[:1000]
                log_message(f"Non-excel content preview: {error_content}", "DEBUG")
            except Exception as text_err:
                log_message(f"Could not get text preview of non-excel content: {text_err}", "WARNING")
            return None
    except requests.exceptions.RequestException as e: log_message(f"Download error: {e}", "ERROR"); logging.exception("Download error:"); return None
    except Exception as e: log_message(f"Unexpected download error: {e}", "ERROR"); logging.exception("Download unexpected error:"); return None

def parse_time_robust(time_str):
    if pd.isna(time_str) or time_str == '-': return None; time_str = str(time_str).strip()
    if isinstance(time_str, datetime.time): return time_str;
    if isinstance(time_str, datetime.datetime): return time_str.time()
    if not time_str: return None
    for fmt in ('%H:%M:%S', '%H:%M', '%Y-%m-%d %H:%M:%S'):
        try: return datetime.datetime.strptime(time_str.split('.')[0], fmt).time()
        except ValueError: continue
    log_message(f"Could not parse time: {time_str}", "DEBUG")
    return None

def parse_date_robust(date_str):
    if pd.isna(date_str) or date_str == '-': return None; date_str = str(date_str).strip()
    if isinstance(date_str, datetime.date): return date_str
    if isinstance(date_str, datetime.datetime): return date_str.date()
    if not date_str: return None
    date_part = date_str.split(' ')[0]
    try: return datetime.datetime.strptime(date_part, '%Y-%m-%d').date()
    except ValueError: pass
    try:
        numeric_date = float(date_part)
        if 30000 < numeric_date < 60000:
             return pd.to_datetime(numeric_date, unit='D', origin='1899-12-30').date()
    except (ValueError, TypeError): pass
    log_message(f"Could not parse date: {date_str}", "DEBUG")
    return None

def combine_date_time(date_val, time_val):
    if isinstance(date_val, datetime.date) and isinstance(time_val, datetime.time):
        return datetime.datetime.combine(date_val, time_val)
    return None

def analyze_attendance(excel_data, sheet_name, target_date):
    log_message(f"Analyzing sheet '{sheet_name}' for {target_date.strftime('%Y-%m-%d')}.")
    target_date_str = target_date.strftime('%Y-%m-%d')
    analysis_result = {
        "notifications": [],
        "summary": {
            "total_employees": 0, "target": 0, "excluded": 0,
            "clocked_in": 0, "missing_in": 0, "clocked_out": 0, "missing_out": 0
        },
        "plain_text_report": "",
        "team_name": "팀"
    }
    employee_statuses = {}

    try:
        header_indices = [0, 1]; log_message(f"Reading Excel with header rows {header_indices[0]+1}-{header_indices[1]+1}.", "INFO")
        try:
             df = pd.read_excel(excel_data, sheet_name=sheet_name, header=header_indices)
        except ValueError as ve:
             if "Worksheet named" in str(ve) and sheet_name in str(ve):
                  log_message(f"FATAL: Excel sheet named '{sheet_name}' not found.", "ERROR")
                  analysis_result["summary"]["total_employees"] = -1; analysis_result["plain_text_report"] = f"{target_date_str} 분석 오류\n엑셀 시트 '{sheet_name}'을 찾을 수 없습니다."; return analysis_result
             else: raise
        log_message(f"Loaded {len(df)} rows.")

        original_multi_columns = df.columns; new_columns = []
        for col_tuple in original_multi_columns:
            level0 = str(col_tuple[0]).strip(); level1 = str(col_tuple[1]).strip(); level0 = '' if 'Unnamed:' in level0 else level0; level1 = '' if 'Unnamed:' in level1 else level1
            if level0 and level1 and level0 != level1: new_col = f"{level0}_{level1}"
            elif level1: new_col = level1
            elif level0: new_col = level0
            else: new_col = f"col_{len(new_columns)}"
            new_columns.append(new_col.strip('_'))
        df.columns = new_columns; log_message(f"Flattened columns: {df.columns.tolist()}", "DEBUG")
        if df.empty: log_message("Excel sheet empty.", "WARNING"); analysis_result["plain_text_report"] = f"{target_date_str} 분석 정보\n데이터 없음."; return analysis_result

        column_mapping = {
            'erp': ['ERP사번'], 'name': ['이름'], 'date': ['일자'],
            'dept': ['부서'],
            'type': ['근태_유형', '유형'], 'category': ['근태_구분', '구분'],
            'clock_in_time': ['출퇴근_출근시간', '출근시간'], 'clock_out_time': ['출퇴근_퇴근시간', '퇴근시간'],
            'leave_start_time': ['휴가/출장/교육 일시_시작시간', '시작시간'], 'leave_end_time': ['휴가/출장/교육 일시_종료시간', '종료시간'],
        }
        col_indices = {}; missing_cols = []; original_columns = df.columns.tolist()
        dept_column_original_name = None
        for key, potential_names in column_mapping.items():
            found = False
            for name in [p.strip() for p in potential_names]:
                for idx, col_name in enumerate(original_columns):
                    if name.lower() == col_name.lower():
                        col_indices[key] = idx
                        if key == 'dept': dept_column_original_name = col_name
                        found = True; break
                if found: break
            if not found and key != 'dept':
                 missing_cols.append(f"{key} (tried: {', '.join(potential_names)})")
            elif not found and key == 'dept':
                 log_message("Optional '부서' column not found, will use default team name.", "WARNING")

        if missing_cols:
            log_message(f"FATAL: Missing required columns: {', '.join(missing_cols)}", "ERROR")
            log_message(f"Available columns in Excel: {original_columns}", "DEBUG")
            analysis_result["summary"]["total_employees"] = -1
            analysis_result["plain_text_report"] = f"{target_date_str} 분석 오류\n필수 컬럼 누락: {', '.join(missing_cols)}\n사용 가능한 컬럼: {original_columns}"
            return analysis_result

        erp_col_name = df.columns[col_indices['erp']]; name_col_name = df.columns[col_indices['name']]
        if erp_col_name in df.columns: df[erp_col_name] = df[erp_col_name].astype(str).replace('nan', '').ffill().fillna('')
        if name_col_name in df.columns: df[name_col_name] = df[name_col_name].astype(str).replace('nan', '').ffill().fillna('')

        select_rename_map = {
            df.columns[col_indices['erp']]: 'ERP_ID', df.columns[col_indices['name']]: '이름',
            df.columns[col_indices['date']]: '일자', df.columns[col_indices['type']]: '유형',
            df.columns[col_indices['category']]: '구분', df.columns[col_indices['clock_in_time']]: '출근시간_raw',
            df.columns[col_indices['clock_out_time']]: '퇴근시간_raw', df.columns[col_indices['leave_start_time']]: '휴가시작시간_raw',
            df.columns[col_indices['leave_end_time']]: '휴가종료시간_raw',
        }
        dept_col_name_target = '부서_raw'
        if dept_column_original_name:
            select_rename_map[dept_column_original_name] = dept_col_name_target
            log_message(f"Mapping original column '{dept_column_original_name}' to '{dept_col_name_target}' for department.", "DEBUG")
        else:
            dept_col_name_target = None

        source_columns_to_keep = list(select_rename_map.keys())
        missing_source_cols = [col for col in source_columns_to_keep if col not in df.columns]
        if missing_source_cols:
            log_message(f"FATAL: Source columns mapped incorrectly or missing after flatten/case check: {missing_source_cols}", "ERROR")
            analysis_result["summary"]["total_employees"] = -1
            analysis_result["plain_text_report"] = f"{target_date_str} 분석 오류\n내부 컬럼 선택 오류."
            return analysis_result

        df_processed = df[source_columns_to_keep].copy(); df_processed = df_processed.rename(columns=select_rename_map)

        try:
            df_processed['일자_dt'] = df_processed['일자'].apply(parse_date_robust)
            df_processed['출근시간_dt'] = df_processed['출근시간_raw'].apply(parse_time_robust)
            df_processed['퇴근시간_dt'] = df_processed['퇴근시간_raw'].apply(parse_time_robust)
            df_processed['휴가시작시간_dt'] = df_processed['휴가시작시간_raw'].apply(parse_time_robust)
            df_processed['휴가종료시간_dt'] = df_processed['휴가종료시간_raw'].apply(parse_time_robust)
        except Exception as parse_err:
            log_message(f"FATAL: Data parsing error: {parse_err}", "ERROR"); logging.exception("Data parsing error:")
            analysis_result["summary"]["total_employees"] = -1; analysis_result["plain_text_report"] = f"{target_date_str} 분석 오류\n데이터 파싱 중 에러."
            return analysis_result

        df_filtered_by_date = df_processed[df_processed['일자_dt'] == target_date].copy()
        if df_filtered_by_date.empty:
            log_message(f"No data found for target date {target_date_str}.", "WARNING")
            analysis_result["summary"]["total_employees"] = 0; analysis_result["plain_text_report"] = f"{target_date_str} 분석 정보\n데이터 없음."
            return analysis_result

        employee_names = df_filtered_by_date['이름'].astype(str).str.strip().replace('', np.nan).dropna()
        analysis_result["summary"]["total_employees"] = employee_names.nunique()
        log_message(f"Total employees identified for {target_date_str}: {analysis_result['summary']['total_employees']} (based on unique names)")

        team_name = "팀"
        if dept_col_name_target and dept_col_name_target in df_filtered_by_date.columns and not df_filtered_by_date.empty:
            try:
                if df_filtered_by_date.index.size > 0:
                    dept_full_name_series = df_filtered_by_date[dept_col_name_target].dropna()
                    if not dept_full_name_series.empty:
                        dept_full_name_obj = dept_full_name_series.iloc[0]
                        dept_full_name = str(dept_full_name_obj).strip() if pd.notna(dept_full_name_obj) else ""
                        log_message(f"Attempting team name extraction from '{dept_col_name_target}': Value='{dept_full_name}'", "DEBUG")

                        if dept_full_name and '-' in dept_full_name:
                            parts = dept_full_name.split('-', 1)
                            split_parts = [p.strip() for p in parts if p.strip()]
                            log_message(f"Split result for '{dept_full_name}' using '-': {split_parts}", "DEBUG")
                            if len(split_parts) > 1: team_name = split_parts[1]
                            elif split_parts: team_name = split_parts[0]
                        elif dept_full_name and len(dept_full_name) < 20 :
                            team_name = dept_full_name
                        else:
                            log_message(f"Department string format not suitable or missing '-': '{dept_full_name}'. Using default team name.", "WARNING")
                    else:
                        log_message(f"'{dept_col_name_target}' column has no valid (non-NaN) values for team name extraction.", "WARNING")
                else:
                    log_message(f"Filtered DataFrame has no valid index for iloc[0] to extract team name.", "WARNING")
            except Exception as e:
                 log_message(f"Error extracting team name: {e}", "WARNING")
                 logging.exception("Team Name Extraction Error")
        else:
             if df_filtered_by_date.empty: log_message("Filtered data is empty, cannot extract team name.", "WARNING")
             elif not dept_col_name_target or dept_col_name_target not in df_filtered_by_date.columns: log_message(f"Column '{dept_col_name_target}' (mapped from '부서') not found in filtered data, cannot extract team name.", "WARNING")
             else: log_message("Cannot extract team name for unknown reason (dept column might be all NaN).", "WARNING")

        analysis_result['team_name'] = team_name
        log_message(f"Final team name stored in analysis_result: '{analysis_result['team_name']}'", "INFO")

        df_filtered_by_date['ERP_ID_Clean'] = df_filtered_by_date['ERP_ID'].astype(str).str.strip().replace(r'^(nan|None|)$', '', regex=True)
        valid_erp_rows_df = df_filtered_by_date[df_filtered_by_date['ERP_ID_Clean'] != ''].copy()

        if valid_erp_rows_df.empty:
            log_message("No rows with valid ERP IDs found after filtering. Cannot process details.", "WARNING")
            grouped_by_erp = pd.DataFrame().groupby(None)
            num_groups_processed = 0
        else:
            grouped_by_erp = valid_erp_rows_df.groupby('ERP_ID_Clean', sort=False)
            num_groups_processed = len(grouped_by_erp)
        log_message(f"Processing details for {num_groups_processed} unique ERP IDs.")


        for erp_id, group_df in grouped_by_erp:
            display_name = str(group_df['이름'].iloc[0]).strip();
            if not display_name: display_name = f"ID:{erp_id}"

            collected_leaves = []; attendance_data = {'clock_in': None, 'clock_out': None, 'raw_in': '', 'raw_out': ''}
            for _, row in group_df.iterrows():
                att_type = str(row.get('유형', '')).strip(); att_cat = str(row.get('구분', '')).strip()
                l_start = row.get('휴가시작시간_dt'); l_end = row.get('휴가종료시간_dt')
                c_in = row.get('출근시간_dt'); c_out = row.get('퇴근시간_dt')

                if att_type in LEAVE_ACTIVITY_TYPES:
                    desc = f"{att_type} ({att_cat})" if att_cat and att_cat != '-' else att_type
                    collected_leaves.append({'type': att_type, 'category': att_cat, 'start': l_start, 'end': l_end, 'desc': desc})
                if att_type == NORMAL_WORK_TYPE:
                    if c_in and not attendance_data['clock_in']:
                        attendance_data['clock_in'] = c_in
                        attendance_data['raw_in'] = str(row.get('출근시간_raw', ''))
                    if c_out:
                        attendance_data['clock_out'] = c_out
                        attendance_data['raw_out'] = str(row.get('퇴근시간_raw', ''))

            is_excluded = False; covers_morn = False; covers_aft = False;
            is_spec_morn_half = False; is_spec_aft_half = False
            min_l_start_actual = STD_WORK_END_TIME; max_l_end_actual = STD_WORK_START_TIME
            leave_descs = set()
            took_any_leave = bool(collected_leaves)

            if collected_leaves:
                for leave in collected_leaves:
                    ls, le, cat, desc = leave['start'], leave['end'], leave['category'], leave['desc']
                    leave_descs.add(desc); is_m = False; is_a = False

                    if cat == MORNING_HALF_LEAVE_REASON: is_m = True; is_spec_morn_half = True
                    elif cat == AFTERNOON_HALF_LEAVE_REASON: is_a = True; is_spec_aft_half = True
                    elif cat in FULL_DAY_REASONS:
                        if not (ls and le and (ls > STD_WORK_START_TIME or le < STD_WORK_END_TIME)):
                            is_m = True; is_a = True
                    elif ls and le:
                        if ls <= STD_WORK_START_TIME and le >= STD_LUNCH_START_TIME: is_m = True
                        if ls < STD_WORK_END_TIME and le >= STD_LUNCH_END_TIME: is_a = True
                    elif ls and not le and leave['type'] == '출장':
                        is_m = True; is_a = True

                    if is_m: covers_morn = True
                    if is_a: covers_aft = True
                    if ls and ls < min_l_start_actual: min_l_start_actual = ls
                    if le and le > max_l_end_actual: max_l_end_actual = le

            leave_detail_for_report = ""
            if covers_morn and covers_aft:
                is_excluded = True
                comb_desc = " + ".join(sorted(list(leave_descs)))
                time_str = ""
                is_full_day_type = any(c in FULL_DAY_REASONS or l['type'] == '출장' for l in collected_leaves for c in [l['category']])
                if is_full_day_type : time_str = " (종일)"
                elif min_l_start_actual != STD_WORK_END_TIME and max_l_end_actual != STD_WORK_START_TIME :
                    time_str = f" ({min_l_start_actual.strftime('%H:%M')} - {max_l_end_actual.strftime('%H:%M')})"
                leave_detail_for_report = f"{comb_desc}{time_str}"
            elif took_any_leave:
                 leave_detail_for_report = " + ".join(sorted(list(leave_descs)))


            employee_statuses[display_name] = {
                'name': display_name,
                'status': 'excluded' if is_excluded else 'target',
                'covers_morning': covers_morn,
                'covers_afternoon': covers_aft,
                'took_leave': took_any_leave,
                'leave_details': leave_detail_for_report,
                'has_clock_in': False,
                'has_clock_out': False,
                'in_time_str': "-",
                'out_time_str': "-",
                'issue_types': []
            }

            if not is_excluded:
                c_in_dt = attendance_data['clock_in']; c_out_dt = attendance_data['clock_out'];
                act_start = combine_date_time(target_date, c_in_dt) if c_in_dt else None;
                act_end = combine_date_time(target_date, c_out_dt) if c_out_dt else None
                has_in = act_start is not None; has_out = act_end is not None
                employee_statuses[display_name]['has_clock_in'] = has_in
                employee_statuses[display_name]['has_clock_out'] = has_out

                exp_start_time = STD_WORK_START_TIME
                if is_spec_morn_half: exp_start_time = STD_MORNING_LEAVE_WORK_START
                elif covers_morn: exp_start_time = STD_LUNCH_END_TIME

                exp_end_time = STD_WORK_END_TIME
                if is_spec_aft_half: exp_end_time = STD_AFTERNOON_LEAVE_WORK_END
                elif covers_aft:
                    min_afternoon_leave_start = STD_WORK_END_TIME; found_afternoon_start = False
                    for leave in collected_leaves:
                        ls = leave.get('start')
                        if ls and ls >= STD_LUNCH_START_TIME:
                            le = leave.get('end'); does_cover_afternoon = False; l_cat = leave.get('category', ''); l_type = leave.get('type', '')
                            if l_cat == AFTERNOON_HALF_LEAVE_REASON: does_cover_afternoon = True
                            elif l_cat in FULL_DAY_REASONS: does_cover_afternoon = True
                            elif ls < STD_WORK_END_TIME and le and le >= STD_LUNCH_END_TIME: does_cover_afternoon = True
                            elif ls < STD_WORK_END_TIME and not le and l_type == '출장': does_cover_afternoon = True
                            if does_cover_afternoon and ls < min_afternoon_leave_start:
                                min_afternoon_leave_start = ls; found_afternoon_start = True
                    if found_afternoon_start and min_afternoon_leave_start < STD_WORK_END_TIME:
                        exp_end_time = min_afternoon_leave_start
                    elif covers_aft and not found_afternoon_start:
                        exp_end_time = STD_LUNCH_START_TIME


                exp_start_dt = datetime.datetime.combine(target_date, exp_start_time)
                exp_end_dt = datetime.datetime.combine(target_date, exp_end_time)
                log_message(f"Debug {display_name}: covers_morn={covers_morn}, covers_aft={covers_aft}, spec_morn={is_spec_morn_half}, spec_aft={is_spec_aft_half} => Exp Start={exp_start_time}, Exp End={exp_end_time}", "DEBUG")

                issue_type_flags = []

                if has_in:
                    if act_start > exp_start_dt:
                        issue_type_flags.append("지각")
                elif not covers_morn:
                     issue_type_flags.append("출근 기록 없음")

                if has_out:
                    actual_end_time = act_end.time()
                    if not covers_aft and actual_end_time < STD_WORK_END_TIME :
                        issue_type_flags.append("조퇴")
                    elif covers_aft and actual_end_time < exp_end_time:
                        issue_type_flags.append("조퇴")
                elif not covers_aft and has_in :
                     issue_type_flags.append("퇴근 기록 없음")

                employee_statuses[display_name]['issue_types'] = issue_type_flags

                in_stat = c_in_dt.strftime('%H:%M:%S') if has_in else ("오전휴가" if covers_morn else "기록 없음")
                out_stat = "-"
                if has_out: out_stat = c_out_dt.strftime('%H:%M:%S')
                else:
                    if covers_aft:
                        leave_start_str = exp_end_time.strftime('%H:%M')
                        out_stat = f"오후휴가({leave_start_str}부터)"
                    elif has_in:
                        out_stat = "기록 없음"
                    elif not covers_morn :
                        out_stat = "미출근"

                employee_statuses[display_name]['in_time_str'] = in_stat
                employee_statuses[display_name]['out_time_str'] = out_stat

        final_target = sum(1 for s in employee_statuses.values() if s['status'] == 'target')
        final_excluded = sum(1 for s in employee_statuses.values() if s['status'] == 'excluded')
        final_c_in = sum(1 for s in employee_statuses.values() if s.get('status') == 'target' and s.get('has_clock_in', False))
        final_m_in = sum(1 for s in employee_statuses.values() if s.get('status') == 'target' and not s.get('covers_morning', False) and not s.get('has_clock_in', False))
        final_c_out = sum(1 for s in employee_statuses.values() if s.get('status') == 'target' and s.get('has_clock_out', False))
        final_m_out = sum(1 for s in employee_statuses.values() if s.get('status') == 'target' and \
                          (s.get('has_clock_in', False) or s.get('covers_morning', False)) and \
                          not s.get('covers_afternoon', False) and not s.get('has_clock_out', False))


        analysis_result["summary"]["target"] = final_target
        analysis_result["summary"]["excluded"] = final_excluded
        analysis_result["summary"]["clocked_in"] = final_c_in
        analysis_result["summary"]["missing_in"] = final_m_in
        analysis_result["summary"]["clocked_out"] = final_c_out
        analysis_result["summary"]["missing_out"] = final_m_out

        calc_total_processed = final_target + final_excluded
        if calc_total_processed != num_groups_processed and num_groups_processed > 0 :
            log_message(f"Count mismatch! Processed groups ({num_groups_processed}) != Target({final_target})+Excluded({final_excluded})={calc_total_processed}. Check ERP/Name uniqueness.", "WARNING")

        log_message(f"Analysis complete. {analysis_result['summary']['target']} target employees, {analysis_result['summary']['excluded']} excluded employees.")
        log_message(f"Final Summary Counts: Total(Name)={analysis_result['summary']['total_employees']}, Target={final_target}, Excl={final_excluded}, ClockedIn={final_c_in}, MissingIn={final_m_in}, ClockedOut={final_c_out}, MissingOut={final_m_out}")

        plain_text = []
        now = datetime.datetime.now().time()
        is_eve_run = now >= datetime.time(EVENING_RUN_THRESHOLD_HOUR, 0)
        summ = analysis_result["summary"]

        title = f"{target_date_str} {'퇴근' if is_eve_run else '출근'} 현황 요약"
        plain_text.append(title)
        plain_text.append('-'*30)
        plain_text.append(f"총 인원: {summ.get('total_employees', 0)}명 (기준: 이름)")
        target_count = summ.get('target', 0)
        excluded_count = summ.get('excluded', 0)
        plain_text.append(f"확인 대상: {target_count}명 (제외: {excluded_count}명)")

        clocked_in_count = summ.get('clocked_in', 0)
        not_yet_clocked_in_count = summ.get('missing_in', 0)
        plain_text.append(f"출근: {clocked_in_count}명 (미기록/오전휴가: {not_yet_clocked_in_count}명)")

        clocked_out_count = summ.get('clocked_out', 0)
        missing_out_count = summ.get('missing_out', 0)
        plain_text.append(f"퇴근: {clocked_out_count}명 (미기록/오후휴가: {missing_out_count}명)")

        leave_takers_list = []
        for name, status_info in sorted(employee_statuses.items()):
            if status_info.get('took_leave', False):
                 leave_detail = status_info.get('leave_details', '정보 없음')
                 leave_takers_list.append(f"- {name}: {leave_detail}")

        if leave_takers_list:
            plain_text.append(f"\n제외 및 휴가 인원 ({len(leave_takers_list)}명):")
            for item in leave_takers_list:
                plain_text.append(f"{item}")
        else:
            plain_text.append(f"\n제외 및 휴가 인원: 없음")

        plain_text.append('\n' + '='*30 + '\n')

        target_employee_details_list = []
        target_employee_count_for_list = 0
        for name, status_info in sorted(employee_statuses.items()):
             if status_info['status'] == 'target':
                 target_employee_count_for_list += 1
                 issue_string = ""
                 issue_types = status_info.get('issue_types', [])
                 if issue_types:
                      issue_string = f"[{'/'.join(issue_types)}] "

                 in_status = status_info.get('in_time_str', '-')
                 out_status = status_info.get('out_time_str', '-')
                 target_employee_details_list.append(f"{target_employee_count_for_list}. {name}: {issue_string}출근={in_status}, 퇴근={out_status}")

        if target_employee_details_list:
            plain_text.append(f"[{'퇴근' if is_eve_run else '출근'} 확인 대상 상세 현황] ({len(target_employee_details_list)}명)")
            plain_text.append('-'*30)
            plain_text.extend(target_employee_details_list)
        else:
            if analysis_result["summary"]["target"] == 0 and analysis_result["summary"]["excluded"] > 0:
                 plain_text.append(f"{target_date_str} 확인 대상 없음 (전원 휴가/제외됨).")
            elif analysis_result["summary"]["target"] == 0 and analysis_result["summary"]["excluded"] == 0:
                 plain_text.append(f"{target_date_str} 확인 대상 없음 (데이터 없음).")
            else:
                 plain_text.append(f"{target_date_str} 확인 대상 상세 정보 생성 오류.")

        analysis_result["plain_text_report"] = "\n".join(plain_text)
        log_message("Plain text report generated.")
        return analysis_result

    except pd.errors.EmptyDataError:
        log_message(f"Excel sheet '{sheet_name}' is empty or unreadable.", "ERROR")
        analysis_result["summary"]["total_employees"] = -1; analysis_result["plain_text_report"] = f"{target_date_str} 분석 오류\n엑셀 시트가 비어있거나 읽을 수 없습니다."; return analysis_result
    except KeyError as e:
        log_message(f"KeyError during analysis, likely a missing column after mapping: {e}", "ERROR")
        logging.exception("Analysis KeyError:")
        analysis_result["summary"]["total_employees"] = -1; analysis_result["plain_text_report"] = f"{target_date_str} 분석 중 컬럼 오류 발생: {e}"; return analysis_result
    except Exception as e:
        log_message(f"Unexpected analysis error: {e}", "ERROR"); logging.exception("Analysis unexpected error:")
        analysis_result["summary"]["total_employees"] = -1; analysis_result["plain_text_report"] = f"{target_date_str} 분석 중 예상치 못한 오류 발생: {e}"; return analysis_result

def send_telegram_message(bot_token, chat_id, message_text):
    if not bot_token or not chat_id:
        log_message("텔레그램 봇 토큰 또는 Chat ID가 설정되지 않았습니다. 메시지 전송을 건너뜁니다.", "ERROR")
        return False

    max_length = 4000
    messages_to_send = []

    if len(message_text) > max_length:
        log_message(f"메시지 길이가 너무 깁니다 ({len(message_text)}자). 분할하여 전송합니다.", "INFO")
        for i in range(0, len(message_text), max_length):
            messages_to_send.append(message_text[i:i + max_length])
    else:
        messages_to_send.append(message_text)

    all_sent_successfully = True
    for i, part_message in enumerate(messages_to_send):
        payload = {'chat_id': chat_id, 'text': part_message}
        send_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        try:
            response = requests.post(send_url, data=payload, timeout=30)
            response.raise_for_status()
            log_message(f"텔레그램 메시지 전송 성공 (부분 {i+1}/{len(messages_to_send)}). 응답: {response.json()}", "INFO")
            if len(messages_to_send) > 1 and i < len(messages_to_send) - 1 :
                time.sleep(1.5)
        except requests.exceptions.RequestException as e:
            log_message(f"텔레그램 메시지 전송 실패 (부분 {i+1}): {e}", "ERROR")
            if hasattr(e, 'response') and e.response is not None:
                log_message(f"텔레그램 응답 내용: {e.response.text}", "ERROR")
            all_sent_successfully = False
            break
    return all_sent_successfully


def run_report_process(config, run_identifier="Scheduled"):
    process_start_log = f"--- Starting report process ({run_identifier}) ---"
    log_message(process_start_log)

    script_start_time = time.time()
    target_date = datetime.date.today()
    target_date_str = target_date.strftime("%Y-%m-%d")
    report_url = REPORT_DOWNLOAD_URL_TEMPLATE.format(date=target_date_str)
    log_message(f"Target date: {target_date_str}")

    driver = None
    analysis_result = {}
    error_occurred = False
    final_status_message = ""
    telegram_sent_successfully = False

    try:
        log_message("Setting up WebDriver for the process...")
        driver = setup_driver()

        try:
            if not config.get("WEBMAIL_USERNAME") or not config.get("WEBMAIL_PASSWORD"):
                raise ValueError("웹메일 계정 정보(ID/PW)가 설정되지 않았습니다.")
            log_message("Attempting login...")
            cookies = login_and_get_cookies(driver, WEBMAIL_LOGIN_URL, WEBMAIL_ID_FIELD_ID, WEBMAIL_PW_FIELD_ID, config["WEBMAIL_USERNAME"], config["WEBMAIL_PASSWORD"])

            log_message("Attempting Excel download...")
            excel_file_data = download_excel_report(report_url, cookies)
            if excel_file_data is None:
                raise Exception("Excel download failed or returned empty/invalid data.")
            log_message("Excel downloaded successfully.")

        except Exception as phase1_err:
            error_occurred = True
            log_message(f"Setup/Login/Download Error: {phase1_err}", "ERROR")
            final_status_message = f"로그인 또는 다운로드 실패: {phase1_err}"
            logging.error(f"Process stopped during Setup/Login/Download: {phase1_err}")
            raise

        log_message("Proceeding with analysis...")
        try:
            analysis_result = analyze_attendance(excel_file_data, EXCEL_SHEET_NAME, target_date)
            if not analysis_result or analysis_result.get("summary", {}).get("total_employees", -1) == -1:
                error_occurred = True
                final_status_message = analysis_result.get("plain_text_report", "분석 실패 (상세 메시지 없음).")
                log_message(f"Analysis failed: {final_status_message}", "ERROR")
            else:
                log_message("Analysis completed successfully.")
        except Exception as phase2_err:
            error_occurred = True
            log_message(f"Analysis Error: {phase2_err}", "ERROR")
            logging.exception("Analysis error:")
            final_status_message = f"분석 오류: {phase2_err}"
            raise

        if not error_occurred and analysis_result and analysis_result.get("summary", {}).get("total_employees", -1) != -1:
            telegram_bot_token = config.get("TELEGRAM_BOT_TOKEN")
            telegram_chat_id = config.get("TELEGRAM_CHAT_ID")

            if telegram_bot_token and telegram_chat_id:
                report_text = analysis_result.get("plain_text_report", "보고서 내용을 가져올 수 없습니다.")
                team_name_from_analysis = analysis_result.get('team_name', '팀')
                
                message_title = f"[{config.get('SENDER_NAME', '근태봇')}] {target_date_str} {team_name_from_analysis} 근태 현황 ({run_identifier})"
                full_message = f"{message_title}\n{'-'*20}\n{report_text}"

                log_message("텔레그램으로 보고서 전송 시도...", "INFO")
                telegram_sent_successfully = send_telegram_message(telegram_bot_token, telegram_chat_id, full_message)

                if telegram_sent_successfully:
                    final_status_message = "텔레그램 메시지 발송 완료됨."
                else:
                    final_status_message = "텔레그램 메시지 발송 실패."
            else:
                log_message("텔레그램 봇 토큰 또는 Chat ID가 설정되지 않았습니다. 메시지 전송을 건너뜁니다.", "WARNING")
                final_status_message = final_status_message or "텔레그램 설정 누락으로 발송 건너뜀"
        elif error_occurred:
            log_message("이전 단계 오류로 인해 텔레그램 발송을 건너뜁니다.", "WARNING")
            final_status_message = final_status_message or "텔레그램 발송 건너뜀 (이전 단계 오류)"
        else:
            log_message("분석 결과가 유효하지 않아 텔레그램 발송을 건너뜁니다.", "WARNING")
            final_status_message = final_status_message or "텔레그램 발송 건너뜀 (분석 결과 없음)"

    except Exception as outer_err:
        log_message(f"Critical error in process ({run_identifier}): {outer_err}", "ERROR")
        logging.exception(f"Critical Process Error ({run_identifier})")
        error_occurred = True
        final_status_message = final_status_message or f"치명적 오류: {outer_err}"
        # 오류 발생 시 텔레그램으로 간략한 오류 알림 (선택 사항)
        if config.get("TELEGRAM_BOT_TOKEN") and config.get("TELEGRAM_CHAT_ID"):
            error_report_text = f"[{config.get('SENDER_NAME', '근태봇')}] {target_date_str} 자동 근태 보고 중 오류 발생 ({run_identifier})\n오류: {str(outer_err)[:500]}..." # 오류 메시지 일부만 전송
            try:
                send_telegram_message(config.get("TELEGRAM_BOT_TOKEN"), config.get("TELEGRAM_CHAT_ID"), error_report_text)
                log_message("오류 발생 사실을 텔레그램으로 알렸습니다.", "INFO")
            except Exception as tel_err_report_err:
                log_message(f"오류 알림 텔레그램 전송 실패: {tel_err_report_err}", "ERROR")


    finally:
        if driver:
            log_message("Process finished. Attempting to quit WebDriver...")
            try:
                driver.quit()
                log_message("WebDriver closed successfully.")
            except NoSuchWindowException:
                 log_message("WebDriver window already closed or inaccessible during quit.", "WARNING")
            except WebDriverException as e:
                if "disconnected" in str(e).lower() or "invalid session id" in str(e).lower() or "unable to connect" in str(e).lower():
                     log_message(f"WebDriver already disconnected or crashed before quit: {e}", "WARNING")
                else:
                     log_message(f"WebDriverException during quit: {e}", "WARNING")
            except Exception as e:
                log_message(f"Unexpected error during WebDriver quit: {e}", "WARNING")
        else:
             log_message("WebDriver instance was not available for quitting (likely setup failed).")

        script_end_time = time.time()
        time_taken = script_end_time - script_start_time

        if not error_occurred and telegram_sent_successfully:
             completion_status = "정상 완료 (텔레그램 발송 성공)"
        elif not error_occurred and not telegram_sent_successfully:
             completion_status = "처리 완료 (텔레그램 발송 실패 또는 건너뜀)"
        else:
             completion_status = "오류 발생"

        status_summary = final_status_message if final_status_message else completion_status
        final_log_message = f"--- Process ({run_identifier}) {completion_status} in {time_taken:.2f} seconds. Status: {status_summary} ---"
        log_message(final_log_message, "ERROR" if error_occurred else "INFO")


def load_config_headless():
    config = DEFAULT_CONFIG.copy()
    config["WEBMAIL_USERNAME"] = os.getenv("WEBMAIL_USERNAME", config["WEBMAIL_USERNAME"])
    config["WEBMAIL_PASSWORD"] = os.getenv("WEBMAIL_PASSWORD", config["WEBMAIL_PASSWORD"])
    config["TELEGRAM_BOT_TOKEN"] = os.getenv("TELEGRAM_BOT_TOKEN", config["TELEGRAM_BOT_TOKEN"])
    config["TELEGRAM_CHAT_ID"] = os.getenv("TELEGRAM_CHAT_ID", config["TELEGRAM_CHAT_ID"])
    config["SENDER_NAME"] = os.getenv("SENDER_NAME", config["SENDER_NAME"])

    required_env_vars = {
        "WEBMAIL_USERNAME": "웹메일 사용자 이름",
        "WEBMAIL_PASSWORD": "웹메일 비밀번호",
        "TELEGRAM_BOT_TOKEN": "텔레그램 봇 토큰",
        "TELEGRAM_CHAT_ID": "텔레그램 채팅 ID"
    }
    missing_vars = [name for var, name in required_env_vars.items() if not config.get(var)]
    if missing_vars:
        msg = f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}"
        log_message(msg, "ERROR")
        raise ValueError(msg)
    return config

if __name__ == "__main__":
    if _path_source_info_lines:
        for line in _path_source_info_lines:
            print(line)

    print(f"User Data Path: {USER_DATA_PATH}")
    print(f"Log File: {LOG_FILE}")
    # print(f"Config File (if used): {CONFIG_FILE}") # JSON 설정 파일 사용 안 함
    print(f"APP_ROOT_PATH (for bundled resources, if any): {APP_ROOT_PATH}")

    log_message(f"--- Starting Headless Attendance Bot (ktMOS_DG_Headless) ---")
    log_message(f"User Data Path set to: {USER_DATA_PATH}")
    log_message(f"Log file: {LOG_FILE}")

    loaded_config = None
    try:
        loaded_config = load_config_headless()
        run_identifier = f"Run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        run_report_process(loaded_config, run_identifier=run_identifier)
    except ValueError as ve:
        log_message(f"Configuration error: {ve}", "ERROR")
        sys.exit(1)
    except Exception as e:
        log_message(f"An unexpected error occurred in the main execution block: {e}", "ERROR")
        logging.exception("Fatal error during main execution.")
        if loaded_config and loaded_config.get("TELEGRAM_BOT_TOKEN") and loaded_config.get("TELEGRAM_CHAT_ID"):
           error_message = f"자동 근태 확인 봇 실행 중 심각한 오류 발생:\n{str(e)[:1000]}\n로그 파일을 확인하세요." # 오류 메시지 길이 제한
           try:
               send_telegram_message(loaded_config["TELEGRAM_BOT_TOKEN"], loaded_config["TELEGRAM_CHAT_ID"], error_message)
               log_message("심각한 오류 발생 사실을 텔레그램으로 알렸습니다.", "INFO")
           except Exception as tel_err_report_err:
               log_message(f"심각한 오류 알림 텔레그램 전송 실패: {tel_err_report_err}", "ERROR")
        sys.exit(1)
    finally:
        log_message("--- Headless Attendance Bot Finished ---")
