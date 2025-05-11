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
from selenium.webdriver.common.action_chains import ActionChains # 메일 본문 입력 시 필요할 수 있음 (현재는 텔레그램 사용)
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
# 기본 설정값은 참고용. 실제 중요한 값들은 환경 변수에서 가져옴.
DEFAULT_CONFIG = {
    "WEBMAIL_USERNAME": "", "WEBMAIL_PASSWORD": "",
    "TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": "", # 텔레그램 설정 추가
    "SENDER_NAME": "근태 확인 봇", # 텔레그램 메시지 제목 등에 활용 가능
    # 스케줄은 GitHub Actions cron으로 대체되므로 여기서 사용 안 함
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
            _final_user_data_path = "" # 폴백으로 이어지도록
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
    if hasattr(sys, '_MEIPASS'): # PyInstaller 번들
        fallback_base_dir = os.path.dirname(sys.executable)
    elif os.getenv('GITHUB_WORKSPACE'): # GitHub Actions 환경
        fallback_base_dir = os.getenv('GITHUB_WORKSPACE')
    else: # 일반 스크립트 실행
        fallback_base_dir = os.path.dirname(os.path.abspath(__file__))

    _final_user_data_path = fallback_base_dir
    _path_source_info_lines.append(f"INFO: 대체 경로 사용. 사용자 데이터는 다음 위치에 저장 시도: {_final_user_data_path}")
    try:
        if not os.path.exists(_final_user_data_path):
            os.makedirs(_final_user_data_path, exist_ok=True)
            _path_source_info_lines.append(f"INFO: 대체 사용자 데이터 디렉토리 생성: {_final_user_data_path}")
    except Exception as e_fallback_create:
        _final_user_data_path = os.getcwd() # 최후의 수단
        _path_source_info_lines.append(f"심각: 대체 디렉토리를 생성할 수 없습니다. 오류: {e_fallback_create}. 현재 작업 디렉토리를 사용합니다: {_final_user_data_path}")

USER_DATA_PATH = _final_user_data_path
CONFIG_FILE = os.path.join(USER_DATA_PATH, "work_day_config_headless.json") # 설정 파일 이름 변경 가능
LOG_FILE = os.path.join(USER_DATA_PATH, 'attendance_bot_headless.log')

# DRIVERS_DIR 및 ICON_FILE은 GUI가 없으므로 중요도 낮음. APP_ROOT_PATH는 참고용.
DRIVERS_DIR = os.path.join(APP_ROOT_PATH, 'drivers') # webdriver-manager 사용 시 덜 중요
ICON_FILE = os.path.join(APP_ROOT_PATH, 'work_day.ico') # 사용 안함

# --- 로깅 설정 ---
logging.basicConfig(level=logging.INFO,
                    handlers=[
                        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
                        logging.StreamHandler(sys.stdout) # 콘솔(GitHub Actions 로그)에도 출력
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
def log_message(message, level="INFO"): # log_to_gui 대체
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    formatted_message = f"[{timestamp} {level}] {message}"
    if level == "ERROR":
        logging.error(message)
    elif level == "WARNING":
        logging.warning(message)
    elif level == "DEBUG":
        logging.debug(message)
    else:
        logging.info(message)

def get_chrome_version():
    # GitHub Actions 환경에서는 Chrome이 미리 설치되어 있을 수 있으므로,
    # 버전 확인이 실패해도 webdriver-manager가 처리하도록 유도 가능.
    # 로컬 실행 시에는 이 함수가 유용.
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
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=False, encoding='utf-8') # check=False로 변경
        elif sys.platform.startswith("linux"): # GitHub Actions (Ubuntu)
            cmd = 'google-chrome --version'
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True, check=False)
        else: # macOS 등
            # macOS에서는 /Applications/Google Chrome.app/Contents/MacOS/Google Chrome --version
            log_message(f"Chrome version check not implemented for {sys.platform}", "WARNING")
            return None # webdriver-manager가 알아서 하도록

        if result.returncode != 0 or not result.stdout:
            log_message(f"Chrome version command failed or returned empty. stderr: {result.stderr}", "ERROR")
            return None

        version_str = result.stdout.strip()
        if "Google Chrome" in version_str: # Linux 출력 예: "Google Chrome 110.0.5481.77"
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
    options.add_argument("--headless") # Headless 모드 필수
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox") # Linux 환경(GitHub Actions)에서 중요
    options.add_argument("--disable-dev-shm-usage") # Linux 환경에서 공유 메모리 문제 방지
    options.add_argument("--window-size=1920,1080")
    # User-Agent는 필요에 따라 설정
    # chrome_major_version = get_chrome_version() # get_chrome_version이 불안정할 수 있으므로, 고정 UA 또는 webdriver-manager에 의존
    # ua_version = chrome_major_version if chrome_major_version else 118 # 최신 안정 버전으로 가정
    options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    prefs = {"credentials_enable_service": False, "profile.password_manager_enabled": False}
    options.add_experimental_option("prefs", prefs)

    service = None
    try:
        log_message("Attempting to install/setup ChromeDriver using webdriver-manager...")
        try:
            service = Service(ChromeDriverManager().install())
        except Exception as wdm_error: # webdriver_manager에서 특정 버전 찾기 실패 등
            log_message(f"webdriver-manager failed: {wdm_error}", "ERROR")
            log_message(f"Attempting to use ChromeDriverManager with a generic version.", "WARNING")
            try: # 특정 버전 지정 없이 시도
                service = Service(ChromeDriverManager().install())
            except Exception as wdm_fallback_error:
                log_message(f"webdriver-manager fallback also failed: {wdm_fallback_error}", "ERROR")
                raise Exception(f"webdriver-manager failed to provide a ChromeDriver: {wdm_fallback_error}")

        log_message(f"Initializing WebDriver with service path: {service.path if service else 'N/A'}")
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(15) # 암시적 대기 시간 증가
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
    log_message(f"Navigating to login page: {url}"); driver.get(url); wait = WebDriverWait(driver, 45); time.sleep(3) # 대기 시간 증가
    try:
        user_field = wait.until(EC.visibility_of_element_located((By.ID, username_id)));
        pw_field = wait.until(EC.visibility_of_element_located((By.ID, password_id)))
        user_field.clear(); time.sleep(0.2); user_field.send_keys(username); time.sleep(0.5)
        pw_field.clear(); time.sleep(0.2); pw_field.send_keys(password); time.sleep(0.5)
        pw_field.send_keys(Keys.RETURN); log_message(f"Submitted login.")
        post_login_locator = (By.ID, "btnWrite") # 메일 쓰기 버튼으로 로그인 성공 확인
        wait.until(EC.presence_of_element_located(post_login_locator)); log_message("Login successful (Mail page loaded)."); time.sleep(2)
        cookies = {c['name']: c['value'] for c in driver.get_cookies()}; log_message(f"Extracted {len(cookies)} cookies."); return cookies
    except TimeoutException:
        current_url = driver.current_url; log_message(f"Timeout waiting for post-login element ({post_login_locator[1]}). URL: {current_url}", "WARNING")
        # 스크린샷 저장 (GitHub Actions에서는 아티팩트로 저장 필요)
        # screenshot_path = os.path.join(USER_DATA_PATH, f"login_timeout_screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        # try:
        #     driver.save_screenshot(screenshot_path)
        #     log_message(f"Screenshot saved to {screenshot_path}", "INFO")
        # except Exception as scr_err:
        #     log_message(f"Failed to save screenshot: {scr_err}", "WARNING")

        login_page_check_url = url.split('?')[0]
        if login_page_check_url in current_url:
            found_error = None;
            try: # 로그인 실패 메시지 확인
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
    user_agent_string = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36' # Consistent UA
    headers = { 'User-Agent': user_agent_string, 'Referer': WEBMAIL_LOGIN_URL.split('/mail2')[0]};
    log_message(f"Using User-Agent for download: {user_agent_string}", "DEBUG")
    try:
        response = session.get(report_url, headers=headers, stream=True, timeout=120); # 타임아웃 증가
        log_message(f"Download HTTP status: {response.status_code}"); response.raise_for_status()
        content_type = response.headers.get('Content-Type', '').lower();
        is_excel = any(m in content_type for m in ['excel', 'spreadsheetml', 'vnd.ms-excel', 'octet-stream'])
        if is_excel:
            excel_data = io.BytesIO(response.content); file_size = excel_data.getbuffer().nbytes; log_message(f"Downloaded Excel data ({file_size} bytes).")
            if file_size < 1024: # 1KB 미만이면 내용 확인
                log_message(f"Small file ({file_size} bytes). Checking content for potential errors.", "WARNING");
                try:
                    preview = excel_data.getvalue()[:500].decode('utf-8', errors='ignore')
                    if any(kw in preview.lower() for kw in ['error', '오류', '로그인', '권한', '세션', 'login', 'session', 'invalid']):
                        log_message(f"Small file content suggests error: {preview}", "ERROR"); return None
                except Exception as prev_err: log_message(f"Small file preview check failed: {prev_err}", "WARNING");
                excel_data.seek(0) # 미리보기 후 포인터 초기화
            return excel_data
        else:
            log_message(f"Downloaded content type is not Excel. Type: {content_type}", "ERROR")
            # 내용을 텍스트로 찍어보기
            try:
                error_content = response.text[:1000] # 처음 1000자
                log_message(f"Non-excel content preview: {error_content}", "DEBUG")
            except Exception as text_err:
                log_message(f"Could not get text preview of non-excel content: {text_err}", "WARNING")
            return None
    except requests.exceptions.RequestException as e: log_message(f"Download error: {e}", "ERROR"); logging.exception("Download error:"); return None
    except Exception as e: log_message(f"Unexpected download error: {e}", "ERROR"); logging.exception("Download unexpected error:"); return None

# parse_time_robust, parse_date_robust, combine_date_time 함수는 기존과 동일하게 사용 가능
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
        if 30000 < numeric_date < 60000: # Excel 날짜 숫자 범위 근사치
             return pd.to_datetime(numeric_date, unit='D', origin='1899-12-30').date()
    except (ValueError, TypeError): pass
    log_message(f"Could not parse date: {date_str}", "DEBUG")
    return None

def combine_date_time(date_val, time_val):
    if isinstance(date_val, datetime.date) and isinstance(time_val, datetime.time):
        return datetime.datetime.combine(date_val, time_val)
    return None

def analyze_attendance(excel_data, sheet_name, target_date):
    # 이 함수는 기존 로직과 거의 동일하게 유지. log_to_gui를 log_message로 변경
    log_message(f"Analyzing sheet '{sheet_name}' for {target_date.strftime('%Y-%m-%d')}.")
    target_date_str = target_date.strftime('%Y-%m-%d')
    analysis_result = {
        "notifications": [], # 사용 안 할 수 있음
        "summary": {
            "total_employees": 0, "target": 0, "excluded": 0,
            "clocked_in": 0, "missing_in": 0, "clocked_out": 0, "missing_out": 0
        },
        "plain_text_report": "",
        "team_name": "팀" # 기본 팀 이름
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
            else: new_col = f"col_{len(new_columns)}" # 이름 없는 컬럼 처리
            new_columns.append(new_col.strip('_'))
        df.columns = new_columns; log_message(f"Flattened columns: {df.columns.tolist()}", "DEBUG")
        if df.empty: log_message("Excel sheet empty.", "WARNING"); analysis_result["plain_text_report"] = f"{target_date_str} 분석 정보\n데이터 없음."; return analysis_result

        # 컬럼 매핑 (엑셀 파일 형식에 따라 매우 중요)
        column_mapping = {
            'erp': ['ERP사번'], 'name': ['이름'], 'date': ['일자'],
            'dept': ['부서'], # 부서 컬럼이 있다면 사용
            'type': ['근태_유형', '유형'], 'category': ['근태_구분', '구분'],
            'clock_in_time': ['출퇴근_출근시간', '출근시간'], 'clock_out_time': ['출퇴근_퇴근시간', '퇴근시간'],
            'leave_start_time': ['휴가/출장/교육 일시_시작시간', '시작시간'], 'leave_end_time': ['휴가/출장/교육 일시_종료시간', '종료시간'],
        }
        col_indices = {}; missing_cols = []; original_columns = df.columns.tolist()
        dept_column_original_name = None
        for key, potential_names in column_mapping.items():
            found = False
            for name in [p.strip() for p in potential_names]: # 공백 제거
                for idx, col_name in enumerate(original_columns):
                    if name.lower() == col_name.lower(): # 대소문자 구분 없이 비교
                        col_indices[key] = idx
                        if key == 'dept': dept_column_original_name = col_name # 실제 부서 컬럼명 저장
                        found = True; break
                if found: break
            if not found and key != 'dept': # 부서는 선택 사항
                 missing_cols.append(f"{key} (tried: {', '.join(potential_names)})")
            elif not found and key == 'dept':
                 log_message("Optional '부서' column not found, will use default team name.", "WARNING")

        if missing_cols:
            log_message(f"FATAL: Missing required columns: {', '.join(missing_cols)}", "ERROR")
            log_message(f"Available columns in Excel: {original_columns}", "DEBUG")
            analysis_result["summary"]["total_employees"] = -1
            analysis_result["plain_text_report"] = f"{target_date_str} 분석 오류\n필수 컬럼 누락: {', '.join(missing_cols)}\n사용 가능한 컬럼: {original_columns}"
            return analysis_result

        # ERP, 이름 컬럼은 데이터 채우기 (ffill)
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
        if dept_column_original_name: # 부서 컬럼이 실제로 존재하면 매핑에 추가
            select_rename_map[dept_column_original_name] = dept_col_name_target
            log_message(f"Mapping original column '{dept_column_original_name}' to '{dept_col_name_target}' for department.", "DEBUG")
        else:
            dept_col_name_target = None # 부서 컬럼 사용 안 함

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

        # 팀 이름 추출 로직 (기존과 동일)
        team_name = "팀" # 기본값
        if dept_col_name_target and dept_col_name_target in df_filtered_by_date.columns and not df_filtered_by_date.empty:
            try:
                if df_filtered_by_date.index.size > 0: # 데이터가 있는지 확인
                    # 첫 번째 유효한 부서명을 가져옴 (모든 직원이 같은 부서라고 가정)
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
                            elif split_parts: team_name = split_parts[0] # '-' 뒤가 없으면 앞부분 사용
                        elif dept_full_name and len(dept_full_name) < 20 : # '-'가 없고 너무 길지 않으면 전체 사용
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


        # --- 직원별 분석 로직 (기존과 동일, log_to_gui -> log_message) ---
        # ERP ID 기준으로 그룹화
        df_filtered_by_date['ERP_ID_Clean'] = df_filtered_by_date['ERP_ID'].astype(str).str.strip().replace(r'^(nan|None|)$', '', regex=True)
        valid_erp_rows_df = df_filtered_by_date[df_filtered_by_date['ERP_ID_Clean'] != ''].copy()

        if valid_erp_rows_df.empty:
            log_message("No rows with valid ERP IDs found after filtering. Cannot process details.", "WARNING")
            grouped_by_erp = pd.DataFrame().groupby(None) # 빈 그룹
            num_groups_processed = 0
        else:
            grouped_by_erp = valid_erp_rows_df.groupby('ERP_ID_Clean', sort=False)
            num_groups_processed = len(grouped_by_erp)
        log_message(f"Processing details for {num_groups_processed} unique ERP IDs.")


        for erp_id, group_df in grouped_by_erp:
            display_name = str(group_df['이름'].iloc[0]).strip();
            if not display_name: display_name = f"ID:{erp_id}" # 이름이 없는 경우 ERP ID로 대체

            collected_leaves = []; attendance_data = {'clock_in': None, 'clock_out': None, 'raw_in': '', 'raw_out': ''}
            for _, row in group_df.iterrows():
                att_type = str(row.get('유형', '')).strip(); att_cat = str(row.get('구분', '')).strip()
                l_start = row.get('휴가시작시간_dt'); l_end = row.get('휴가종료시간_dt')
                c_in = row.get('출근시간_dt'); c_out = row.get('퇴근시간_dt')

                if att_type in LEAVE_ACTIVITY_TYPES:
                    desc = f"{att_type} ({att_cat})" if att_cat and att_cat != '-' else att_type
                    collected_leaves.append({'type': att_type, 'category': att_cat, 'start': l_start, 'end': l_end, 'desc': desc})
                if att_type == NORMAL_WORK_TYPE: # "출퇴근" 유형
                    if c_in and not attendance_data['clock_in']: # 첫 출근 기록만 인정
                        attendance_data['clock_in'] = c_in
                        attendance_data['raw_in'] = str(row.get('출근시간_raw', ''))
                    if c_out: # 퇴근은 마지막 기록으로 덮어쓰기 가능 (또는 필요시 로직 수정)
                        attendance_data['clock_out'] = c_out
                        attendance_data['raw_out'] = str(row.get('퇴근시간_raw', ''))

            # 휴가/제외 로직 (기존과 유사)
            is_excluded = False; covers_morn = False; covers_aft = False;
            is_spec_morn_half = False; is_spec_aft_half = False # 명시적 반차 여부
            min_l_start_actual = STD_WORK_END_TIME; max_l_end_actual = STD_WORK_START_TIME # 실제 휴가 시간 범위
            leave_descs = set() # 휴가 종류 설명 집합
            took_any_leave = bool(collected_leaves)

            if collected_leaves:
                for leave in collected_leaves:
                    ls, le, cat, desc = leave['start'], leave['end'], leave['category'], leave['desc']
                    leave_descs.add(desc); is_m = False; is_a = False # 이번 휴가가 오전/오후 커버하는지

                    if cat == MORNING_HALF_LEAVE_REASON: is_m = True; is_spec_morn_half = True
                    elif cat == AFTERNOON_HALF_LEAVE_REASON: is_a = True; is_spec_aft_half = True
                    elif cat in FULL_DAY_REASONS:
                        # 종일 휴가인데 시간이 이상하게 찍힌 경우 (예: 09:00-09:00) 제외
                        if not (ls and le and (ls > STD_WORK_START_TIME or le < STD_WORK_END_TIME)):
                            is_m = True; is_a = True
                    elif ls and le: # 시간 범위가 있는 휴가
                        if ls <= STD_WORK_START_TIME and le >= STD_LUNCH_START_TIME: is_m = True # 오전 근무 시간 커버
                        if ls < STD_WORK_END_TIME and le >= STD_LUNCH_END_TIME: is_a = True # 오후 근무 시간 커버
                    elif ls and not le and leave['type'] == '출장': # 출장인데 종료시간이 없는 경우 종일로 간주
                        is_m = True; is_a = True

                    if is_m: covers_morn = True
                    if is_a: covers_aft = True
                    if ls and ls < min_l_start_actual: min_l_start_actual = ls
                    if le and le > max_l_end_actual: max_l_end_actual = le

            leave_detail_for_report = ""
            if covers_morn and covers_aft: # 오전, 오후 모두 휴가 등으로 커버되면 제외 대상
                is_excluded = True
                comb_desc = " + ".join(sorted(list(leave_descs)))
                time_str = ""
                # 종일 유형인지, 아니면 시간 범위인지 표시
                is_full_day_type = any(c in FULL_DAY_REASONS or l['type'] == '출장' for l in collected_leaves for c in [l['category']])
                if is_full_day_type : time_str = " (종일)"
                elif min_l_start_actual != STD_WORK_END_TIME and max_l_end_actual != STD_WORK_START_TIME :
                    time_str = f" ({min_l_start_actual.strftime('%H:%M')} - {max_l_end_actual.strftime('%H:%M')})"
                leave_detail_for_report = f"{comb_desc}{time_str}"
            elif took_any_leave: # 일부만 휴가인 경우
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
                'in_time_str': "-", # 보고서용 출근 시간 문자열
                'out_time_str': "-", # 보고서용 퇴근 시간 문자열
                'issue_types': [] # 지각, 조퇴 등
            }

            # 확인 대상 직원에 대한 처리
            if not is_excluded:
                c_in_dt = attendance_data['clock_in']; c_out_dt = attendance_data['clock_out'];
                act_start = combine_date_time(target_date, c_in_dt) if c_in_dt else None;
                act_end = combine_date_time(target_date, c_out_dt) if c_out_dt else None
                has_in = act_start is not None; has_out = act_end is not None
                employee_statuses[display_name]['has_clock_in'] = has_in
                employee_statuses[display_name]['has_clock_out'] = has_out

                # 예상 근무 시작/종료 시간 계산 (휴가 고려)
                exp_start_time = STD_WORK_START_TIME
                if is_spec_morn_half: exp_start_time = STD_MORNING_LEAVE_WORK_START # 명시적 오전반차
                elif covers_morn: exp_start_time = STD_LUNCH_END_TIME # 그 외 오전 커버 휴가

                exp_end_time = STD_WORK_END_TIME
                if is_spec_aft_half: exp_end_time = STD_AFTERNOON_LEAVE_WORK_END # 명시적 오후반차
                elif covers_aft: # 그 외 오후 커버 휴가 시, 실제 오후 휴가 시작 시간을 기준으로 예상 퇴근 시간 조정
                    min_afternoon_leave_start = STD_WORK_END_TIME; found_afternoon_start = False
                    for leave in collected_leaves:
                        ls = leave.get('start')
                        if ls and ls >= STD_LUNCH_START_TIME: # 점심시간 이후 시작하는 휴가만 고려
                            le = leave.get('end'); does_cover_afternoon = False; l_cat = leave.get('category', ''); l_type = leave.get('type', '')
                            if l_cat == AFTERNOON_HALF_LEAVE_REASON: does_cover_afternoon = True
                            elif l_cat in FULL_DAY_REASONS: does_cover_afternoon = True # 오후반차 외 종일휴가도 오후 커버
                            elif ls < STD_WORK_END_TIME and le and le >= STD_LUNCH_END_TIME: does_cover_afternoon = True
                            elif ls < STD_WORK_END_TIME and not le and l_type == '출장': does_cover_afternoon = True # 종료시간 없는 출장
                            if does_cover_afternoon and ls < min_afternoon_leave_start:
                                min_afternoon_leave_start = ls; found_afternoon_start = True
                    if found_afternoon_start and min_afternoon_leave_start < STD_WORK_END_TIME:
                        exp_end_time = min_afternoon_leave_start
                    elif covers_aft and not found_afternoon_start: # 오후를 커버하지만, 구체적인 오후 휴가 시작 시간 못찾음 (예: 교육)
                        exp_end_time = STD_LUNCH_START_TIME # 점심시간까지만 근무로 간주


                exp_start_dt = datetime.datetime.combine(target_date, exp_start_time)
                exp_end_dt = datetime.datetime.combine(target_date, exp_end_time)
                log_message(f"Debug {display_name}: covers_morn={covers_morn}, covers_aft={covers_aft}, spec_morn={is_spec_morn_half}, spec_aft={is_spec_aft_half} => Exp Start={exp_start_time}, Exp End={exp_end_time}", "DEBUG")


                current_issues = [] # 텍스트 설명용 (사용 안 함, issue_types로 대체)
                issue_type_flags = [] # 보고서용 flag

                # 출근 확인
                if has_in:
                    if act_start > exp_start_dt: # 예상 출근시간보다 늦음
                        # start_info = f"({exp_start_time.strftime('%H:%M')}부터) " if exp_start_time != STD_WORK_START_TIME else ""
                        # leave_prefix = "오전휴가 후 " if covers_morn else ""
                        # current_issues.append(f"{leave_prefix}{start_info}지각: {c_in_dt.strftime('%H:%M:%S')}")
                        issue_type_flags.append("지각")
                elif not covers_morn: # 오전 휴가도 없는데 출근 기록 없음
                     # current_issues.append("출근 기록 없음")
                     issue_type_flags.append("출근 기록 없음")

                # 퇴근 확인
                if has_out:
                    actual_end_time = act_end.time()
                    # 오후 휴가 없이, 표준 퇴근 시간 또는 예상 퇴근 시간(오후 휴가 고려)보다 일찍 퇴근
                    if not covers_aft and actual_end_time < STD_WORK_END_TIME :
                        # current_issues.append(f"조퇴: {c_out_dt.strftime('%H:%M:%S')}")
                        issue_type_flags.append("조퇴")
                    elif covers_aft and actual_end_time < exp_end_time: # 오후 휴가 있는데, 그 휴가 시작 시간보다도 일찍 퇴근
                        # current_issues.append(f"조퇴({exp_end_time.strftime('%H:%M')} 이전): {c_out_dt.strftime('%H:%M:%S')}")
                        issue_type_flags.append("조퇴")
                elif not covers_aft and has_in : # 오후 휴가도 없고, 출근은 했는데 퇴근 기록 없음
                     # current_issues.append("퇴근 기록 없음")
                     issue_type_flags.append("퇴근 기록 없음")

                # if current_issues: # 사용 안 함
                #     analysis_result["notifications"].append(f"{display_name}: {', '.join(current_issues)}")
                employee_statuses[display_name]['issue_types'] = issue_type_flags

                # 보고서용 출퇴근 시간 문자열 설정
                in_stat = c_in_dt.strftime('%H:%M:%S') if has_in else ("오전휴가" if covers_morn else "기록 없음")
                out_stat = "-" # 기본값
                if has_out: out_stat = c_out_dt.strftime('%H:%M:%S')
                else: # 퇴근 기록이 없을 때
                    if covers_aft: # 오후 휴가면
                        leave_start_str = exp_end_time.strftime('%H:%M') # 오후 휴가 시작 시간
                        out_stat = f"오후휴가({leave_start_str}부터)"
                    elif has_in: # 출근은 했는데 퇴근 기록 없고 오후 휴가도 아님
                        out_stat = "기록 없음"
                    elif not covers_morn : # 출근도 안했고 오전휴가도 아님 (사실상 미출근)
                        out_stat = "미출근" # in_stat도 "기록 없음"일 것임

                employee_statuses[display_name]['in_time_str'] = in_stat
                employee_statuses[display_name]['out_time_str'] = out_stat

        # 최종 요약 카운트 (기존과 동일)
        final_target = sum(1 for s in employee_statuses.values() if s['status'] == 'target')
        final_excluded = sum(1 for s in employee_statuses.values() if s['status'] == 'excluded')
        final_c_in = sum(1 for s in employee_statuses.values() if s.get('status') == 'target' and s.get('has_clock_in', False))
        final_m_in = sum(1 for s in employee_statuses.values() if s.get('status') == 'target' and not s.get('covers_morning', False) and not s.get('has_clock_in', False))
        final_c_out = sum(1 for s in employee_statuses.values() if s.get('status') == 'target' and s.get('has_clock_out', False))
        # 퇴근 미기록: 확인 대상이고, (출근했거나 오전휴가였고), 오후휴가가 아닌데, 퇴근 기록이 없는 경우
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
        if calc_total_processed != num_groups_processed and num_groups_processed > 0 : # 0명일때는 경고 안띄움
            log_message(f"Count mismatch! Processed groups ({num_groups_processed}) != Target({final_target})+Excluded({final_excluded})={calc_total_processed}. Check ERP/Name uniqueness.", "WARNING")

        log_message(f"Analysis complete. {analysis_result['summary']['target']} target employees, {analysis_result['summary']['excluded']} excluded employees.")
        log_message(f"Final Summary Counts: Total(Name)={analysis_result['summary']['total_employees']}, Target={final_target}, Excl={final_excluded}, ClockedIn={final_c_in}, MissingIn={final_m_in}, ClockedOut={final_c_out}, MissingOut={final_m_out}")


        # 텍스트 보고서 생성 (기존과 동일)
        plain_text = []
        now = datetime.datetime.now().time()
        is_eve_run = now >= datetime.time(EVENING_RUN_THRESHOLD_HOUR, 0) # 저녁 실행 여부 (제목에 반영)
        summ = analysis_result["summary"]

        # 텔레그램은 Markdown 등을 지원하므로, 필요시 마크다운 문자 추가 가능
        title = f"{target_date_str} {'퇴근' if is_eve_run else '출근'} 현황 요약"
        plain_text.append(title)
        plain_text.append('-'*30) # 구분선
        plain_text.append(f"총 인원: {summ.get('total_employees', 0)}명 (기준: 이름)")
        target_count = summ.get('target', 0)
        excluded_count = summ.get('excluded', 0)
        plain_text.append(f"확인 대상: {target_count}명 (제외: {excluded_count}명)")

        clocked_in_count = summ.get('clocked_in', 0)
        # 출근 미기록자 수 = 확인 대상자 중 (오전 휴가가 아니고 and 출근 기록이 없는 사람)
        not_yet_clocked_in_count = summ.get('missing_in', 0)
        plain_text.append(f"출근: {clocked_in_count}명 (미기록/오전휴가: {not_yet_clocked_in_count}명)")

        clocked_out_count = summ.get('clocked_out', 0)
        # 퇴근 미기록자 수 = 확인 대상자 중 (오후 휴가가 아니고 and (출근했거나 오전휴가였고) and 퇴근 기록 없는 사람)
        missing_out_count = summ.get('missing_out', 0)
        plain_text.append(f"퇴근: {clocked_out_count}명 (미기록/오후휴가: {missing_out_count}명)")


        # 휴가자 및 제외자 명단
        leave_takers_list = []
        for name, status_info in sorted(employee_statuses.items()): # 이름순 정렬
            if status_info.get('took_leave', False): # 휴가를 하나라도 갔거나
                 leave_detail = status_info.get('leave_details', '정보 없음')
                 leave_takers_list.append(f"- {name}: {leave_detail}")

        if leave_takers_list:
            plain_text.append(f"\n제외 및 휴가 인원 ({len(leave_takers_list)}명):")
            for item in leave_takers_list:
                plain_text.append(f"{item}")
        else:
            plain_text.append(f"\n제외 및 휴가 인원: 없음")

        plain_text.append('\n' + '='*30 + '\n') # 구분선

        # 확인 대상자 상세 현황
        target_employee_details_list = []
        target_employee_count_for_list = 0
        for name, status_info in sorted(employee_statuses.items()): # 이름순 정렬
             if status_info['status'] == 'target':
                 target_employee_count_for_list += 1
                 issue_string = ""
                 issue_types = status_info.get('issue_types', [])
                 if issue_types:
                      issue_string = f"[{'/'.join(issue_types)}] " # 예: [지각/조퇴]

                 in_status = status_info.get('in_time_str', '-')
                 out_status = status_info.get('out_time_str', '-')
                 target_employee_details_list.append(f"{target_employee_count_for_list}. {name}: {issue_string}출근={in_status}, 퇴근={out_status}")

        if target_employee_details_list:
            plain_text.append(f"[{'퇴근' if is_eve_run else '출근'} 확인 대상 상세 현황] ({len(target_employee_details_list)}명)")
            plain_text.append('-'*30) # 구분선
            plain_text.extend(target_employee_details_list)
        else: # 확인 대상자가 없을 때의 메시지
            if analysis_result["summary"]["target"] == 0 and analysis_result["summary"]["excluded"] > 0:
                 plain_text.append(f"{target_date_str} 확인 대상 없음 (전원 휴가/제외됨).")
            elif analysis_result["summary"]["target"] == 0 and analysis_result["summary"]["excluded"] == 0:
                 plain_text.append(f"{target_date_str} 확인 대상 없음 (데이터 없음).")
            else: # 이 경우는 거의 없어야 함
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

# --- 텔레그램 메시지 전송 함수 ---
def send_telegram_message(bot_token, chat_id, message_text):
    if not bot_token or not chat_id:
        log_message("텔레그램 봇 토큰 또는 Chat ID가 설정되지 않았습니다. 메시지 전송을 건너뜁니다.", "ERROR")
        return False

    max_length = 4000  # 텔레그램 메시지 최대 길이 근사치 (실제 4096)
    messages_to_send = []

    if len(message_text) > max_length:
        log_message(f"메시지 길이가 너무 깁니다 ({len(message_text)}자). 분할하여 전송합니다.", "INFO")
        for i in range(0, len(message_text), max_length):
            messages_to_send.append(message_text[i:i + max_length])
    else:
        messages_to_send.append(message_text)

    all_sent_successfully = True
    for i, part_message in enumerate(messages_to_send):
        # MarkdownV2 사용 시 특수 문자 이스케이프 (선택 사항)
        # 주의: 이스케이프를 잘못하면 오히려 메시지가 이상해질 수 있으니, 내용에 따라 조절.
        # 간단한 텍스트는 parse_mode 없이 보내는 것이 안전할 수 있음.
        # part_message_escaped = part_message.replace('.', '\\.').replace('-', '\\-').replace('!', '\\!') # 예시
        # payload = {'chat_id': chat_id, 'text': part_message_escaped, 'parse_mode': 'MarkdownV2'}

        payload = {'chat_id': chat_id, 'text': part_message} # parse_mode 없이 기본 전송
        send_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        try:
            response = requests.post(send_url, data=payload, timeout=30) # 타임아웃 증가
            response.raise_for_status()
            log_message(f"텔레그램 메시지 전송 성공 (부분 {i+1}/{len(messages_to_send)}). 응답: {response.json()}", "INFO")
            if len(messages_to_send) > 1 and i < len(messages_to_send) - 1 :
                time.sleep(1.5) # 메시지 분할 시 약간의 딜레이
        except requests.exceptions.RequestException as e:
            log_message(f"텔레그램 메시지 전송 실패 (부분 {i+1}): {e}", "ERROR")
            if hasattr(e, 'response') and e.response is not None:
                log_message(f"텔레그램 응답 내용: {e.response.text}", "ERROR")
            all_sent_successfully = False
            break # 한 부분이라도 실패하면 중단
    return all_sent_successfully


def run_report_process(config, run_identifier="Scheduled"): # run_identifier 기본값 변경
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
        # wait = WebDriverWait(driver, 30) # login_and_get_cookies 내부에서 wait 객체 생성

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
            logging.error(f"Process stopped during Setup/Login/Download: {phase1_err}") # logging 모듈 사용
            # 오류 발생 시에도 드라이버는 finally에서 종료
            raise # 이 예외는 외부 try-except에서 잡힘

        # 분석 단계
        log_message("Proceeding with analysis...")
        try:
            analysis_result = analyze_attendance(excel_file_data, EXCEL_SHEET_NAME, target_date)
            if not analysis_result or analysis_result.get("summary", {}).get("total_employees", -1) == -1:
                error_occurred = True # 분석 실패 시 오류로 간주
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

        # 텔레그램 발송 단계
        if not error_occurred and analysis_result and analysis_result.get("summary", {}).get("total_employees", -1) != -1:
            telegram_bot_token = config.get("TELEGRAM_BOT_TOKEN")
            telegram_chat_id = config.get("TELEGRAM_CHAT_ID")

            if telegram_bot_token and telegram_chat_id:
                report_text = analysis_result.get("plain_text_report", "보고서 내용을 가져올 수 없습니다.")
                team_name_from_analysis = analysis_result.get('team_name', '팀') # 분석에서 가져온 팀 이름
                
                # 텔레그램 메시지 제목에 팀 이름과 실행 식별자 추가
                # Markdown을 사용한다면 * 같은 특수문자 주의
                message_title = f"[{config.get('SENDER_NAME', '근태봇')}] {target_date_str} {team_name_from_analysis} 근태 현황 ({run_identifier})"
                full_message = f"{message_title}\n{'-'*20}\n{report_text}"

                log_message("텔레그램으로 보고서 전송 시도...", "INFO")
                telegram_sent_successfully = send_telegram_message(telegram_bot_token, telegram_chat_id, full_message)

                if telegram_sent_successfully:
                    final_status_message = "텔레그램 메시지 발송 완료됨."
                else:
                    final_status_message = "텔레그램 메시지 발송 실패."
                    # 텔레그램 발송 실패를 전체 프로세스 오류로 간주할지 여부 결정
                    # error_occurred = True # 필요시
            else:
                log_message("텔레그램 봇 토큰 또는 Chat ID가 설정되지 않았습니다. 메시지 전송을 건너뜁니다.", "WARNING")
                final_status_message = final_status_message or "텔레그램 설정 누락으로 발송 건너뜀"
        elif error_occurred: # 이전 단계에서 오류 발생
            log_message("이전 단계 오류로 인해 텔레그램 발송을 건너뜁니다.", "WARNING")
            final_status_message = final_status_message or "텔레그램 발송 건너뜀 (이전 단계 오류)"
        else: # 분석 결과가 유효하지 않음
            log_message("분석 결과가 유효하지 않아 텔레그램 발송을 건너뜁니다.", "WARNING")
            final_status_message = final_status_message or "텔레그램 발송 건너뜀 (분석 결과 없음)"

    except Exception as outer_err: # run_report_process 내부의 모든 예외를 캐치
        log_message(f"Critical error in process ({run_identifier}): {outer_err}", "ERROR")
        logging.exception(f"Critical Process Error ({run_identifier})") # 스택 트레이스 포함 로깅
        error_occurred = True # 최종적으로 오류 상태임을 명시
        final_status_message = final_status_message or f"치명적 오류: {outer_err}"
        # 오류 발생 시에도 빈 보고서라도 보내고 싶다면 여기서 텔레그램 전송 로직 추가 가능
        # if config.get("TELEGRAM_BOT_TOKEN") and config.get("TELEGRAM_CHAT_ID"):
        #     error_report_text = f"[{config.get('SENDER_NAME', '근태봇')}] {target_date_str} 처리 중 오류 발생 ({run_identifier})\n오류: {outer_err}"
        #     send_telegram_message(config.get("TELEGRAM_BOT_TOKEN"), config.get("TELEGRAM_CHAT_ID"), error_report_text)

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
        else: # error_occurred is True
             completion_status = "오류 발생"

        status_summary = final_status_message if final_status_message else completion_status
        final_log_message = f"--- Process ({run_identifier}) {completion_status} in {time_taken:.2f} seconds. Status: {status_summary} ---"
        log_message(final_log_message, "ERROR" if error_occurred else "INFO")


def load_config_headless():
    """ Headless 환경을 위한 설정 로드 (환경 변수 우선) """
    config = DEFAULT_CONFIG.copy()

    # 환경 변수에서 주요 설정 로드
    config["WEBMAIL_USERNAME"] = os.getenv("WEBMAIL_USERNAME", config["WEBMAIL_USERNAME"])
    config["WEBMAIL_PASSWORD"] = os.getenv("WEBMAIL_PASSWORD", config["WEBMAIL_PASSWORD"])
    config["TELEGRAM_BOT_TOKEN"] = os.getenv("TELEGRAM_BOT_TOKEN", config["TELEGRAM_BOT_TOKEN"])
    config["TELEGRAM_CHAT_ID"] = os.getenv("TELEGRAM_CHAT_ID", config["TELEGRAM_CHAT_ID"])
    config["SENDER_NAME"] = os.getenv("SENDER_NAME", config["SENDER_NAME"])

    # (선택 사항) JSON 설정 파일도 지원하려면
    # if Path(CONFIG_FILE).exists():
    #     try:
    #         with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    #             file_config = json.load(f)
    #         # 파일 설정이 환경 변수보다 우선순위가 낮도록 하거나, 필요한 부분만 업데이트
    #         for key in config:
    #             if key in file_config and not os.getenv(key.upper()): # 환경변수가 없을때만 파일값 사용
    #                 config[key] = file_config[key]
    #         log_message(f"Loaded additional config from {CONFIG_FILE}", "INFO")
    #     except Exception as e:
    #         log_message(f"Error loading config file {CONFIG_FILE}: {e}", "WARNING")

    # 필수 값 확인
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
        raise ValueError(msg) # 프로그램 중단

    return config

# --- Main Execution ---
if __name__ == "__main__":
    if _path_source_info_lines: # 경로 결정 로그 출력
        for line in _path_source_info_lines:
            print(line) # 시작 시 표준 출력으로 보여줌

    print(f"User Data Path: {USER_DATA_PATH}")
    print(f"Log File: {LOG_FILE}")
    print(f"Config File (if used): {CONFIG_FILE}")
    print(f"APP_ROOT_PATH (for bundled resources, if any): {APP_ROOT_PATH}")

    log_message(f"--- Starting Headless Attendance Bot (ktMOS_DG_Headless) ---")
    log_message(f"User Data Path set to: {USER_DATA_PATH}")
    log_message(f"Log file: {LOG_FILE}")

    loaded_config = None
    try:
        loaded_config = load_config_headless()
        run_identifier = f"Run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        run_report_process(loaded_config, run_identifier=run_identifier)
    except ValueError as ve: # load_config_headless에서 필수 설정 누락 시 발생
        log_message(f"Configuration error: {ve}", "ERROR")
        sys.exit(1) # 오류 종료
    except Exception as e:
        log_message(f"An unexpected error occurred in the main execution block: {e}", "ERROR")
        logging.exception("Fatal error during main execution.")
        # 심각한 오류 발생 시에도 텔레그램으로 알림을 보내고 싶다면 여기에 로직 추가 가능
        # if loaded_config and loaded_config.get("TELEGRAM_BOT_TOKEN") and loaded_config.get("TELEGRAM_CHAT_ID"):
        #    error_message = f"자동 근태 확인 봇 실행 중 심각한 오류 발생:\n{e}\n로그 파일을 확인하세요: {LOG_FILE}"
        #    send_telegram_message(loaded_config["TELEGRAM_BOT_TOKEN"], loaded_config["TELEGRAM_CHAT_ID"], error_message)
        sys.exit(1) # 오류 종료
    finally:
        log_message("--- Headless Attendance Bot Finished ---")
