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
EVENING_RUN_THRESHOLD_HOUR = 18

# --- Credential Check ---
# Check if environment variables are set
missing_secrets = []
if not WEBMAIL_USERNAME: missing_secrets.append("WEBMAIL_USERNAME")
if not WEBMAIL_PASSWORD: missing_secrets.append("WEBMAIL_PASSWORD")
if not TELEGRAM_BOT_TOKEN: missing_secrets.append("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_CHAT_ID: missing_secrets.append("TELEGRAM_CHAT_ID")

if missing_secrets:
    error_message = f"!!! CRITICAL ERROR: Missing required environment variables: {', '.join(missing_secrets)} !!!"
    logging.critical(error_message)
    # Attempt to send a Telegram message if possible (token/chat_id might be missing)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            # Use the send_telegram_message function if defined later, otherwise basic request
             send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, escape_markdown(error_message))
        except Exception as e:
             logging.error(f"Could not send Telegram error notification: {e}")
    exit(1) # Exit if secrets are missing

# --- Constants for Leave Types ---
# ... (rest of your constants) ...
FULL_DAY_LEAVE_REASONS = {"연차", "보건휴가", "출산휴가", "출산전후휴가", "청원휴가", "가족돌봄휴가", "특별휴가", "공가", "공상", "예비군훈련", "민방위훈련", "공로휴가", "병가", "보상휴가"}
FULL_DAY_LEAVE_TYPES = {"법정휴가", "병가/휴직", "보상휴가", "공가"}
MORNING_HALF_LEAVE = "오전반차"
AFTERNOON_HALF_LEAVE = "오후반차"
ATTENDANCE_TYPE = "출퇴근"

# --- Helper Functions (setup_driver, login_and_get_cookies, download_excel_report, ...) ---
# Make sure setup_driver uses headless mode correctly for the action environment
def setup_driver():
    logging.info("Setting up ChromeDriver...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new") # Ensure headless is used
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox") # Crucial for Linux environments like GitHub Actions runners
    options.add_argument("--disable-dev-shm-usage") # Crucial for Linux environments
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36") # Or update to a newer agent
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    try:
        # webdriver-manager handles finding the driver binary
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(10) # Adjust wait time if needed
        logging.info("ChromeDriver setup complete (running headless).")
        return driver
    except Exception as e:
        logging.error(f"ChromeDriver setup error: {e}")
        logging.error(traceback.format_exc()) # Add traceback for more details
        raise

# ... (login_and_get_cookies - no changes needed) ...
# ... (download_excel_report - no changes needed) ...
# ... (parse_time_robust, parse_date_robust, combine_date_time, escape_markdown - no changes needed) ...

# Helper function to find system fonts more reliably in GitHub Actions
def find_korean_font():
    # Check common locations and installed fonts
    common_font_files = ["NanumGothic.ttf", "malgun.ttf", "AppleGothic.ttf", "gulim.ttc"]
    font_paths_to_check = [
        "/usr/share/fonts/truetype/nanum/", # Common location for Nanum fonts on Linux
        "/usr/share/fonts/opentype/nanum/",
         os.path.expanduser("~/.fonts/")
    ] + fm.get_font_paths()

    for path in font_paths_to_check:
        try:
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    if filename in common_font_files:
                        found_path = os.path.join(path, filename)
                        logging.info(f"Found Korean font: {found_path}")
                        return found_path
        except OSError: # Handle potential permission errors or non-existent paths
             continue

    # If not found in common paths, search system fonts more broadly
    try:
        for f in fm.findSystemFonts(fontpaths=None, fontext='ttf'):
            if Path(f).name in common_font_files:
                logging.info(f"Found Korean font via findSystemFonts: {f}")
                return f
    except Exception as e:
        logging.warning(f"Error searching system fonts with font_manager: {e}")

    logging.warning("Korean font not found in common locations or system search.")
    return None

def create_table_image(df, title, output_path="table_image.png"):
    logging.info("Creating table image...")
    if df.empty: logging.warning("DataFrame is empty, skipping image generation."); return None
    try:
        # Use the helper function to find the font
        font_path = find_korean_font()
        if font_path:
            prop = fm.FontProperties(fname=font_path, size=10)
            plt.rcParams['font.family'] = prop.get_name()
            plt.rcParams['axes.unicode_minus'] = False
            logging.info(f"Using font: {font_path}")
        else:
            logging.warning("Korean font not found. Table image might have broken characters.")
            # Optionally fall back to a default font if needed, but Korean will likely be broken
            # plt.rcParams['font.family'] = 'sans-serif'

    except Exception as e:
        logging.warning(f"Error setting font properties: {e}.")

    nr, nc = df.shape
    # Adjust figsize calculation if needed, ensure it's not excessively large
    fw = min(max(8, nc * 1.2), 25) # Capped max width
    fh = min(max(4, nr * 0.4 + 1.5), 50) # Capped max height
    logging.info(f"Table dimensions: {nr} rows, {nc} columns. Figure size: ({fw:.1f}, {fh:.1f})")

    fig, ax = plt.subplots(figsize=(fw, fh)); ax.axis('off')
    tab = Table(ax, bbox=[0, 0, 1, 1]);

    # Add Header
    for j, col in enumerate(df.columns):
        tab.add_cell(0, j, 1, 1, text=str(col), loc='center', facecolor='lightgray', width=1.0/nc if nc > 0 else 1) # Set width

    # Add Rows
    for i in range(nr):
        for j in range(nc):
            txt = str(df.iloc[i, j]); max_len = 50 # Adjust max_len if needed
            if len(txt) > max_len: txt = txt[:max_len - 3] + '...'
            cell_color = 'white' if i % 2 == 0 else '#f0f0f0' # Alternate row colors
            tab.add_cell(i + 1, j, 1, 1, text=txt, loc='center', facecolor=cell_color, width=1.0/nc if nc > 0 else 1) # Set width

    tab.auto_set_font_size(False); tab.set_fontsize(8) # Consider adjusting font size
    ax.add_table(tab)
    plt.title(title, fontsize=12, pad=20);
    plt.tight_layout(pad=1.5) # Add some padding

    try:
        plt.savefig(output_path, bbox_inches='tight', dpi=100); # Check DPI if image quality is low
        plt.close(fig)
        logging.info(f"Table image saved successfully: {output_path}")
        size_bytes = Path(output_path).stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        logging.info(f"Image file size: {size_mb:.2f} MB")
        if size_mb > 10: # Telegram bot API photo size limit (check current limits)
            logging.warning(f"Generated image size ({size_mb:.2f} MB) might exceed Telegram's limit.")
        return output_path
    except Exception as e:
        logging.error(f"Failed to save table image: {e}");
        logging.error(traceback.format_exc())
        plt.close(fig); # Ensure figure is closed even on error
        return None


# ... (send_telegram_photo - no changes needed, but ensure it handles potential large file errors) ...
# ... (analyze_attendance - no changes needed, relies on global vars) ...
# ... (send_telegram_message - no changes needed) ...

# --- Main Execution ---
if __name__ == "__main__":
    script_start_time = time.time()
    # Check for secrets happens earlier now
    logging.info(f"--- Script started for date: {TARGET_DATE_STR} ---")
    # ... (rest of your main execution block - no significant changes needed) ...

    # Ensure driver is properly managed
    driver = None
    excel_file_data = None
    error_occurred = False
    analysis_result = {}

    try:
        driver = setup_driver()
        # Pass credentials read from environment vars
        cookies = login_and_get_cookies(driver, WEBMAIL_LOGIN_URL, WEBMAIL_ID_FIELD_ID, WEBMAIL_PW_FIELD_ID, WEBMAIL_USERNAME, WEBMAIL_PASSWORD)
        if not cookies:
             raise Exception("Failed to login and get cookies.") # Make sure error is raised if login fails

        excel_file_data = download_excel_report(REPORT_URL, cookies)
        if excel_file_data is None:
            error_occurred = True
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(초기 단계\\):\n엑셀 보고서 다운로드 실패\\.")

    except Exception as e:
        logging.error(f"Critical setup/login/download error: {e}", exc_info=True) # Use exc_info=True for traceback
        error_occurred = True
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(초기 단계\\):\n{escape_markdown(str(e))}")
    finally:
        if driver:
            try:
                driver.quit()
                logging.info("WebDriver closed.")
            except (WebDriverException, NoSuchWindowException) as e:
                logging.warning(f"Non-critical WebDriver close error: {e}")
            except Exception as e:
                logging.error(f"Unexpected WebDriver close error: {e}", exc_info=True)


    # --- Analysis and Reporting ---
    if excel_file_data and not error_occurred:
        try:
            analysis_result = analyze_attendance(excel_file_data, EXCEL_SHEET_NAME)
            # Check analysis result more thoroughly
            if not isinstance(analysis_result, dict) or "summary" not in analysis_result or analysis_result.get("summary", {}).get("total_employees", -1) == -1:
                logging.error("Analysis failed or returned invalid/empty result structure.")
                error_occurred = True
                # Send specific error if possible based on analysis result content
                send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 분석 오류*:\n분석 함수에서 유효하지 않은 결과 반환됨\\.")
            else:
                # ... (Existing reporting logic: Check evening, generate messages, create image) ...
                now_time = datetime.datetime.now().time()
                is_evening = now_time >= datetime.time(EVENING_RUN_THRESHOLD_HOUR, 0)
                attendance_issues = analysis_result.get("notifications", [])
                detailed_statuses = analysis_result.get("detailed_status", [])
                analysis_summary = analysis_result.get("summary", {})
                excluded_employees = analysis_result.get("excluded_employees", []) # Now contains formatted time
                df_for_image = analysis_result.get("df_processed")

                # --- Send Table Image ---
                if df_for_image is not None and not df_for_image.empty:
                    df_display = df_for_image
                    img_title = f"{TARGET_DATE_STR} 근태 현황 (전체 {len(df_display)}건)"
                    image_path = create_table_image(df_display, img_title, "attendance_table.png") # Use default name or generate one
                    if image_path:
                        caption = f"*{escape_markdown(TARGET_DATE_STR)} 근태 현황 전체*"
                        try:
                            send_telegram_photo(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, image_path, caption)
                            logging.info("Successfully sent attendance table photo to Telegram.")
                        except Exception as e:
                            logging.error(f"Failed to send photo to Telegram: {e}", exc_info=True)
                            error_occurred = True
                            # Try sending a text notification about the image failure
                            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 이미지 전송 실패*:\n표 이미지 생성은 성공했으나 전송 중 오류 발생\\.")
                        finally:
                            try:
                                Path(image_path).unlink(missing_ok=True)
                                logging.info(f"Deleted temporary image file: {image_path}")
                            except Exception as del_err:
                                logging.warning(f"Could not delete temporary image file {image_path}: {del_err}")
                    else:
                        logging.error("Failed to create table image. No image sent.")
                        error_occurred = True
                        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 이미지 생성 실패*:\n근태 현황 표 이미지를 생성할 수 없습니다\\.")
                elif df_for_image is None:
                    logging.warning("No processed DataFrame available for image generation.")
                else: # df_for_image is empty
                    logging.info("Processed DataFrame is empty, skipping image generation.")
                    # Optionally send a message indicating no data to image
                    # send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 현황*:\n이미지 생성 대상 데이터 없음\\.")


                # --- Send Detailed Report ---
                escaped_date_header = escape_markdown(TARGET_DATE_STR)
                message_lines = []
                report_title = ""
                # ... (rest of detailed report logic) ...
                if is_evening:
                    logging.info("Generating evening detailed status report.")
                    report_title = f"*{escaped_date_header} 퇴근 근태 확인 필요*"
                    if detailed_statuses:
                        for idx, status in enumerate(detailed_statuses): line = f"{idx + 1}\\. *{escape_markdown(status['name'])}*: {escape_markdown(status['in_status'])} \\| {escape_markdown(status['out_status'])}"; message_lines.append(line)
                    else: logging.info("No non-excluded employees for evening report.")
                else:
                    logging.info("Generating morning issue report.")
                    report_title = f"*{escaped_date_header} 출근 근태 확인 필요*"
                    if attendance_issues:
                        for idx, issue in enumerate(attendance_issues): line = f"{idx + 1}\\. {issue}"; message_lines.append(line) # Issue already markdown escaped in analyze func
                    else: logging.info("No specific morning issues for report.")

                if message_lines:
                    msg_header = f"{report_title}\n{escape_markdown('-'*20)}\n\n";
                    msg_body = "\n".join(message_lines) # Single newline separation
                    full_msg = msg_header + msg_body;
                    logging.info(f"Sending detailed report ({len(message_lines)} entries).")
                    if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, full_msg):
                         error_occurred = True
                         logging.error("Failed to send detailed report Telegram message.")
                else:
                    logging.info("No detailed report content generated (no issues/statuses to report).")
                    # Optionally send a confirmation that there were no issues
                    # confirmation_msg = f"*{escaped_date_header} {'퇴근' if is_evening else '출근'} 근태*: 확인 대상 특이사항 없음\\."
                    # send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, confirmation_msg)

        except Exception as e:
            logging.error(f"Error during analysis/reporting phase: {e}", exc_info=True)
            error_occurred = True
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(결과 처리/알림 중\\):\n{escape_markdown(str(e))}")

    # --- Send Summary Report ---
    # Check if analysis_result exists and is valid before proceeding
    if isinstance(analysis_result, dict) and "summary" in analysis_result and analysis_result.get("summary", {}).get("total_employees", -1) != -1:
        try:
            analysis_summary = analysis_result.get("summary", {})
            now_time = datetime.datetime.now().time()
            is_evening = now_time >= datetime.time(EVENING_RUN_THRESHOLD_HOUR, 0)
            total = analysis_summary.get("total_employees", 0)
            target = analysis_summary.get("target", 0)
            excluded = analysis_summary.get("excluded", 0)
            clock_in = analysis_summary.get("clocked_in", 0)
            miss_in = analysis_summary.get("missing_in", 0)
            clock_out = analysis_summary.get("clocked_out", 0)
            miss_out = analysis_summary.get("missing_out", 0)
            excluded_list = analysis_result.get("excluded_employees", []) # Already formatted with times
            escaped_date_summary = escape_markdown(TARGET_DATE_STR)
            summary_msg = ""

            if not is_evening:
                summary_title = f"☀️ {escaped_date_summary} 출근 현황 요약"
                summary_details = (
                    f"\\- 전체 인원: {total}명\n"
                    f"\\- 확인 대상: {target}명 \\(제외: {excluded}명\\)\n"
                    f"\\- 출근 기록: {clock_in}명\n"
                    f"\\- 출근 미기록: {miss_in}명"
                )
            else:
                summary_title = f"🌙 {escaped_date_summary} 퇴근 현황 요약"
                summary_details = (
                    f"\\- 전체 인원: {total}명\n"
                    f"\\- 확인 대상: {target}명 \\(제외: {excluded}명\\)\n"
                    f"\\- 출근 기록: {clock_in}명 \\(미기록: {miss_in}명\\)\n"
                    f"\\- 퇴근 기록: {clock_out}명\n"
                    f"\\- 퇴근 미기록 \\(출근자 중\\): {miss_out}명"
                )

            # Append excluded list if not empty
            if excluded_list:
                 # Excluded items are already escaped in analyze_attendance
                excluded_items = "\n  ".join([f"\\- {item}" for item in excluded_list])
                summary_details += f"\n\n*제외 인원*:\n  {excluded_items}"

            summary_msg = f"{summary_title}\n{escape_markdown('-'*20)}\n{summary_details}"
            logging.info("Sending summary report.")
            if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, summary_msg):
                error_occurred = True
                logging.error("Failed to send summary Telegram message.")
        except Exception as summary_err:
            logging.error(f"Error generating or sending summary report: {summary_err}", exc_info=True)
            error_occurred = True
            # Send specific error message about summary failure
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류*\n요약 보고서 생성/전송 중 오류 발생\\: {escape_markdown(str(summary_err))}")
    elif not error_occurred: # Only log/notify if no previous error prevented analysis
        logging.warning("Analysis result was invalid or missing, skipping summary report.")
        # Optionally notify about the missing summary due to analysis failure
        # send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 요약 불가*:\n분석 단계 실패로 요약 보고서를 생성할 수 없습니다\\.")


    # --- Final Completion Message ---
    script_end_time = time.time()
    time_taken = script_end_time - script_start_time
    logging.info(f"--- Script finished in {time_taken:.2f} seconds ---")
    completion_status = "오류 발생" if error_occurred else "정상 완료"
    escaped_final_date = escape_markdown(TARGET_DATE_STR)
    escaped_final_status = escape_markdown(completion_status)
    time_taken_str = f"{time_taken:.1f}"
    escaped_final_time = escape_markdown(time_taken_str)
    final_message = f"✅ *{escaped_final_date} 근태 확인 스크립트*: {escaped_final_status} \\(소요시간: {escaped_final_time}초\\)"
    if error_occurred:
        final_message = f"❌ *{escaped_final_date} 근태 확인 스크립트*: {escaped_final_status} \\(소요시간: {escaped_final_time}초\\)"

    # Try sending final status regardless, but log if it fails
    try:
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, final_message)
    except Exception as final_msg_err:
        logging.error(f"Failed to send final completion status message: {final_msg_err}")

    # Exit with appropriate code for GitHub Actions
    exit(1 if error_occurred else 0)
