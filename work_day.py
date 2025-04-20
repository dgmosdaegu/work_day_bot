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

TARGET_DATE = datetime.date.today() # This will use the runner's date (likely UTC)
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")

REPORT_DOWNLOAD_URL_TEMPLATE = "http://gw.ktmos.co.kr/owattend/rest/dclz/report/CompositeStatus/sumr/user/days/excel?startDate={date}&endDate={date}&deptSeq=1231&erpNumDisplayYn=Y"
REPORT_URL = REPORT_DOWNLOAD_URL_TEMPLATE.format(date=TARGET_DATE_STR)

EXCEL_SHEET_NAME = "세부현황_B"
STANDARD_START_TIME_STR = "09:00:00"
STANDARD_END_TIME_STR = "18:00:00"
EVENING_RUN_THRESHOLD_HOUR = 18 # Use 18:00 UTC as threshold if running on UTC runner

# --- Credential Check ---
# Check if environment variables are set
missing_secrets = []
if not WEBMAIL_USERNAME: missing_secrets.append("WEBMAIL_USER")
if not WEBMAIL_PASSWORD: missing_secrets.append("WEBMAIL_PASS")
if not TELEGRAM_BOT_TOKEN: missing_secrets.append("TELEGRAM_TOKEN")
if not TELEGRAM_CHAT_ID: missing_secrets.append("TELEGRAM_CHAT")

if missing_secrets:
    logging.critical(f"!!! CRITICAL ERROR: Missing required environment variables (GitHub Secrets): {', '.join(missing_secrets)} !!!")
    # Optional: Send a basic Telegram message if possible, otherwise just exit
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
         # Very basic send attempt, might fail if requests isn't installed yet, but worth a try
         try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", data={'chat_id':TELEGRAM_CHAT_ID, 'text':f"CRITICAL: GitHub Actions failed. Missing secrets: {', '.join(missing_secrets)}"})
         except: pass
    exit(1)

# --- Constants for Leave Types ---
FULL_DAY_LEAVE_REASONS = {"연차", "보건휴가", "출산휴가", "출산전후휴가", "청원휴가", "가족돌봄휴가", "특별휴가", "공가", "공상", "예비군훈련", "민방위훈련", "공로휴가", "병가", "보상휴가"}
FULL_DAY_LEAVE_TYPES = {"법정휴가", "병가/휴직", "보상휴가", "공가"}
MORNING_HALF_LEAVE = "오전반차"
AFTERNOON_HALF_LEAVE = "오후반차"
ATTENDANCE_TYPE = "출퇴근"

# --- Helper Functions (setup_driver, login_and_get_cookies, download_excel_report, parse_time_robust, parse_date_robust, combine_date_time, escape_markdown, create_table_image, send_telegram_photo, send_telegram_message) ---
# ... (Keep your existing helper functions exactly as they are) ...
def setup_driver():
    logging.info("Setting up ChromeDriver...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new") # Run headless - ESSENTIAL FOR GITHUB ACTIONS
    options.add_argument("--disable-gpu") # Often needed in headless/linux
    options.add_argument("--no-sandbox") # Often needed in CI/Docker environments
    options.add_argument("--disable-dev-shm-usage") # Overcomes limited resource problems
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36") # Use a common user agent
    options.add_argument("--window-size=1920,1080") # Specify window size
    options.add_experimental_option("excludeSwitches", ["enable-logging"]) # Suppress some logs
    try:
        # Use webdriver-manager to automatically handle chromedriver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(10) # Adjust wait time if needed
        logging.info("ChromeDriver setup complete (running headless).")
        return driver
    except Exception as e:
        logging.error(f"ChromeDriver setup error: {e}")
        logging.error(traceback.format_exc()) # More detailed error for CI logs
        raise

# ... (login_and_get_cookies function - No changes needed) ...
# ... (download_excel_report function - No changes needed) ...
# ... (parse_time_robust function - No changes needed) ...
# ... (parse_date_robust function - No changes needed) ...
# ... (combine_date_time function - No changes needed) ...
# ... (escape_markdown function - No changes needed) ...

def create_table_image(df, title, output_path="table_image.png"):
    logging.info("Creating table image...")
    if df.empty: logging.warning("DF empty, skip image."); return None
    try:
        # Explicitly try to find Nanum fonts commonly installed on Linux runners
        font_path = None
        possible_fonts = ["NanumGothic.ttf", "NanumBarunGothic.ttf", "malgun.ttf", "AppleGothic.ttf"]
        system_fonts = fm.findSystemFonts(fontpaths=None, fontext='ttf')
        logging.debug(f"Found {len(system_fonts)} system fonts.") # Debug log

        # Check specific paths where fonts might be installed in Actions
        action_font_paths = ["/usr/share/fonts/truetype/nanum/"]
        for p in action_font_paths:
             if Path(p).exists():
                 system_fonts.extend([str(f) for f in Path(p).glob("*.ttf")])

        found_korean_font = False
        for f in system_fonts:
            font_name = Path(f).name
            # More robust checking, handle case and variations
            if any(pf.lower() in font_name.lower() for pf in possible_fonts):
                font_path = f
                logging.info(f"Using Korean font: {font_path}")
                found_korean_font = True
                break

        if found_korean_font:
             prop = fm.FontProperties(fname=font_path, size=10)
             plt.rcParams['font.family'] = prop.get_name()
             # Crucial for displaying minus signs correctly with CJK fonts
             plt.rcParams['axes.unicode_minus'] = False
             logging.info(f"Set font family to: {prop.get_name()}")
        else:
            logging.warning("Korean font (e.g., NanumGothic) not found. Text in image might render incorrectly. Ensure fonts are installed in the GitHub Actions runner.")
            # Fallback to default sans-serif if no Korean font is found
            plt.rcParams['font.family'] = 'sans-serif'
            plt.rcParams['axes.unicode_minus'] = False


    except Exception as e:
        logging.warning(f"Font setup error: {e}. Text rendering might be affected.")

    nr, nc = df.shape
    # Adjust figsize calculation if needed based on runner rendering
    fw = min(max(8, nc * 1.2), 25) # Allow wider images if needed
    fh = min(max(4, nr * 0.4 + 1.5), 50) # Allow taller images if needed
    logging.info(f"Table: {nr} rows, {nc} cols. Figure size: ({fw:.1f}, {fh:.1f}) inches")

    fig, ax = plt.subplots(figsize=(fw, fh))
    ax.axis('off') # Turn off axis lines and labels

    tab = Table(ax, bbox=[0, 0, 1, 1]) # Create table to fill the axis

    # Add header row
    for j, col in enumerate(df.columns):
        cell = tab.add_cell(0, j, 1, 1, text=str(col), loc='center', facecolor='lightgray')
        cell.set_fontsize(9) # Slightly larger font for header
        cell.set_text_props(weight='bold') # Bold header text

    # Add data rows
    for i in range(nr):
        for j in range(nc):
            txt = str(df.iloc[i, j])
            # Truncate long text to prevent excessively wide cells
            max_len = 60
            if len(txt) > max_len:
                txt = txt[:max_len-3] + '...'
            cell = tab.add_cell(i + 1, j, 1, 1, text=txt, loc='center', facecolor='white')
            cell.set_fontsize(8) # Set data cell font size

    # Add the table to the axes
    ax.add_table(tab)

    # Add title above the table
    plt.title(title, fontsize=12, pad=20) # `pad` adds space between title and table

    # Adjust layout to prevent clipping
    plt.tight_layout(pad=1.0) # Add some padding around the elements

    try:
        plt.savefig(output_path, bbox_inches='tight', dpi=120) # Increase DPI slightly for better quality
        plt.close(fig) # Close the figure to free memory
        logging.info(f"Table image saved successfully: {output_path}")
        try:
            size_mb = Path(output_path).stat().st_size / (1024 * 1024)
            logging.info(f"Image file size: {size_mb:.2f} MB")
            # Warning if image is very large (Telegram limit is 50MB for photos, but smaller is better)
            if size_mb > 10:
                logging.warning(f"Generated image is large ({size_mb:.2f} MB). Consider reducing data or complexity if issues arise.")
        except Exception as size_err:
            logging.warning(f"Could not get image file size: {size_err}")
        return output_path
    except Exception as e:
        logging.error(f"Failed to save table image: {e}")
        logging.error(traceback.format_exc())
        plt.close(fig) # Ensure figure is closed even on error
        return None


# ... (send_telegram_photo function - No changes needed) ...
# ... (analyze_attendance function - No changes needed, it uses the global TARGET_DATE) ...
# ... (send_telegram_message function - No changes needed) ...

# --- Main Execution ---
if __name__ == "__main__":
    script_start_time = time.time()
    # Log the target date being used (important for verifying timezone effects)
    logging.info(f"--- Script started for date (runner's perspective): {TARGET_DATE_STR} ---")
    driver = None
    excel_file_data = None
    error_occurred = False
    analysis_result = {} # Initialize analysis_result

    # --- Initial Setup and Download ---
    try:
        # Driver setup now happens within the try block
        driver = setup_driver()
        cookies = login_and_get_cookies(driver, WEBMAIL_LOGIN_URL, WEBMAIL_ID_FIELD_ID, WEBMAIL_PW_FIELD_ID, WEBMAIL_USERNAME, WEBMAIL_PASSWORD)
        excel_file_data = download_excel_report(REPORT_URL, cookies)
        if excel_file_data is None:
            logging.error("Excel report download failed.")
            error_occurred = True
            # Use the helper function to send Telegram message
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(초기 단계\\):\n엑셀 보고서 다운로드 실패\\.")

    except Exception as e:
        logging.error(f"Critical setup/login/download error: {e}")
        logging.error(traceback.format_exc()) # Log traceback for detailed debugging
        error_occurred = True
        # Use the helper function to send Telegram message
        send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(초기 단계\\):\n{escape_markdown(str(e))}")
    finally:
        if driver:
            try:
                driver.quit()
                logging.info("WebDriver closed.")
            except (WebDriverException, NoSuchWindowException) as e:
                # These might happen if the browser crashed or couldn't be reached
                logging.warning(f"Non-critical WebDriver close error: {e}")
            except Exception as e:
                # Catch any other unexpected errors during quit
                logging.error(f"Unexpected error closing WebDriver: {e}")

    # --- Analysis and Reporting ---
    # Proceed only if download was successful and no prior critical errors
    if excel_file_data and not error_occurred:
        try:
            analysis_result = analyze_attendance(excel_file_data, EXCEL_SHEET_NAME)

            # Check if analysis itself indicated failure (e.g., wrong columns, empty data)
            if not analysis_result or analysis_result.get("summary", {}).get("total_employees", 0) == -1:
                 logging.error("Analysis function failed or returned invalid/empty result.")
                 error_occurred = True
                 # Avoid sending specific analysis error message here if one was already sent inside analyze_attendance
            else:
                # Determine if it's morning or evening based on runner's time
                # Note: Runner time is likely UTC. Adjust EVENING_RUN_THRESHOLD_HOUR accordingly.
                # e.g., If you want evening report after 6 PM KST (UTC+9), threshold is 18-9 = 9 UTC.
                # Let's assume threshold is UTC for now.
                now_time = datetime.datetime.utcnow().time() # Use UTC time
                is_evening = now_time >= datetime.time(EVENING_RUN_THRESHOLD_HOUR, 0)
                logging.info(f"Current UTC time {now_time.strftime('%H:%M:%S')}. Reporting as {'Evening' if is_evening else 'Morning'}.")

                attendance_issues = analysis_result.get("notifications", [])
                detailed_statuses = analysis_result.get("detailed_status", [])
                analysis_summary = analysis_result.get("summary", {})
                excluded_employees = analysis_result.get("excluded_employees", [])
                df_for_image = analysis_result.get("df_processed")

                # --- Send Table Image ---
                image_sent_ok = False
                if df_for_image is not None and not df_for_image.empty:
                    df_display = df_for_image
                    img_title = f"{TARGET_DATE_STR} 근태 현황 (처리: {len(df_display)}명)" # Adjusted title
                    image_path = create_table_image(df_display, img_title, "attendance_table.png")
                    if image_path:
                        caption = f"*{escape_markdown(TARGET_DATE_STR)} 근태 현황 상세*" # Adjusted caption
                        try:
                            # Make sure send_telegram_photo exists and works
                             send_telegram_photo(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, image_path, caption)
                             logging.info("Attempted to send table image via Telegram.")
                             image_sent_ok = True # Assume okay, though send_telegram_photo doesn't return status
                        except Exception as e:
                             logging.error(f"Failed to send Telegram photo: {e}")
                             error_occurred = True
                        finally:
                            # Clean up the generated image file
                            try:
                                Path(image_path).unlink(missing_ok=True)
                                logging.info(f"Deleted temporary image file: {image_path}")
                            except Exception as del_err:
                                logging.warning(f"Could not delete image file {image_path}: {del_err}")
                    else:
                        logging.error("Failed to create the table image file.")
                        error_occurred = True
                elif df_for_image is None:
                     logging.warning("Analysis did not produce a DataFrame for the image.")
                else: # df_for_image is empty
                     logging.info("DataFrame for image is empty, skipping image generation and sending.")

                # --- Send Detailed Report (Issues or Status) ---
                message_lines = []
                report_title = ""
                escaped_date_header = escape_markdown(TARGET_DATE_STR)

                if is_evening:
                    logging.info("Generating evening detailed status report.")
                    report_title = f"*{escaped_date_header} 퇴근 현황 상세*"
                    if detailed_statuses:
                        # Format evening status report
                        for idx, status in enumerate(detailed_statuses):
                             # Escape name and status details
                             name_esc = escape_markdown(status.get('name', 'N/A'))
                             in_esc = escape_markdown(status.get('in_status', 'N/A'))
                             out_esc = escape_markdown(status.get('out_status', 'N/A'))
                             line = f"{idx + 1}\\. *{name_esc}*: 출근[{in_esc}] 퇴근[{out_esc}]"
                             message_lines.append(line)
                    else:
                         logging.info("No detailed status entries found for evening report.")
                         message_lines.append(escape_markdown("모든 대상 인원의 출퇴근 기록이 확인되었습니다.") if not error_occurred else escape_markdown("상세 현황 데이터 없음."))

                else: # Morning Run
                    logging.info("Generating morning issue report.")
                    report_title = f"*{escaped_date_header} 출근 확인 필요*"
                    if attendance_issues:
                         # Morning issues are already formatted with markdown escapes
                        for idx, issue in enumerate(attendance_issues):
                            line = f"{idx + 1}\\. {issue}" # Issue text should already be escaped
                            message_lines.append(line)
                    else:
                        logging.info("No specific morning attendance issues found.")
                        message_lines.append(escape_markdown("모든 대상 인원의 출근 기록이 정상입니다 (지각/미기록 없음).") if not error_occurred else escape_markdown("출근 확인 필요 데이터 없음."))


                if message_lines:
                     # Add title and separator to the message
                     msg_header = f"{report_title}\n{escape_markdown('-'*20)}\n\n"
                     msg_body = "\n".join(message_lines)
                     full_msg = msg_header + msg_body
                     logging.info(f"Sending detailed {'evening status' if is_evening else 'morning issue'} report.")
                     if not send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, full_msg):
                         error_occurred = True
                         logging.error("Failed to send detailed report message.")
                else:
                     logging.warning("No content generated for the detailed report message.")


        except Exception as e:
            # Catch errors during the analysis or reporting phase
            logging.error(f"Error during analysis/reporting phase: {e}")
            logging.error(traceback.format_exc())
            error_occurred = True
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류* \\(결과 처리/알림 중\\):\n{escape_markdown(str(e))}")

    # --- Send Summary Report ---
    # Send summary regardless of image/detail success, unless analysis itself failed critically
    if analysis_result and analysis_result.get("summary", {}).get("total_employees", -1) != -1:
        try:
            analysis_summary = analysis_result.get("summary", {})
            # Need to determine is_evening again or pass it from above
            now_time = datetime.datetime.utcnow().time() # Use UTC time
            is_evening = now_time >= datetime.time(EVENING_RUN_THRESHOLD_HOUR, 0)

            total = analysis_summary.get("total_employees", 0)
            target = analysis_summary.get("target", 0)
            excluded = analysis_summary.get("excluded", 0)
            clock_in = analysis_summary.get("clocked_in", 0)
            miss_in = analysis_summary.get("missing_in", 0)
            clock_out = analysis_summary.get("clocked_out", 0)
            miss_out = analysis_summary.get("missing_out", 0)
            # Excluded list should already contain escaped names and reasons/times
            excluded_list = analysis_result.get("excluded_employees", [])

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
                error_occurred = True # Mark error if sending fails
                logging.error("Failed to send summary report message.")

        except Exception as summary_err:
            logging.error(f"Error generating or sending summary report: {summary_err}")
            logging.error(traceback.format_exc())
            error_occurred = True
            # Try to send a fallback error message
            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, f"*{escape_markdown(TARGET_DATE_STR)} 스크립트 오류*\n요약 보고서 생성/전송 중 오류 발생\\: {escape_markdown(str(summary_err))}")
    elif not error_occurred:
         # This case means analysis failed early on (e.g. -1 total employees)
         logging.warning("Skipping summary report because analysis result was invalid or indicated failure.")


    # --- Final Completion Message ---
    script_end_time = time.time()
    time_taken = script_end_time - script_start_time
    logging.info(f"--- Script finished in {time_taken:.2f} seconds ---")

    completion_status = "오류 발생" if error_occurred else "정상 완료"
    escaped_final_date = escape_markdown(TARGET_DATE_STR)
    escaped_final_status = escape_markdown(completion_status)
    time_taken_str = f"{time_taken:.1f}"
    escaped_final_time = escape_markdown(time_taken_str)

    final_message = f"*{escaped_final_date} 근태 확인 스크립트*: {escaped_final_status} \\(소요시간: {escaped_final_time}초\\)"
    # Try sending final status, but don't mark error_occurred if this specific message fails
    send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, final_message)

    # Exit with appropriate code for GitHub Actions
    exit(1 if error_occurred else 0)

# Ensure the main block is correctly indented if you copied parts separately
