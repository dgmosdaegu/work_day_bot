# -*- coding: utf-8 -*-
import time
import datetime
import logging
import os
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# 환경변수로부터 설정값 읽기
WEBMAIL_USERNAME = os.getenv("WEBMAIL_ID")
WEBMAIL_PASSWORD = os.getenv("WEBMAIL_PW")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

REPORT_DATE = datetime.date.today().strftime("%Y-%m-%d")
REPORT_URL = f"http://gw.ktmos.co.kr/owattend/rest/dclz/report/CompositeStatus/sumr/user/days/excel?startDate={REPORT_DATE}&endDate={REPORT_DATE}&deptSeq=1231&erpNumDisplayYn=Y"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def setup_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disable-extensions')
    options.add_argument('--window-size=1920x1080')
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def login_and_get_cookies():
    driver = setup_driver()
    driver.get("http://gw.ktmos.co.kr/mail2/loginPage.do")
    time.sleep(1)

    email_input = driver.find_element(By.ID, "userEmail")
    pw_input = driver.find_element(By.ID, "userPw")
    email_input.send_keys(WEBMAIL_USERNAME)
    pw_input.send_keys(WEBMAIL_PASSWORD)
    pw_input.send_keys(Keys.RETURN)

    time.sleep(3)
    cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
    driver.quit()
    return cookies

def download_excel(cookies):
    session = requests.Session()
    session.cookies.update(cookies)
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "http://gw.ktmos.co.kr/mail2/"}
    response = session.get(REPORT_URL, headers=headers)
    if response.ok:
        with open(f"report_{REPORT_DATE}.xlsx", "wb") as f:
            f.write(response.content)
        logging.info("리포트 다운로드 성공")
        return True
    else:
        logging.error("리포트 다운로드 실패")
        return False

def send_telegram_message(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
    requests.post(url, data=payload)

if __name__ == "__main__":
    try:
        logging.info("로그인 시도 중...")
        cookies = login_and_get_cookies()
        if download_excel(cookies):
            send_telegram_message(f"[{REPORT_DATE}] 리포트 다운로드 성공 ✅")
        else:
            send_telegram_message(f"[{REPORT_DATE}] 리포트 다운로드 실패 ❌")
    except Exception as e:
        logging.error("스크립트 오류 발생", exc_info=True)
        send_telegram_message(f"[{REPORT_DATE}] 스크립트 오류 ❌: {e}")

