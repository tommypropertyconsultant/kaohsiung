#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import os
import logging
import pandas as pd
import random
import threading
import ssl
import re
import csv # 確保匯入 csv 模組

# SSL 修正
ssl._create_default_https_context = ssl._create_unverified_context

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException, NoAlertPresentException

# 設定 Log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# ==========================================
# 🎯 設定存檔路徑
BASE_PATH = r'/Users/wangliwen/Desktop/ JLL/陌生開發/建築存根/高雄市'

# 🎯 設定年份
TARGET_YEARS = ["114", "113", "112", "111", "110"]

# 🎯 設定範圍
START_NUM = 1
END_NUM = 3000

# 🛑 停損設定
MAX_CONSECUTIVE_FAILS = 20
# ==========================================

# 📍 高雄市 38 行政區
KAOHSIUNG_DISTRICTS = [
    "楠梓區", "左營區", "鼓山區", "三民區", "鹽埕區", "前金區", "新興區", "苓雅區", "前鎮區", "旗津區", "小港區", 
    "鳳山區", "大寮區", "鳥松區", "林園區", "仁武區", "大樹區", "大社區", 
    "岡山區", "路竹區", "橋頭區", "梓官區", "彌陀區", "永安區", "燕巢區", "田寮區", "阿蓮區", "茄萣區", "湖內區", 
    "旗山區", "美濃區", "內門區", "杉林區", "甲仙區", "六龜區", "茂林區", "桃源區", "那瑪夏區"
]

class KaohsiungDataSafeScraper:
    def __init__(self, target_year, start_num, end_num, output_filename):
        self.url = "https://buildmis.kcg.gov.tw/bupic/pages/querylic"
        self.target_year = target_year
        self.start_num = start_num
        self.end_num = end_num
        # 🔥 強制將檔名改為 .csv，避免 Excel 開不起來
        self.output_filename = output_filename.replace(".xlsx", ".csv")
        self.csv_filename = self.output_filename
        self.driver = None
        self.target_folder = os.path.join(BASE_PATH, self.target_year)
        if not os.path.exists(self.target_folder):
            try: os.makedirs(self.target_folder)
            except: pass
        self.init_csv()

    def init_csv(self):
        csv_path = os.path.join(self.target_folder, self.csv_filename)
        # 只有當檔案不存在時才寫入 Header
        if not os.path.exists(csv_path):
            columns = [
                "搜尋編號", "執照號碼", "起造人", "行政區", "建築地點", 
                "使用分區", "層棟戶數", "基地面積(合計)", "建築面積(其他)", 
                "法定空地面積", "總樓地板面積", "發照日期", "使用類組"
            ]
            try:
                with open(csv_path, mode='w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=columns)
                    writer.writeheader()
                    f.flush() # 強制寫入
                logger.info(f"📁 [{self.target_year}] CSV 建立成功: {csv_path}")
            except Exception as e:
                logger.error(f"❌ CSV 建立失敗: {e}")

    def save_row_to_csv(self, record):
        """🔥 數據保全核心：寫入後立即 Flush"""
        csv_path = os.path.join(self.target_folder, self.csv_filename)
        try:
            with open(csv_path, mode='a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=record.keys())
                writer.writerow(record)
                f.flush() # 🔥 關鍵：確保資料寫入硬碟，防止崩潰遺失
                os.fsync(f.fileno()) # 雙重保險
        except Exception as e:
            logger.error(f"❌ 寫入 CSV 失敗: {e}")

    def init_driver(self):
        options = Options()
        options.add_argument('--headless=new') 
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def close_driver(self):
        if self.driver:
            try: self.driver.quit()
            except: pass
            self.driver = None

    def js_click(self, element):
        self.driver.execute_script("arguments[0].click();", element)

    def get_captcha_vue(self):
        try:
            script = """
                var app = document.querySelector('#wrapper');
                if (app && app.__vue_app__) {
                    var inst = app.__vue_app__._instance;
                    if (inst.data && inst.data.code) return inst.data.code;
                    if (inst.ctx && inst.ctx.code) return inst.ctx.code;
                    if (inst.proxy && inst.proxy.code) return inst.proxy.code;
                }
                return "";
            """
            code = self.driver.execute_script(script)
            if code: return str(code).replace('"', '').replace("'", "").strip()
        except: pass
        return ""

    def get_full_text_safe(self):
        try: return self.driver.execute_script("var text = document.body.innerText; return text;")
        except: return ""

    def extract_value_from_text(self, text_source, start_keywords, end_keywords=None):
        for key in start_keywords:
            if key in text_source:
                try:
                    temp = text_source.split(key, 1)[1].strip()
                    if temp.startswith(":") or temp.startswith("："): temp = temp[1:].strip()
                    if end_keywords:
                        for end_key in end_keywords:
                            if end_key in temp:
                                temp = temp.split(end_key, 1)[0].strip()
                                break
                    lines = temp.split('\n')
                    if lines: return lines[0].strip()
                except: continue
        return ""

    def process_detail_page(self, search_num):
        try:
            # logger.info(f"   Using process_detail_page for {search_num}...")
            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            full_text = self.get_full_text_safe()
            
            license_no = ""
            match = re.search(fr"(\(\s*{self.target_year}\s*\).*?號)", full_text)
            if match: license_no = match.group(1)
            else:
                match = re.search(r"((高市|高建|府建).*?字.*?號)", full_text)
                license_no = match.group(1) if match else ""
            if not license_no: license_no = f"[需人工確認] {search_num}"
            
            builder = self.extract_value_from_text(full_text, ["姓名"], ["事務所", "電話"]) 
            if not builder: builder = self.extract_value_from_text(full_text, ["起造人"], ["設計人"])

            raw_location = self.extract_value_from_text(full_text, ["建築地點", "地號"], ["使用分區", "基地面積"])
            district = ""
            clean_location = raw_location
            for dist in KAOHSIUNG_DISTRICTS:
                if dist in raw_location:
                    district = dist
                    clean_location = raw_location.replace(dist, "").strip()
                    break
            
            if not district:
                match_loc = re.search(r"(.+?[區鄉鎮市])", raw_location)
                if match_loc:
                    district = match_loc.group(1)
                    clean_location = raw_location.replace(district, "").strip()

            zoning = self.extract_value_from_text(full_text, ["使用分區"], ["基地面積", "建物概要"])
            units = self.extract_value_from_text(full_text, ["層棟戶數"], ["設計建蔽率", "法定空地"])
            site_area = self.extract_value_from_text(full_text, ["合計", "基地面積"], ["㎡", "m2", "騎樓"])
            legal_open = self.extract_value_from_text(full_text, ["法定空地面積", "法定空地"], ["㎡", "m2"])
            floor_area = self.extract_value_from_text(full_text, ["總樓地板面積", "樓地板面積"], ["㎡", "m2"])
            date = self.extract_value_from_text(full_text, ["發照日期"], ["注意事項"])
            
            usage_data = ""
            if "使用類組" in full_text:
                try: usage_data = full_text.split("使用類組", 1)[1].split("備註", 1)[0].strip()[:100]
                except: pass

            record = {
                "搜尋編號": search_num,
                "執照號碼": license_no,
                "起造人": builder,
                "行政區": district,
                "建築地點": clean_location,
                "使用分區": zoning,
                "層棟戶數": units,
                "基地面積(合計)": site_area,
                "建築面積(其他)": "",
                "法定空地面積": legal_open,
                "總樓地板面積": floor_area,
                "發照日期": date,
                "使用類組": usage_data
            }
            
            # 🔥 關鍵：立即存檔
            self.save_row_to_csv(record)
            logger.info(f"   ✅ [{self.target_year}年] 已寫入: {license_no} | {district}")

        except Exception as e:
            logger.error(f"   ❌ [{self.target_year}] 解析失敗: {e}")

    def search_and_process_single_try(self, number_val):
        num_str = f"{number_val:05d}"
        
        try:
            self.driver.get(self.url)
            # 隱藏 footer
            try: self.driver.execute_script("document.querySelector('.footer').style.display='none';")
            except: pass
            
            time.sleep(random.uniform(2.5, 4.5))
            wait = WebDriverWait(self.driver, 20)

            year_input = wait.until(EC.visibility_of_element_located((By.ID, "license_yy")))
            year_input.clear()
            year_input.send_keys(self.target_year)

            no_input = self.driver.find_element(By.ID, "license_no1")
            no_input.clear()
            no_input.send_keys(num_str)

            time.sleep(0.5)
            code_text = self.get_captcha_vue()
            
            if code_text:
                self.driver.find_element(By.ID, "inputCode").send_keys(code_text)
                time.sleep(0.5)
            else:
                logger.warning(f"⚠️ [{num_str}] 驗證碼讀取失敗")
                return False

            btn = self.driver.find_element(By.ID, "btnLogin")
            self.js_click(btn)
            
            # 智慧等待
            try: WebDriverWait(self.driver, 2).until(EC.visibility_of_element_located((By.ID, "loading_div")))
            except: pass
            
            try:
                WebDriverWait(self.driver, 30).until(EC.invisibility_of_element_located((By.ID, "loading_div")))
            except TimeoutException:
                self.driver.refresh()
                return False

            # 檢查 Alert
            try:
                if EC.alert_is_present()(self.driver):
                    alert = self.driver.switch_to.alert
                    alert.accept()
                    return False 
            except NoAlertPresentException: pass

            # 檢查表格
            try:
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.licstable a")))
                links = self.driver.find_elements(By.CSS_SELECTOR, "table.licstable a")
                
                if links:
                    logger.info(f"🔎 [{self.target_year}年][{num_str}] 找到 {len(links)} 筆")
                    main_window = self.driver.current_window_handle
                    
                    for i in range(len(links)):
                        links = self.driver.find_elements(By.CSS_SELECTOR, "table.licstable a")
                        if i >= len(links): break
                        
                        href = links[i].get_attribute('href')
                        # 🔥 使用 JS 開新分頁
                        self.driver.execute_script(f"window.open('{href}', '_blank');")
                        time.sleep(2.0)
                        
                        new_window = [w for w in self.driver.window_handles if w != main_window][0]
                        self.driver.switch_to.window(new_window)
                        
                        # 進入詳情頁
                        self.process_detail_page(num_str)
                        
                        self.driver.close()
                        self.driver.switch_to.window(main_window)
                    return True
            except: pass

        except Exception as e:
            logger.warning(f"⚠️ [{self.target_year}] 連線異常，冷卻 5 秒... {e}")
            time.sleep(5)
            self.close_driver()
            self.init_driver()
        
        return False 

    def run(self):
        try:
            self.init_driver()
            logger.info(f"🟢 [{self.target_year}年] 數據保全版啟動 | 範圍: {self.start_num}~{self.end_num}")
            
            consecutive_fails = 0
            
            for i in range(self.start_num, self.end_num + 1):
                success = False
                for retry in range(2):
                    if self.search_and_process_single_try(i):
                        success = True
                        break
                    time.sleep(2)

                if success:
                    consecutive_fails = 0
                else:
                    consecutive_fails += 1
                    if consecutive_fails % 10 == 0:
                        logger.info(f"   [{self.target_year}年] 連續 {consecutive_fails} 筆無資料...")

                if consecutive_fails >= MAX_CONSECUTIVE_FAILS: 
                    logger.info(f"🛑 [{self.target_year}年] 連續 {MAX_CONSECUTIVE_FAILS} 筆無資料，結束。")
                    break

                time.sleep(random.uniform(2.5, 4.0))
        except Exception as e:
            logger.error(f"❌ 線程 [{self.target_year}] 崩潰: {e}")
        finally:
            self.driver.quit()

if __name__ == "__main__":
    print(f"🚀 啟動高雄市 v14 數據保全版")
    print(f"✨ 特點: 強制 .csv 格式 | 立即寫入硬碟 | 平行執行")

    threads = []
    for year in TARGET_YEARS:
        t = threading.Thread(target=lambda y: KaohsiungDataSafeScraper(y, START_NUM, END_NUM, f"kaohsiung_v14_{y}.xlsx").run(), args=(year,))
        threads.append(t)
        t.start()
        print(f"⏳ 啟動 [{year}年] 線程，休息 10 秒再啟動下一個...")
        time.sleep(10) 

    for t in threads:
        t.join()
