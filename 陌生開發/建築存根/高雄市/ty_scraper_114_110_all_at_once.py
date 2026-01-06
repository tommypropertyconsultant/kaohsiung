#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import os
import logging
import pandas as pd
import random
import re
import threading
import csv
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# 設定 Log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# ==========================================
# ✅ 設定存檔路徑
BASE_PATH = r'/Users/wangliwen/Desktop/ JLL/陌生開發/建築存根/桃園市'

# 🎯 設定年份組 (一次全開！)
# 將所有年份放在同一個列表中，程式會同時啟動 5 個視窗
YEAR_BATCHES = [
    ["114", "113", "112", "111", "110"]
]

# 🎯 設定範圍
START_NUM = 0
END_NUM = 3000

# 🛑 停損設定 (維持嚴格標準)
MAX_SAME_NUM_RETRIES = 3       # 單號重試 3 次
MAX_CONSECUTIVE_YEAR_FAILS = 5 # 連續 5 號空就停
# ==========================================

class TyScraperStrict114:
    def __init__(self, target_year, start_num, end_num, output_filename):
        self.url = "https://building.tycg.gov.tw/bupic/preLoginFormAction.do"
        self.target_year = target_year
        self.start_num = start_num
        self.end_num = end_num
        self.output_filename = output_filename
        self.csv_filename = output_filename.replace(".xlsx", ".csv")
        self.driver = None
        self.results = []
        
        self.target_folder = os.path.join(BASE_PATH, self.target_year)
        if not os.path.exists(self.target_folder):
            try:
                os.makedirs(self.target_folder)
                logger.info(f"📁 [{self.target_year}] 資料夾準備就緒")
            except: pass

        self.init_csv()

    def init_csv(self):
        csv_path = os.path.join(self.target_folder, self.csv_filename)
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
            except Exception as e:
                logger.error(f"⚠️ CSV 初始化失敗: {e}")

    def save_row_to_csv(self, record):
        csv_path = os.path.join(self.target_folder, self.csv_filename)
        try:
            with open(csv_path, mode='a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=record.keys())
                writer.writerow(record)
        except Exception as e:
            logger.error(f"⚠️ 單筆寫入失敗: {e}")

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

    def solve_captcha_direct(self):
        try:
            return self.driver.find_element(By.ID, "checkCode").text.strip() or \
                   self.driver.execute_script("return document.getElementById('checkCode').innerText")
        except: return ""

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

    def extract_usage_between_keywords(self, full_text):
        start_key = "使用類組"
        end_key = "備註" 
        backup_end_keys = ["注意事項", "起造人", "設計人", "說明", "發照日期"]
        if start_key not in full_text: return ""
        try:
            content_after_start = full_text.split(start_key, 1)[1]
            if content_after_start.strip().startswith(":") or content_after_start.strip().startswith("："):
                content_after_start = content_after_start.strip()[1:]
            target_content = ""
            if end_key in content_after_start:
                target_content = content_after_start.split(end_key, 1)[0]
            else:
                found_backup = False
                for k in backup_end_keys:
                    if k in content_after_start:
                        target_content = content_after_start.split(k, 1)[0]
                        found_backup = True
                        break
                if not found_backup:
                    target_content = content_after_start[:100]
            return target_content.strip()
        except: return ""

    def process_detail_page_in_new_tab(self, search_num):
        try:
            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            time.sleep(0.5) 
            full_text = self.get_full_text_safe()
            
            license_no = ""
            regex_pattern = fr"(\(\s*{self.target_year}\s*\).*?號)"
            match = re.search(regex_pattern, full_text)
            if match: license_no = match.group(1)
            else:
                match = re.search(r"(桃市.*?執照.*?號)", full_text)
                license_no = match.group(1) if match else ""

            if not license_no and ("執照" not in full_text): return
            if not license_no: license_no = f"[需人工確認] {search_num}"

            builder = self.extract_value_from_text(full_text, ["姓名"], ["事務所", "電話"]) 
            if not builder: builder = self.extract_value_from_text(full_text, ["起造人"], ["設計人"])

            raw_location = self.extract_value_from_text(full_text, ["地址", "建築地點", "地號", "基地坐落"], ["使用分區", "基地面積"])
            district = ""
            clean_location = raw_location
            if "區" in raw_location:
                try:
                    idx = raw_location.find("區")
                    start = max(0, idx - 3)
                    candidate = raw_location[start:idx+1]
                    if "市" in candidate: district = candidate.split("市")[-1]
                    else: district = candidate[-3:]
                    clean_location = raw_location.strip()
                except: pass

            zoning = self.extract_value_from_text(full_text, ["使用分區"], ["基地面積", "建物概要"])
            units = self.extract_value_from_text(full_text, ["層棟戶數"], ["設計建蔽率", "法定空地"])
            
            site_area_total = self.extract_value_from_text(full_text, ["合計", "基地面積"], ["㎡", "m2", "騎樓地"])
            if site_area_total and "㎡" not in site_area_total: site_area_total += " ㎡"

            build_area_other = ""
            if "建築面積" in full_text:
                try:
                    text_after_build = full_text.split("建築面積", 1)[1]
                    build_area_other = self.extract_value_from_text(text_after_build, ["其他"], ["㎡", "m2"])
                    if build_area_other: build_area_other += " ㎡"
                except: pass

            legal_open = self.extract_value_from_text(full_text, ["法定空地面積", "法定空地"], ["㎡", "m2"])
            if legal_open: legal_open += " ㎡"
            floor_area = self.extract_value_from_text(full_text, ["總樓地板面積", "樓地板面積"], ["㎡", "m2"])
            if floor_area: floor_area += " ㎡"
            date = self.extract_value_from_text(full_text, ["發照日期"], ["注意事項", "供公眾"])
            usage_data = self.extract_usage_between_keywords(full_text)

            record = {
                "搜尋編號": search_num,
                "執照號碼": license_no,
                "起造人": builder,
                "行政區": district,
                "建築地點": clean_location,
                "使用分區": zoning,
                "層棟戶數": units,
                "基地面積(合計)": site_area_total,
                "建築面積(其他)": build_area_other,
                "法定空地面積": legal_open,
                "總樓地板面積": floor_area,
                "發照日期": date,
                "使用類組": usage_data
            }
            
            self.results.append(record)
            self.save_row_to_csv(record)
            logger.info(f"   ✅ [{self.target_year}年] {license_no} | {district} | 地點: {clean_location[:10]}")

        except Exception as e:
            logger.error(f"   ❌ [{self.target_year}年] 解析失敗: {e}")

    def search_and_process_single_try(self, number_val):
        num_str = f"{number_val:05d}"
        try:
            self.driver.get(self.url)
            wait = WebDriverWait(self.driver, 10)
            
            # 🔥 嚴格使用 .clear()
            year_input = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[contains(@placeholder, '年度')] | //input[@name='keYear']")))
            year_input.clear()
            year_input.send_keys(self.target_year)
            
            no_input = self.driver.find_element(By.XPATH, "//input[contains(@placeholder, '號碼')] | //input[@name='keNo']")
            no_input.clear()
            no_input.send_keys(num_str)

            code = self.solve_captcha_direct()
            if code:
                self.driver.find_element(By.XPATH, "//input[contains(@placeholder, '驗證碼')] | //input[@name='checkCode']").send_keys(code)
            
            self.driver.find_element(By.XPATH, "//input[@type='button' and @value='查詢'] | //button[contains(., '查詢')]").click()
            
            try:
                WebDriverWait(self.driver, 2).until(EC.alert_is_present())
                self.driver.switch_to.alert.accept()
                return False 
            except TimeoutException: pass

            try: wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except: return False 

            links = self.driver.find_elements(By.XPATH, "//table//tr/td//a[contains(@href, 'do')]")
            if not links: return False 

            logger.info(f"🔎 [{self.target_year}年][{num_str}] 找到 {len(links)} 筆")
            self.main_window = self.driver.current_window_handle
            
            for i in range(len(links)):
                links = self.driver.find_elements(By.XPATH, "//table//tr/td//a[contains(@href, 'do')]")
                if i >= len(links): break
                href = links[i].get_attribute('href')
                self.driver.execute_script(f"window.open('{href}', '_blank');")
                time.sleep(2.0)
                new_window = [w for w in self.driver.window_handles if w != self.main_window][0]
                self.driver.switch_to.window(new_window)
                self.process_detail_page_in_new_tab(num_str)
                self.driver.close()
                self.driver.switch_to.window(self.main_window)
                time.sleep(0.5)
            
            return True 

        except Exception as e:
            if "unexpectedly exited" in str(e) or "disconnected" in str(e):
                logger.warning(f"🚨 Driver 崩潰，重啟中...")
                self.close_driver()
                time.sleep(3)
                self.init_driver()
            return False

    def run(self):
        self.init_driver()
        logger.info(f"🟢 [{self.target_year}年] 火力全開版啟動 | 範圍: {self.start_num}~{self.end_num}")
        
        consecutive_year_fails = 0 
        counter = 0
        
        for i in range(self.start_num, self.end_num + 1):
            
            if counter > 0 and counter % 50 == 0:
                logger.info(f"♻️ [{self.target_year}年] 換氣釋放記憶體...")
                self.close_driver()
                time.sleep(2)
                self.init_driver()

            current_num_found = False
            for retry in range(1, MAX_SAME_NUM_RETRIES + 1):
                if self.search_and_process_single_try(i):
                    current_num_found = True
                    break 
                else:
                    if retry < MAX_SAME_NUM_RETRIES:
                        time.sleep(1.0) 
            
            if current_num_found:
                consecutive_year_fails = 0 
            else:
                consecutive_year_fails += 1
                logger.warning(f"❌ [{self.target_year}年][{i:05d}] 空號 (累積 {consecutive_year_fails}/{MAX_CONSECUTIVE_YEAR_FAILS})")

            if consecutive_year_fails >= MAX_CONSECUTIVE_YEAR_FAILS:
                logger.info(f"🛑 [{self.target_year}年] 連續 {MAX_CONSECUTIVE_YEAR_FAILS} 筆空號，判定結束。")
                break 

            counter += 1
            time.sleep(random.uniform(2.0, 3.5)) 
            
        if self.results:
            try:
                output_path = os.path.join(self.target_folder, self.output_filename)
                pd.DataFrame(self.results).to_excel(output_path, index=False)
                logger.info(f"💾 [{self.target_year}年] Excel 產出: {output_path}")
            except: pass
        else:
            logger.info(f"⚠️ [{self.target_year}年] 無資料")
            
        self.driver.quit()

def run_scraper_thread(year, start, end):
    filename = f"tycg_permits_{year}_ALL_AT_ONCE_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    try:
        bot = TyScraperStrict114(year, start, end, filename)
        bot.run()
    except Exception as e:
        logger.error(f"❌ 線程 [{year}年] 錯誤: {e}")

if __name__ == "__main__":
    print(f"🚀 啟動 [114~110年] 五視窗火力全開版")
    print(f"✨ 執行模式: 5 年份同時執行 (請確保電源已接上)")
    print(f"✨ 使用 .clear() 嚴格搜尋 | CSV 即時存檔")

    for batch in YEAR_BATCHES:
        print(f"\n======== 🎬 開始執行批次：{batch} ========")
        threads = []
        for year in batch:
            t = threading.Thread(target=run_scraper_thread, args=(year, START_NUM, END_NUM))
            threads.append(t)
            t.start()
            time.sleep(5) 

        for t in threads:
            t.join()
        
        print(f"✅ 任務完成！")

    print("\n🏁 114~110 全數任務完成！")
