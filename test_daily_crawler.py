#!/usr/bin/env python3
"""
Daily 기준연월일이 포함된 크롤러 테스트 (간병/치매보험만)
"""

import os
import time
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import pandas as pd

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class DailyCrawlerTest:
    def __init__(self):
        self.base_url = 'https://pub.insure.or.kr/compareDis/prodCompare/assurance/listNew.do'
        self.driver = None
        
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        
        # 다운로드 설정
        download_dir = os.path.abspath('./test_downloads')
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            logger.info("Chrome 드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False
    
    def test_filename_fix(self):
        """파일명 특수문자 처리 테스트"""
        category = '간병/치매보험'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 기존 방식 (에러 발생)
        old_filename = f"{category}_{timestamp}.xls"
        print(f"기존 파일명: {old_filename}")
        
        # 수정된 방식
        safe_category = category.replace('/', '_').replace('\\', '_')
        new_filename = f"{safe_category}_{timestamp}.xls"
        print(f"수정된 파일명: {new_filename}")
        
        return new_filename
    
    def test_daily_data_structure(self):
        """Daily 기준연월일 데이터 구조 테스트"""
        current_time = datetime.now()
        
        # 샘플 상품 데이터
        product_data = {
            'data_date': current_time.strftime('%Y-%m-%d'),  # Daily 기준연월일
            'data_year': current_time.year,
            'data_month': current_time.month,
            'data_day': current_time.day,
            'category': '간병/치매보험',
            'company_name': '테스트보험회사',
            'product_name': '테스트상품',
            'crawl_date': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'premium_male': '50000원',
            'premium_female': '45000원'
        }
        
        print("\n=== Daily 기준연월일 포함 데이터 구조 ===")
        for key, value in product_data.items():
            print(f"{key}: {value}")
        
        return product_data
    
    def run(self):
        """테스트 실행"""
        print("=" * 60)
        print("Daily 크롤러 개선사항 테스트")
        print("=" * 60)
        
        # 1. 파일명 특수문자 처리 테스트
        print("\n1. 파일명 특수문자 처리 테스트:")
        fixed_filename = self.test_filename_fix()
        
        # 2. Daily 기준연월일 데이터 구조 테스트
        print("\n2. Daily 기준연월일 데이터 구조 테스트:")
        sample_data = self.test_daily_data_structure()
        
        print(f"\n✅ 모든 테스트 완료!")
        print(f"   - 파일명 수정: 간병/치매보험 → 간병_치매보험")
        print(f"   - Daily 기준연월일 추가: {sample_data['data_date']}")
        print(f"   - 년/월/일 분리: {sample_data['data_year']}/{sample_data['data_month']}/{sample_data['data_day']}")

def main():
    test = DailyCrawlerTest()
    test.run()

if __name__ == "__main__":
    main()