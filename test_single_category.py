#!/usr/bin/env python3
"""
단일 카테고리 테스트 - 암보험만
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

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class SingleCategoryTest:
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
    
    def wait_for_download(self, download_dir, timeout=30):
        """다운로드 완료 대기 - 개선된 버전"""
        start_time = time.time()
        files_before = set(os.listdir(download_dir))
        logger.info(f"다운로드 시작 전 파일들: {len(files_before)}개")
        
        # 다운로드 시작 감지 (crdownload 파일 또는 새 파일 생성)
        download_started = False
        
        while time.time() - start_time < timeout:
            current_files = set(os.listdir(download_dir))
            
            # 다운로드 시작 감지
            if not download_started:
                # .crdownload 파일이 생성되었는지 확인
                crdownload_files = [f for f in current_files if f.endswith('.crdownload')]
                if crdownload_files:
                    download_started = True
                    logger.info(f"다운로드 시작 감지: {crdownload_files[0]}")
                
                # 또는 새로운 엑셀 파일이 바로 생성되었는지 확인
                new_files = current_files - files_before
                excel_files = [f for f in new_files if f.endswith(('.xls', '.xlsx', '.tmp'))]
                if excel_files:
                    download_started = True
                    logger.info(f"다운로드 완료 파일 감지: {excel_files[0]}")
                    return excel_files[0]
            
            # 다운로드 완료 확인
            if download_started:
                # .crdownload 파일이 사라지고 완성된 파일이 있는지 확인
                crdownload_files = [f for f in current_files if f.endswith('.crdownload')]
                if not crdownload_files:
                    # 새로 생성된 엑셀 파일 찾기
                    new_files = current_files - files_before
                    excel_files = [f for f in new_files if f.endswith(('.xls', '.xlsx', '.tmp'))]
                    if excel_files:
                        logger.info(f"다운로드 완료: {excel_files[0]}")
                        return excel_files[0]
            
            time.sleep(1)
        
        # 타임아웃 시 최종 확인
        final_files = set(os.listdir(download_dir))
        new_files = final_files - files_before
        excel_files = [f for f in new_files if f.endswith(('.xls', '.xlsx', '.tmp'))]
        
        if excel_files:
            logger.info(f"타임아웃 후 파일 발견: {excel_files[0]}")
            return excel_files[0]
        
        logger.warning("다운로드 타임아웃 - 파일을 찾을 수 없음")
        return None
    
    def test_cancer_download(self):
        """암보험 다운로드 테스트"""
        try:
            category = '암보험'
            logger.info(f"테스트 시작: {category}")
            
            # 메인 페이지로 이동
            self.driver.get(self.base_url)
            time.sleep(3)
            
            # 암보험 카테고리 클릭
            selectors = [
                f"//span[text()='{category}']",
                f"//label[contains(text(), '{category}')]",
                f"//*[text()='{category}']"
            ]
            
            category_element = None
            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for elem in elements:
                        if elem.text.strip() == category:
                            category_element = elem
                            break
                    if category_element:
                        break
                except:
                    continue
            
            if not category_element:
                logger.error(f"{category} 카테고리를 찾을 수 없습니다.")
                return False
            
            # 클릭
            self.driver.execute_script("arguments[0].scrollIntoView();", category_element)
            time.sleep(1)
            category_element.click()
            time.sleep(3)
            
            # 다운로드 버튼 클릭
            download_selectors = [
                "//button[contains(text(), '다운로드')]",
                "//button[contains(text(), '엑셀')]",
                "//a[contains(text(), '다운로드')]",
                "//a[contains(text(), '엑셀')]"
            ]
            
            download_button = None
            for selector in download_selectors:
                try:
                    buttons = self.driver.find_elements(By.XPATH, selector)
                    if buttons:
                        download_button = buttons[0]
                        break
                except:
                    continue
            
            if not download_button:
                logger.error(f"{category} 다운로드 버튼을 찾을 수 없습니다.")
                return False
            
            # 다운로드 실행
            download_dir = './test_downloads'
            
            # 다운로드 전 파일 목록 저장
            files_before = set(os.listdir(download_dir))
            logger.info(f"다운로드 전 파일들: {files_before}")
            
            download_button.click()
            logger.info("다운로드 버튼 클릭됨")
            time.sleep(3)  # 다운로드 시작 대기
            
            # 다운로드 완료 대기 (단순한 방법)
            downloaded_file = None
            for i in range(30):  # 30초 대기
                current_files = set(os.listdir(download_dir))
                new_files = current_files - files_before
                
                # .crdownload 파일 확인
                crdownload_files = [f for f in current_files if f.endswith('.crdownload')]
                if crdownload_files:
                    logger.info(f"다운로드 진행 중: {crdownload_files[0]}")
                
                # 완료된 파일 확인
                excel_files = [f for f in new_files if f.endswith(('.xls', '.xlsx', '.tmp'))]
                if excel_files and not crdownload_files:
                    downloaded_file = excel_files[0]
                    logger.info(f"다운로드 완료 감지: {downloaded_file}")
                    break
                
                time.sleep(1)
            
            if not downloaded_file:
                # 마지막으로 새 파일이 있는지 확인
                final_files = set(os.listdir(download_dir))
                new_files = final_files - files_before
                excel_files = [f for f in new_files if f.endswith(('.xls', '.xlsx', '.tmp'))]
                if excel_files:
                    downloaded_file = excel_files[0]
                    logger.info(f"최종 확인에서 파일 발견: {downloaded_file}")
            
            if downloaded_file:
                original_path = os.path.join(download_dir, downloaded_file)
                
                # 타임스탬프로 파일명 변경
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                file_extension = '.xls' if downloaded_file.endswith(('.xls', '.xlsx')) else '.tmp'
                new_filename = f"{category}_{timestamp}{file_extension}"
                new_path = os.path.join(download_dir, new_filename)
                
                logger.info(f"파일명 변경 시도: {original_path} -> {new_path}")
                
                # 파일명 변경
                time.sleep(1)
                try:
                    if os.path.exists(original_path):
                        logger.info(f"원본 파일 존재 확인: {original_path}")
                        os.rename(original_path, new_path)
                        logger.info(f"✅ 파일명 변경 성공: {new_filename}")
                        return True
                    else:
                        logger.error(f"원본 파일을 찾을 수 없음: {original_path}")
                        return False
                except Exception as e:
                    logger.error(f"파일명 변경 실패: {e}")
                    return False
            else:
                logger.error(f"{category} 다운로드 실패")
                return False
                
        except Exception as e:
            logger.error(f"테스트 실패: {e}")
            return False
    
    def run(self):
        """테스트 실행"""
        print("=" * 60)
        print("단일 카테고리 다운로드 테스트 (암보험)")
        print("=" * 60)
        
        if not self.setup_driver():
            print("드라이버 설정 실패")
            return False
        
        try:
            success = self.test_cancer_download()
            
            if success:
                print("\n✅ 테스트 성공! 파일명이 올바르게 변경되었습니다.")
            else:
                print("\n❌ 테스트 실패!")
                
            return success
            
        finally:
            if self.driver:
                self.driver.quit()

def main():
    test = SingleCategoryTest()
    test.run()

if __name__ == "__main__":
    main()