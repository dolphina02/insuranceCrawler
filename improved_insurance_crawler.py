#!/usr/bin/env python3
"""
개선된 보험 데이터 크롤러 - 전체 데이터 파싱
"""

import os
import time
import csv
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

class ImprovedInsuranceCrawler:
    def __init__(self):
        self.base_url = 'https://pub.insure.or.kr/compareDis/prodCompare/assurance/listNew.do'
        self.driver = None
        self.results = []
        
        # 보험 카테고리 목록 (전체 9개)
        self.categories = [
            '종신보험', '정기보험', '질병보험', '암보험', 'CI보험', 
            '상해보험', '어린이보험', '치아보험', '간병/치매보험'
        ]
    
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        options = Options()
        
        # 헤드리스 모드 (리눅스 서버용)
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36')
        
        # 다운로드 설정
        download_dir = os.path.abspath('./downloads')
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
        potential_file = None
        
        while time.time() - start_time < timeout:
            current_files = set(os.listdir(download_dir))
            
            # 다운로드 시작 감지
            if not download_started:
                # .crdownload 파일이 생성되었는지 확인
                crdownload_files = [f for f in current_files if f.endswith('.crdownload')]
                if crdownload_files:
                    download_started = True
                    potential_file = crdownload_files[0].replace('.crdownload', '')
                    logger.info(f"다운로드 시작 감지: {crdownload_files[0]}")
                
                # 또는 새로운 엑셀 파일이 바로 생성되었는지 확인
                new_files = current_files - files_before
                excel_files = [f for f in new_files if f.endswith(('.xls', '.xlsx', '.tmp'))]
                if excel_files:
                    download_started = True
                    potential_file = excel_files[0]
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
    
    def click_category_and_download(self, category):
        """카테고리 클릭하고 다운로드 - 기존 로직 유지"""
        try:
            logger.info(f"처리 중: {category}")
            
            # 메인 페이지로 이동
            self.driver.get(self.base_url)
            time.sleep(3)
            
            # 카테고리 클릭
            category_element = None
            selectors = [
                f"//span[text()='{category}']",
                f"//label[contains(text(), '{category}')]",
                f"//*[text()='{category}']"
            ]
            
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
                logger.warning(f"{category} 카테고리를 찾을 수 없습니다.")
                return None
            
            # 클릭
            self.driver.execute_script("arguments[0].scrollIntoView();", category_element)
            time.sleep(1)
            
            try:
                parent = category_element.find_element(By.XPATH, "..")
                if parent.tag_name in ['label', 'a', 'button']:
                    parent.click()
                else:
                    category_element.click()
            except:
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
                logger.warning(f"{category} 다운로드 버튼을 찾을 수 없습니다.")
                return None
            
            # 다운로드 실행
            download_dir = './downloads'
            
            # 다운로드 전 파일 목록 저장
            files_before = set(os.listdir(download_dir))
            logger.info(f"다운로드 전 파일들: {len(files_before)}개")
            
            download_button.click()
            logger.info("다운로드 버튼 클릭됨")
            time.sleep(3)  # 다운로드 시작 대기
            
            # 다운로드 완료 대기 (단순하고 확실한 방법)
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
                # 파일명에서 특수문자 제거
                safe_category = category.replace('/', '_').replace('\\', '_')
                new_filename = f"{safe_category}_{timestamp}{file_extension}"
                new_path = os.path.join(download_dir, new_filename)
                
                # 파일명 변경 (약간의 대기 후)
                time.sleep(1)  # 파일 잠금 해제 대기
                logger.info(f"파일명 변경 시도: {original_path} -> {new_path}")
                try:
                    if os.path.exists(original_path):
                        logger.info(f"원본 파일 존재 확인: {original_path}")
                        os.rename(original_path, new_path)
                        logger.info(f"{category} 다운로드 완료 (파일명 변경됨): {new_filename}")
                        return new_path
                    else:
                        logger.warning(f"원본 파일을 찾을 수 없음: {original_path}")
                        # 다운로드 폴더의 모든 파일 확인
                        files_in_dir = os.listdir(download_dir)
                        logger.info(f"다운로드 폴더 내 파일들: {files_in_dir}")
                        return None
                except Exception as e:
                    logger.error(f"파일명 변경 실패: {e}")
                    logger.info(f"{category} 다운로드 완료 (원본 파일명 유지): {downloaded_file}")
                    return original_path
            else:
                logger.warning(f"{category} 다운로드 실패")
                return None
                
        except Exception as e:
            logger.error(f"{category} 처리 실패: {e}")
            return None
    
    def parse_excel_file_improved(self, file_path, category):
        """개선된 엑셀 파일 파싱 - 모든 데이터 추출"""
        try:
            # HTML 테이블로 읽기
            df = pd.read_html(file_path, encoding='utf-8')[0]
            logger.info(f"{category} 파일 크기: {df.shape}")
            
            # 멀티레벨 컬럼 처리 개선
            if isinstance(df.columns[0], tuple):
                # 컬럼명을 의미있게 조합
                new_columns = []
                for col in df.columns:
                    if isinstance(col, tuple):
                        # 튜플의 모든 레벨을 조합해서 의미있는 컬럼명 생성
                        col_parts = [str(part) for part in col if str(part) != 'nan' and str(part).strip()]
                        if col_parts:
                            new_columns.append('_'.join(col_parts))
                        else:
                            new_columns.append(f'col_{len(new_columns)}')
                    else:
                        new_columns.append(str(col))
                df.columns = new_columns
            
            # 데이터 추출 - 모든 컬럼 정보 보존
            products = []
            for index, row in df.iterrows():
                try:
                    # 첫 번째 컬럼이 보험회사명인지 확인
                    company = str(row.iloc[0]).strip() if len(row) > 0 else ''
                    
                    # 빈 행이나 헤더 행 건너뛰기
                    if not company or company == 'nan' or '보험회사' in company:
                        continue
                    
                    # 두 번째 컬럼이 상품명
                    product = str(row.iloc[1]).strip() if len(row) > 1 else ''
                    
                    if company and product and product != 'nan':
                        # 모든 컬럼 정보를 딕셔너리로 저장 (Daily 기준연월일 포함)
                        current_time = datetime.now()
                        product_data = {
                            'category': category,
                            'company_name': company,
                            'product_name': product,
                            'crawl_date': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'data_date': current_time.strftime('%Y-%m-%d'),  # Daily 기준연월일
                            'data_year': current_time.year,
                            'data_month': current_time.month,
                            'data_day': current_time.day
                        }
                        
                        # 나머지 컬럼들도 추가
                        for i, col_name in enumerate(df.columns):
                            if i < len(row):
                                value = str(row.iloc[i]).strip()
                                if value and value != 'nan':
                                    # 컬럼명 정리
                                    clean_col_name = col_name.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '')
                                    product_data[clean_col_name] = value
                        
                        products.append(product_data)
                        
                except Exception as e:
                    logger.debug(f"행 {index} 처리 중 오류: {e}")
                    continue
            
            logger.info(f"{category} 파싱 완료: {len(products)}개 상품")
            return products
            
        except Exception as e:
            logger.error(f"{category} 파싱 실패: {e}")
            return []
    
    def save_to_csv_improved(self, filename='insurance_products_full.csv'):
        """개선된 CSV 저장 - 모든 컬럼 정보 포함"""
        try:
            if not self.results:
                logger.warning("저장할 데이터가 없습니다.")
                return False
            
            # 모든 가능한 컬럼명 수집
            all_columns = set()
            for product in self.results:
                all_columns.update(product.keys())
            
            # 기본 컬럼들을 앞에 배치 (Daily 기준연월일 포함)
            basic_columns = ['data_date', 'data_year', 'data_month', 'data_day', 'category', 'company_name', 'product_name', 'crawl_date']
            other_columns = sorted([col for col in all_columns if col not in basic_columns])
            fieldnames = basic_columns + other_columns
            
            # CSV 파일 작성
            with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for product in self.results:
                    writer.writerow(product)
            
            logger.info(f"CSV 저장 완료: {filename} ({len(self.results)}개 상품, {len(fieldnames)}개 컬럼)")
            return True
            
        except Exception as e:
            logger.error(f"CSV 저장 실패: {e}")
            return False
    
    def run(self):
        """메인 실행 함수"""
        print("=" * 60)
        print("개선된 보험 데이터 크롤러 시작")
        print("=" * 60)
        
        # 드라이버 설정
        if not self.setup_driver():
            print("드라이버 설정 실패")
            return False
        
        try:
            total_products = 0
            
            # 각 카테고리 처리
            for category in self.categories:
                print(f"\n[{category}] 처리 중...")
                
                # 다운로드
                file_path = self.click_category_and_download(category)
                
                if file_path:
                    # 개선된 파싱
                    products = self.parse_excel_file_improved(file_path, category)
                    
                    if products:
                        self.results.extend(products)
                        total_products += len(products)
                        print(f"[{category}] ✓ {len(products)}개 상품 수집")
                    else:
                        print(f"[{category}] ✗ 데이터 파싱 실패")
                else:
                    print(f"[{category}] ✗ 다운로드 실패")
                
                # 카테고리 간 대기
                time.sleep(2)
            
            # 결과 저장
            if self.results:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                # 전체 데이터 (모든 컬럼 포함) - downloads 폴더에 저장
                all_filename = f'downloads/insurance_products_full_{timestamp}.csv'
                self.save_to_csv_improved(all_filename)
                
                # 암보험만 별도 저장 - downloads 폴더에 저장
                cancer_products = [p for p in self.results if p['category'] == '암보험']
                if cancer_products:
                    cancer_filename = f'downloads/cancer_insurance_full_{timestamp}.csv'
                    
                    # 암보험 전용 CSV 저장
                    all_columns = set()
                    for product in cancer_products:
                        all_columns.update(product.keys())
                    
                    basic_columns = ['data_date', 'data_year', 'data_month', 'data_day', 'category', 'company_name', 'product_name', 'crawl_date']
                    other_columns = sorted([col for col in all_columns if col not in basic_columns])
                    fieldnames = basic_columns + other_columns
                    
                    with open(cancer_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        for product in cancer_products:
                            writer.writerow(product)
                    
                    logger.info(f"암보험 전체 데이터 CSV 저장: {cancer_filename} ({len(cancer_products)}개 상품, {len(fieldnames)}개 컬럼)")
                
                print(f"\n" + "=" * 60)
                print(f"크롤링 완료!")
                print(f"총 수집 상품: {total_products}개")
                print(f"전체 데이터: {all_filename}")
                if cancer_products:
                    print(f"암보험 전체 데이터: {cancer_filename}")
                print("=" * 60)
                
                return True
            else:
                print("수집된 데이터가 없습니다.")
                return False
                
        except Exception as e:
            logger.error(f"크롤링 실패: {e}")
            return False
        
        finally:
            if self.driver:
                self.driver.quit()
            
            # 다운로드 완료 메시지
            logger.info("다운로드된 파일들이 downloads/ 폴더에 저장되었습니다.")

def main():
    """메인 함수"""
    crawler = ImprovedInsuranceCrawler()
    success = crawler.run()
    
    if success:
        print("\n개선된 크롤링이 성공적으로 완료되었습니다! 🎉")
        print("이제 모든 컬럼 정보가 포함된 CSV 파일을 확인하세요.")
    else:
        print("\n크롤링 중 오류가 발생했습니다. ❌")

if __name__ == "__main__":
    main()