import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExcelToDatabaseConverter:
    def __init__(self, excel_folder='./downloads', db_path='insurance_data.db'):
        self.excel_folder = excel_folder
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """데이터베이스 초기화"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 보험 상품 정보 테이블
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS insurance_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                company_name TEXT NOT NULL,
                product_name TEXT NOT NULL,
                coverage_type TEXT,
                coverage_amount TEXT,
                premium_male TEXT,
                premium_female TEXT,
                sales_channel TEXT,
                sales_date TEXT,
                product_summary_url TEXT,
                special_notes TEXT,
                contact_number TEXT,
                crawl_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_file TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("데이터베이스 초기화 완료")
    
    def extract_category_from_filename(self, filename):
        """파일명에서 카테고리 추출"""
        categories = ['종신보험', '정기보험', '질병보험', '암보험', 'CI보험', '상해보험', '어린이보험', '치아보험', '간병보험', '치매보험']
        
        for category in categories:
            if category in filename:
                return category
        
        return '기타'
    
    def clean_data(self, df):
        """데이터 정리"""
        # 빈 행 제거
        df = df.dropna(how='all')
        
        logger.info(f"원본 컬럼 수: {len(df.columns)}")
        logger.info(f"원본 컬럼명: {list(df.columns)}")
        
        # 실제 컬럼 수에 맞춰 동적으로 처리
        col_count = len(df.columns)
        
        if col_count >= 7:
            # 기본 컬럼들만 사용
            new_columns = ['선택', '보험회사명', '상품명', '보장내용_및_보험료']
            
            # 나머지 컬럼들은 기타로 처리
            for i in range(4, col_count):
                if i == col_count - 1:
                    new_columns.append('상품운영정보')
                else:
                    new_columns.append(f'기타{i-3}')
            
            df.columns = new_columns
        
        # 불필요한 컬럼 제거
        columns_to_keep = ['보험회사명', '상품명', '보장내용_및_보험료', '상품운영정보']
        existing_columns = [col for col in columns_to_keep if col in df.columns]
        
        if existing_columns:
            df = df[existing_columns]
        
        return df
    
    def parse_product_info(self, row):
        """상품 정보 파싱"""
        try:
            product_info = {
                'company_name': str(row.get('보험회사명', '')).strip(),
                'product_name': str(row.get('상품명', '')).strip(),
                'coverage_details': str(row.get('보장내용_및_보험료', '')).strip(),
                'operation_info': str(row.get('상품운영정보', '')).strip()
            }
            
            # 보장내용에서 보험료 정보 추출
            coverage_text = product_info['coverage_details']
            
            # 보험료 추출 (남자/여자)
            import re
            
            # 남자 보험료 패턴
            male_pattern = r'남자?\s*(\d{1,3}(?:,\d{3})*)\s*원'
            female_pattern = r'여자?\s*(\d{1,3}(?:,\d{3})*)\s*원'
            
            male_premium = ''
            female_premium = ''
            
            male_match = re.search(male_pattern, coverage_text)
            if male_match:
                male_premium = male_match.group(1)
            
            female_match = re.search(female_pattern, coverage_text)
            if female_match:
                female_premium = female_match.group(1)
            
            product_info['premium_male'] = male_premium
            product_info['premium_female'] = female_premium
            
            # 운영정보에서 판매채널, 판매일자, 연락처 추출
            operation_text = product_info['operation_info']
            
            # 판매채널
            if '대면채널' in operation_text:
                product_info['sales_channel'] = '대면채널'
            elif '온라인' in operation_text:
                product_info['sales_channel'] = '온라인'
            else:
                product_info['sales_channel'] = ''
            
            # 판매일자 추출
            date_pattern = r'(\d{4}-\d{2}-\d{2})'
            date_match = re.search(date_pattern, operation_text)
            product_info['sales_date'] = date_match.group(1) if date_match else ''
            
            # 연락처 추출
            phone_pattern = r'(\d{4}-\d{4}|\d{3}-\d{3,4}-\d{4})'
            phone_match = re.search(phone_pattern, operation_text)
            product_info['contact_number'] = phone_match.group(1) if phone_match else ''
            
            return product_info
            
        except Exception as e:
            logger.error(f"상품 정보 파싱 실패: {e}")
            return None
    
    def process_excel_file(self, file_path, category):
        """엑셀 파일 처리 (실제로는 HTML 파일)"""
        try:
            logger.info(f"파일 처리 시작: {file_path}")
            
            # 파일이 HTML 형식인지 확인
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if '<html' in content.lower():
                # HTML 파일로 처리
                logger.info("HTML 형식 파일로 인식")
                df = pd.read_html(file_path, encoding='utf-8')[0]  # 첫 번째 테이블
            else:
                # 실제 엑셀 파일로 처리
                logger.info("엑셀 형식 파일로 인식")
                df = pd.read_excel(file_path)
            
            logger.info(f"원본 데이터 행 수: {len(df)}")
            
            # 데이터 정리
            df = self.clean_data(df)
            logger.info(f"정리 후 데이터 행 수: {len(df)}")
            
            # 데이터베이스에 저장
            conn = sqlite3.connect(self.db_path)
            
            processed_count = 0
            
            for index, row in df.iterrows():
                try:
                    # 빈 행 건너뛰기
                    if pd.isna(row.get('보험회사명')) or str(row.get('보험회사명')).strip() == '':
                        continue
                    
                    product_info = self.parse_product_info(row)
                    
                    if product_info and product_info['company_name'] and product_info['product_name']:
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO insurance_products 
                            (category, company_name, product_name, coverage_type, 
                             premium_male, premium_female, sales_channel, sales_date, 
                             contact_number, source_file)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            category,
                            product_info['company_name'],
                            product_info['product_name'],
                            product_info['coverage_details'][:500],  # 길이 제한
                            product_info['premium_male'],
                            product_info['premium_female'],
                            product_info['sales_channel'],
                            product_info['sales_date'],
                            product_info['contact_number'],
                            os.path.basename(file_path)
                        ))
                        
                        processed_count += 1
                        
                except Exception as e:
                    logger.error(f"행 {index} 처리 실패: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
            logger.info(f"{category} 카테고리 처리 완료: {processed_count}개 상품 저장")
            return processed_count
            
        except Exception as e:
            logger.error(f"엑셀 파일 처리 실패 {file_path}: {e}")
            return 0
    
    def convert_all_files(self):
        """모든 엑셀 파일을 데이터베이스로 변환"""
        try:
            if not os.path.exists(self.excel_folder):
                logger.error(f"다운로드 폴더가 없습니다: {self.excel_folder}")
                return
            
            excel_files = [f for f in os.listdir(self.excel_folder) if f.endswith('.xls') or f.endswith('.xlsx')]
            
            if not excel_files:
                logger.error("처리할 엑셀 파일이 없습니다.")
                return
            
            logger.info(f"총 {len(excel_files)}개 엑셀 파일 발견")
            
            total_products = 0
            
            for excel_file in excel_files:
                file_path = os.path.join(self.excel_folder, excel_file)
                category = self.extract_category_from_filename(excel_file)
                
                logger.info(f"\n처리 중: {excel_file} (카테고리: {category})")
                
                count = self.process_excel_file(file_path, category)
                total_products += count
            
            logger.info(f"\n=== 변환 완료 ===")
            logger.info(f"총 {total_products}개 상품이 데이터베이스에 저장되었습니다.")
            
            # 통계 출력
            self.print_statistics()
            
        except Exception as e:
            logger.error(f"전체 변환 실패: {e}")
    
    def print_statistics(self):
        """데이터베이스 통계 출력"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 전체 상품 수
            cursor.execute("SELECT COUNT(*) FROM insurance_products")
            total_count = cursor.fetchone()[0]
            
            # 카테고리별 상품 수
            cursor.execute("""
                SELECT category, COUNT(*) as count 
                FROM insurance_products 
                GROUP BY category 
                ORDER BY count DESC
            """)
            category_stats = cursor.fetchall()
            
            # 보험회사별 상품 수 (상위 10개)
            cursor.execute("""
                SELECT company_name, COUNT(*) as count 
                FROM insurance_products 
                GROUP BY company_name 
                ORDER BY count DESC 
                LIMIT 10
            """)
            company_stats = cursor.fetchall()
            
            conn.close()
            
            print(f"\n=== 데이터베이스 통계 ===")
            print(f"전체 상품 수: {total_count}")
            
            print(f"\n카테고리별 상품 수:")
            for category, count in category_stats:
                print(f"  {category}: {count}개")
            
            print(f"\n보험회사별 상품 수 (상위 10개):")
            for company, count in company_stats:
                print(f"  {company}: {count}개")
                
        except Exception as e:
            logger.error(f"통계 출력 실패: {e}")
    
    def export_to_csv(self, category=None):
        """특정 카테고리 또는 전체 데이터를 CSV로 내보내기"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            if category:
                query = "SELECT * FROM insurance_products WHERE category = ?"
                df = pd.read_sql_query(query, conn, params=(category,))
                filename = f"{category}_products.csv"
            else:
                query = "SELECT * FROM insurance_products"
                df = pd.read_sql_query(query, conn)
                filename = "all_insurance_products.csv"
            
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            conn.close()
            
            logger.info(f"CSV 내보내기 완료: {filename} ({len(df)}개 상품)")
            return filename
            
        except Exception as e:
            logger.error(f"CSV 내보내기 실패: {e}")
            return None

def main():
    """메인 실행 함수"""
    converter = ExcelToDatabaseConverter()
    
    print("엑셀 파일을 데이터베이스로 변환합니다...")
    
    # 모든 엑셀 파일 변환
    converter.convert_all_files()
    
    # 암보험 데이터만 CSV로 내보내기
    print("\n암보험 데이터를 CSV로 내보내시겠습니까? (y/n): ", end="")
    if input().lower() == 'y':
        converter.export_to_csv('암보험')
    
    print("변환 작업이 완료되었습니다!")

if __name__ == "__main__":
    main()