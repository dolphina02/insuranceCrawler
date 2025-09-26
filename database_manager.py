import sqlite3
# import pandas as pd  # pandas 없이 실행
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path='cancer_insurance.db'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """데이터베이스 및 테이블 초기화"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 암보험 상품 정보 테이블
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cancer_insurance_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT NOT NULL,
                product_name TEXT NOT NULL,
                product_code TEXT,
                coverage_type TEXT,
                premium_info TEXT,
                coverage_details TEXT,
                special_features TEXT,
                min_age INTEGER,
                max_age INTEGER,
                crawl_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_url TEXT,
                UNIQUE(company_name, product_name, product_code)
            )
        ''')
        
        # 보험료 정보 테이블
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS premium_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER,
                age INTEGER,
                gender TEXT,
                premium_amount INTEGER,
                coverage_amount INTEGER,
                payment_period TEXT,
                coverage_period TEXT,
                FOREIGN KEY (product_id) REFERENCES cancer_insurance_products (id)
            )
        ''')
        
        # 보장내용 상세 테이블
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS coverage_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER,
                coverage_name TEXT,
                coverage_amount INTEGER,
                coverage_condition TEXT,
                FOREIGN KEY (product_id) REFERENCES cancer_insurance_products (id)
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("데이터베이스 초기화 완료")
    
    def insert_product(self, product_data):
        """상품 정보 삽입"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO cancer_insurance_products 
                (company_name, product_name, product_code, coverage_type, 
                 premium_info, coverage_details, special_features, 
                 min_age, max_age, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                product_data.get('company_name', ''),
                product_data.get('product_name', ''),
                product_data.get('product_code', ''),
                product_data.get('coverage_type', ''),
                product_data.get('premium_info', ''),
                product_data.get('coverage_details', ''),
                product_data.get('special_features', ''),
                product_data.get('min_age'),
                product_data.get('max_age'),
                product_data.get('source_url', '')
            ))
            
            product_id = cursor.lastrowid
            conn.commit()
            return product_id
            
        except sqlite3.IntegrityError as e:
            logger.warning(f"중복 상품 데이터: {e}")
            return None
        except Exception as e:
            logger.error(f"상품 데이터 삽입 실패: {e}")
            return None
        finally:
            conn.close()
    
    def insert_premium_detail(self, product_id, premium_data):
        """보험료 상세 정보 삽입"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO premium_details 
                (product_id, age, gender, premium_amount, coverage_amount, 
                 payment_period, coverage_period)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                product_id,
                premium_data.get('age'),
                premium_data.get('gender', ''),
                premium_data.get('premium_amount'),
                premium_data.get('coverage_amount'),
                premium_data.get('payment_period', ''),
                premium_data.get('coverage_period', '')
            ))
            
            conn.commit()
            return cursor.lastrowid
            
        except Exception as e:
            logger.error(f"보험료 데이터 삽입 실패: {e}")
            return None
        finally:
            conn.close()
    
    def get_products_by_company(self, company_name):
        """보험회사별 상품 조회"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM cancer_insurance_products WHERE company_name = ?", (company_name,))
        data = cursor.fetchall()
        conn.close()
        return data
    
    def get_all_products(self):
        """모든 상품 조회"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM cancer_insurance_products")
        data = cursor.fetchall()
        conn.close()
        return data
    
    def export_to_csv(self, filename='cancer_insurance_export.csv'):
        """데이터를 CSV로 내보내기"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM cancer_insurance_products")
        data = cursor.fetchall()
        
        # 컬럼명 가져오기
        columns = [description[0] for description in cursor.description]
        
        # CSV 파일 작성
        import csv
        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)  # 헤더 작성
            writer.writerows(data)    # 데이터 작성
        
        conn.close()
        logger.info(f"데이터를 {filename}로 내보내기 완료 ({len(data)}개 상품)")
        return filename
    
    def get_statistics(self):
        """데이터 통계 조회"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 전체 상품 수
        cursor.execute("SELECT COUNT(*) FROM cancer_insurance_products")
        total_products = cursor.fetchone()[0]
        
        # 보험회사별 상품 수
        cursor.execute("""
            SELECT company_name, COUNT(*) as product_count 
            FROM cancer_insurance_products 
            GROUP BY company_name 
            ORDER BY product_count DESC
        """)
        company_stats = cursor.fetchall()
        
        conn.close()
        
        return {
            'total_products': total_products,
            'company_stats': company_stats
        }