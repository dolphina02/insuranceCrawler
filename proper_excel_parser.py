#!/usr/bin/env python3
"""
실제 엑셀 파일을 제대로 파싱해서 PostgreSQL에 넣기
"""

import pandas as pd
import psycopg2
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_and_import_excel():
    connection_string = "postgresql://neondb_owner:npg_xnKiwN18QFSu@ep-square-shadow-a174zj2p-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    excel_file = "downloads/암보험_보장성_상품비교_20250925084245830.xls"
    
    try:
        # PostgreSQL 연결
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        logger.info("PostgreSQL 연결 완료")
        
        # 기존 테이블 삭제
        cursor.execute("DROP TABLE IF EXISTS cancer_insurance_full_data")
        
        # 실제 엑셀 구조에 맞는 테이블 생성
        cursor.execute('''
            CREATE TABLE cancer_insurance_full_data (
                id SERIAL PRIMARY KEY,
                data_date VARCHAR(8) NOT NULL,  -- 기준일자 (YYYYMMDD 형식)
                company_name TEXT,
                product_name TEXT,
                coverage_type TEXT,
                benefit_name TEXT,
                payment_reason TEXT,
                payment_amount TEXT,
                coverage_amount TEXT,
                premium_male TEXT,
                premium_female TEXT,
                fixed_rate TEXT,
                current_rate TEXT,
                min_guaranteed_rate TEXT,
                price_index_male TEXT,
                price_index_female TEXT,
                coverage_index_cancer_diagnosis TEXT,
                coverage_index_cancer_hospitalization TEXT,
                additional_premium_index_male TEXT,
                additional_premium_index_female TEXT,
                contract_cost_index_male TEXT,
                contract_cost_index_female TEXT,
                surrender_value TEXT,
                renewal_period TEXT,
                universal_type TEXT,
                sales_channel TEXT,
                sales_date TEXT,
                special_notes TEXT,
                contact_number TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 엑셀 파일 읽기 (HTML 방식)
        df = pd.read_html(excel_file, encoding='utf-8')[0]
        logger.info(f"엑셀 파일 로드: {df.shape}")
        
        # 멀티레벨 컬럼 단순화
        if isinstance(df.columns[0], tuple):
            # 컬럼명을 단순화
            simple_columns = [
                'company_name', 'product_name', 'coverage_type', 'benefit_name', 
                'payment_reason', 'payment_amount', 'coverage_amount', 'premium_male', 
                'premium_female', 'fixed_rate', 'current_rate', 'min_guaranteed_rate',
                'price_index_male', 'price_index_female', 'coverage_index_cancer_diagnosis',
                'coverage_index_cancer_hospitalization', 'additional_premium_index_male',
                'additional_premium_index_female', 'contract_cost_index_male', 
                'contract_cost_index_female', 'surrender_value', 'renewal_period',
                'universal_type', 'sales_channel', 'sales_date', 'special_notes', 'contact_number'
            ]
            
            # 실제 컬럼 수에 맞춰 조정
            actual_columns = min(len(simple_columns), len(df.columns))
            df.columns = simple_columns[:actual_columns]
        
        # 데이터 삽입
        inserted_count = 0
        for index, row in df.iterrows():
            try:
                # 빈 행 건너뛰기
                if pd.isna(row.get('company_name')) or str(row.get('company_name')).strip() == '':
                    continue
                
                # 헤더 행 건너뛰기
                if '보험회사' in str(row.get('company_name', '')):
                    continue
                
                # 기준일자 생성 (YYYYMMDD 형식)
                data_date = datetime.now().strftime('%Y%m%d')
                
                cursor.execute('''
                    INSERT INTO cancer_insurance_full_data 
                    (data_date, company_name, product_name, coverage_type, benefit_name, payment_reason,
                     payment_amount, coverage_amount, premium_male, premium_female, fixed_rate,
                     current_rate, min_guaranteed_rate, price_index_male, price_index_female,
                     coverage_index_cancer_diagnosis, coverage_index_cancer_hospitalization,
                     additional_premium_index_male, additional_premium_index_female,
                     contract_cost_index_male, contract_cost_index_female, surrender_value,
                     renewal_period, universal_type, sales_channel, sales_date, special_notes, contact_number)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    data_date,  # 기준일자 추가
                    str(row.get('company_name', '')).strip() if pd.notna(row.get('company_name')) else None,
                    str(row.get('product_name', '')).strip() if pd.notna(row.get('product_name')) else None,
                    str(row.get('coverage_type', '')).strip() if pd.notna(row.get('coverage_type')) else None,
                    str(row.get('benefit_name', '')).strip() if pd.notna(row.get('benefit_name')) else None,
                    str(row.get('payment_reason', '')).strip() if pd.notna(row.get('payment_reason')) else None,
                    str(row.get('payment_amount', '')).strip() if pd.notna(row.get('payment_amount')) else None,
                    str(row.get('coverage_amount', '')).strip() if pd.notna(row.get('coverage_amount')) else None,
                    str(row.get('premium_male', '')).strip() if pd.notna(row.get('premium_male')) else None,
                    str(row.get('premium_female', '')).strip() if pd.notna(row.get('premium_female')) else None,
                    str(row.get('fixed_rate', '')).strip() if pd.notna(row.get('fixed_rate')) else None,
                    str(row.get('current_rate', '')).strip() if pd.notna(row.get('current_rate')) else None,
                    str(row.get('min_guaranteed_rate', '')).strip() if pd.notna(row.get('min_guaranteed_rate')) else None,
                    str(row.get('price_index_male', '')).strip() if pd.notna(row.get('price_index_male')) else None,
                    str(row.get('price_index_female', '')).strip() if pd.notna(row.get('price_index_female')) else None,
                    str(row.get('coverage_index_cancer_diagnosis', '')).strip() if pd.notna(row.get('coverage_index_cancer_diagnosis')) else None,
                    str(row.get('coverage_index_cancer_hospitalization', '')).strip() if pd.notna(row.get('coverage_index_cancer_hospitalization')) else None,
                    str(row.get('additional_premium_index_male', '')).strip() if pd.notna(row.get('additional_premium_index_male')) else None,
                    str(row.get('additional_premium_index_female', '')).strip() if pd.notna(row.get('additional_premium_index_female')) else None,
                    str(row.get('contract_cost_index_male', '')).strip() if pd.notna(row.get('contract_cost_index_male')) else None,
                    str(row.get('contract_cost_index_female', '')).strip() if pd.notna(row.get('contract_cost_index_female')) else None,
                    str(row.get('surrender_value', '')).strip() if pd.notna(row.get('surrender_value')) else None,
                    str(row.get('renewal_period', '')).strip() if pd.notna(row.get('renewal_period')) else None,
                    str(row.get('universal_type', '')).strip() if pd.notna(row.get('universal_type')) else None,
                    str(row.get('sales_channel', '')).strip() if pd.notna(row.get('sales_channel')) else None,
                    str(row.get('sales_date', '')).strip() if pd.notna(row.get('sales_date')) else None,
                    str(row.get('special_notes', '')).strip() if pd.notna(row.get('special_notes')) else None,
                    str(row.get('contact_number', '')).strip() if pd.notna(row.get('contact_number')) else None
                ))
                
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    logger.info(f"진행률: {inserted_count}/{len(df)}")
                    
            except Exception as e:
                logger.error(f"행 {index} 처리 실패: {e}")
                continue
        
        conn.commit()
        
        # 결과 확인
        cursor.execute("SELECT COUNT(*) FROM cancer_insurance_full_data")
        count = cursor.fetchone()[0]
        
        logger.info(f"✅ 임포트 완료: {count}개 레코드")
        
        # 샘플 데이터 확인
        cursor.execute("SELECT company_name, product_name, benefit_name, premium_male, premium_female FROM cancer_insurance_full_data WHERE company_name IS NOT NULL LIMIT 5")
        samples = cursor.fetchall()
        
        print(f"\n{'='*60}")
        print(f"PostgreSQL 전체 데이터 임포트 완료!")
        print(f"{'='*60}")
        print(f"테이블명: cancer_insurance_full_data")
        print(f"레코드 수: {count:,}개")
        print(f"\n샘플 데이터:")
        for sample in samples:
            print(f"  {sample[0]} | {sample[1]} | {sample[2]} | {sample[3]} | {sample[4]}")
        
        print(f"\n🔍 데이터 확인:")
        print("psql 'postgresql://neondb_owner:npg_xnKiwN18QFSu@ep-square-shadow-a174zj2p-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'")
        print("SELECT * FROM cancer_insurance_full_data LIMIT 10;")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"임포트 실패: {e}")
        return False

if __name__ == "__main__":
    parse_and_import_excel()