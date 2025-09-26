#!/usr/bin/env python3
"""
전체 컬럼 구조로 샘플 데이터 10개만 테스트
"""

import pandas as pd
import psycopg2
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_sample_with_full_structure():
    connection_string = "postgresql://neondb_owner:npg_xnKiwN18QFSu@ep-square-shadow-a174zj2p-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    
    try:
        # CSV 파일 읽기 (샘플 10개만)
        csv_file = 'downloads/insurance_products_full_20250925_172246.csv'
        df = pd.read_csv(csv_file, nrows=10)
        logger.info(f"샘플 데이터 로드: {df.shape}")
        
        # PostgreSQL 연결
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        logger.info("PostgreSQL 연결 완료")
        
        # 기준일자 생성
        data_date = datetime.now().strftime('%Y%m%d')
        
        # 테스트 테이블 삭제 및 생성
        cursor.execute("DROP TABLE IF EXISTS sample_insurance_full")
        
        # 컬럼명 정리 (인덱스 기반으로 중복 방지)
        clean_columns = []
        for i, col in enumerate(df.columns):
            clean_col = f"col_{i:02d}_{col[:20].replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '').lower()}"
            clean_col = ''.join(c for c in clean_col if c.isalnum() or c == '_')
            clean_columns.append(clean_col)
        
        # 테이블 생성
        column_definitions = ["id SERIAL PRIMARY KEY", f"data_date VARCHAR(8) NOT NULL DEFAULT '{data_date}'"]
        column_definitions.extend([f"{clean_col} TEXT" for clean_col in clean_columns])
        column_definitions.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        create_sql = f"""
            CREATE TABLE sample_insurance_full (
                {', '.join(column_definitions)}
            )
        """
        
        cursor.execute(create_sql)
        logger.info("샘플 테이블 생성 완료")
        
        # 데이터 삽입
        insert_columns = ['data_date'] + clean_columns
        placeholders = ', '.join(['%s'] * len(insert_columns))
        
        insert_sql = f"""
            INSERT INTO sample_insurance_full ({', '.join(insert_columns)})
            VALUES ({placeholders})
        """
        
        inserted_count = 0
        for index, row in df.iterrows():
            try:
                values = [data_date]  # 기준일자
                
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value):
                        values.append(None)
                    else:
                        # 텍스트 길이 제한 (너무 긴 텍스트 방지)
                        text_value = str(value).strip()
                        if len(text_value) > 2000:
                            text_value = text_value[:2000] + "..."
                        values.append(text_value)
                
                cursor.execute(insert_sql, values)
                inserted_count += 1
                
            except Exception as e:
                logger.error(f"행 {index} 처리 실패: {e}")
                continue
        
        conn.commit()
        
        # 결과 확인
        cursor.execute("SELECT COUNT(*) FROM sample_insurance_full")
        count = cursor.fetchone()[0]
        
        # 샘플 데이터 조회
        cursor.execute("""
            SELECT data_date, col_00_category, col_01_company_name, col_02_product_name, 
                   col_19_보장내용_및_보험료_보험료_보험료_남, col_20_보장내용_및_보험료_보험료_보험료_여,
                   col_25_상품운용_대표번호_대표번호_대표번호
            FROM sample_insurance_full 
            LIMIT 5
        """)
        samples = cursor.fetchall()
        
        print(f"\n{'='*80}")
        print(f"샘플 데이터 전체 컬럼 구조 테스트 완료!")
        print(f"{'='*80}")
        print(f"테이블명: sample_insurance_full")
        print(f"총 컬럼 수: {len(df.columns) + 3}개 (data_date, id, created_at 포함)")
        print(f"삽입된 레코드: {count}개")
        print(f"기준일자: {data_date}")
        
        print(f"\n📊 샘플 데이터 (상위 5개):")
        print(f"{'기준일자':<10} {'카테고리':<10} {'보험회사':<15} {'상품명':<30} {'남자보험료':<15} {'여자보험료':<15} {'연락처':<12}")
        print("-" * 120)
        
        for sample in samples:
            data_date_val = sample[0] or ''
            category = (sample[1] or '')[:8]
            company = (sample[2] or '')[:12]
            product = (sample[3] or '')[:25] + "..." if sample[3] and len(sample[3]) > 25 else (sample[3] or '')
            premium_m = (sample[4] or '')[:12]
            premium_f = (sample[5] or '')[:12]
            contact = sample[6] or ''
            
            print(f"{data_date_val:<10} {category:<10} {company:<15} {product:<30} {premium_m:<15} {premium_f:<15} {contact:<12}")
        
        # 카테고리별 통계
        cursor.execute("SELECT col_00_category, COUNT(*) FROM sample_insurance_full GROUP BY col_00_category")
        category_stats = cursor.fetchall()
        
        print(f"\n📈 카테고리별 통계:")
        for category, cnt in category_stats:
            print(f"  {category}: {cnt}개")
        
        print(f"\n🔍 데이터 확인:")
        print("psql 'postgresql://neondb_owner:npg_xnKiwN18QFSu@ep-square-shadow-a174zj2p-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'")
        print("SELECT data_date, col_00_category, col_01_company_name FROM sample_insurance_full;")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"테스트 실패: {e}")
        return False

if __name__ == "__main__":
    success = test_sample_with_full_structure()
    if success:
        print("\n✅ 전체 컬럼 구조 샘플 테스트 성공!")
        print("기준일자(data_date)와 모든 컬럼이 정상적으로 저장되었습니다.")
    else:
        print("\n❌ 테스트 실패!")