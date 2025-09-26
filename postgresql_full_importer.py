#!/usr/bin/env python3
"""
전체 보험 데이터를 PostgreSQL에 임포트 (기준일자 포함)
"""

import pandas as pd
import psycopg2
from datetime import datetime
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def import_csv_to_postgresql():
    connection_string = "postgresql://neondb_owner:npg_xnKiwN18QFSu@ep-square-shadow-a174zj2p-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    
    # CSV 파일 찾기
    csv_files = []
    for file in os.listdir('downloads'):
        if file.startswith('insurance_products_full_') and file.endswith('.csv'):
            csv_files.append(os.path.join('downloads', file))
    
    if not csv_files:
        print("❌ downloads 폴더에서 insurance_products_full_*.csv 파일을 찾을 수 없습니다.")
        return False
    
    # 가장 최신 파일 선택
    latest_csv = max(csv_files, key=os.path.getmtime)
    logger.info(f"사용할 CSV 파일: {latest_csv}")
    
    try:
        # PostgreSQL 연결
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        logger.info("PostgreSQL 연결 완료")
        
        # 기존 테이블 삭제
        cursor.execute("DROP TABLE IF EXISTS insurance_products_full")
        
        # 기준일자가 포함된 테이블 생성
        cursor.execute('''
            CREATE TABLE insurance_products_full (
                id SERIAL PRIMARY KEY,
                data_date VARCHAR(8) NOT NULL,  -- 기준일자 (YYYYMMDD)
                category VARCHAR(50),
                company_name TEXT,
                product_name TEXT,
                crawl_date TIMESTAMP,
                -- 동적으로 추가될 컬럼들을 위한 JSONB
                additional_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # CSV 파일 읽기
        df = pd.read_csv(latest_csv)
        logger.info(f"CSV 파일 로드: {df.shape}")
        
        # 기준일자 생성
        data_date = datetime.now().strftime('%Y%m%d')
        
        # 데이터 삽입
        inserted_count = 0
        for index, row in df.iterrows():
            try:
                # 기본 컬럼들
                category = str(row.get('category', '')).strip() if pd.notna(row.get('category')) else None
                company_name = str(row.get('company_name', '')).strip() if pd.notna(row.get('company_name')) else None
                product_name = str(row.get('product_name', '')).strip() if pd.notna(row.get('product_name')) else None
                
                # 빈 행 건너뛰기
                if not company_name or company_name == 'nan':
                    continue
                
                # crawl_date 처리
                crawl_date = None
                if pd.notna(row.get('crawl_date')):
                    try:
                        crawl_date = pd.to_datetime(row.get('crawl_date'))
                    except:
                        crawl_date = None
                
                # 나머지 모든 컬럼을 JSONB로 저장
                additional_data = {}
                for col in df.columns:
                    if col not in ['category', 'company_name', 'product_name', 'crawl_date']:
                        value = row.get(col)
                        if pd.notna(value) and str(value).strip() != '':
                            additional_data[col] = str(value).strip()
                
                cursor.execute('''
                    INSERT INTO insurance_products_full 
                    (data_date, category, company_name, product_name, crawl_date, additional_data)
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (
                    data_date,
                    category,
                    company_name,
                    product_name,
                    crawl_date,
                    psycopg2.extras.Json(additional_data)
                ))
                
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    logger.info(f"진행률: {inserted_count}/{len(df)}")
                    
            except Exception as e:
                logger.error(f"행 {index} 처리 실패: {e}")
                continue
        
        conn.commit()
        
        # 결과 확인
        cursor.execute("SELECT COUNT(*) FROM insurance_products_full")
        count = cursor.fetchone()[0]
        
        # 기준일자별 통계
        cursor.execute("SELECT data_date, COUNT(*) FROM insurance_products_full GROUP BY data_date")
        date_stats = cursor.fetchall()
        
        # 카테고리별 통계
        cursor.execute("SELECT category, COUNT(*) FROM insurance_products_full GROUP BY category ORDER BY COUNT(*) DESC")
        category_stats = cursor.fetchall()
        
        logger.info(f"✅ 임포트 완료: {count}개 레코드")
        
        print(f"\n{'='*60}")
        print(f"PostgreSQL 전체 데이터 임포트 완료!")
        print(f"{'='*60}")
        print(f"테이블명: insurance_products_full")
        print(f"총 레코드 수: {count:,}개")
        print(f"기준일자: {data_date}")
        
        print(f"\n📅 기준일자별 통계:")
        for date, cnt in date_stats:
            print(f"  {date}: {cnt:,}개")
        
        print(f"\n📊 카테고리별 통계:")
        for category, cnt in category_stats:
            print(f"  {category}: {cnt:,}개")
        
        print(f"\n🔍 데이터 확인:")
        print("psql 'postgresql://neondb_owner:npg_xnKiwN18QFSu@ep-square-shadow-a174zj2p-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'")
        print("SELECT data_date, category, COUNT(*) FROM insurance_products_full GROUP BY data_date, category;")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"임포트 실패: {e}")
        return False

if __name__ == "__main__":
    # psycopg2.extras 임포트 추가
    import psycopg2.extras
    
    success = import_csv_to_postgresql()
    if success:
        print("\n✅ 전체 데이터 임포트 성공!")
    else:
        print("\n❌ 임포트 실패!")