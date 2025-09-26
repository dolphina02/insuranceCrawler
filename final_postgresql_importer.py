#!/usr/bin/env python3
"""
최종 PostgreSQL 임포터 - 전체 데이터 (기준일자 포함)
"""

import pandas as pd
import psycopg2
from datetime import datetime
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def final_import_to_postgresql():
    connection_string = "postgresql://neondb_owner:npg_xnKiwN18QFSu@ep-square-shadow-a174zj2p-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    
    # 최신 CSV 파일 찾기
    csv_files = []
    for file in os.listdir('downloads'):
        if file.startswith('insurance_products_full_') and file.endswith('.csv'):
            csv_files.append(os.path.join('downloads', file))
    
    if not csv_files:
        print("❌ downloads 폴더에서 insurance_products_full_*.csv 파일을 찾을 수 없습니다.")
        return False
    
    latest_csv = max(csv_files, key=os.path.getmtime)
    logger.info(f"사용할 CSV 파일: {latest_csv}")
    
    try:
        # PostgreSQL 연결
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        logger.info("PostgreSQL 연결 완료")
        
        # CSV 파일 읽기
        df = pd.read_csv(latest_csv)
        logger.info(f"CSV 파일 로드: {df.shape}")
        
        # 기준일자 생성
        data_date = datetime.now().strftime('%Y%m%d')
        
        # 최종 테이블 삭제 및 생성
        cursor.execute("DROP TABLE IF EXISTS insurance_products_final")
        
        # 컬럼명 정리 (테스트에서 검증된 방식)
        clean_columns = []
        for i, col in enumerate(df.columns):
            clean_col = f"col_{i:02d}_{col[:20].replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '').lower()}"
            clean_col = ''.join(c for c in clean_col if c.isalnum() or c == '_')
            clean_columns.append(clean_col)
        
        # 테이블 생성
        column_definitions = [
            "id SERIAL PRIMARY KEY", 
            f"data_date VARCHAR(8) NOT NULL DEFAULT '{data_date}'"
        ]
        column_definitions.extend([f"{clean_col} TEXT" for clean_col in clean_columns])
        column_definitions.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        create_sql = f"""
            CREATE TABLE insurance_products_final (
                {', '.join(column_definitions)}
            )
        """
        
        cursor.execute(create_sql)
        logger.info(f"최종 테이블 생성 완료: {len(df.columns) + 3}개 컬럼")
        
        # 데이터 삽입 준비
        insert_columns = ['data_date'] + clean_columns
        placeholders = ', '.join(['%s'] * len(insert_columns))
        
        insert_sql = f"""
            INSERT INTO insurance_products_final ({', '.join(insert_columns)})
            VALUES ({placeholders})
        """
        
        # 배치 삽입
        batch_size = 100
        inserted_count = 0
        batch_data = []
        
        for index, row in df.iterrows():
            try:
                values = [data_date]  # 기준일자
                
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value):
                        values.append(None)
                    else:
                        # 텍스트 길이 제한
                        text_value = str(value).strip()
                        if len(text_value) > 5000:  # 5000자 제한
                            text_value = text_value[:5000] + "..."
                        values.append(text_value)
                
                batch_data.append(values)
                
                # 배치 단위로 삽입
                if len(batch_data) >= batch_size:
                    cursor.executemany(insert_sql, batch_data)
                    inserted_count += len(batch_data)
                    batch_data = []
                    
                    if inserted_count % 500 == 0:
                        logger.info(f"진행률: {inserted_count}/{len(df)}")
                        conn.commit()
                
            except Exception as e:
                logger.error(f"행 {index} 처리 실패: {e}")
                continue
        
        # 남은 배치 처리
        if batch_data:
            cursor.executemany(insert_sql, batch_data)
            inserted_count += len(batch_data)
        
        conn.commit()
        
        # 결과 확인
        cursor.execute("SELECT COUNT(*) FROM insurance_products_final")
        total_count = cursor.fetchone()[0]
        
        # 기준일자별 통계
        cursor.execute("SELECT data_date, COUNT(*) FROM insurance_products_final GROUP BY data_date")
        date_stats = cursor.fetchall()
        
        # 카테고리별 통계
        cursor.execute("SELECT col_00_category, COUNT(*) FROM insurance_products_final WHERE col_00_category IS NOT NULL GROUP BY col_00_category ORDER BY COUNT(*) DESC")
        category_stats = cursor.fetchall()
        
        # 보험회사별 통계 (상위 10개)
        cursor.execute("SELECT col_01_company_name, COUNT(*) FROM insurance_products_final WHERE col_01_company_name IS NOT NULL GROUP BY col_01_company_name ORDER BY COUNT(*) DESC LIMIT 10")
        company_stats = cursor.fetchall()
        
        print(f"\n{'='*80}")
        print(f"🎉 최종 PostgreSQL 임포트 완료!")
        print(f"{'='*80}")
        print(f"테이블명: insurance_products_final")
        print(f"총 레코드 수: {total_count:,}개")
        print(f"총 컬럼 수: {len(df.columns) + 3}개 (data_date, id, created_at 포함)")
        print(f"기준일자: {data_date}")
        
        print(f"\n📅 기준일자별 통계:")
        for date, cnt in date_stats:
            print(f"  {date}: {cnt:,}개")
        
        print(f"\n📊 카테고리별 통계:")
        for category, cnt in category_stats:
            print(f"  {category}: {cnt:,}개")
        
        print(f"\n🏢 상위 보험회사:")
        for company, cnt in company_stats:
            print(f"  {company}: {cnt:,}개")
        
        print(f"\n🔍 데이터 확인:")
        print("psql 'postgresql://neondb_owner:npg_xnKiwN18QFSu@ep-square-shadow-a174zj2p-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'")
        print("SELECT data_date, col_00_category, col_01_company_name, COUNT(*) FROM insurance_products_final GROUP BY data_date, col_00_category, col_01_company_name LIMIT 10;")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"최종 임포트 실패: {e}")
        return False

if __name__ == "__main__":
    print("🚀 최종 PostgreSQL 임포트 시작...")
    success = final_import_to_postgresql()
    
    if success:
        print("\n✅ 최종 임포트 성공!")
        print("모든 보험 데이터가 기준일자와 함께 PostgreSQL에 저장되었습니다.")
        print("Daily 기준으로 데이터 추적이 가능합니다.")
    else:
        print("\n❌ 최종 임포트 실패!")