import sqlite3
import pandas as pd

def check_database():
    """데이터베이스 내용 확인"""
    try:
        conn = sqlite3.connect('insurance_data.db')
        
        # 전체 테이블 확인
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"테이블 목록: {tables}")
        
        # 전체 데이터 수 확인
        cursor.execute("SELECT COUNT(*) FROM insurance_products")
        total_count = cursor.fetchone()[0]
        print(f"전체 상품 수: {total_count}")
        
        # 카테고리별 확인
        cursor.execute("SELECT category, COUNT(*) FROM insurance_products GROUP BY category")
        categories = cursor.fetchall()
        print(f"\n카테고리별 상품 수:")
        for category, count in categories:
            print(f"  {category}: {count}개")
        
        # 암보험 데이터 샘플 확인
        cursor.execute("SELECT * FROM insurance_products WHERE category='암보험' LIMIT 5")
        cancer_samples = cursor.fetchall()
        
        print(f"\n암보험 샘플 데이터:")
        for i, row in enumerate(cancer_samples):
            print(f"  {i+1}: {row[:5]}...")  # 처음 5개 컬럼만
        
        # 컬럼 정보 확인
        cursor.execute("PRAGMA table_info(insurance_products)")
        columns = cursor.fetchall()
        print(f"\n테이블 컬럼 정보:")
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
        
        conn.close()
        
    except Exception as e:
        print(f"데이터베이스 확인 실패: {e}")

if __name__ == "__main__":
    check_database()