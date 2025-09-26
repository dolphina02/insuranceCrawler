#!/usr/bin/env python3
"""
데이터베이스 테이블을 보기 좋게 출력하는 스크립트
"""

import sqlite3
import pandas as pd
from tabulate import tabulate
import argparse
import os

class DatabaseViewer:
    def __init__(self, db_path='insurance_data.db'):
        self.db_path = db_path
        
    def check_database_exists(self):
        """데이터베이스 파일 존재 확인"""
        if not os.path.exists(self.db_path):
            print(f"❌ 데이터베이스 파일을 찾을 수 없습니다: {self.db_path}")
            return False
        return True
    
    def get_table_list(self):
        """테이블 목록 조회"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    
    def view_table(self, table_name, limit=50):
        """특정 테이블 데이터 조회"""
        try:
            conn = sqlite3.connect(self.db_path)
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if df.empty:
                print(f"📋 테이블 '{table_name}'이 비어있습니다.")
                return
            
            print(f"\n📊 테이블: {table_name} (상위 {len(df)}개 레코드)")
            print("=" * 80)
            print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
            
        except Exception as e:
            print(f"❌ 테이블 조회 실패: {e}")
    
    def view_summary(self):
        """데이터베이스 요약 정보"""
        if not self.check_database_exists():
            return
            
        conn = sqlite3.connect(self.db_path)
        
        print(f"\n🗄️  데이터베이스: {self.db_path}")
        print("=" * 60)
        
        # 테이블별 레코드 수
        tables = self.get_table_list()
        table_info = []
        
        for table in tables:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            table_info.append([table, f"{count:,}"])
        
        print("\n📋 테이블 목록:")
        print(tabulate(table_info, headers=['테이블명', '레코드 수'], tablefmt='grid'))
        
        # 보험 상품 통계
        try:
            # 카테고리별 통계
            query = """
            SELECT c.category_name, COUNT(p.product_id) as count
            FROM insurance_categories c
            LEFT JOIN insurance_products p ON c.category_id = p.category_id
            GROUP BY c.category_name, c.display_order
            ORDER BY c.display_order
            """
            df_category = pd.read_sql_query(query, conn)
            
            if not df_category.empty:
                print(f"\n📊 카테고리별 상품 수:")
                print(tabulate(df_category, headers=['카테고리', '상품 수'], tablefmt='grid', showindex=False))
            
            # 회사별 통계 (상위 10개)
            query = """
            SELECT c.company_name, COUNT(p.product_id) as count
            FROM insurance_companies c
            LEFT JOIN insurance_products p ON c.company_id = p.company_id
            GROUP BY c.company_name
            ORDER BY count DESC
            LIMIT 10
            """
            df_company = pd.read_sql_query(query, conn)
            
            if not df_company.empty:
                print(f"\n🏢 상위 보험회사 (상품 수):")
                print(tabulate(df_company, headers=['보험회사', '상품 수'], tablefmt='grid', showindex=False))
                
        except Exception as e:
            print(f"⚠️  통계 조회 중 오류: {e}")
        
        conn.close()
    
    def view_products(self, limit=50, company=None, category=None):
        """보험 상품 상세 조회"""
        if not self.check_database_exists():
            return
            
        conn = sqlite3.connect(self.db_path)
        
        # 기본 쿼리
        query = """
        SELECT 
            c.company_name as '보험회사',
            cat.category_name as '카테고리',
            p.product_name as '상품명',
            p.product_type as '상품유형',
            p.crawl_date as '수집일자'
        FROM insurance_products p
        JOIN insurance_companies c ON p.company_id = c.company_id
        JOIN insurance_categories cat ON p.category_id = cat.category_id
        """
        
        conditions = []
        params = []
        
        if company:
            conditions.append("c.company_name LIKE ?")
            params.append(f"%{company}%")
            
        if category:
            conditions.append("cat.category_name LIKE ?")
            params.append(f"%{category}%")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += f" ORDER BY c.company_name, p.product_name LIMIT {limit}"
        
        try:
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            if df.empty:
                print("📋 조건에 맞는 상품이 없습니다.")
                return
            
            print(f"\n📊 보험 상품 목록 ({len(df)}개)")
            if company:
                print(f"🔍 회사 필터: {company}")
            if category:
                print(f"🔍 카테고리 필터: {category}")
            print("=" * 100)
            
            print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
            
        except Exception as e:
            print(f"❌ 상품 조회 실패: {e}")

def main():
    parser = argparse.ArgumentParser(description='데이터베이스 테이블 뷰어')
    parser.add_argument('--db-path', default='insurance_data.db', help='데이터베이스 파일 경로')
    parser.add_argument('--table', help='특정 테이블 조회')
    parser.add_argument('--limit', type=int, default=50, help='조회할 레코드 수')
    parser.add_argument('--company', help='보험회사 필터')
    parser.add_argument('--category', help='카테고리 필터')
    parser.add_argument('--products', action='store_true', help='상품 목록 조회')
    
    args = parser.parse_args()
    
    viewer = DatabaseViewer(args.db_path)
    
    if args.table:
        # 특정 테이블 조회
        viewer.view_table(args.table, args.limit)
    elif args.products:
        # 상품 목록 조회
        viewer.view_products(args.limit, args.company, args.category)
    else:
        # 요약 정보 조회
        viewer.view_summary()

if __name__ == "__main__":
    main()