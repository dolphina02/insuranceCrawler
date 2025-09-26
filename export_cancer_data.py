import sqlite3
import pandas as pd

def export_cancer_insurance():
    """암보험 데이터를 CSV로 내보내기"""
    try:
        conn = sqlite3.connect('insurance_data.db')
        
        # 암보험 데이터 조회
        query = "SELECT * FROM insurance_products WHERE category='암보험'"
        df = pd.read_sql_query(query, conn)
        
        print(f"암보험 데이터 수: {len(df)}")
        print(f"컬럼: {list(df.columns)}")
        
        # CSV로 저장
        df.to_csv('cancer_insurance_final.csv', index=False, encoding='utf-8-sig')
        print("암보험 데이터를 cancer_insurance_final.csv로 저장했습니다.")
        
        # 샘플 데이터 출력
        print("\n샘플 데이터:")
        print(df[['company_name', 'product_name', 'coverage_type', 'premium_male', 'premium_female']].head(10))
        
        # 보험회사별 통계
        company_stats = df['company_name'].value_counts().head(10)
        print(f"\n암보험 상위 보험회사:")
        for company, count in company_stats.items():
            print(f"  {company}: {count}개")
        
        conn.close()
        
    except Exception as e:
        print(f"내보내기 실패: {e}")

if __name__ == "__main__":
    export_cancer_insurance()