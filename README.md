# 보험 데이터 크롤러 & PostgreSQL 임포터

한국 생명보험협회 공시 데이터를 자동으로 수집하고 PostgreSQL에 저장하는 Daily 데이터 관리 시스템입니다.

## 🎯 주요 기능

- **9개 보험 카테고리** 자동 수집
- **멀티헤더 → 단일헤더** 자동 변환
- **타임스탬프 파일명** 자동 생성
- **PostgreSQL 전체 컬럼** 저장
- **Daily 기준일자** (YYYYMMDD) 자동 추가
- **4,600+ 보험 상품** 데이터 처리

## 📦 설치

```bash
# 필요 패키지 설치
pip install -r requirements.txt
```

**필요 환경:**
- Python 3.7+
- Chrome 브라우저
- PostgreSQL 접근 권한

## 🚀 사용법

### 1️⃣ 테스트 (개발/검증용)

```bash
# 1개 카테고리 크롤링 테스트 (암보험)
python test_single_category.py

# 샘플 데이터 PostgreSQL 임포트 (10개 레코드)
python test_sample_with_full_structure.py
```

### 2️⃣ 운영 (전체 데이터)

```bash
# 전체 9개 카테고리 크롤링
python improved_insurance_crawler.py

# 전체 데이터 PostgreSQL 임포트
python final_postgresql_importer.py
```

## 📊 수집 데이터

### 보험 카테고리 (9개)
- 종신보험 (1,325개)
- 정기보험 (740개) 
- 질병보험 (721개)
- 암보험 (551개)
- 상해보험 (387개)
- 어린이보험 (267개)
- 치아보험 (248개)
- 간병/치매보험 (227개)
- CI보험 (137개)

### 데이터 구조
- **총 컬럼**: 43개 (원본 40개 + data_date + id + created_at)
- **기준일자**: `20250926` (YYYYMMDD 형식)
- **보험료**: 남자/여자 보험료
- **보장내용**: 지급사유, 지급금액
- **상품정보**: 갱신주기, 판매채널, 특이사항 등

## 🗄️ 데이터베이스 구조

```sql
-- 최종 테이블: insurance_products_final
CREATE TABLE insurance_products_final (
    id SERIAL PRIMARY KEY,
    data_date VARCHAR(8) NOT NULL,     -- 기준일자: 20250926
    col_00_category TEXT,              -- 카테고리
    col_01_company_name TEXT,          -- 보험회사명
    col_02_product_name TEXT,          -- 상품명
    col_19_premium_male TEXT,          -- 남자보험료
    col_20_premium_female TEXT,        -- 여자보험료
    -- ... (총 40개 원본 컬럼)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 📁 파일 구조

```
LifeInsuranceAssociation/
├── downloads/                              # 다운로드된 엑셀 파일들
│   ├── 종신보험_20250926_123456.xls
│   ├── 암보험_20250926_123457.xls
│   └── insurance_products_full_20250926_123458.csv
├── test_single_category.py                # 🧪 1개 카테고리 테스트
├── improved_insurance_crawler.py          # 🚀 메인 크롤러 (9개 카테고리)
├── test_sample_with_full_structure.py     # 🧪 샘플 임포터 (10개)
├── final_postgresql_importer.py           # 🚀 메인 임포터 (전체)
├── view_database_tables.py                # 👀 데이터 조회 도구
├── database_schema.sql                    # 📋 DB 스키마
├── sample_queries.sql                     # 📝 샘플 쿼리
└── README.md                              # 📖 이 문서
```

## 🔍 데이터 조회

### PostgreSQL 연결
```bash
psql 'postgresql://username:password@host:port/database'
```

### 주요 쿼리
```sql
-- 기준일자별 데이터 확인
SELECT data_date, col_00_category, COUNT(*) 
FROM insurance_products_final 
GROUP BY data_date, col_00_category;

-- 특정 날짜 데이터 조회
SELECT * FROM insurance_products_final 
WHERE data_date = '20250926';

-- 보험회사별 상품 수
SELECT col_01_company_name, COUNT(*) 
FROM insurance_products_final 
GROUP BY col_01_company_name 
ORDER BY COUNT(*) DESC;
```

## 📅 Daily 데이터 관리

### 일별 실행
```bash
# 매일 실행하면 새로운 기준일자로 데이터 수집
python improved_insurance_crawler.py
python final_postgresql_importer.py
```

### 데이터 추적
- **오늘**: `data_date = '20250926'` (4,603개)
- **내일**: `data_date = '20250927'` (새로 수집)
- **일별 변화**: 자동 추적 가능

## 🎯 주요 특징

### ✅ 완전 자동화
- 웹사이트 접속 → 카테고리 선택 → 다운로드 → 파싱 → DB 저장

### ✅ 데이터 무손실
- 원본 엑셀의 모든 40개 컬럼 보존
- 멀티레벨 헤더 → 의미있는 단일 컬럼명 변환

### ✅ 파일명 관리
- `종신보험_20250926_165228.xls` 형식
- 실행 시점 타임스탬프 자동 생성

### ✅ Daily 기준일자
- 모든 row에 `data_date` 컬럼 자동 추가
- YYYYMMDD 형식으로 일별 데이터 추적

## 🛠️ 개발자 정보

- **언어**: Python 3.7+
- **웹 크롤링**: Selenium + Chrome WebDriver
- **데이터 처리**: Pandas
- **데이터베이스**: PostgreSQL
- **파일 형식**: Excel (.xls) → CSV → PostgreSQL

## 📈 성능

- **크롤링 속도**: 9개 카테고리 약 3-5분
- **데이터 처리**: 4,603개 레코드 약 1-2분
- **총 소요시간**: 약 5-7분 (전체 프로세스)

---

**🎉 프로젝트 완성도: 100%**

