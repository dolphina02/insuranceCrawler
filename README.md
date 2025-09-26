# 보험 데이터 크롤러

생명보험협회 공시 데이터를 자동으로 수집하고 PostgreSQL에 저장하는 시스템입니다.

## 주요 기능

- 9개 보험 카테고리 데이터 자동 수집
- 엑셀 파일 다운로드 및 전체 데이터 파싱
- PostgreSQL 데이터베이스 자동 저장
- 모든 컬럼 정보 보존 (보험료, 지급조건, 특약 등)

## 설치 및 실행

```bash
# 의존성 설치
pip install -r requirements.txt

# 1. 데이터 크롤링 (엑셀 다운로드 + CSV 저장)
python improved_insurance_crawler.py

# 2. PostgreSQL에 데이터 저장
python proper_excel_parser.py
```

## 수집 데이터

### 보험 카테고리
- 종신보험, 정기보험, 질병보험, 암보험
- CI보험, 상해보험, 어린이보험, 치아보험, 간병/치매보험

### 수집 정보
- 보험회사명, 상품명, 보장내용
- 남자/여자 보험료, 지급금액, 지급조건
- 보험가격지수, 보장범위지수
- 갱신주기, 판매채널, 특이사항 등 (총 27개 컬럼)

## 파일 구조

### 핵심 파일
- `improved_insurance_crawler.py` - 메인 크롤러 (다운로드 + 파싱)
- `proper_excel_parser.py` - PostgreSQL 임포터
- `view_database_tables.py` - 데이터 조회 도구
- `database_schema.sql` - 데이터베이스 스키마
- `sample_queries.sql` - 샘플 쿼리

### 지원 파일
- `requirements.txt` - 필요 패키지 목록
- `README.md` - 프로젝트 설명서

## 사용 예시

### 1. 전체 보험 데이터 수집
```bash
python improved_insurance_crawler.py
```

### 2. PostgreSQL에 저장
```bash
python proper_excel_parser.py
```

### 3. 데이터 조회
```bash
python view_database_tables.py
```

## PostgreSQL 연결

```sql
-- 연결 문자열 예시
psql 'postgresql://username:password@host:port/database'

-- 데이터 확인
SELECT * FROM cancer_insurance_full_data LIMIT 10;
```

## 주의사항

- Chrome 브라우저 필요 (Selenium 사용)
- 안정적인 인터넷 연결 필요
- PostgreSQL 데이터베이스 접근 권한 필요