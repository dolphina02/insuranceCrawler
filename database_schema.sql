-- 보험 데이터 RDB 스키마 설계
-- 현재 CSV: category, company_name, product_name, coverage_info, crawl_date

-- =====================================================
-- 1. 기본 테이블 구조 (정규화)
-- =====================================================

-- 보험회사 마스터 테이블
CREATE TABLE insurance_companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL UNIQUE,
    company_code VARCHAR(20),
    contact_number VARCHAR(20),
    website_url VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 보험 카테고리 마스터 테이블
CREATE TABLE insurance_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL UNIQUE,
    category_code VARCHAR(10),
    description TEXT,
    display_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 보험 상품 메인 테이블
CREATE TABLE insurance_products (
    product_id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES insurance_companies(company_id),
    category_id INTEGER NOT NULL REFERENCES insurance_categories(category_id),
    product_name VARCHAR(200) NOT NULL,
    product_code VARCHAR(50),
    product_type VARCHAR(50), -- 주계약/특약 구분
    
    -- 기본 정보
    sales_start_date DATE,
    sales_end_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- 메타 정보
    crawl_date TIMESTAMP NOT NULL,
    source_url VARCHAR(500),
    data_version INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 인덱스 및 제약조건
    UNIQUE(company_id, product_name, product_type)
);

-- 보장 내용 상세 테이블
CREATE TABLE product_coverages (
    coverage_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES insurance_products(product_id),
    coverage_name VARCHAR(100) NOT NULL,
    coverage_type VARCHAR(50), -- 주계약/특약
    coverage_amount DECIMAL(15,2),
    coverage_condition TEXT,
    premium_male DECIMAL(10,2),
    premium_female DECIMAL(10,2),
    age_min INTEGER,
    age_max INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 크롤링 이력 테이블
CREATE TABLE crawl_history (
    crawl_id SERIAL PRIMARY KEY,
    crawl_date TIMESTAMP NOT NULL,
    category_name VARCHAR(50),
    total_products INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    crawl_duration_seconds INTEGER,
    status VARCHAR(20) DEFAULT 'SUCCESS', -- SUCCESS, PARTIAL, FAILED
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 2. 인덱스 생성
-- =====================================================

-- 성능 최적화를 위한 인덱스
CREATE INDEX idx_products_company_category ON insurance_products(company_id, category_id);
CREATE INDEX idx_products_crawl_date ON insurance_products(crawl_date);
CREATE INDEX idx_products_active ON insurance_products(is_active);
CREATE INDEX idx_coverages_product ON product_coverages(product_id);
CREATE INDEX idx_crawl_history_date ON crawl_history(crawl_date);

-- 검색용 인덱스
CREATE INDEX idx_products_name_search ON insurance_products USING gin(to_tsvector('korean', product_name));
CREATE INDEX idx_companies_name_search ON insurance_companies USING gin(to_tsvector('korean', company_name));

-- =====================================================
-- 3. 기본 데이터 삽입
-- =====================================================

-- 보험 카테고리 기본 데이터
INSERT INTO insurance_categories (category_name, category_code, display_order) VALUES
('종신보험', 'LIFE', 1),
('정기보험', 'TERM', 2),
('질병보험', 'DISEASE', 3),
('암보험', 'CANCER', 4),
('CI보험', 'CI', 5),
('상해보험', 'ACCIDENT', 6),
('어린이보험', 'CHILD', 7),
('치아보험', 'DENTAL', 8),
('간병/치매보험', 'CARE', 9);

-- =====================================================
-- 4. 뷰 생성 (조회 편의성)
-- =====================================================

-- 상품 전체 정보 뷰
CREATE VIEW v_product_summary AS
SELECT 
    p.product_id,
    c.company_name,
    cat.category_name,
    p.product_name,
    p.product_type,
    p.sales_start_date,
    p.is_active,
    p.crawl_date,
    COUNT(pc.coverage_id) as coverage_count
FROM insurance_products p
JOIN insurance_companies c ON p.company_id = c.company_id
JOIN insurance_categories cat ON p.category_id = cat.category_id
LEFT JOIN product_coverages pc ON p.product_id = pc.product_id
GROUP BY p.product_id, c.company_name, cat.category_name, p.product_name, 
         p.product_type, p.sales_start_date, p.is_active, p.crawl_date;

-- 암보험 전용 뷰
CREATE VIEW v_cancer_insurance AS
SELECT 
    c.company_name,
    p.product_name,
    p.product_type,
    pc.coverage_name,
    pc.premium_male,
    pc.premium_female,
    pc.coverage_amount,
    p.crawl_date
FROM insurance_products p
JOIN insurance_companies c ON p.company_id = c.company_id
JOIN insurance_categories cat ON p.category_id = cat.category_id
LEFT JOIN product_coverages pc ON p.product_id = pc.product_id
WHERE cat.category_name = '암보험'
AND p.is_active = TRUE;

-- 회사별 상품 통계 뷰
CREATE VIEW v_company_stats AS
SELECT 
    c.company_name,
    cat.category_name,
    COUNT(p.product_id) as product_count,
    MAX(p.crawl_date) as last_updated
FROM insurance_companies c
LEFT JOIN insurance_products p ON c.company_id = p.company_id
LEFT JOIN insurance_categories cat ON p.category_id = cat.category_id
WHERE p.is_active = TRUE
GROUP BY c.company_name, cat.category_name
ORDER BY c.company_name, cat.category_name;

-- =====================================================
-- 5. 저장 프로시저 (데이터 삽입용)
-- =====================================================

-- CSV 데이터 삽입 프로시저
CREATE OR REPLACE FUNCTION insert_csv_data(
    p_category_name VARCHAR,
    p_company_name VARCHAR,
    p_product_name VARCHAR,
    p_coverage_info VARCHAR,
    p_crawl_date TIMESTAMP
) RETURNS INTEGER AS $$
DECLARE
    v_company_id INTEGER;
    v_category_id INTEGER;
    v_product_id INTEGER;
BEGIN
    -- 회사 ID 조회 또는 생성
    SELECT company_id INTO v_company_id 
    FROM insurance_companies 
    WHERE company_name = p_company_name;
    
    IF v_company_id IS NULL THEN
        INSERT INTO insurance_companies (company_name)
        VALUES (p_company_name)
        RETURNING company_id INTO v_company_id;
    END IF;
    
    -- 카테고리 ID 조회
    SELECT category_id INTO v_category_id
    FROM insurance_categories
    WHERE category_name = p_category_name;
    
    -- 상품 삽입 또는 업데이트
    INSERT INTO insurance_products (
        company_id, category_id, product_name, 
        product_type, crawl_date
    ) VALUES (
        v_company_id, v_category_id, p_product_name,
        p_coverage_info, p_crawl_date
    )
    ON CONFLICT (company_id, product_name, product_type)
    DO UPDATE SET 
        crawl_date = p_crawl_date,
        updated_at = CURRENT_TIMESTAMP
    RETURNING product_id INTO v_product_id;
    
    RETURN v_product_id;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 6. 샘플 쿼리
-- =====================================================

-- 암보험 상품 조회
/*
SELECT 
    company_name,
    product_name,
    product_type,
    crawl_date
FROM v_cancer_insurance
ORDER BY company_name, product_name;
*/

-- 회사별 상품 수 통계
/*
SELECT 
    company_name,
    SUM(product_count) as total_products
FROM v_company_stats
GROUP BY company_name
ORDER BY total_products DESC;
*/

-- 최근 크롤링된 데이터
/*
SELECT 
    category_name,
    COUNT(*) as product_count,
    MAX(crawl_date) as last_crawl
FROM v_product_summary
GROUP BY category_name
ORDER BY last_crawl DESC;
*/