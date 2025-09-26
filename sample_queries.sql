-- 보험 데이터 분석용 샘플 쿼리들

-- =====================================================
-- 1. 기본 조회 쿼리
-- =====================================================

-- 전체 상품 수 조회
SELECT COUNT(*) as total_products FROM insurance_products;

-- 카테고리별 상품 수
SELECT 
    c.category_name,
    COUNT(p.product_id) as product_count
FROM insurance_categories c
LEFT JOIN insurance_products p ON c.category_id = p.category_id
GROUP BY c.category_name, c.display_order
ORDER BY c.display_order;

-- 보험회사별 상품 수 (상위 10개)
SELECT 
    c.company_name,
    COUNT(p.product_id) as product_count
FROM insurance_companies c
LEFT JOIN insurance_products p ON c.company_id = p.company_id
GROUP BY c.company_name
ORDER BY product_count DESC
LIMIT 10;

-- =====================================================
-- 2. 암보험 전용 쿼리
-- =====================================================

-- 암보험 상품 목록
SELECT 
    comp.company_name,
    p.product_name,
    p.product_type,
    p.crawl_date
FROM insurance_products p
JOIN insurance_companies comp ON p.company_id = comp.company_id
JOIN insurance_categories cat ON p.category_id = cat.category_id
WHERE cat.category_name = '암보험'
ORDER BY comp.company_name, p.product_name;

-- 암보험 보험회사별 상품 수
SELECT 
    comp.company_name,
    COUNT(p.product_id) as cancer_product_count
FROM insurance_products p
JOIN insurance_companies comp ON p.company_id = comp.company_id
JOIN insurance_categories cat ON p.category_id = cat.category_id
WHERE cat.category_name = '암보험'
GROUP BY comp.company_name
ORDER BY cancer_product_count DESC;

-- 암보험 상품 타입별 분포
SELECT 
    p.product_type,
    COUNT(*) as count
FROM insurance_products p
JOIN insurance_categories cat ON p.category_id = cat.category_id
WHERE cat.category_name = '암보험'
GROUP BY p.product_type
ORDER BY count DESC;

-- =====================================================
-- 3. 시계열 분석 쿼리
-- =====================================================

-- 크롤링 날짜별 수집 상품 수
SELECT 
    DATE(crawl_date) as crawl_date,
    COUNT(*) as products_collected
FROM insurance_products
GROUP BY DATE(crawl_date)
ORDER BY crawl_date DESC;

-- 최근 업데이트된 상품들
SELECT 
    comp.company_name,
    cat.category_name,
    p.product_name,
    p.crawl_date
FROM insurance_products p
JOIN insurance_companies comp ON p.company_id = comp.company_id
JOIN insurance_categories cat ON p.category_id = cat.category_id
ORDER BY p.crawl_date DESC
LIMIT 20;

-- =====================================================
-- 4. 비즈니스 분석 쿼리
-- =====================================================

-- 보험회사별 카테고리 다양성
SELECT 
    comp.company_name,
    COUNT(DISTINCT cat.category_id) as category_diversity,
    COUNT(p.product_id) as total_products
FROM insurance_companies comp
LEFT JOIN insurance_products p ON comp.company_id = p.company_id
LEFT JOIN insurance_categories cat ON p.category_id = cat.category_id
GROUP BY comp.company_name
HAVING COUNT(p.product_id) > 0
ORDER BY category_diversity DESC, total_products DESC;

-- 상품명에 특정 키워드가 포함된 상품 검색
SELECT 
    comp.company_name,
    cat.category_name,
    p.product_name
FROM insurance_products p
JOIN insurance_companies comp ON p.company_id = comp.company_id
JOIN insurance_categories cat ON p.category_id = cat.category_id
WHERE p.product_name LIKE '%간편%'
   OR p.product_name LIKE '%무배당%'
ORDER BY comp.company_name, cat.category_name;

-- 주계약 vs 특약 비율
SELECT 
    cat.category_name,
    SUM(CASE WHEN p.product_type = '주계약' THEN 1 ELSE 0 END) as main_contract,
    SUM(CASE WHEN p.product_type = '특약' THEN 1 ELSE 0 END) as rider,
    COUNT(*) as total
FROM insurance_products p
JOIN insurance_categories cat ON p.category_id = cat.category_id
GROUP BY cat.category_name
ORDER BY total DESC;

-- =====================================================
-- 5. 데이터 품질 체크 쿼리
-- =====================================================

-- 중복 상품 체크
SELECT 
    company_id,
    product_name,
    COUNT(*) as duplicate_count
FROM insurance_products
GROUP BY company_id, product_name
HAVING COUNT(*) > 1;

-- 빈 값 체크
SELECT 
    'product_name' as field_name,
    COUNT(*) as null_count
FROM insurance_products 
WHERE product_name IS NULL OR product_name = ''
UNION ALL
SELECT 
    'company_name' as field_name,
    COUNT(*) as null_count
FROM insurance_products p
JOIN insurance_companies c ON p.company_id = c.company_id
WHERE c.company_name IS NULL OR c.company_name = '';

-- 최신 데이터 현황
SELECT 
    cat.category_name,
    MAX(p.crawl_date) as latest_crawl,
    COUNT(*) as product_count
FROM insurance_products p
JOIN insurance_categories cat ON p.category_id = cat.category_id
GROUP BY cat.category_name
ORDER BY latest_crawl DESC;

-- =====================================================
-- 6. 성능 최적화용 인덱스 (SQLite)
-- =====================================================

-- 검색 성능 향상을 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_products_company_category 
ON insurance_products(company_id, category_id);

CREATE INDEX IF NOT EXISTS idx_products_crawl_date 
ON insurance_products(crawl_date);

CREATE INDEX IF NOT EXISTS idx_products_name 
ON insurance_products(product_name);

CREATE INDEX IF NOT EXISTS idx_companies_name 
ON insurance_companies(company_name);

-- =====================================================
-- 7. 유용한 뷰 생성
-- =====================================================

-- 상품 요약 뷰
CREATE VIEW IF NOT EXISTS v_product_summary AS
SELECT 
    p.product_id,
    comp.company_name,
    cat.category_name,
    p.product_name,
    p.product_type,
    p.crawl_date,
    p.created_at
FROM insurance_products p
JOIN insurance_companies comp ON p.company_id = comp.company_id
JOIN insurance_categories cat ON p.category_id = cat.category_id;

-- 암보험 전용 뷰
CREATE VIEW IF NOT EXISTS v_cancer_insurance AS
SELECT 
    comp.company_name,
    p.product_name,
    p.product_type,
    p.coverage_info,
    p.crawl_date
FROM insurance_products p
JOIN insurance_companies comp ON p.company_id = comp.company_id
JOIN insurance_categories cat ON p.category_id = cat.category_id
WHERE cat.category_name = '암보험';

-- 회사별 통계 뷰
CREATE VIEW IF NOT EXISTS v_company_statistics AS
SELECT 
    comp.company_name,
    COUNT(DISTINCT cat.category_id) as categories_offered,
    COUNT(p.product_id) as total_products,
    MAX(p.crawl_date) as last_updated
FROM insurance_companies comp
LEFT JOIN insurance_products p ON comp.company_id = p.company_id
LEFT JOIN insurance_categories cat ON p.category_id = cat.category_id
GROUP BY comp.company_name, comp.company_id
ORDER BY total_products DESC;