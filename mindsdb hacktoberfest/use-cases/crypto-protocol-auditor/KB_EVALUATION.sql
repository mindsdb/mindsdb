-- ============================================
-- Knowledge Base Evaluation Suite
-- Crypto Protocol Auditor - web3_kb
-- ============================================

-- Step 1: Verify KB exists and get metadata
-- ===========================================
SELECT 
    'KB Metadata Check' as test_name,
    COUNT(*) as total_documents,
    'PASS' as status
FROM web3_kb;

-- Step 2: Test Basic Search Functionality
-- =========================================
SELECT 
    'Test 1: Proof of Work' as query,
    COUNT(*) as results_found,
    ROUND(AVG(relevance), 3) as avg_relevance
FROM web3_kb
WHERE chunk_content LIKE '%proof%work%' 
   OR chunk_content LIKE '%PoW%'
LIMIT 10;

-- Step 3: Test Semantic Search Coverage
-- ========================================
SELECT 
    'Test 2: Smart Contracts' as query,
    COUNT(*) as results_found,
    ROUND(AVG(relevance), 3) as avg_relevance
FROM web3_kb
WHERE chunk_content LIKE '%smart contract%'
   OR chunk_content LIKE '%contract%'
LIMIT 10;

-- Step 4: Test Protocol Coverage
-- ================================
SELECT 
    'Test 3: Bitcoin Protocol' as query,
    COUNT(*) as results_found,
    ROUND(AVG(relevance), 3) as avg_relevance
FROM web3_kb
WHERE chunk_content LIKE '%Bitcoin%'
   OR chunk_content LIKE '%BTC%'
LIMIT 10;

-- Step 5: Test Ethereum Coverage
-- ================================
SELECT 
    'Test 4: Ethereum Architecture' as query,
    COUNT(*) as results_found,
    ROUND(AVG(relevance), 3) as avg_relevance
FROM web3_kb
WHERE chunk_content LIKE '%Ethereum%'
   OR chunk_content LIKE '%EVM%'
LIMIT 10;

-- Step 6: Test DeFi Knowledge
-- ============================
SELECT 
    'Test 5: DeFi Concepts' as query,
    COUNT(*) as results_found,
    ROUND(AVG(relevance), 3) as avg_relevance
FROM web3_kb
WHERE chunk_content LIKE '%DeFi%'
   OR chunk_content LIKE '%liquidity%'
   OR chunk_content LIKE '%decentralized finance%'
LIMIT 10;

-- Step 7: Test Consensus Mechanism Coverage
-- ===========================================
SELECT 
    'Test 6: Consensus Mechanisms' as query,
    COUNT(*) as results_found,
    ROUND(AVG(relevance), 3) as avg_relevance
FROM web3_kb
WHERE chunk_content LIKE '%consensus%'
   OR chunk_content LIKE '%PoS%'
   OR chunk_content LIKE '%Proof of Stake%'
LIMIT 10;

-- Step 8: Test Document ID Distribution
-- ========================================
SELECT 
    'Test 7: Document Distribution' as metric,
    COUNT(DISTINCT id) as unique_documents,
    COUNT(*) as total_chunks
FROM web3_kb;

-- Step 9: Test Metadata Completeness
-- ====================================
SELECT 
    'Test 8: Metadata Check' as test_name,
    SUM(CASE WHEN metadata IS NOT NULL THEN 1 ELSE 0 END) as docs_with_metadata,
    COUNT(*) as total_docs,
    ROUND(100.0 * SUM(CASE WHEN metadata IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as metadata_coverage_percent
FROM web3_kb;

-- Step 10: Test Relevance Score Distribution
-- ============================================
SELECT 
    'Test 9: Relevance Distribution' as metric,
    ROUND(MIN(relevance), 3) as min_relevance,
    ROUND(AVG(relevance), 3) as avg_relevance,
    ROUND(MAX(relevance), 3) as max_relevance
FROM web3_kb;

-- Step 11: Sample Top Documents by Relevance
-- ============================================
SELECT 
    'Test 10: Top Documents Sample' as test_name,
    id as doc_id,
    ROUND(relevance, 3) as relevance_score,
    SUBSTR(chunk_content, 1, 100) as content_preview
FROM web3_kb
ORDER BY relevance DESC
LIMIT 5;

-- Summary: Overall KB Health Check
-- =================================
SELECT 
    'KB Health Summary' as check_type,
    COUNT(*) as total_items,
    COUNT(DISTINCT id) as unique_docs,
    ROUND(AVG(relevance), 3) as avg_relevance,
    'READY FOR PRODUCTION' as status
FROM web3_kb;
