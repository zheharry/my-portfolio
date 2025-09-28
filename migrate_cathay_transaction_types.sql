-- Migration script to standardize Cathay transaction types from Chinese to English
-- This script updates existing Cathay transactions to use BUY/SELL instead of 買進/賣出

-- Update Cathay buy transactions (買進 → BUY)
UPDATE transactions 
SET transaction_type = 'BUY'
WHERE broker = 'CATHAY' 
  AND transaction_type = '買進';

-- Update Cathay sell transactions (賣出 → SELL)  
UPDATE transactions 
SET transaction_type = 'SELL'  
WHERE broker = 'CATHAY'
  AND transaction_type = '賣出';

-- Verify the migration results
SELECT 
    'BEFORE - Chinese Types' as status,
    transaction_type, 
    COUNT(*) as count 
FROM transactions 
WHERE broker = 'CATHAY' 
  AND transaction_type IN ('買進', '賣出')
GROUP BY transaction_type

UNION ALL

SELECT 
    'AFTER - English Types' as status,
    transaction_type, 
    COUNT(*) as count 
FROM transactions 
WHERE broker = 'CATHAY' 
  AND transaction_type IN ('BUY', 'SELL')
GROUP BY transaction_type;