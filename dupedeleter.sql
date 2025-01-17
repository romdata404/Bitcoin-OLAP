CREATE TABLE cleaned_data AS
SELECT 
    transaction_id,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY transaction_id, address, btc_amount ORDER BY address) > 1 
        THEN NULL 
        ELSE address 
    END AS address,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY transaction_id, address, btc_amount ORDER BY address) > 1 
        THEN NULL 
        ELSE btc_amount 
    END AS btc_amount,
    other_columns -- Include any other relevant columns you want to retain
FROM my_data;
