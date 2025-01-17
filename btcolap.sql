DELETE FROM `sincere-cat-433505-t6.address_data.address_table2`
WHERE 
  STRUCT(wallet, address) IN (
    SELECT STRUCT(wallet, address)
    FROM (
      SELECT
        wallet,
        address,
        ROW_NUMBER() OVER (PARTITION BY wallet, address ORDER BY wallet) AS row_num
      FROM
        `sincere-cat-433505-t6.address_data.address_table2`
    )
    WHERE
      row_num > 1
  );

--double check for dupes
/*
SELECT
    wallet,
    address,
    COUNT(*) AS occurrence_count
FROM
    `sincere-cat-433505-t6.address_data.address_table2`
GROUP BY
    wallet,
    address
HAVING
    COUNT(*) > 1
ORDER BY
    occurrence_count DESC;*/



CREATE TABLE `sincere-cat-433505-t6.address_data.txid_input2` AS
WITH addr AS (
  SELECT
    wallet,
    address
  FROM 
    `sincere-cat-433505-t6.address_data.address_table2`
)

SELECT
  block_timestamp,
  addr.wallet,
  iaddress AS wallet_address,
  transaction_hash,
  block_number
FROM
  --filtered_inputs AS inputs,
  `bigquery-public-data.crypto_bitcoin.inputs` AS inputs,
  UNNEST(addresses) AS iaddress
JOIN
  addr
ON
  iaddress = addr.address
ORDER BY
  block_timestamp, wallet;


CREATE TABLE `sincere-cat-433505-t6.address_data.txid_output2` AS
WITH addr AS (
  SELECT
    wallet,
    address
  FROM 
    `sincere-cat-433505-t6.address_data.address_table2`
)

SELECT
  block_timestamp,
  addr.wallet,
  iaddress,
  transaction_hash,
  block_number
FROM
  `bigquery-public-data.crypto_bitcoin.outputs` AS outputs,
  UNNEST(addresses) AS iaddress
JOIN
  addr
ON
  iaddress = addr.address
ORDER BY
  block_timestamp, wallet;



CREATE TABLE `sincere-cat-433505-t6.address_data.simple_flow` AS
WITH txid_filter AS (
  SELECT
    block_timestamp,
    wallet,
    wallet_address,
    transaction_hash
  FROM `sincere-cat-433505-t6.address_data.txid_sum2`
  WHERE TIMESTAMP(block_timestamp) > '2021-09-24'
),

in_data AS(
  SELECT
    tx.block_timestamp,
    txid_filter.wallet,
    txid_filter.wallet_address,
    tx.hash AS txid,
    'input' AS type,
    input_addr AS addr,
    -input.value/ 100000000 AS val
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    tx.inputs AS input,
    UNNEST(input.addresses) AS input_addr
  JOIN
    txid_filter
  ON txid_filter.transaction_hash = tx.hash
  WHERE tx.input_count = 1 OR tx.output_count = 1
),

out_data AS(
  SELECT
    tx.block_timestamp,
    txid_filter.wallet,
    txid_filter.wallet_address,
    tx.hash AS txid,
    'output' AS type,
    output_addr AS addr,
    output.value/ 100000000 AS val
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    tx.outputs AS output,
    UNNEST(output.addresses) AS output_addr
  JOIN
  txid_filter
  ON txid_filter.transaction_hash = tx.hash
  WHERE tx.input_count = 1 OR tx.output_count = 1
)

SELECT *
FROM in_data
UNION ALL
SELECT *
FROM out_data
ORDER BY txid, wallet, wallet_address;


CREATE TABLE `sincere-cat-433505-t6.address_data.simple_flow` AS
WITH txid_filter AS (
  SELECT
    block_timestamp,
    wallet,
    wallet_address,
    transaction_hash
  FROM `sincere-cat-433505-t6.address_data.txid_sum2`
  WHERE TIMESTAMP(block_timestamp) > '2021-09-24'
  LIMIT 10
),

in_data AS(
  SELECT
    tx.block_timestamp,
    txid_filter.wallet,
    txid_filter.wallet_address,
    tx.hash AS txid,
    input_addr AS addr,
    -input.value AS val
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    tx.inputs AS input,
    UNNEST(input.addresses) AS input_addr
  JOIN
    txid_filter
  ON txid_filter.transaction_hash = tx.hash
  WHERE tx.input_count = 1
),

out_data AS(
  SELECT
    tx.block_timestamp,
    txid_filter.wallet,
    txid_filter.wallet_address,
    tx.hash AS txid,
    output_addr AS addr,
    output.value AS val
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    tx.outputs AS output,
    UNNEST(output.addresses) AS output_addr
  JOIN
  txid_filter
  ON txid_filter.transaction_hash = tx.hash
  WHERE tx.input_count = tx.output_count
)

SELECT *
FROM in_data
UNION ALL
SELECT *
FROM out_data
ORDER BY block_timestamp, txid, wallet, wallet_address;


CREATE TABLE `sincere-cat-433505-t6.address_data.simple_flow` AS
WITH txid_filter AS (
  SELECT 
    transaction_hash
  FROM `sincere-cat-433505-t6.address_data.txid_sum`
  WHERE TIMESTAMP(block_timestamp) > '2021-09-24'
  LIMIT 10
),

transaction_filter AS (
  SELECT 
    txid,
    wallet,
    wallet_address,
    tx_address,
    flow,
    CASE WHEN flow < 0 THEN 'input' ELSE 'output' END AS type
  FROM `sincere-cat-433505-t6.address_data.txid_all`
  INNER JOIN txid_filter
  ON txid_filter.transaction_hash = txid
)

SELECT 
  txid,
  wallet,
  wallet_address,
  COUNT(CASE WHEN type = 'input' THEN 1 END) AS input_count,
  COUNT(CASE WHEN type = 'output' THEN 1 END) AS output_count,
  ARRAY_AGG(CASE WHEN type = 'input' AND tx_address IS NOT NULL THEN tx_address END IGNORE NULLS) AS input_addresses,
  ARRAY_AGG(CASE WHEN type = 'output' AND tx_address IS NOT NULL THEN tx_address END IGNORE NULLS) AS output_addresses
FROM transaction_filter
GROUP BY txid, wallet, wallet_address
HAVING (input_count = 1 AND output_count = 1 AND input_addresses[OFFSET(0)] != output_addresses[OFFSET(0)])
   OR (input_count > 1 AND output_count = 1);


CREATE TABLE `sincere-cat-433505-t6.address_data.congruent` AS
WITH in_data AS(
  SELECT
    tx.hash AS txid,
    tx.input_count,
    tx.output_count,
    input_addr AS addr,
    -input.value AS val
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    tx.inputs AS input,
    UNNEST(input.addresses) AS input_addr
  WHERE tx.input_count = tx.output_count
),

out_data AS(
  SELECT
    tx.hash AS txid,
    tx.input_count,
    tx.output_count,
    output_addr AS addr,
    output.value AS val
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    tx.outputs AS output,
    UNNEST(output.addresses) AS output_addr
  WHERE tx.input_count = tx.output_count
)

SELECT *
FROM in_data
UNION ALL
SELECT *
FROM out_data
ORDER BY txid;




CREATE TABLE `sincere-cat-433505-t6.address_data.txid_sum2` AS

SELECT
  block_timestamp,
  wallet,
  wallet_address,
  transaction_hash
FROM 
  `sincere-cat-433505-t6.address_data.txid_input2`
UNION DISTINCT
SELECT
  block_timestamp,
  wallet,
  iaddress AS wallet_address,
  transaction_hash
FROM 
  `sincere-cat-433505-t6.address_data.txid_output2`

DELETE FROM `sincere-cat-433505-t6.address_data.txid_sum2`
WHERE transaction_hash IN (
  SELECT transaction_hash
  FROM (
    SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY transaction_hash ORDER BY transaction_hash) AS row_num
    FROM `sincere-cat-433505-t6.address_data.txid_sum2`)
  WHERE row_num > 1
);


CREATE TABLE `sincere-cat-433505-t6.address_data.txid_sum3` AS

SELECT
  block_timestamp,
  transaction_hash
FROM 
  `sincere-cat-433505-t6.address_data.txid_input2`
UNION DISTINCT
SELECT
  block_timestamp,
  transaction_hash
FROM 
  `sincere-cat-433505-t6.address_data.txid_output2`


CREATE TABLE `sincere-cat-433505-t6.address_data.simple_flow_preSAFE` AS
WITH txid_filter AS (
  SELECT
    block_timestamp,
    transaction_hash -- multiple wallets and addresses may fall into one txid
  FROM `sincere-cat-433505-t6.address_data.txid_sum3`
  WHERE TIMESTAMP(block_timestamp) < '2021-09-24'
),

in_data AS(
  SELECT
    tx.block_timestamp,
    tx.hash AS txid,
    'input' AS type,
    input_addr AS addr,
    -input.value/ 100000000 AS val
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    tx.inputs AS input,
    UNNEST(input.addresses) AS input_addr
  JOIN
    txid_filter
  ON txid_filter.transaction_hash = tx.hash
),

out_data AS(
  SELECT
    tx.block_timestamp,
    tx.hash AS txid,
    'output' AS type,
    output_addr AS addr,
    output.value/ 100000000 AS val
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    tx.outputs AS output,
    UNNEST(output.addresses) AS output_addr
  JOIN
    txid_filter
  ON txid_filter.transaction_hash = tx.hash
)

SELECT *
FROM in_data
UNION ALL
SELECT *
FROM out_data
ORDER BY txid, block_timestamp;



CREATE TABLE `sincere-cat-433505-t6.address_data.simple_flow_postSAFE` AS
WITH txid_filter AS (
  SELECT
    block_timestamp,
    transaction_hash -- multiple wallets and addresses may fall into one txid
  FROM `sincere-cat-433505-t6.address_data.txid_sum3`
  WHERE TIMESTAMP(block_timestamp) >= '2021-09-24'
),

in_data AS(
  SELECT
    tx.block_timestamp,
    tx.hash AS txid,
    'input' AS type,
    input_addr AS addr,
    -input.value/ 100000000 AS val
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    tx.inputs AS input,
    UNNEST(input.addresses) AS input_addr
  JOIN
    txid_filter
  ON txid_filter.transaction_hash = tx.hash
),

out_data AS(
  SELECT
    tx.block_timestamp,
    tx.hash AS txid,
    'output' AS type,
    output_addr AS addr,
    output.value/ 100000000 AS val
  FROM `bigquery-public-data.crypto_bitcoin.transactions` AS tx,
    tx.outputs AS output,
    UNNEST(output.addresses) AS output_addr
  JOIN
    txid_filter
  ON txid_filter.transaction_hash = tx.hash
)

SELECT *
FROM in_data
UNION ALL
SELECT *
FROM out_data
ORDER BY txid, block_timestamp;



CREATE TABLE `sincere-cat-433505-t6.address_data.simple_flow_postSAFE_walletlocation` AS
WITH simple AS (
  SELECT *
  FROM 
    `sincere-cat-433505-t6.address_data.simple_flow_postSAFE_wallet`
)

SELECT
  block_timestamp,
  txid,
  type,
  location,
  wall.wallet AS wallet,
  simple.addr AS addr,
  val

FROM simple
JOIN `sincere-cat-433505-t6.address_data.wallet_location` AS wall
ON simple.wallet = wall.wallet;


CREATE TABLE `sincere-cat-433505-t6.address_data.simple_flow_preSAFE_walletlocation` AS
WITH simple AS (
  SELECT *
  FROM 
    `sincere-cat-433505-t6.address_data.simple_flow_preSAFE_wallet`
)

SELECT
  block_timestamp,
  txid,
  type,
  location,
  wall.wallet AS wallet,
  simple.addr AS addr,
  val

FROM simple
JOIN `sincere-cat-433505-t6.address_data.wallet_location` AS wall
ON simple.wallet = wall.wallet;

WITH infilter AS (
    SELECT *
    FROM 'ledger_table.parquet'
    WHERE block_timestamp > '2018-09-24')
SELECT
    DATE_TRUNC('day', block_timestamp) AS date,
    SUM(-val) AS txid_input
FROM infilter
WHERE type = 'input' AND location = 'China'
GROUP BY DATE_TRUNC('day', block_timestamp)
ORDER BY txid_input;