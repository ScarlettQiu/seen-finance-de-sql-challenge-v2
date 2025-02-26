WITH tran_90_day AS(
SELECT consumer_id
      , account_id
      , merchant_name
      , merchant_category_code
      , transaction_date
      , transaction_amount
FROM accounts_transactions
WHERE transaction_date >=DATEADD(DAY,-90,CONVERT(date,GETDATE()))
AND transaction_amount < 0 
),
--determine the bounds
bounds AS(
SELECT account_id
      , Q1-1.5*(Q3 - Q1) AS lower
      , Q3 + 1.5*(Q3 - Q1) AS upper
FROM (
SELECT account_id,
       PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY transaction_amount) OVER (PARTITION BY account_id) AS Q1,
       PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY transaction_amount) OVER (PARTITION BY account_id) AS Q3
FROM tran_90_day) AS A
)

SELECT DISTINCT * FROM
(
SELECT DISTINCT consumer_id
      , t.account_id, merchant_nam2
      , merchant_category_code
      , transaction_date
      , transaction_amount
      , CASE WHEN transaction_amount > upper OR transaction_amount < lower THEN 1
        ELSE 0
        END AS unusual
FROM tran_90_day t
LEFT JOIN bounds b
ON T.account_id = B.account_id) AS B
WHERE unusual = 1
