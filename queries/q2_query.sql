with avg_per_cust_calc as(
	select customer_id,
	AVG(CAST(transaction_amount as float)) as avg_trans_amt
	from accounts_transactions
	where CAST(transaction_amount as float) > 0
	group by customer_id
),
stddev_per_cust_calc as(
	select acct.customer_id,
	apcc.avg_trans_amt,
	SQRT((SUM((CAST(acct.transaction_amount as float) - apcc.avg_trans_amt) * (CAST(acct.transaction_amount as float) - apcc.avg_trans_amt))) / ((COUNT(*) - 1))) as stddev_amt
	from accounts_transactions acct
	INNER JOIN avg_per_cust_calc apcc on acct.customer_id = apcc.customer_id
	where CAST(acct.transaction_amount AS float) > 0
	group by acct.customer_id
),
query_unusual_trans as(
	select acct.customer_id,
	acct.account_id,
	acct.transaction_date,
	CAST(acct.transaction_amount as float) as transaction_amount,
	calc.avg_trans_amt,
	calc.stddev_amt,
	((CAST(acct.transaction_amount AS float) - calc.avg_trans_amt) / calc.stddev_amt) as z_score,
	CASE WHEN (ABS(acct.transaction_amount - calc.avg_trans_amt)) > (calc.stddev_amt * 3) then 1 else 0 end as flagged_trans
	from accounts_transactions acct
	inner join stddev_per_cust_calc calc on acct.customer_id = calc.customer_id
	where 
		CAST(acct.transaction_amount AS float) > 0 and calc.stddev_amt > 0
)
select * from query_unusual_trans
where flagged_trans = 1
order by ABS(z_score) desc;
--select * from query_unusual_trans;
--select * from stddev_per_cust_calc;
--select * from avg_per_cust_calc;
--select * from accounts_transactions at2 where customer_id = '189436ef-adf4-48ba-9f0b-d3da331c49ac'