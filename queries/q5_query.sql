with BaseData as(
    select
        customer_id,
        account_id,
        date(asof_date) as asof_date,
        cast(days_past_due as int) as days_past_due,
        ROW_NUMBER() OVER(order by customer_id, asof_date) as row_num
    from accounts_days_past_due
),
RecursiveUpdate as(
    select
        customer_id,
        account_id,
        asof_date,
        days_past_due,
        row_num
    from BaseData
    where days_past_due > 1
    union all 
    select
        b.customer_id,
        b.account_id,
        b.asof_date,
        r.days_past_due - 1 AS days_past_due,
        b.row_num
    from BaseData b
    JOIN RecursiveUpdate r
    ON b.customer_id = r.customer_id
    AND b.account_id = r.account_id
    AND b.row_num = r.row_num - 1
    where b.days_past_due = 0
    AND r.days_past_due > 1
),
combined as(
	select
	    customer_id,
	    account_id,
	    asof_date,
	    days_past_due,
	  	row_num
	from BaseData
	where row_num NOT IN (select row_num from RecursiveUpdate)
	union all
	select
	    customer_id,
	    account_id,
	    asof_date,
	    days_past_due,
	  row_num
	from RecursiveUpdate
	ORDER BY row_num
),
calc as(
  	select 
  	customer_id,
  	account_id,
  	asof_date,
  	days_past_due as delinquency_period,
  	lead(days_past_due) over (partition by customer_id, account_id order by asof_date asc) as nxt_days_past_due,
  	DATE(asof_date, '-' || days_past_due || ' days', '+1 days') as delinquency_start_date,
  	asof_date as delinquency_end_date
  from combined
)
select 
customer_id, account_id, asof_date, delinquency_period, delinquency_start_date, case when nxt_days_past_due is null then NULL else asof_date end as delinquency_end_date
from calc where delinquency_period > 0 and (nxt_days_past_due = 0 or nxt_days_past_due is null) order by customer_id;