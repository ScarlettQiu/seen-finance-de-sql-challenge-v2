## Seen Finance Data Engineering SQL Challenge V2

#### **Q2: Write a query that lists unusual transactions. It’s up to you to define what unusual means. You might want to use some statistical functions.**

- Since the definition of unusual transactions was not specified, I identified them using the 1.5 IQR rule with a 90-day rolling window.
- To detect outliers, I calculated the 25th percentile (Q1) and 75th percentile (Q3), then determined the bounds using:
   - Lower Bound: Q1 - 1.5 * (Q3 - Q1)
   - Upper Bound: Q3 + 1.5 * (Q3 - Q1)
- Any transaction exceeding the upper bound within the 90-day rolling window is flagged as unusual.
- This query specifically considers money outflows.
- The analysis is conducted at the account level, not the customer level.


    ```SQL
    TBD
    ```

#### **Q3: Using the data in “page_view_events”, what are the 5 most common paths that people follow in the app? Please keep in mind that we are looking for complete paths. (a → b → c).**

- Here, `by complete paths` I am `assuming` that the `path begins from '/product-disclosures'` and the `path ends at '/download-app'`, and here is the query that looks at all such paths and gives out the `top 5 paths that starts and ends with '/product-disclosures' and '/download-app`'.

    ```SQL
    with complete_paths_cte as(
        select STRING_AGG(pve.path, " to " order by pve.event_time) as "complete_user_path",
        pve.visitor_id
        from page_view_events pve
        group by visitor_id
    ),
    user_path_cnt as(
        select cpc.complete_user_path,
        COUNT(*) as total_path_visits,
        DENSE_RANK() OVER(order by count(*) desc) as rnk
        from
        complete_paths_cte cpc
        WHERE complete_user_path LIKE '/product-disclosures%' AND complete_user_path LIKE '%/download-app'
        group by 1
    )
    select complete_user_path, total_path_visits
    from user_path_cnt
    where rnk <= 5;
    ```

- Now, if we don't want to make any such assumptions about the start and end points of the path, we can remove the condition to look at the most common paths, but I think the above solution is more suitable for analysis that requires about the complete path taken by each customer when applying for a credit card, and the below solution is useful when you want to analyze about at what step did the customer visit and could not go to the next step or did not the complete the next steps in the process. This relates a bit to conversion, as to how many people actually convert from one step to another and what steps do we still need to work on or follow up on the user to remind them to complete the remaining steps.

    ```SQL
    with complete_paths_cte as(
        select STRING_AGG(pve.path, " to " order by pve.event_time) as "complete_user_path",
        pve.visitor_id
        from page_view_events pve
        group by visitor_id
    ),
    user_path_cnt as(
        select cpc.complete_user_path,
        COUNT(*) as total_path_visits,
        DENSE_RANK() OVER(order by count(*) desc) as rnk
        from
        complete_paths_cte cpc
        group by 1
    )
    select complete_user_path, total_path_visits
    from user_path_cnt
    where rnk <= 5;
    ```

#### **Q4: Using the data in “page_view_events”, write a query that can be used to display the following funnel. The order of the steps is outlined above. Please create a query that outputs the page name, the count of visitors that entered the step, and the percentage which dropped off before the next step.**

    /product-disclosures
    /input-email
    /input-password
    /verify-email
    /input-phone
    /input-sms
    /kyc-info
    /input-name
    /input-address
    /date-of-birth
    /input-ssn
    /input-income
    /confirm-details

- My approach here was to `first create a look-up table`, as we already know that this is the path that users should follow, `we can make this a separate look-up table`. Query to create a look-up table is as follows, and you need to run this first, because it is going to be used in the final query:

    ```SQL
    -- Query to create auxillary table, which will be used in the final query
    CREATE TABLE IF NOT EXISTS user_path (
        path_number NUMBER,
        path_name STRING
    );

    INSERT INTO user_path values
    (1,"/product-disclosures"),
    (2,"/input-email"),
    (3,"/input-password"),
    (4,"/verify-email"),
    (5,'/input-phone'),
    (6,"/input-sms"),
    (7,"/kyc-info"),
    (8,"/input-name"),
    (9,"/input-address"),
    (10,'/date-of-birth'),
    (11,"/input-ssn"),
    (12,"/input-income"),
    (13,"/confirm-details");
    ```

- Now using this as the look-up table, we first give each visitor's visit to a specific path a number, ordered by the `event_time` in the CTE `each_visitor_path_seq`. This will help us ensure that the user follows the sequence of paths correctly. In the `count_visitor_by_correct_path` CTE, we count the number of visitors per path and make sure that the sequence of paths is followed by each user. And lastly, in the `drop_off_calc` CTE, we calculate the drop off percentage of number of visitors that dropped of before the next step. Naturally, the last path `/confirm-details` will have `NULL` as drop off `percentage`, because there are no other steps after this and we can't calculate how many visitors dropped off before going to the next step, and I replace that with `0`.

    ```SQL
    with each_visitor_path_seq as(
        select up.path_number, up.path_name,
        pve.visitor_id,
        ROW_NUMBER() OVER(partition by pve.visitor_id order by pve.event_time) as step_number
        from
        page_view_events pve 
        inner join
        user_path up on up.path_name = pve.path
    ),
    count_visitor_by_correct_path as(
        select evps.path_number, evps.path_name,
        COUNT(DISTINCT evps.visitor_id) as num_visitors
        from each_visitor_path_seq evps
        where step_number = path_number
        group by path_number
    ),
    drop_off_calc as(
        select a.path_number, a.path_name,
        a.num_visitors,
        IFNULL(ROUND(((a.num_visitors - b.num_visitors) * 100.0 / (a.num_visitors)),3),0) as drop_off_perc
        from count_visitor_by_correct_path a
        left join count_visitor_by_correct_path b on a.path_number = b.path_number - 1
        order by a.path_number
    )
    select * from drop_off_calc;
    ```

#### Q5: Using the data within the “accounts_days_past_due” table , write a query that shows the delinquency periods for a customer, using the context in the above section as a guide. Your output should one row per delinquency period, with the following columns: customer_id , account_id , delinquency_period , delinquency_start_date , delinquency_end_date

- `What was the total count of rows in your output? : 15`
- The approach for this query was to fill in the gaps where the `days_past_due` was `0` where the customer was delinquent. Used a recursive CTE, for each row from the previous step `RecursiveUpdate`, adjust `days_past_due` by subtracting 1 and join it with the next row from `BaseData` where `days_past_due` is 0. This helps to extend delinquency periods backward. Then it was just a matter of finding the start and the end of deliquency period, and handel edge cases where the customer was still deliquent. Even though this approach achieves what we want it to do, it uses a recursive cte, which will start becoming costly once the number of backfills we have to do keep increasing. This depends on the number of recursive iterations. In the worst case, it could be `O(m * k)`, where `m` is the number of rows we have to bounce back and `k` is the maximum recursion depth. Each recursive join has to process previous rows, so if there are many rows and each row might have many predecessors to check. The complexity of the final select statement will be `O(n log n)`, where `n` will be the number of rows and `log n` for sorting operation. So the final complexity will become `O(n log n + m * k)`, which can be improved, if we perform an intermediate step in the ETL pipeline of filling in the days_past_due column with the correct number of days past due for each customer, before writing the final table in the database, and then we can eleminate the recursive calculation all together, bringing the time complexity down to `O(n log n)`.

    ```SQL
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
    ```
