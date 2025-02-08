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

--with complete_paths_cte as(
--	select STRING_AGG(pve.path, " to " order by pve.event_time) as "complete_user_path",
--	pve.visitor_id
--	from page_view_events pve
--	group by visitor_id
--),
--user_path_cnt as(
--	select cpc.complete_user_path,
--	COUNT(*) as total_path_visits,
--	DENSE_RANK() OVER(order by count(*) desc) as rnk
--	from
--	complete_paths_cte cpc
--	group by 1
--)
--select complete_user_path, total_path_visits
--from user_path_cnt
--where rnk <= 5;