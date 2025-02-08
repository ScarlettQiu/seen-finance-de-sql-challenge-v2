with each_visitor_path_seq as(
	select up.path_number, up.path_name,
	pve.visitor_id,
	ROW_NUMBER() OVER(partition by pve.visitor_id order by pve.event_time) as step_number
	from
	page_view_events pve 
	inner join
	user_path up on up.path_name = pve.path
),
--select * from each_visitor_path_seq;
count_visitor_by_correct_path as(
	select evps.path_number, evps.path_name,
	COUNT(DISTINCT evps.visitor_id) as num_visitors
	from each_visitor_path_seq evps
	where step_number = path_number
	group by path_number
),
-- select * from count_visitor_by_correct_path
drop_off_calc as(
	select a.path_number, a.path_name,
	a.num_visitors,
	IFNULL(ROUND(((a.num_visitors - b.num_visitors) * 100.0 / (a.num_visitors)),3),0) as drop_off_perc
	from count_visitor_by_correct_path a
	left join count_visitor_by_correct_path b on a.path_number = b.path_number - 1
	order by a.path_number
)
select * from drop_off_calc;