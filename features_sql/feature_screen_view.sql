with screen_class as 
(select 
event_date,
event_timestamp,
user_id,
user_pseudo_id,
category,
mobile_model_name,
mobile_brand_name,
platform,
version,
region,
event_name,
screen_class_current,
LAG(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS event_timestamp_before,
LAG(screen_class_current,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS screen_class_before,
LEAD(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS event_timestamp_after,
LEAD(screen_class_current,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS screen_class_after,
(event_timestamp - LAG(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp))/(1000*1000*60) time_before,
case when (LEAD(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) - event_timestamp) > (5*1000*1000*60) then 0 else (LEAD(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) - event_timestamp)/(1000*1000*60) end time_after,
CASE 
  WHEN screen_class_current <> LAG(screen_class_current,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) 
  OR LAG(screen_class_current,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp)  IS NULL THEN 1 ELSE 0 
  END is_new_screen,
CASE
  WHEN event_timestamp - LAG(event_timestamp,1,0) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) > (5*60*1000*1000)
  OR LAG(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) IS NULL THEN 1 ELSE 0
  END AS is_new_session
from 
( 
select 
event_date,
event_timestamp,
user_id,
user_pseudo_id,
device.category,
device.mobile_model_name,
device.mobile_brand_name,
platform,
app_info.version,
geo.region,
event_name,
params.value.string_value screen_class_current
from
`tikiandroid-1047.data_log.events_new`,
unnest(event_params) as params
where 
1=1
and event_date >= '2019-12-22'
and event_date < '2019-12-23'
and params.key = 'firebase_screen_class'
)
),

screen_class_1 as 
(
select 
screen_class.*,
SUM(is_new_screen) over (partition by user_pseudo_id order by event_timestamp) AS screen_id,
SUM(is_new_session) over (partition by user_pseudo_id order by event_timestamp) AS session_id
from screen_class
)

select
screen_class_3.*,
coalesce(svs.screen_name, screen_class_after) screen_name_after,
coalesce(svs.section_l2, screen_class_after) section_l2_after,
case when screen_class_after is null then 'Exit' else 'Not_exit' end Exit_screen
from
(
select 
screen_class_2.*,
LEAD(screen_class_current,1) OVER (PARTITION BY user_pseudo_id ORDER BY screen_id) AS screen_class_after
from
(
select 
UNIX_MILLIS(timestamp(event_date)) created,
event_date,
user_pseudo_id,
category,
mobile_model_name,
mobile_brand_name,
sc1.platform,
version,
region,
screen_class_current,
coalesce(svs.screen_name, screen_class_current) screen_name_current,
coalesce(svs.section_l2, screen_class_current) section_l2_current,
session_id,
screen_id,
sum(time_after) time_spent,
count(1) number_of_event
from screen_class_1 sc1
left join `tiki-dwh.consumer_product.screen_view_section_import` svs on sc1.screen_class_current = svs.firebase_screen_class and sc1.platform = svs.platform
where 1=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
) screen_class_2 
) screen_class_3 
left join `tiki-dwh.consumer_product.screen_view_section_import` svs on screen_class_3.screen_class_after = svs.firebase_screen_class and screen_class_3.platform = svs.platform