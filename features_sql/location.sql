#standardSQL 
CREATE OR REPLACE TABLE `tiki-dwh.consumer_product.feature_location` as
with location as (
select event_date,
                event_timestamp,
                user_pseudo_id as deviceID, 
                geo.city city
from `tikiandroid-1047.data_log.events_new`
where event_date between '2019-10-01' and '2019-10-31'
group by 1, 2, 3, 4

union all 

select parse_date("%Y%m%d",date) as time
            , visitStartTime + hits.time as event_timestamp
            , fullVisitorId
            , geoNetwork.city 
from `tiki-gap.129159136.ga_sessions_201910*`, unnest(hits) hits
group by 1, 2, 3, 4
)

,urban as (

select event_date, deviceID
                            , countif(city in ('Hanoi', 'Ho Chi Minh City')) as t
from location 
group by 1, 2
)

select event_date, deviceID, case when t > 0 then 1 else 0 end as _urban
from urban 