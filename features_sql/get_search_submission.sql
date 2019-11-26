#standardSQL
CREATE or replace table `tiki-dwh.consumer_product.feature_search_submission` as
with ga_search as (
    select parse_date("%Y%m%d",date) as date
                    , visitStartTime as event_timestamp
                    , fullVisitorId as deviceID
                    , hits.eventInfo.eventCategory as event_name
                    , case when device.deviceCategory IN ('mobile',
                                         'tablet') THEN 'mobile'
                 when device.deviceCategory = 'desktop' THEN 'desktop'
             END AS device_category
    from `tiki-gap.129159136.ga_sessions_201910*`, unnest(hits) hits 
    where 1=1
    and hits.eventInfo.eventCategory = 'Search'
    and hits.eventInfo.eventAction = 'Engagement'
    and hits.dataSource != 'app'
    group by 1, 2, 3, 4, 5
)

, firebase_search as (
    select event_date
                    , event_timestamp
                    , user_pseudo_id
                    , event_name
                    , device.operating_system as device_category
    from `tikiandroid-1047.data_log.events_new`, unnest(event_params) params 
    where 1=1
    and event_date between '2019-10-01'  and '2019-10-31'
    and event_name = 'search_submission'
    and params.key = 'keyword'
    group by 1, 2, 3, 4, 5
),

final as (

select *
from ga_search 
union all 
select *
from firebase_search
)

select deviceID 
                , countif(device_category='ANDROID') as android_search
                , countif(device_category= 'IOS') as ios_search
                , countif(device_category='mobile') as mobile_search
                , countif(device_category='desktop') as desktop_search
from final
group by 1