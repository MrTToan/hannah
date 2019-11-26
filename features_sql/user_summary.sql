#standardSQL
with all_deviceID as (
    select distinct user_pseudo_id as deviceID
    from `tikiandroid-1047.data_log.events_new`
    where event_date between '2019-10-01' and '2019-10-31'

    union all

    select distinct fullvisitorId as deviceID
    from `tiki-gap.129159136.ga_sessions_201910*`
)

, search_submission as (
    select parse_date("%Y%m%d",date)
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

union all 

    select event_date
                    , event_timestamp
                    , user_pseudo_id
                    , event_name
                    , device.operating_system as device_category
    from `tikiandroid-1047.data_log.events_new`, unnest(event_params) params 
    where 1=1
    and event_date between '2019-10-01' and '2019-10-31'
    and event_name = 'search_submission'
    and params.key = 'keyword'
    group by 1, 2, 3, 4, 5
)

, add_to_cart as (
select  event_date as time
                , event_timestamp
                , user_pseudo_id as deviceID
                , device.operating_system as device_category
                , event_name as metrics
from `tikiandroid-1047.data_log.events_new`, unnest(event_params) params 
where 1=1
and event_date between '2019-10-01' and '2019-10-31'
and event_name = 'add_to_cart'
group by 1,2,3,4,5

union all

select parse_date("%Y%m%d",date) as time
                , visitStartTime
                , fullVisitorId
                , case when device.isMobile = True then 'mobile-web' else 'desktop-web'  
                                                                        end as device_category
                , hits.eventInfo.eventAction
from `tiki-gap.129159136.ga_sessions_*`, unnest(hits) hits
where _table_suffix between '20191001' and '20191031'
and hits.eventInfo.eventAction = 'Add To Cart - Primary'
group by 1, 2, 3,4,5
)

, coupon_raw as (
select  event_date as time
                , event_timestamp
                , user_pseudo_id as deviceID
                , device.operating_system as device_category
                , event_name as metrics
                , (select params.value.string_value from unnest(event_params) params where params.key = 'coupon') as coupon_code
from `tikiandroid-1047.data_log.events_new`, unnest(event_params) params 
where 1=1
and event_date between '2019-10-01' and '2019-10-31'
and event_name = 'coupon_delivery'
group by 1,2,3,4,5,6
)


, coupon as (

    select c.*, r.simple_action as coupon_type
    from coupon_raw c 
    left join `tiki-dwh.ecom.promo_salesrule_coupon_20*` sc on c.coupon_code = sc.code
    left join `tiki-dwh.ecom.promo_salesrule` r on sc.rule_id = r.id 
    where sc._table_suffix >= '19*'
)


select a.deviceID
            ,(select count(distinct concat(cast(event_timestamp as string), search.deviceID)) from search_submission where device_category = 'ANDROID') as android_search
            ,(select count(distinct concat(cast(event_timestamp as string), search.deviceID)) from search_submission where device_category = 'IOS') as ios_search
            ,(select count(distinct concat(cast(event_timestamp as string), search.deviceID)) from search_submission where device_category = 'mobile') as mobile_search
            ,(select count(distinct concat(cast(event_timestamp as string), search.deviceID)) from search_submission where device_category = 'desktop') as desktop_search
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from add_to_cart where device_category = 'ANDROID') as android_ATC
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from add_to_cart where device_category = 'IOS') as ios_ATC
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from add_to_cart where device_category = 'mobile') as mobile_ATC
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from add_to_cart where device_category = 'desktop') as desktop_ATC
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from coupon where device_category = 'ANDROID' and coupon_type = 'reward') as android_coupon_reward
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from coupon where device_category = 'ANDROID' and coupon_type = 'fixed_price') as android_coupon_fixedprice
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from coupon where device_category = 'ANDROID' and coupon_type = 'by_percent') as android_coupon_percent
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from coupon where device_category = 'ANDROID' and coupon_type = 'freegift') as android_coupon_gift
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from coupon where device_category = 'ANDROID' and coupon_type = 'cart_fixed') as android_coupon_cart
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from coupon where device_category = 'IOS' and coupon_type = 'reward') as ios_coupon_reward
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from coupon where device_category = 'IOS' and coupon_type = 'fixed_price') as ios_coupon_fixedprice
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from coupon where device_category = 'IOS' and coupon_type = 'by_percent') as ios_coupon_reward_percent
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from coupon where device_category = 'IOS' and coupon_type = 'freegift') as ios_coupon_reward_gift
            ,(select count(distinct concat(cast(event_timestamp as string), add.deviceID)) from coupon where device_category = 'IOS' and coupon_type = 'cart_fixed') as ios_coupon_reward_cart
            ,()
            
from all_deviceID a 
left join search_submission search on search.deviceID = a.deviceID 
left join add_to_cart add on add.deviceID = a.deviceID 
left join coupon on coupon.deviceID = a.deviceID 
