#standardSQL
CREATE OR REPLACE TABLE `tiki-dwh.consumer_product.feature_tikinow` as 
with tikinow as (
select  event_date as date
                , event_timestamp
                , user_pseudo_id as deviceID
                , device.operating_system as device_category
                , event_name as metrics
                , case when params.value.string_value like '%fast_delivery%' then 1 
                        else 0 end as tikinow
from `tikiandroid-1047.data_log.events_new`, unnest(event_params) params 
where 1=1
and event_date between '2019-10-01' and '2019-10-31'
and event_name = 'pdp_shipping_method'
and params.key = 'method'
group by 1,2,3,4,5,6

union all

SELECT  parse_date("%Y%m%d",date) as time
            , visitStartTime + hits.time
            , fullVisitorId
            , case when device.deviceCategory in  ('mobile', 'tablet') then 'mobile'
                when device.deviceCategory = 'desktop' then 'desktop' end as device_category 
            , hits.eventInfo.eventAction
            , case when customDimensions.value = '2-hour' then 1 
                    else 0 end as tikinow
FROM `tiki-gap.122443995.ga_sessions_201910*`, 
                                UNNEST(hits) hits, 
                                UNNEST(hits.product) product,
                                UNNEST(product.customDimensions) customDimensions
                                    
WHERE customDimensions.index = 25
AND customDimensions.value = "2-hour"
)

select  date, deviceID
                    , countif(device_category= 'ANDROID') as android_tknow
                    , countif(device_category= 'IOS') as ios_tknow
                    , countif(device_category= 'mobile') as mobile_tknow
                    , countif(device_category= 'desktop') as desktop_tknow
from tikinow
group by 1, 2