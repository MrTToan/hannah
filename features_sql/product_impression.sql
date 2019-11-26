#standardSQL
CREATE OR REPLACE TABLE `tiki-dwh.consumer_product.feature_product_impresion` as 
with product_impression as (
select  event_date as date
                , event_timestamp
                , user_pseudo_id as deviceID
                , device.operating_system as device_category
                , event_name as metrics
from `tikiandroid-1047.data_log.events_new`, unnest(event_params) params 
where 1=1
and event_date between '2019-10-01' and '2019-10-31'
and event_name = 'product_impression'
group by 1,2,3,4,5

union all

select parse_date("%Y%m%d",date) as time
                , visitStartTime + hits.time
                , fullVisitorId
                , case when device.isMobile = True then 'mobile-web' else 'desktop-web'  
                                                                        end as device_category
                , hits.eventInfo.eventAction
from `tiki-gap.129159136.ga_sessions_*`, unnest(hits) hits, unnest(hits.product) product
where _table_suffix between '20191001' and '20191031'
and product.isImpression = True
group by 1, 2, 3,4,5
)

select date, deviceID
                    , countif(device_category='ANDROID') as android_product_impression
                    , countif(device_category='IOS') as ios_product_impression
                    , countif(device_category='mobile') as mobile_product_impression
                    , countif(device_category='desktop') as desktop_product_impression
from product_impression
group by 1, 2