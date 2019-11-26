#standardSQL
CREATE OR REPLACE TABLE `tiki-dwh.consumer_product.feature_pdp` as 
with app_spid as (
select  event_date as date
                , event_timestamp
                , user_pseudo_id as deviceID
                , device.operating_system as platform
                , (select params.value.string_value from unnest(event_params) params where params.key = 'spid') as spid
from `tikiandroid-1047.data_log.events_new`, unnest(event_params) params 
where 1=1
and event_date = '2019-10-01' 
and event_name = 'ecom_product_view'
group by 1,2,3,4,5
),

desktop_sku as (

SELECT 
          DATE(TIMESTAMP_MILLIS(visitStartTime*1000 + hits.time),'Asia/Bangkok') AS event_date,
          visitStartTime,
          fullVisitorID, 
          case when device.isMobile = True then 'mobile' 
               when device.isMobile = False then 'desktop' end as platform,
          (SELECT STRING_AGG (customDimensions.value,',' )
                          FROM UNNEST(hits.product) AS product, 
                               UNNEST(product.customDimensions) AS customDimensions
                          WHERE index = 15) AS sku 
      FROM `tiki-gap.129159136.ga_sessions_20191001`
           ,UNNEST(hits) AS hits 
      WHERE 1=1
      group by 1, 2, 3, 4, 5
      HAVING IFNULL(sku,'0') <> '0'
),


pdp as (


select a.*, dp.tier_price, dp.productset_name, dp.cate1, dp.cate2, dp.sub_cate_report
from app_spid a 
left join `tiki-dwh.dwh.dim_product_full` dp on a.spid = cast(dp.product_key as string)
group by 1, 2, 3, 4, 5 ,6, 7, 8, 9, 10,11

union all 

select pc.*, dp.tier_price, dp.productset_name, dp.cate1, dp.cate2, dp.sub_cate_report
from desktop_sku pc 
left join `tiki-dwh.dwh.dim_product_full` dp on pc.sku = dp.sku
group by 1, 2, 3, 4, 5 ,6, 7, 8, 9, 10
)

select * from pdp

-- left join (
--      select user_pseudo_id, event_timestamp, params.value.string_value as tikinow
--      from `tikiandroid-1047.data_log.events_new`, unnest(event_params) params 
--      where event_date = '2019-10-01'
--      and event_name = 'pdp_shipping_method'
--      and params.key = 'method'
--      group by 1, 2, 3
-- ) now 
--      on now.user_pseudo_id = a.deviceID and substr(cast(now.event_timestamp as string),1, 10) = substr(cast(a.event_timestamp as string), 1, 10)
    