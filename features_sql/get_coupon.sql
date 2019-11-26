#standardSQL
CREATE OR REPLACE TABLE `tiki-dwh.consumer_product.feature_coupon` as 
with coupon_raw as (
select  event_date as date
                , event_timestamp
                , user_pseudo_id as deviceID
                , device.operating_system as platform
                , event_name as metrics
                , (select params.value.string_value from unnest(event_params) params where params.key = 'coupon') as coupon_code
from `tikiandroid-1047.data_log.events_new`, unnest(event_params) params 
where 1=1
and event_date between '2019-10-01' and '2019-10-31'
and event_name = 'coupon_delivery'
group by 1,2,3,4,5,6
),


coupon as (

    select c.*, r.simple_action as coupon_type
    from coupon_raw c 
    left join `tiki-dwh.ecom.promo_salesrule_coupon_20*` sc on c.coupon_code = sc.code
    left join `tiki-dwh.ecom.promo_salesrule` r on sc.rule_id = r.id 
    where sc._table_suffix >= '190*'
)

select date, deviceID
                    , countif(coupon_type = 'reward') as reward
                    , countif(coupon_type = 'fixed_price') as fixed_price
                    , countif(coupon_type = 'by_percent') as by_percent
                    , countif(coupon_type = 'freegift') as freegift
                    , countif(coupon_type = 'cart_fixed') as cart_fixed
from coupon
group by 1, 2