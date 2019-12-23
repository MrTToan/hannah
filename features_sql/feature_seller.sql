CREATE OR REPLACE TABLE consumer_product.feature_seller_1511 as
with temp as (
select date, event_timestamp, deviceID, seller_name,
            case when seller_name = 'Tiki Trading' then 1
                    else 0 end as seller_check
from consumer_product.feature_pdp_1511 f
join `dwh.dim_product_full` t on f.spid = cast(t.product_key as string) and f.platform in ('ANDROID', 'IOS')

union all 

select date, event_timestamp, deviceID, seller_name,
                               case when seller_name = 'Tiki Trading' then 1
                                        else 0 end as seller_check
from consumer_product.feature_pdp_1511 f 
join dwh.dim_product_full t on f.spid = t.sku and f.platform in ('desktop', 'mobile')
)

select date, deviceID, sum(seller_check) as _check, 
                    case when sum(seller_check) > 0 then 1
                    else 0 end as seller
                    from temp 
                    group by 1, 2