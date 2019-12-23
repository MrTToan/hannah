CREATE OR REPLACE TABLE consumer_product.feature_review_and_ratings AS
with review_and_ratings as (
select f.*
            , ifnull(p.number_of_review,0) as number_of_review
            , ifnull(p.number_review_verified,0) as number_review_verified
            , ifnull(p.number_review_helpful,0) as number_review_helpful
            , ifnull(p.rating,0) as rating
            , ifnull(p.rating_score_group,0) as rating_score_group
from `consumer_product.feature_pdp_1511` f
join `dwh.dim_product_full` t on f.spid = cast(t.product_key as string) and f.platform in ('ANDROID', 'IOS')
join `druid.druid_programs_review_products_20191215` p 
on t.pmaster_id = p.pmaster_id

union all 

select f.*
            , ifnull(p.number_of_review,0)
            , ifnull(p.number_review_verified,0)
            , ifnull(p.number_review_helpful,0)
            , ifnull(p.rating,0)
            , ifnull(p.rating_score_group,0)
from `consumer_product.feature_pdp_1511` f
join `dwh.dim_product_full` t on f.spid = cast(t.sku as string) and f.platform in ('mobile', 'desktop')
join `druid.druid_programs_review_products_20191215` p 
on t.pmaster_id = p.pmaster_id
)

select date, deviceID, avg(number_review_verified) as avg_review_verified
                    , avg(rating) as rating 
from review_and_ratings
group by 1, 2