CREATE OR REPLACE TABLE consumer_product.feature_tier_price_1511 AS
select date, deviceID
                ,countif(tier_price is null) as null_price 
                ,countif(tier_price = '100000 - 200000') as price_100000_2000000
                ,countif(tier_price = '0 - 50000') as price_0_50000
                ,countif(tier_price = '200000 - 500000') as price_200000_500000
                ,countif(tier_price = '50000 - 100000') as price_50000_100000
                ,countif(tier_price = '500000 - 2000000') as price_500000_2000000
                ,countif(tier_price = '2000000 - 5000000') as price_2000000_5000000
                ,countif(tier_price = '>5000000') as price_bigger_5000000
from `consumer_product.feature_pdp_1511` 
group by 1,2 