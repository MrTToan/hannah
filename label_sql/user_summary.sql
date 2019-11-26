CREATE OR REPLACE TABLE consumer_product.up_user_summary AS (
SELECT *
FROM
(
SELECT T1.deviceID
,no_of_screen
,no_of_session
,T2.android_ATC+t2.ios_ATC+t2.mobile_ATC+t2.desktop_ATC sum_atc
,transportation_cate
,beauty_health_cate	
,home_living_cate	
,accessories_cate	
,voucher_cate	
,electronic_cate	
,book_cate	
,phone_tablet_cate	
,service_cate	
,lifestyle_cate	
,phieu_dat_coc_cate	
,digital_device_cate	
,sport_cate	
,cross_border_cate	
,laptop_cate	
,camera_cate	
,mom_baby_cate	
,recycle_bin_cate	
,electric_appliances_cate	
,promotion_cate
#sum all cate
,transportation_cate
+beauty_health_cate	
+home_living_cate	
+accessories_cate	
+voucher_cate	
+electronic_cate	
+book_cate	
+phone_tablet_cate	
+service_cate	
+lifestyle_cate	
+phieu_dat_coc_cate	
+digital_device_cate	
+sport_cate	
+cross_border_cate	
+laptop_cate	
+camera_cate	
+mom_baby_cate	
+recycle_bin_cate	
+electric_appliances_cate	
+promotion_cate
sum_cate
,reward	
,fixed_price
,by_percent	
,freegift	
,cart_fixed
,_urban urban_area
,android_search+ios_search+mobile_search+desktop_search sum_search
,_check check_s
,seller
,price_100000_2000000
,price_0_50000
,price_200000_500000	
,price_50000_100000	
,price_500000_2000000	
,price_2000000_5000000	
,price_bigger_5000000	
,android_tknow+ios_tknow+mobile_tknow+desktop_tknow sum_tikinow
,avg_review_verified
,rating
,type_
-- ,user_label
,CASE WHEN date_diff_login >= 7 THEN 1 ELSE 0 END user_label
FROM consumer_product.up_user_label_v3 t1
LEFT JOIN consumer_product.feature_add_to_cart t2 ON T1.deviceID = T2.deviceID AND T1.min_device_date = t2.date
LEFT JOIN consumer_product.feature_cate_view t3 ON T1.deviceID = T3.deviceID AND T1.min_device_date = t3.date
LEFT JOIN consumer_product.feature_coupon t4 ON T1.deviceID = T4.deviceID AND T1.min_device_date = t4.date
LEFT JOIN consumer_product.feature_location t5 ON T1.deviceID = T5.deviceID AND T1.min_device_date = t5.event_date
LEFT JOIN consumer_product.feature_search_submission t6 ON T1.deviceID = T6.deviceID AND T1.min_device_date = t6.date
LEFT JOIN consumer_product.feature_seller t7 ON T1.deviceID = T7.deviceID AND T1.min_device_date = t7.date
LEFT JOIN consumer_product.feature_tier_price t8 ON T1.deviceID = T8.deviceID AND T1.min_device_date = t8.date
LEFT JOIN consumer_product.feature_tikinow t9 ON T1.deviceID = T9.deviceID AND T1.min_device_date = t9.date
LEFT JOIN consumer_product.feature_review_and_ratings t10 ON T1.deviceID = T10.deviceID AND T1.min_device_date = t10.date
LEFT JOIN consumer_product.feature_screen_view t11 ON T1.deviceID = t11.deviceID AND T1.min_device_date = t11.event_date
WHERE 1=1
AND (date_diff_open_buy > 0 OR date_diff_open_buy IS NULL)
AND (min_access_date = min_device_date)
AND min_access_date >= date(2019,10,01)
AND min_access_date < date(2019,11,01)
)
WHERE 1=1
AND sum_search > 0
)
