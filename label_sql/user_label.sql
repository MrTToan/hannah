#up_user_label
CREATE OR REPLACE TABLE consumer_product.up_user_label_v3 AS
(
WITH purchase_summary AS 
   (
    SELECT
      id,
      SUM(value) total_order_value,
      COUNT(DISTINCT cate1) number_of_cate1,
      COUNT(DISTINCT cate2) number_of_cate2,
      COUNT(DISTINCT sub_cate_report) number_of_sub_cate,
      COUNT(DISTINCT platform_group) number_of_platform,
      MIN(DATE(date_at)) min_buy_date,
      MAX(DATE(date_at)) max_buy_date,
      DATE_DIFF(MAX(DATE(date_at)), MIN(DATE(date_at)), DAY) day_diff_buy,
      COUNT(DISTINCT product_key) number_of_product,
      COUNT(DISTINCT original_code) number_of_order,
      STRING_AGG(DISTINCT platform_group, ', ') platform_buy
    FROM
    (
        SELECT
          id,
          original_code,
          product_key,
          discount,
          value,
          date_at,
          cate1,
          cate2,
          sub_cate_report,
          productset_name,
          product_name,
          platform_group,
          full_name
        FROM
         (
             SELECT 
               t4.id, 
               t1.original_code,
               COALESCE(T3.pmaster_id, T3.psuper_id, T1.product_key) product_key,
               discount,
               value+handling_fee+shipping_value-discount-shipping_discount_value value,
               date_at,
               T3.cate1,
               T3.cate2,
               T3.sub_cate_report,
               T3.productset_name,
               T3.product_name,
               T2.platform_group,
               T4.full_name,
               T1.order_type,
               ROW_NUMBER() OVER (PARTITION BY T1.order_code, t1.product_key ORDER BY date_at DESC) ROWNUM
             FROM `tiki-dwh.dwh.fact_sales_order_nmv` T1
               LEFT JOIN `tiki-dwh.dwh.dim_platform` T2 ON T1.platform_key = T2.platform_key
               LEFT JOIN `tiki-dwh.dwh.dim_product_full` T3 ON T1.product_key = T3.product_key
               LEFT JOIN `ecom.customer` T4 ON T1.customer_key = T4.backend_id
             WHERE 1=1
             AND date(T1.date_at) <= date(2019,11,15)
         )
        WHERE 1=1
        AND ORDER_TYPE = 1
        AND ROWNUM = 1
        )
        GROUP BY 1
    ),
    
    users AS
         (SELECT 
            id,
            total_order_value monetary,
            number_of_cate1,
            number_of_cate2,
            number_of_sub_cate,
            number_of_platform,
            platform_buy,
            min_buy_date,
            max_buy_date,
            day_diff_buy,
            number_of_product,
            number_of_order,
            number_of_order frequency,
            SUM(total_order_value) OVER (PARTITION BY 1 ORDER BY total_order_value DESC) running_sum,
            SUM(total_order_value) over () as total,
            COUNT(CASE WHEN min_buy_date IS NOT NULL THEN id END) OVER () AS total_user,
            ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY total_order_value DESC) ranking_order
          FROM purchase_summary
            WHERE 1=1
              AND id IS NOT NULL
          ),
          
    user_info AS
         (
         SELECT 
          user_pseudo_id deviceID, 
          user_id, 
          min(DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Bangkok')) min_device_date,
          max(DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Bangkok')) max_device_date 
        FROM `tikiandroid-1047.analytics_153801291.events_20*`
         WHERE 1=1
          AND _TABLE_SUFFIX >= '190101'
        GROUP BY 1,2
        
        UNION ALL
        
        SELECT
        fullVisitorId
        ,customDimension1
        ,DATETIME(min(start_time),'Asia/Bangkok')
        ,DATETIME(max(start_time),'Asia/Bangkok')
        FROM
        (
        SELECT fullVisitorId,
                device.deviceCategory AS deviceCategory,
                hits.dataSource AS dataSource,
           (SELECT MAX(IF(INDEX=1, value, NULL))
            FROM UNNEST(hits.customDimensions)) AS customDimension1,
           TIMESTAMP_MILLIS(visitStartTime*1000 + hits.time) AS start_time 
         FROM `tiki-gap.129159136.ga_sessions_20*`,
              UNNEST(hits) AS hits
         WHERE 1=1
             AND _TABLE_SUFFIX >= '190101'
        )
        WHERE 1=1
        AND dataSource <> 'app'
        GROUP BY 1,2
        )
        ,
     
     user_first_access AS
         (
         SELECT 
          user_id
          ,min_buy_date
          ,min(min_device_date) min_access_date
          ,max(max_device_date) max_access_date
        FROM user_info T1
          LEFT JOIN users T2 ON T1.user_id = CAST(T2.id AS STRING)
        WHERE 1=1
          AND user_id IS NOT NULL
        GROUP BY 1,2
        ),
        
      user_tam AS
        (
      SELECT 
        deviceID
        ,T1.user_id
        ,CASE WHEN min_access_date < min_device_date THEN min_access_date ELSE min_device_date END min_access_date
        ,min_device_date
        ,CASE WHEN max_access_date > max_device_date THEN max_access_date ELSE max_device_date END max_access_date
        ,max_device_date
        ,min_buy_date
      FROM user_info T1
      LEFT JOIN user_first_access T2 ON T1.user_id = T2.user_id
        )
      
      SELECT 
      deviceID
--       , min(min_device_date) min_device_time
--       , min(min_access_date) min_access_time
      , DATE(min(min_device_date)) min_device_date
      , DATE(min(min_access_date)) min_access_date
      , DATE(max(max_access_date)) max_access_date
      , min(min_buy_date) min_buy_date
      , case when min(min_access_date) < min(min_device_date) THEN 'access_before'
             when min(min_access_date) >= min(min_device_date) THEN 'first_time' ELSE null END type_
      , DATE_DIFF(DATE(min(min_device_date)), DATE(min(min_access_date)), DAY) date_diff_access
      , DATE_DIFF(MIN(min_buy_date), DATE(min(min_device_date)), DAY) date_diff_open_buy
      , DATE_DIFF(DATE(Max(max_access_date)), DATE(min(min_device_date)), DAY) date_diff_login
      , CASE WHEN DATE_DIFF(min(min_buy_date), DATE(min(min_device_date)), DAY) <= 14 THEN 1 ELSE 0 END user_label
      , CASE WHEN DATE_DIFF(min(min_buy_date), DATE(min(min_device_date)), DAY) <= 7 THEN 1 ELSE 0 END user_label_7
      FROM user_tam
      GROUP BY 1 
--       HAVING date_diff_access >=0 OR date_diff_access IS NULL
)         
          
