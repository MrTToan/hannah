CREATE OR REPLACE TABLE `consumer_product.cr_user_platform` AS 
WITH app_users_0 AS
  (
  SELECT
  device_category_detail
  ,device_category
  ,user_id
  ,user_id_track
  ,user_pseudo_id
  ,event_date
  FROM
        (
        SELECT
        device_category_detail
        ,device_category
        ,user_id
        ,user_id_track
        ,user_pseudo_id
        ,event_date
        ,ROW_NUMBER() OVER (PARTITION BY USER_PSEUDO_ID, USER_ID_TRACK ORDER BY event_date ASC) ROWNUM
        FROM (
               SELECT
                CONCAT('app', ' - ', LOWER(platform)) AS device_category_detail,
                CONCAT('app') AS device_category,
                LOWER(CASE
                          WHEN IFNULL(user_id, '0') <> '0' THEN user_id
                          ELSE user_pseudo_id
                      END) AS user_id,
                CASE WHEN IFNULL(user_id, '0') <> '0' THEN user_id END user_id_track,
                LOWER(user_pseudo_id) user_pseudo_id,
                PARSE_DATE("%Y%m%d", event_date) event_date
         FROM `tikiandroid-1047.analytics_153801291.events_*`
         WHERE 1=1
          AND _TABLE_SUFFIX >= '20190101'
               )
          )
          WHERE ROWNUM = 1
  ),
  
  app_users_1 AS
  (
          SELECT 
              user_pseudo_id, 
              COUNT(1) SL,
              min(event_date) event_date 
          FROM app_users_0
          WHERE 1=1
          GROUP BY 1
  ),
  
  app_users AS
  (
          SELECT 
              T1.device_category_detail
              ,T1.device_category
              ,T1.user_id
              ,T1.user_id_track
              ,T1.user_pseudo_id
              ,T2.event_date 
              ,T2.SL
          FROM app_users_0 T1
          LEFT JOIN app_users_1 T2 ON T1.user_pseudo_id = t2.user_pseudo_id
          WHERE 1=1
          AND (T2.SL = 1 
          OR T1.user_id_track IS NOT NULL)
          AND T2.SL <= 3
  ),  
  
  web_users_0 AS
  (
  SELECT
      device_category
      ,user_id
      ,customDimension1
      ,fullVisitorId
      ,event_date
  FROM
  (
  SELECT 
      device_category
      ,user_id
      ,customDimension1
      ,fullVisitorId
      ,event_date
      ,ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY event_date ASC) ROWNUM1
  FROM
  (SELECT
  device_category
  ,user_id
  ,customDimension1
  ,fullVisitorId
  ,event_date
  ,ROW_NUMBER() OVER (PARTITION BY fullVisitorId, customDimension1 ORDER BY event_date ASC) ROWNUM
   FROM
     (SELECT CASE
                 WHEN dataSource = 'app' THEN 'app'
                 WHEN deviceCategory IN ('mobile',
                                         'tablet') THEN 'mobile'
                 WHEN deviceCategory = 'desktop' THEN 'desktop'
             END AS device_category,
             LOWER(CASE
                       WHEN IFNULL(customDimension1, '0') <> '0' THEN customDimension1
                       ELSE fullVisitorId
                   END) AS user_id,
             CASE
                       WHEN IFNULL(customDimension1, '0') <> '0' THEN customDimension1 END customDimension1,
             fullVisitorId,
             event_date
      FROM
        (
          SELECT fullVisitorId,
                device.deviceCategory AS deviceCategory,
                hits.dataSource AS dataSource,
           (SELECT MAX(IF(INDEX=1, value, NULL))
            FROM UNNEST(hits.customDimensions)) AS customDimension1,
           PARSE_DATE("%Y%m%d", CAST(date AS STRING)) event_date    
         FROM `tiki-gap.129159136.ga_sessions_*`,
              UNNEST(hits) AS hits
         WHERE 1=1
             AND _TABLE_SUFFIX >= '20190101'
--           AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) 
--           AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) 
         ))
   WHERE 1=1
     AND device_category <> 'app' )
   WHERE ROWNUM = 1
   )
   WHERE 1=1
   ),
   
   web_users_1 AS
  (
          SELECT 
              fullVisitorId, 
              COUNT(1) SL,
              min(event_date) event_date 
          FROM web_users_0
          WHERE 1=1

GROUP BY 1
  ),
  
  web_users AS
  (
          SELECT 
              T1.device_category
              ,T1.user_id
              ,T1.customDimension1
              ,T1.fullVisitorId
              ,T2.event_date 
              ,T2.SL
          FROM web_users_0 T1
          LEFT JOIN web_users_1 T2 ON T1.fullVisitorId = t2.fullVisitorId
          WHERE 1=1
          AND (T2.SL = 1 
          OR T1.customDimension1 IS NOT NULL)
          AND T2.SL <= 3
  ),
    
    union_users AS
  (SELECT device_category,
          user_id,
          event_date
   FROM
     (SELECT device_category,
             user_id,
             event_date
      FROM web_users)
   UNION ALL
     (SELECT device_category,
             user_id,
             event_date
      FROM app_users)),

     dedupe_duplicate_devices AS
  (SELECT user_id,
          device_category,
          min(event_date) event_date
   FROM union_users
   GROUP BY user_id,
            device_category          
   ),
            
     concat_device_category AS
  (SELECT user_id,
          STRING_AGG(device_category, ', ' ORDER BY event_date ASC) platform_traffic,
          min(event_date) min_event_date
   FROM
     (SELECT *
      FROM dedupe_duplicate_devices)
   GROUP BY user_id)
   
    SELECT 
    T1.user_id,
    T1.platform_traffic,
    T1.min_event_date,
    T2.user_pseudo_id,
    T2.event_date event_date_app,
    T3.fullVisitorId,
    T3.event_date event_date_web
    FROM concat_device_category T1
    LEFT JOIN app_users T2 ON T1.user_id = t2.user_id
    LEFT JOIN web_users T3 ON T1.user_id = t3.user_id