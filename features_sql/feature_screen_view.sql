WITH screen_class AS (
     SELECT event_date, event_timestamp, user_pseudo_id, platform, screen_class_current
            , LEAD(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS event_timestamp_after
            , LEAD(screen_class_current,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS screen_class_after
            , CASE WHEN (LEAD(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) - event_timestamp) > (60*1000*1000*60) 
                            THEN 0 ELSE (LEAD(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) - event_timestamp)/(1000*1000*60) 
                            END AS time_after_minute
            , CASE WHEN screen_class_current <> LAG(screen_class_current,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) 
                    OR LAG(screen_class_current,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) IS NULL THEN 1 
                    ELSE 0 
                    END AS is_new_screen
            , CASE WHEN event_timestamp - LAG(event_timestamp,1,0) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) > (60*60*1000*1000) 
                    OR LAG(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) IS NULL THEN 1 
                    ELSE 0 
                    END AS is_new_session 
    FROM ( SELECT event_date, event_timestamp, user_pseudo_id, platform, params.value.string_value screen_class_current 
            FROM `tikiandroid-1047.analytics_153801291.events_*`, UNNEST(event_params) AS params 
            WHERE 1=1 
            AND _TABLE_SUFFIX >= '20191001' --FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY)) 
            AND _TABLE_SUFFIX < '20191101' AND params.key = 'firebase_screen_class' 
            AND platform = 'ANDROID' AND params.value.string_value NOT IN ('SplashActivity')

            UNION ALL 

            SELECT event_date, event_timestamp, user_pseudo_id, platform, params.value.string_value screen_class_current 
            FROM `tikiandroid-1047.analytics_153801291.events_*`, UNNEST(event_params) AS params 
            WHERE 1=1 AND _TABLE_SUFFIX >= '20191001' --FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY)) 
            AND _TABLE_SUFFIX < '20191101' --'20191101' 
            AND params.key = 'page_view' --page_view 
            AND platform = 'IOS' 
            AND params.value.string_value NOT IN ('SplashActivity') ) )
    
    , screen_class_1 AS ( 
        SELECT screen_class.*, SUM(is_new_screen) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS screen_id
                            , SUM(is_new_session) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS session_id 
        FROM screen_class )
    
    , web_view AS ( SELECT date_key ,pagePath ,type ,pagePath_remove_param ,fullVisitorId ,visitId 
                    FROM `tiki-dwh.dwh.web_page_info` 
                    WHERE 1=1 
                    AND date_key >= date(2019,10,01) 
                    AND date_key < date(2019,11,01) ) 

SELECT PARSE_DATE('%Y%m%d', event_date) event_date, user_pseudo_id deviceID
        , COUNT(DISTINCT screen_class_current) no_of_screen
        , COUNT(DISTINCT session_id) no_of_session
        , STRING_AGG(DISTINCT screen_class_current) screen_class 
FROM screen_class_1 sc1 
WHERE 1=1 
GROUP BY 1,2 

UNION ALL 

SELECT date_key, fullVisitorId
        , COUNT(DISTINCT pagePath)
        , COUNT(DISTINCT visitId)
        , STRING_AGG(DISTINCT pagePath_remove_param) 
FROM web_view 
WHERE 1=1 
GROUP BY 1,2	