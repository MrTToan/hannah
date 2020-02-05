#standardSQL
merge `tiki-dwh.consumer_product.cs_user_info_access` a 
using (
    select user_pseudo_id deviceID
            , user_id 
            , min(DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Bangkok')) min_device_date
            , max(DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Bangkok')) max_device_date 
    from `tikiandroid-1047.data_log.events_new`
    where event_date = '{{macros.localtz.ds(ti)}}'
    group by 1, 2

    union all

    select fullVisitorId
            , customDimension1
            , DATETIME(min(start_time),'Asia/Bangkok')
            , DATETIME(max(start_time),'Asia/Bangkok')
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
             AND _TABLE_SUFFIX = format_date("%y%m%d", '{{macros.localtz.ds_nodash(ti)}}')
)       WHERE 1=1
        AND dataSource <> 'app'
        GROUP BY 1,2
) r
on r.deviceID = a.deviceID
when not matched by target then 
    insert row
when matched then 
    update set max_device_date = r.max_device_date




