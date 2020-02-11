#standardSQL
select c.*
        , vendor_id
from consumer_product.cs_user_summary c 
left join 
    (select user_pseudo_id, users.value.string_value vendor_id
        from `tikiandroid-1047.data_log_events_new`, unnest(user_properties) users
        where event_date = '{{macros.localtz.ds(ti)}}'
        and users.key = 'device_id'
        group by 1, 2) a
    on c.deviceID = a.user_pseudo_id
left join `tiki-gap.129159136.ga_sessions_20*` w 
    on c.deviceID = w.fullVisitorId and w._table_suffix = format_date("%y%m%d", '{{macros.localtz.ds(ti)}}')
where 1=1
and min_device_date = '{{macros.localtz.ds(ti)}}'
