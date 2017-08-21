!# /bin/bash

DATE=$(date -d "1 day ago" +"%Y-%m-%d")

hive --databases ads -e"
set mapreduced.job.quenename=ads;
select

request.properties['account_id'] 
as accountid,

(case
	when request.properties['product_id'] is = '0' or request.properties['product_id'] is = '-1' then  '0'
	else request.properties['product_id'] 
end)
as productid,

request.properties['plan_id'] 
as planid,

request.properties['idea_id'] 
as idad_id,

request.properties['material_id'] 
as materialid,

(from_unixtime(cast(properties['charge_timestamp'] / 1000 as bigint), 'yyyy-MM-dd'))
as date,

(from_unixtime(cast(properties['charge_timestamp'] / 1000 as bigint), 'H'))
as hour,

(case
	when request.platform = 'IOS' then 0
	when request.platform = 'Android' then 1
	else 2
end) 
as platform,

(case 
	when request.app_id = 'wifi_key' then 'cf_dsp_wifi_key' 
	else request.app_id
end)
as app,

request.properties['template'] 
as templateid,

request.properties['tp'] 
as showtype,


(case
	when request.properties['event'] = 'view' then 1
	else 0
end)
as view,

(case
	when request.properties['event'] = 'click' then 1
	else 0
end)
as click,

(case
	when request.properties['event'] = 'app_start_download' then 1
	else 0
end)
as download_start,

(case
	when request.properties['event'] = 'app_download_success' then 1
	else 0
end)
as download_success,

(case
	when request.properties['event'] = 'video_start' then 1
	else 0
end)
as video_start,

(case
	when request.properties['event'] = 'video_finish' then 1
	else 0
end)
as video_finish,

(case
	when properties["is_charge"] = 1
	then request['properties'].cost
	else 0
end)
as cost,

(case
	when properties["is_charge"] = 1
	then request['properties'].cost_b
	else 0
end)
as cost_balance,

(case
	when properties["is_charge"] = 1
	then request['properties'].cost_r
	else 0
end)
as cost_rebate,

(case
	when properties["is_charge"] = 1
	then request['properties'].cost_i
	else 0
end)
as cost_invest,

from
(select * from
ads.ods_ads_lingxi_billing_backup
where
properties['is_spam'] = 0 and
properties['is_real_time'] = 1 and
p_day = '$DATE'

group by
request.properties['account_id'] ,
(case
	when request.properties['product_id'] is = '0' or request.properties['product_id'] is = '-1' then  '0'
	else request.properties['product_id'] 
end),
request.properties['plan_id'],
request.properties['idea_id'],
request.properties['material_id'],
(from_unixtime(cast(properties['charge_timestamp'] as bigint), 'yyyy-MM-dd')),
(from_unixtime(cast(properties['charge_timestamp'] as bigint), 'H')),
(case
	when request.platform = 'IOS' then 0
	when request.platform = 'Android' then 1
	else 2
end),
(case 
	when request.app_id = 'wifi_key' then 'cf_dsp_wifi_key' 
	else request.app_id
end),
request.properties['template'],
request.properties['tp']
"| grep -v WARN > /data/home/zhaoyang5/zhaoyang5/file/ads.ods_ads_lingxi_backup/idea_report_all_0820.txt