USE GEO_TAGGED_MEASUREMENTS;
LOAD DATA LOCAL INPATH 'geo_call_data_nohead.csv' OVERWRITE INTO TABLE GEO_TAGGED_MEASUREMENTS_HIVE_MYSQL;
INSERT OVERWRITE TABLE GEO_TAGGED_MEASUREMENTS_HIVE_INTMEDTABLE select t.end_cell_id, count(*),t.end_cell_id,floor(t.end_location_lat/1.61877889) as e_lat,
floor(t.end_location_lon/(1.61877889/cos(t.end_location_lat))) as e_lon,concat_ws('-',substr(hex(t.end_cell_id),0,5),substr(hex(t.end_cell_id),6,7)) AS global_cell_id,
10*log10(avg(pow(10, (t.rsrp * 1.0)/10))),10*log10(avg(pow(10, (t.rsrq * 1.0)/10))),
from_unixtime(unix_timestamp(), 'yyyy-MM-dd') from GEO_TAGGED_MEASUREMENTS_HIVE_MYSQL t 
where t.rsrp is not null and t.rsrq is not null group by t.end_cell_id, t.end_location_lat, t.end_location_lon,t.rsrp, t.rsrq;
