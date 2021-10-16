#!/bin/bash
MATOMO_DATABASE_USER=$1
MATOMO_DATABASE_PASS=$2
MATOMO_SITE_ID=$3
MATOMO_DATE_START=$4
MATOMO_DATE_END=$5

mysql -u $MATOMO_DATABASE_USER -p matomo --password=$MATOMO_DATABASE_PASS -e "SELECT server_time AS serverTime, config_browser_name AS browserName, config_browser_version AS browserVersion, inet_ntoa(conv(hex(location_ip), 16, 10)) AS ip, location_latitude AS latitude, location_longitude AS longitude, name AS actionName FROM matomo_log_link_visit_action LEFT JOIN matomo_log_visit ON matomo_log_visit.idvisit = matomo_log_link_visit_action.idvisit LEFT JOIN matomo_log_action ON matomo_log_action.idaction = matomo_log_link_visit_action.idaction_url WHERE matomo_log_link_visit_action.idsite = '$MATOMO_SITE_ID' AND server_time >= "$MATOMO_DATE_START" AND server_time < "$MATOMO_DATE_END" ORDER BY ip;" > "$MATOMO_DATE_START.tsv"
