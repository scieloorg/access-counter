#!/bin/bash
year=$1
month=$2
user=$3
pass=$4

for i in $(seq 1 31); do
    if [ $i -lt 10 ]
    then
        start=$year-$month-0$i
        if [ $((i + 1)) -lt 10 ]
        then
            end=$year-$month-0$((i + 1))
        else
            end=$year-$month-$((i + 1))
        fi
    else
        start=$year-$month-$i
        if [ $i -lt 31 ]
        then
            end=$year-$month-$((i + 1))
        else
            nextmonth=$((month+1))
            if [ $nextmonth -lt 10 ]
            then
                end=$year-0$nextmonth-01
            else
                end=$year-$nextmonth-01
            fi
        fi
    fi
    
    mysql -u $3 -p matomo --password=$4 -e "SELECT matomo_log_visit.idvisit as visitId, server_time as serverTime, config_browser_name as browserName, config_browser_version as browserVersion, inet_ntoa(conv(hex(location_ip), 16, 10)) as ip, conv(hex(matomo_log_visit.idvisitor), 16, 16) as visitorId, matomo_log_action.idaction as actionId, name as actionName from matomo_log_link_visit_action LEFT JOIN matomo_log_visit on matomo_log_visit.idvisit = matomo_log_link_visit_action.idvisit LEFT JOIN matomo_log_action on matomo_log_action.idaction = matomo_log_link_visit_action.idaction_url WHERE server_time >= '$start' AND server_time < '$end';" > "results_"$start".tsv"
done
