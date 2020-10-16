#!/bin/bash
year=$1
month=$2
user=$3
pass=$4
idsite=$5

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
    echo "Extraindo dados de $start..."
    mysql -u $user -p matomo --password=$pass -e "SELECT server_time as serverTime, config_browser_name as browserName, config_browser_version as browserVersion, inet_ntoa(conv(hex(location_ip), 16, 10)) as ip, name as actionName from matomo_log_link_visit_action LEFT JOIN matomo_log_visit on matomo_log_visit.idvisit = matomo_log_link_visit_action.idvisit LEFT JOIN matomo_log_action on matomo_log_action.idaction = matomo_log_link_visit_action.idaction_url WHERE matomo_log_visit.idsite = $idsite AND server_time >= '$start' AND server_time < '$end' order by ip;" > "results_$start.tsv"
done
