#!/bin/bash
start_date=$1
end_date=$2
user=$3
pass=$4

if [[ $start_date > $end_date ]]
then
    echo "Período inválido. Data final ($end_date) é menor que data inicial ($start_date)"
    exit
fi

next_date="$start_date"
until [[ $next_date > $end_date ]]; do
    current_date="$next_date"
    next_date=$(date -I -d "$next_date + 1 day")

    timer_start=`date +%s`

    echo "Extraindo dados de [$current_date, $next_date["
    mysql -u $user -p matomo --password=$pass -e "SELECT server_time as serverTime, config_browser_name as browserName, config_browser_version as browserVersion, inet_ntoa(conv(hex(location_ip), 16, 10)) as ip, location_latitude as latitude, location_longitude as longitude, name as actionName from matomo_log_link_visit_action LEFT JOIN matomo_log_visit on matomo_log_visit.idvisit = matomo_log_link_visit_action.idvisit LEFT JOIN matomo_log_action on matomo_log_action.idaction = matomo_log_link_visit_action.idaction_url WHERE server_time >= '$current_date' AND server_time < '$next_date' ORDER BY location_ip;" > "$current_date.tsv"

    timer_end=`date +%s`
    runtime=$((timer_end-timer_start))
    echo "Concluído em $runtime segundos"
    echo ""
done
