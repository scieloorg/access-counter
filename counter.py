from utils import map_helper, pid_tools


# Métricas para Item
METRICS_ITEM = {'total_item_requests': 0,
                'unique_item_requests': 0,
                'total_item_investigations': 0,
                'unique_item_investigations': 0}


class CounterStat:
    """
    Modelo de dados utilizado para representar as métricas COUNTER R5
    """
    def __init__(self):
        self.metrics = {}

    def _get_hits_by_session_and_content_type(self, hits: list):
        """
        Obtém um mapa de sessão -> content_type -> [hits]

        :param hits: lista de hits
        :return: um mapa de sessão para content_type e lista de hits
        """
        session_to_content_type_and_hits = {}

        for hit in hits:
            if hit.session_id not in session_to_content_type_and_hits:
                session_to_content_type_and_hits[hit.session_id] = {}

            if hit.content_type not in session_to_content_type_and_hits[hit.session_id]:
                session_to_content_type_and_hits[hit.session_id][hit.content_type] = []

            session_to_content_type_and_hits[hit.session_id][hit.content_type].append(hit)

        return session_to_content_type_and_hits

    def _get_total(self, hits: list, hit_type, content_type_list):
        """
        Obtém o número total de acessos nos moldes COUNTER R5.

        :param hits: lista de hits
        :param hit_type: tipo de hit a ser considerado (article, issue, journal, platform)
        :content_type_list: lista que contém content_types a serem considerados
        :return: número total e acessos
        """
        return sum([1 for x in hits if x.hit_type == hit_type and x.content_type in content_type_list])

    def _get_unique(self, hits: list, hit_type, content_type_list):
        """
        Obtém o número de acessos únicos nos moldes COUNTER R5.

        :param hits: lista de hits
        :param hit_type: tipo de hit a ser considerado (article, issue, journal, platform)
        :content_type_list: lista que contém content_types a serem considerados
        :return: número de acessos únicos
        """
        unique_requests = 0
        valid_hits = [h for h in hits if h.hit_type == hit_type and h.content_type in content_type_list]

        session_to_content_type_and_hits = self._get_hits_by_session_and_content_type(valid_hits)
        for session, content_type in session_to_content_type_and_hits.items():
            unique_requests += len(content_type)

        return unique_requests

    def _calculate(self, datefied_hits: dict, key, target: dict):
        for ymd in datefied_hits:
            if key not in target:
                target[key] = {ymd: METRICS_ITEM.copy()}

            target[key][ymd]['total_item_requests'] = self._get_total(
                datefied_hits[ymd],
                map_helper.HIT_TYPE_ARTICLE,
                map_helper.COUNTER_ARTICLE_ITEM_REQUESTS)

            target[key][ymd]['total_item_investigations'] = self._get_total(
                datefied_hits[ymd],
                map_helper.HIT_TYPE_ARTICLE,
                map_helper.COUNTER_ARTICLE_ITEM_INVESTIGATIONS)

            target[key][ymd]['unique_item_requests'] = self._get_unique(
                datefied_hits[ymd],
                map_helper.HIT_TYPE_ARTICLE,
                map_helper.COUNTER_ARTICLE_ITEM_REQUESTS)

            target[key][ymd]['unique_item_investigations'] = self._get_unique(
                datefied_hits[ymd],
                map_helper.HIT_TYPE_ARTICLE,
                map_helper.COUNTER_ARTICLE_ITEM_INVESTIGATIONS)

    def calculate_metrics(self, pid_format_lang_localization_to_hits):
        """
        Calcula métricas COUNTER e armazena os resultados no campo self.metrics

        @param pid_format_lang_localization_to_hits: dicionário
            (pid, format, language, localization) --> [hit1, hit2, ...]
        """
        for pfllly, hits in pid_format_lang_localization_to_hits.items():
            pid, data_format, lang, latitude, longitude, yop = pfllly

            if pid_tools.get_pid_type(pid) == map_helper.HIT_TYPE_ARTICLE:
                datefied_hits = self.get_datefied_hits(hits)
                self._calculate(datefied_hits, pfllly, self.metrics)

    def get_datefied_hits(self, hits):
        """
        Obtém os hits organizados por data (ANO -> MESES -> DIAS)

        :param hits: lista de objetos Hit
        :return: dicionário de hits do tipo YYYY -> {M1, M2, ... -> {D1, D2, ... -> [Hit1, Hit2, ...]}}
        """
        date_to_hits = {}

        for hit in hits:
            year = hit.server_time.year
            month = hit.server_time.month
            day = hit.server_time.day

            year_month_day = '-'.join([str(year), str(month).zfill(2), str(day).zfill(2)])

            if year_month_day not in date_to_hits:
                date_to_hits[year_month_day] = []
            date_to_hits[year_month_day].append(hit)

        return date_to_hits
