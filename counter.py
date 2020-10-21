from utils import map_helper, pid_tools


# Métricas para Item
METRICS_ITEM = {'total_item_requests': 0,
                'unique_item_requests': 0,
                'total_item_investigations': 0,
                'unique_item_investigations': 0}

# Métricas para Title (o title de um artigo é o periódico em que foi publicado)
METRICS_TITLE = {'total_title_requests': 0,
                 'unique_title_requests': 0,
                 'total_title_investigations': 0,
                 'unique_title_investigations': 0}


class CounterStat:
    """
    Modelo de dados utilizado para representar as métricas COUNTER R5
    """
    def __init__(self):
        self.journals_metrics = {}
        self.issues_metrics = {}
        self.articles_metrics = {}
        self.platform_metrics = {}

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

    def populate_counter(self, pid_to_hits):
        """
        Povoa self.articles, self.journals, self.platform no formato COUNTER R5

        :param pid_to_hits: dicionário que contém PIDs associados a seus respectivos objetos Hit
        """
        pid_article_to_hits = {}
        pid_journal_to_hits = {}

        pid_articles = [pid for pid in pid_to_hits if pid_tools.get_pid_type(pid) == map_helper.HIT_TYPE_ARTICLE]
        for pid in pid_articles:
            if pid not in pid_article_to_hits:
                pid_article_to_hits[pid] = []
            pid_article_to_hits[pid] = pid_to_hits[pid]

        pid_journals = [pid for pid in pid_to_hits if pid_tools.get_pid_type(pid) == map_helper.HIT_TYPE_JOURNAL]
        for pid in pid_journals:
            if pid not in pid_journal_to_hits:
                pid_journal_to_hits[pid] = []
            pid_journal_to_hits[pid] = pid_to_hits[pid]

        self._populate_articles(pid_article_to_hits)
        self._populate_journals(pid_journal_to_hits)

    def _populate_articles(self, pid_to_hits: dict):
        """
        Povoa self.articles com os acessos no formato COUNTER R5

        :param pid_to_hits: dicionário que contém PIDs associados a seus respectivos objetos Hit
        """
        # TODO: Contabilizar idioma de acesso (padrão, pdf e html)
        for pid, hits in pid_to_hits.items():
            datefied_hits = self.get_datefied_hits(hits)
            for ymd in datefied_hits:
                if pid not in self.articles_metrics:
                    self.articles_metrics[pid] = {ymd: METRICS_ITEM.copy()}
                    self.articles_metrics[pid][ymd]['total_item_requests'] = self._get_total(
                        datefied_hits[ymd],
                        map_helper.HIT_TYPE_ARTICLE,
                        map_helper.COUNTER_ARTICLE_ITEM_REQUESTS)

                    self.articles_metrics[pid][ymd]['total_item_investigations'] = self._get_total(
                        datefied_hits[ymd],
                        map_helper.HIT_TYPE_ARTICLE,
                        map_helper.COUNTER_ARTICLE_ITEM_INVESTIGATIONS)

                    self.articles_metrics[pid][ymd]['unique_item_requests'] = self._get_unique(
                        datefied_hits[ymd],
                        map_helper.HIT_TYPE_ARTICLE,
                        map_helper.COUNTER_ARTICLE_ITEM_REQUESTS)

                    self.articles_metrics[pid][ymd]['unique_item_investigations'] = self._get_unique(
                        datefied_hits[ymd],
                        map_helper.HIT_TYPE_ARTICLE,
                        map_helper.COUNTER_ARTICLE_ITEM_INVESTIGATIONS)

    def _populate_issues(self, pid_to_hits: dict):
        """
        Povoa self.issues com os acessos no formato COUNTER R5

        :param pid_to_hits: dicionário que contém PIDs associados a seus respectivos hits
        """
        pass

    def _populate_journals(self, pid_to_hits: dict):
        """
        Povoa self.journals com os acessos no formato COUNTER R5

        :param pid_to_hits: dicionário que contém PIDs associados a seus respectivos objetos Hit
        """
        for pid, hits in pid_to_hits.items():
            datefied_hits = self.get_datefied_hits(hits)
            for ymd in datefied_hits:
                if pid not in self.journals_metrics:
                    self.journals_metrics[pid] = {ymd: METRICS_ITEM.copy()}

                # TODO: Adequar maps para URLs de JOURNALS
                #   Contabilizar, também, acesso às páginas de ISSUE e de ARTICLE para cada ISSN
                #   Contabilizar que um periódico está associado a mais de um ISSN
                self.journals_metrics[pid][ymd]['total_item_requests'] = self._get_total(
                    datefied_hits[ymd],
                    map_helper.HIT_TYPE_JOURNAL,
                    map_helper.COUNTER_JOURNAL_ITEM_INVESTIGATIONS)

                self.journals_metrics[pid][ymd]['total_item_investigations'] = self._get_total(
                    datefied_hits[ymd],
                    map_helper.HIT_TYPE_JOURNAL,
                    map_helper.COUNTER_JOURNAL_ITEM_INVESTIGATIONS)

                self.journals_metrics[pid][ymd]['unique_item_requests'] = self._get_unique(
                    datefied_hits[ymd],
                    map_helper.HIT_TYPE_JOURNAL,
                    map_helper.COUNTER_JOURNAL_ITEM_REQUESTS)

                self.journals_metrics[pid][ymd]['unique_item_investigations'] = self._get_unique(
                    datefied_hits[ymd],
                    map_helper.HIT_TYPE_JOURNAL,
                    map_helper.COUNTER_JOURNAL_ITEM_REQUESTS)

    def _populate_platform(self, pid_to_hits: dict):
        """
        Povoa self.platform com os acessos no formato COUNTER R5

        :param pid_to_hits: dicionário que contém PIDs associados a seus respectivos hits
        """
        pass

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
