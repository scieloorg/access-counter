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
        self.journals = {}
        self.issues = {}
        self.articles = {}
        self.platform = {}

    def _get_hits_by_session_and_content_type(self, hits: list):
        """
        Obtém um mapa de sessão -> ``content_type -> [hits]``

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
        Povoa ``self.articles``, ``self.journals``, ``self.platform`` no formato COUNTER R5

        :param pid_to_hits: dicionário que contém PIDs associados a seus respectivos objetos Hit
        """
        pid_article_to_hits = {}
        pid_journal_to_hits = {}
        pid_issue_to_hits = {}
        platform_to_hits = {}

        pid_articles = [pid for pid in pid_to_hits if pid_tools.get_pid_type(pid) == map_helper.HIT_TYPE_ARTICLE]
        for pid in pid_articles:
            if pid not in pid_article_to_hits:
                pid_article_to_hits[pid] = []
            pid_article_to_hits[pid] = pid_to_hits[pid]

        for pid, hits in pid_to_hits.items():
            pid_type = pid_tools.get_pid_type(pid)

            if pid_type == map_helper.HIT_TYPE_ARTICLE:
                if pid not in pid_article_to_hits:
                    pid_article_to_hits[pid] = []
                pid_article_to_hits[pid].extend(hits)
            elif pid_type == map_helper.HIT_TYPE_JOURNAL:
                if pid not in pid_journal_to_hits:
                    pid_journal_to_hits[pid] = []
                pid_journal_to_hits[pid].extend(hits)
            elif pid_type == map_helper.HIT_TYPE_ISSUE:
                if pid not in pid_issue_to_hits:
                    pid_issue_to_hits[pid] = []
                pid_issue_to_hits[pid].extend(hits)
            else:
                if pid not in platform_to_hits:
                    platform_to_hits[pid] = []
                platform_to_hits[pid].extend(hits)

        self._populate_articles(pid_article_to_hits)
        # self._populate_issues(pid_issue_to_hits)
        # self._populate_journals(pid_journal_to_hits)
        # self._populate_platform(platform_to_hits)

    def _populate_articles(self, pid_to_hits: dict):
        """
        Povoa ``self.articles`` com os acessos no formato COUNTER R5

        :param pid_to_hits: dicionário que contém PIDs associados a seus respectivos objetos Hit
        """
        for pid, hits in pid_to_hits.items():
            datefied_hits = self.get_datefied_hits(hits)
            for year in datefied_hits:
                for month in datefied_hits[year]:
                    for day in datefied_hits[year][month]:

                        if pid not in self.articles:
                            self.articles[pid] = {year: {}}

                        if month not in self.articles[pid][year]:
                            self.articles[pid][year][month] = {}

                        if day not in self.articles[pid][year][month]:
                            self.articles[pid][year][month][day] = METRICS_ITEM.copy()

                        self.articles[pid][year][month][day]['total_item_requests'] = self._get_total(
                            datefied_hits[year][month][day],
                            map_helper.HIT_TYPE_ARTICLE,
                            map_helper.COUNTER_ARTICLE_ITEM_REQUESTS)

                        self.articles[pid][year][month][day]['total_item_investigations'] = self._get_total(
                            datefied_hits[year][month][day],
                            map_helper.HIT_TYPE_ARTICLE,
                            map_helper.COUNTER_ARTICLE_ITEM_INVESTIGATIONS)

                        self.articles[pid][year][month][day]['unique_item_requests'] = self._get_unique(
                            datefied_hits[year][month][day],
                            map_helper.HIT_TYPE_ARTICLE,
                            map_helper.COUNTER_ARTICLE_ITEM_REQUESTS)

                        self.articles[pid][year][month][day]['unique_item_investigations'] = self._get_unique(
                            datefied_hits[year][month][day],
                            map_helper.HIT_TYPE_ARTICLE,
                            map_helper.COUNTER_ARTICLE_ITEM_INVESTIGATIONS)

    def _populate_issues(self, pid_to_hits: dict):
        """
        Povoa ``self.issues` com os acessos no formato COUNTER R5

        :param pid_to_hits: dicionário que contém PIDs associados a seus respectivos hits
        """
        pass

    def _populate_journals(self, pid_to_hits: dict):
        """
        Povoa ``self.journals`` com os acessos no formato COUNTER R5

        :param pid_to_hits: dicionário que contém PIDs associados a seus respectivos objetos Hit
        """
        for pid, hits in pid_to_hits.items():
            datefied_hits = self.get_datefied_hits(hits)
            for year in datefied_hits:
                for month in datefied_hits[year]:
                    for day in datefied_hits[year][month]:

                        if pid not in self.journals:
                            self.journals[pid] = {year: {}}

                        if month not in self.journals[pid][year]:
                            self.journals[pid][year][month] = {}

                        if day not in self.journals[pid][year][month]:
                            self.journals[pid][year][month][day] = METRICS_ITEM.copy()

                        self.journals[pid][year][month][day]['total_item_requests'] = self._get_total(
                            datefied_hits[year][month][day],
                            map_helper.HIT_TYPE_ARTICLE,
                            map_helper.COUNTER_ARTICLE_ITEM_REQUESTS)

                        self.journals[pid][year][month][day]['total_item_investigations'] = self._get_total(
                            datefied_hits[year][month][day],
                            map_helper.HIT_TYPE_ARTICLE,
                            map_helper.COUNTER_ARTICLE_ITEM_INVESTIGATIONS)

                        self.journals[pid][year][month][day]['unique_item_requests'] = self._get_unique(
                            datefied_hits[year][month][day],
                            map_helper.HIT_TYPE_ARTICLE,
                            map_helper.COUNTER_ARTICLE_ITEM_REQUESTS)

                        self.journals[pid][year][month][day]['unique_item_investigations'] = self._get_unique(
                            datefied_hits[year][month][day],
                            map_helper.HIT_TYPE_ARTICLE,
                            map_helper.COUNTER_ARTICLE_ITEM_INVESTIGATIONS)

    def _populate_platform(self, pid_to_hits: dict):
        """
        Povoa ``self.platform`` com os acessos no formato COUNTER R5

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

            if year not in date_to_hits:
                date_to_hits[year] = {}

            if month not in date_to_hits[year]:
                date_to_hits[year][month] = {}

            if day not in date_to_hits[year][month]:
                date_to_hits[year][month][day] = []

            date_to_hits[year][month][day].append(hit)

        return date_to_hits
