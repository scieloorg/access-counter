from utils import map_helper


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
        self.articles = {}
        self.platform = {}

    def get_total_item_requests(self, type_to_hits: dict):
        total_requests = 0
        for hit_type, hits in type_to_hits.items():
            if hit_type in map_helper.COUNTER_ARTICLE_ITEM_REQUESTS:
                total_requests += len(hits)
        return total_requests

    def get_unique_item_requests(self):
        pass

    def get_total_item_investigations(self, type_to_hits: dict):
        total_investigations = 0
        for hit_type, hits in type_to_hits.items():
            if hit_type in map_helper.COUNTER_ARTICLE_ITEM_INVESTIGATIONS:
                total_investigations += len(hits)
        return total_investigations

    def get_unique_item_investigations(self):
        pass

    def get_total_title_requests(self):
        pass

    def get_unique_title_requests(self):
        pass

    def get_total_title_investigations(self):
        pass

    def get_unique_title_investigations(self):
        pass

    def populate_reports(self, pid_to_hits):
        self._populate_articles(pid_to_hits)
        self._populate_journals()
        self._populate_platform()

    def _populate_articles(self, pid_to_hits):
        for pid in pid_to_hits:
            pid_date_to_hits = self.get_datefied_hits(pid_to_hits[pid])
            for year in pid_date_to_hits:
                for month in pid_date_to_hits[year]:
                    for day in pid_date_to_hits[year][month]:
                        pid_total_item_requests = self.get_total_item_requests(pid_to_hits[pid])
                        pid_total_item_investigations = self.get_total_item_investigations(pid_to_hits[pid])

                        if pid not in self.articles:
                            self.articles[pid] = {year: {}}

                        if month not in self.articles[pid][year]:
                            self.articles[pid][year][month] = {}

                        if day not in self.articles[pid][year][month]:
                            self.articles[pid][year][month][day] = METRICS_ITEM.copy()

                        self.articles[pid][year][month][day]['total_item_requests'] = pid_total_item_requests
                        self.articles[pid][year][month][day]['total_item_investigations'] = pid_total_item_investigations

    def _populate_journals(self):
        pass

    def _populate_platform(self):
        pass

    def get_datefied_hits(self, hits):
        """
        Obtém os hits organizados por data (ANO -> MESES -> DIAS)

        :param hits: lista de hits
        :return: dicionário de hits do tipo YYYY -> {M1, M2, ... -> {D1, D2, ... -> [Hit1, Hit2, ...]}}
        """
        date_to_hits = {}

        for hit_type, hits in hits.items():
            for hit in hits:
                year = hit.server_time.year
                month = hit.server_time.month
                day = hit.server_time.day

                if year not in date_to_hits:
                    date_to_hits[year] = {}

                if month not in date_to_hits[year]:
                    date_to_hits[year][month] = {}

                if day not in date_to_hits[year][month]:
                    date_to_hits[year][month][day] = {}

                if hit_type not in date_to_hits[year][month][day]:
                    date_to_hits[year][month][day][hit_type] = []

                date_to_hits[year][month][day][hit_type].append(hit)

        return date_to_hits
