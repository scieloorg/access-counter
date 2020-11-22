import logging

from datetime import datetime
from socket import inet_ntoa
from urllib import parse
from utils import counter_tools, map_helper, pid_tools


class Hit:
    """
    Classe que representa o acesso a uma página (ação)
    """
    __slots__ = ['ip',
                 'latitude',
                 'longitude',
                 'server_time',
                 'browser_name',
                 'browser_version',
                 'action_name',
                 'action_params',
                 'session_id',
                 'pid',
                 'lang',
                 'script',
                 'hit_type',
                 'content_type',
                 'format',
                 'issn']

    def __init__(self, **kargs):
        # Endereço IP
        self.ip = kargs.get('ip', '')

        # Localização associada ao IP
        self.latitude = kargs.get('latitude', '')
        self.longitude = kargs.get('longitude', '')

        # Data e horário do acesso
        if isinstance(kargs.get('serverTime', ''), datetime):
            self.server_time = kargs.get('serverTime')
        else:
            self.server_time = datetime.strptime(kargs.get('serverTime', ''), '%Y-%m-%d %H:%M:%S')

        # Nome do navegador utilizado
        self.browser_name = kargs.get('browserName', '').lower()

        # Versão do navegador utilizado
        self.browser_version = kargs.get('browserVersion', '').lower()

        # URL da ação
        self.action_name = kargs.get('actionName', '').lower()

        # Extrai parâmetros da URL da ação
        self.action_params = dict(parse.parse_qsl(parse.urlsplit(self.action_name).query))

        # Gera um ID de sessão
        self.session_id = counter_tools.generate_session_id(self.ip,
                                                            self.browser_name,
                                                            self.browser_version,
                                                            self.server_time)

        self.pid = self.action_params.get('pid', '').upper()
        self.lang = self.action_params.get('tlng', '')
        self.script = self.action_params.get('script', '')

        self.hit_type = ''
        self.content_type = ''
        self.format = ''
        self.issn = self.action_params.get('issn', '').upper()

        # Atribui latitude e longitude 999.99 caso seja acesso da rede local
        if self.latitude == 'NULL' or self.longitude == 'NULL':
            self.latitude = -999.00
            self.longitude = -999.99

    def __str__(self):
        return '|'.join([self.session_id, self.server_time.strftime("%M:%S"), self.action_name])


class HitManager:
    """
    Classe que gerencia objetos Hit
    """
    __slots__ = ['session_to_pid_format_lang',
                 'pid_format_lang_localization_to_hits',
                 'pdf_path_to_pid',
                 'issn_to_acronym',
                 'pid_to_format_lang']

    def __init__(self, path_pdf_to_pid, issn_to_acronym, pid_to_format_lang):
        self.session_to_pid_format_lang = {}
        self.pid_format_lang_localization_to_hits = {}

        # Dicionários para tratamento de PID
        self.pdf_path_to_pid = path_pdf_to_pid
        self.issn_to_acronym = issn_to_acronym
        self.pid_to_format_lang = pid_to_format_lang

    def create_hit_from_sql_data(self, row):
        """
        Cria objeto Hit a partir de dados extraídos diretamente o Matomo

        @param row: um objeto LogLinkVisitAction
        @return: um objeto Hit
        """
        dict_attrs = {'serverTime': row.server_time}

        if row.visit:
            dict_attrs['browserName'] = row.visit.config_browser_name
            dict_attrs['browserVersion'] = row.visit.config_browser_version
            dict_attrs['ip'] = inet_ntoa(row.visit.location_ip)
            dict_attrs['latitude'] = row.visit.location_latitude
            dict_attrs['longitude'] = row.visit.location_longitude
        else:
            logging.warning('Hit ignorado, campo visit vazio para idlink_va %s' % row.idlink_va)
            return

        if row.action:
            dict_attrs['actionName'] = row.action.name

            new_hit = Hit(**dict_attrs)
            if not new_hit.pid:
                self.set_pid(new_hit)

            self.set_hit_type(new_hit)
            self.set_content_type(new_hit)

            if new_hit.hit_type == map_helper.HIT_TYPE_ARTICLE:
                self.set_format(new_hit)
                self.set_lang(new_hit)

            return new_hit
        else:
            logging.warning('Hit ignorado, campo action vazio para idlink_va %s' % row.idlink_va)
            return

    def create_hit_from_log_line(self, **log_row):
        """
        Cria um Hit a partir de uma linha de log

        :param log_row: linha de log
        :return: Hit povoado com os dados da linha de log
        """
        new_hit = Hit(**log_row)

        if new_hit.action_name != 'NULL':
            if not new_hit.pid:
                self.set_pid(new_hit)

            self.set_hit_type(new_hit)
            self.set_content_type(new_hit)

            if new_hit.hit_type == map_helper.HIT_TYPE_ARTICLE:
                self.set_format(new_hit)
                self.set_lang(new_hit)

            return new_hit
        else:
            logging.warning('Hit ignorado, campo action vazio para session_id %s' % new_hit.session_id)
            return

    def reset(self):
        """
        Limpa registros do HitManager
        """
        self.session_to_pid_format_lang = {}
        self.pid_format_lang_localization_to_hits = {}

    def add_hit(self, hit: Hit):
        pfl = (hit.pid, hit.format, hit.lang)

        if hit.session_id not in self.session_to_pid_format_lang:
            self.session_to_pid_format_lang[hit.session_id] = {}

        if pfl not in self.session_to_pid_format_lang[hit.session_id]:
            self.session_to_pid_format_lang[hit.session_id][pfl] = []

        self.session_to_pid_format_lang[hit.session_id][pfl].append(hit)

    def group_by_pid_format_lang_localization(self):
        for s, pfl_hits in self.session_to_pid_format_lang.items():
            for pfl, hits in pfl_hits.items():
                for h in hits:
                    pflll = pfl + (h.latitude, h.longitude)
                    if pflll not in self.pid_format_lang_localization_to_hits:
                        self.pid_format_lang_localization_to_hits[pflll] = []
                    self.pid_format_lang_localization_to_hits[pflll].append(h)

    def remove_double_clicks(self):
        """
        Remove cliques duplos. São comparados os hits dentro de uma sessão e
        que possuem a mesma trinca (pid, format, language)
        """
        for session, pfl_hits in self.session_to_pid_format_lang.items():
            for pfl, hits in pfl_hits.items():
                cleaned_hits = []

                if len(hits) > 1:
                    hits = sorted(hits, key=lambda x: x.server_time)

                    for i in range(len(hits) - 1):
                        past_hit = hits[i]
                        current_hit = hits[i + 1]

                        if not counter_tools.is_double_click(past_hit, current_hit):
                            cleaned_hits.append(past_hit)
                            if i + 2 == len(hits):
                                cleaned_hits.append(current_hit)
                        elif i + 2 == len(hits):
                            cleaned_hits.append(current_hit)
                else:
                    cleaned_hits.extend(hits)

                pfl_hits[pfl] = cleaned_hits

    def set_pid(self, hit: Hit):
        """
        Atribui o valor de pid a um Hit. Primeiro tenta obter por meio de caminho de PDF, após, por parâmetro ISSN.
        E nesse caso, o Hit é a uma URL de periódico o PID armazena o ISSN

        @param hit: Hit a ser atribuído
        """
        if not hit.pid:
            self._set_pid_from_pdf(hit)

        if not hit.pid:
            self._set_pid_as_issn(hit)

    def _set_pid_from_pdf(self, hit: Hit):
        """
        Atribui o PID de um artigo a partir de dicionário de path_pdf para PID

        :param hit: um Hit a arquivo PDF
        """
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, map_helper.DEFAULT_COLLECTION)

        extracted_pid = sorted(self.pdf_path_to_pid.get(collection, {}).get(url_parsed.path, set()))
        if extracted_pid:
            # TODO - À espera de um dicionário corrigido
            #   Como solução temporária, é considerado o primeiro PID da lista ordenada
            hit.pid = extracted_pid[0]

    def _set_pid_as_issn(self, hit: Hit):
        """
        Caso PID seja nulo ou vazio e se há ISSN, atribui ISSN ao PID (o Hit é a uma URL de periódico)

        @param hit: um objeto Hit
        """
        if not hit.pid:
            if hit.issn:
                hit.pid = hit.issn

    def set_hit_type(self, hit: Hit):
        """
        Atribui o tipo de item acessado (article, issue, journal ou platform) conforme o parâmetro PID de um hit.
        Também procura por ISSN caso PID seja vazio

        :param hit: um Hit
        """
        hit.hit_type = pid_tools.get_pid_type(hit.pid)

    def set_content_type(self, hit: Hit):
        """
        Atribui o tipo de conteúdo acessado (para article, por exemplo, há os seguintes tipos de contéudo: texto
        completo, página plus do texto completo, pdf, xml, resumo, como citar, texto completo traduzido)

        :param hit: um Hit
        """
        if hit.hit_type == map_helper.HIT_TYPE_ARTICLE:
            hit.content_type = self.get_article_content_type(hit)
        elif hit.hit_type == map_helper.HIT_TYPE_ISSUE:
            hit.content_type = self.get_issue_content_type(hit)
        elif hit.hit_type == map_helper.HIT_TYPE_JOURNAL:
            hit.content_type = self.get_journal_content_type(hit)
        elif hit.hit_type == map_helper.HIT_TYPE_PLATFORM:
            hit.content_type = self.get_platform_content_type(hit)

    def set_lang(self, hit: Hit):
        """
        Atribui o idioma do recurso acessado com base em dicionário de PIDs, formato e idiomas

        @param hit: um Hit
        """
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, map_helper.DEFAULT_COLLECTION)

        # Idioma padrão originário do dicionário
        pid_langs = self.pid_to_format_lang.get(collection, {}).get(hit.pid)
        if not pid_langs:
            logging.warning('Não foi possível localizar o PID %s no dicionário' % hit.pid)

        default_lang = self.pid_to_format_lang.get(collection, {}).get(hit.pid, {}).get('default')
        if not default_lang:
            logging.warning('Não foi possível localizar o idioma padrão para o PID %s' % hit.pid)
            default_lang = map_helper.ARTICLE_DEFAULT_LANG

        # Se idioma já está definido, verifica se é válido
        if hit.lang:
            # Idiomas possíveis
            format_possible_langs = self.pid_to_format_lang.get(collection, {}).get(hit.pid, {}).get(hit.format, [])

            # Se idioma obtido da URL não é válido, atribui idioma padrão
            if hit.lang not in format_possible_langs:
                hit.lang = default_lang
        else:
            hit.lang = default_lang

    def set_format(self, hit: Hit):
        """
        Atribui o formato do recurso acessado. Pode ser PDF ou HTML, conforme ArticleMeta

        @param hit: um Hit
        """
        if hit.content_type:
            if hit.content_type == map_helper.ARTICLE_CONTENT_TYPE_PDF:
                hit.format = map_helper.ARTICLE_FORMAT_PDF
            else:
                hit.format = map_helper.ARTICLE_FORMAT_HTML
        else:
            hit.format = map_helper.ARTICLE_DEFAULT_FORMAT

    def get_article_content_type(self, hit: Hit):
        """
        Obtém o tipo de conteúdo acessado relacionado a artigo e com base na URL da ação

        :return: tipo de conteúdo acessado
        """
        if self._article_content_is_full_text(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_FULL_TEXT
        elif self._article_content_is_full_text_plus(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_FULL_TEXT_PLUS
        elif self._article_content_is_abstract(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_ABSTRACT
        elif self._article_content_is_xml(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_XML
        elif self._article_content_is_pdf(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_PDF
        elif self._article_content_is_how_to_cite(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_HOW_TO_CITE
        elif self._article_content_is_translated(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_TRANSLATED
        else:
            return map_helper.ARTICLE_CONTENT_TYPE_UNDEFINED

    def get_issue_content_type(self, hit: Hit):
        """
        Obtém o tipo de conteúdo acessado relacionado a fascículo e com base na URL da ação

        @param hit: um objeto do tipo Hit
        @return: tipo de conteúdo acessado
        """
        # TODO: rastrear URLs de fascículo
        return

    def get_platform_content_type(self, hit: Hit):
        """
        Obtém o tipo de conteúdo acessado relacionado a plataforma e com base na URL da ação

        @param hit: um objeto do tipo Hit
        @return: tipo de conteúdo acessado
        """
        # TODO: rastrear URLs de plataforma
        return

    def get_journal_content_type(self, hit: Hit):
        """
        Obtém o tipo de conteúdo acessado relacionado com periódico e com base na URL da ação

        :return: tipo de conteúdo acessado
        """
        if self._journal_content_is_main_page(hit):
            return map_helper.JOURNAL_CONTENT_TYPE_MAIN_PAGE
        elif self._journal_content_is_list(hit):
            return map_helper.JOURNAL_CONTENT_TYPE_LIST
        elif self._journal_content_is_about(hit):
            return map_helper.JOURNAL_CONTENT_TYPE_ABOUT
        elif self._journal_content_is_editorial_board(hit):
            return map_helper.JOURNAL_CONTENT_TYPE_EDITORIAL_BOARD
        elif self._journal_content_is_instructions(hit):
            return map_helper.JOURNAL_CONTENT_TYPE_AUTHOR_INSTRUCTIONS
        elif self._journal_content_is_subscription(hit):
            return map_helper.JOURNAL_CONTENT_TYPE_SUBSCRIPTION
        elif self._journal_content_is_google_metrics_h5_m5(hit):
            return map_helper.JOURNAL_CONTENT_TYPE_GOOGLE_METRICS_H5_M5
        else:
            return map_helper.JOURNAL_CONTENT_TYPE_UNDEFINED

    def _article_content_is_pdf(self, hit):
        if map_helper.ARTICLE_URL_PDF in hit.action_name:
            return True

    def _article_content_is_how_to_cite(self, hit):
        if map_helper.ARTICLE_URL_HOW_TO_CITE in hit.action_name:
            if hit.script == 'sci_isoref':
                return True

    def _article_content_is_full_text(self, hit):
        if map_helper.ARTICLE_URL_FULL_TEXT in hit.action_name:
            if hit.script == 'sci_arttext':
                return True

    def _article_content_is_full_text_plus(self, hit):
        if map_helper.ARTICLE_URL_FULL_TEXT_PLUS in hit.action_name:
            return True

    def _article_content_is_abstract(self, hit):
        if map_helper.ARTICLE_URL_ABSTRACT in hit.action_name:
            if hit.script == 'sci_abstract':
                return True

    def _article_content_is_translated(self, hit):
        if map_helper.ARTICLE_URL_TRANSLATED in hit.action_name:
            return True

    def _article_content_is_xml(self, hit):
        if map_helper.ARTICLE_URL_XML in hit.action_name:
            return True

    def _journal_content_is_main_page(self, hit):
        if map_helper.JOURNAL_URL_MAIN_PAGE in hit.action_name:
            if hit.script == 'sci_serial':
                return True

    def _journal_content_is_list(self, hit):
        if map_helper.JOURNAL_URL_LIST in hit.action_name:
            if hit.script == 'sci_issues':
                return True

    def _journal_content_is_about(self, hit):
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, map_helper.DEFAULT_COLLECTION)
        acronym = self.issn_to_acronym.get(collection, {}).get(hit.pid, '')
        if acronym:
            if map_helper.JOURNAL_URL_ABOUT.format(acronym) in hit.action_name:
                return True

    def _journal_content_is_editorial_board(self, hit):
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, map_helper.DEFAULT_COLLECTION)
        acronym = self.issn_to_acronym.get(collection, {}).get(hit.pid, '')
        if acronym:
            if map_helper.JOURNAL_URL_EDITORIAL_BOARD.format(acronym) in hit.action_name:
                return True

    def _journal_content_is_subscription(self, hit):
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, map_helper.DEFAULT_COLLECTION)
        acronym = self.issn_to_acronym.get(collection, {}).get(hit.pid, '')
        if acronym:
            if map_helper.JOURNAL_URL_SUBSCRIPTION.format(acronym) in hit.action_name:
                return True

    def _journal_content_is_instructions(self, hit):
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, map_helper.DEFAULT_COLLECTION)
        acronym = self.issn_to_acronym.get(collection, {}).get(hit.pid, '')
        if acronym:
            if map_helper.JOURNAL_URL_INSTRUCTIONS.format(acronym) in hit.action_name:
                return True

    def _journal_content_is_google_metrics_h5_m5(self, hit):
        if map_helper.JOURNAL_URL_GOOGLE_METRICS_H5 in hit.action_name:
            return True
