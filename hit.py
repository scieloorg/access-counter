import logging

from datetime import datetime
from socket import inet_ntoa
from utils import counter_tools, map_actions as at, hit_tools as ht


class Hit:
    """
    Classe que representa o acesso a uma página (ação)
    """
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

    def _is_from_local_network(self):
        if self.latitude == '' or not self.latitude:
            return True
        if self.longitude == '' or not self.longitude:
            return True
        return False

    def _is_null_action(self):
        if self.action_name == '' or not self.action_name or self.action_name == 'null':
            return True
        return False

    def is_valid_hit(self):
        # Verifica se Hit é da rede local
        if self._is_from_local_network():
            logging.debug('Hit ignorado. Acesso é da rede local (IP: %s, ActionName: %s)' % (self.ip, self.action_name))
            return False

        # Verifica se Hit é relacionado a ação nula
        if self._is_null_action():
            logging.debug('Hit ignorado. Atributo action_name está vazio (IP: %s, ActionName: %s)' % (self.ip, self.action_name))
            return False

        return True

    def is_trackable_hit(self, flag_include_other_hit_types=False):
        # Ignora Hits não artigo (a depender de flag_include_other_hit_types)
        if not flag_include_other_hit_types:
            if self.hit_type != at.HIT_TYPE_ARTICLE:
                return False

        # Verifica se Hit possui conteúdo indefinido
        if self.content_type == at.HIT_CONTENT_OTHERS:
            logging.debug('Hit ignorado. Conteúdo não é rastreável (HitType: %s, ActionName: %s)' % (self.hit_type, self.action_name))
            return False

        # Verifica se Hit possui tipo indefinido
        if self.hit_type == at.HIT_CONTENT_OTHERS:
            logging.debug('Hit ignorado. Tipo não foi determinado (IP: %s, ActionName: %s)' % (self.ip, self.action_name))
            return False

        if not self.issn and self.hit_type in {at.HIT_TYPE_ARTICLE,
                                               at.HIT_TYPE_ISSUE,
                                               at.HIT_TYPE_JOURNAL}:
            logging.debug('Hit ignorado. ISSN não está definido (HitType: %s, ActionName: %s' % (self.hit_type, self.action_name))
            return False

        return True


class HitManager:
    """
    Classe que gerencia objetos Hit
    """
    __slots__ = ['session_to_pid_format_lang',
                 'pid_format_lang_localization_to_hits',
                 'pdf_path_to_pid',
                 'issn_to_acronym',
                 'pid_to_format_lang',
                 'pid_to_yop']

    def __init__(self, path_pdf_to_pid, issn_to_acronym, pid_to_format_lang, pid_to_yop):
        self.session_to_pid_format_lang = {}
        self.pid_format_lang_localization_to_hits = {}

        # Dicionários para tratamento de PID
        self.pdf_path_to_pid = path_pdf_to_pid
        self.issn_to_acronym = issn_to_acronym
        self.pid_to_format_lang = pid_to_format_lang
        self.pid_to_yop = pid_to_yop

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

            if not new_hit.is_from_local_network():
                if not new_hit.pid:
                    self.set_pid(new_hit)

                self.set_hit_type(new_hit)
                self.set_content_type(new_hit)

                if new_hit.hit_type == map_helper.HIT_TYPE_ARTICLE:
                    self.set_format(new_hit)
                    self.set_lang(new_hit)
                    self.set_yop(new_hit)

                # Ignoramos tudo que não for acesso a artigo
                else:
                    logging.debug('Hit ignorado, URL não é de acesso a artigo (%s, %s)' % (new_hit.ip, new_hit.action_name))
                    return

                if new_hit.content_type == -1:
                    logging.warning('Hit ignorado, URL é descartável ou desconhecida (%s, %s) ' % (new_hit.ip, new_hit.action_name))
                    return

                return new_hit

            else:
                logging.warning('Hit ignorado, acesso é da rede local (%s,%s) ' % (new_hit.ip, new_hit.action_name))
                return
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

        if new_hit.is_from_local_network():
            logging.warning('Hit ignorado, acesso é da rede local (%s,%s) ' % (new_hit.ip, new_hit.action_name))
            return

        if new_hit.is_null_action():
            logging.warning('Hit ignorado, campo action vazio para %s' % new_hit.session_id)
            return

        if not new_hit.pid:
            self.set_pid(new_hit)

        self.set_hit_type(new_hit)
        self.set_content_type(new_hit)

        if new_hit.content_type == -1:
            logging.warning('Hit ignorado, URL é descartável ou desconhecida (%s, %s) ' % (new_hit.ip, new_hit.action_name))
            return

        if new_hit.hit_type == map_helper.HIT_TYPE_ARTICLE:
            self.set_format(new_hit)
            self.set_lang(new_hit)
            self.set_yop(new_hit)

        # Ignoramos tudo que não for acesso a artigo
        else:
            logging.debug('Hit ignorado, URL não é de artigo (%s, %s) ' % (new_hit.ip, new_hit.action_name))
            return

        return new_hit

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

    def group_by_pid_format_lang_localization_yop(self):
        for s, pfl_hits in self.session_to_pid_format_lang.items():
            for pfl, hits in pfl_hits.items():
                for h in hits:
                    pfllly = pfl + (h.latitude, h.longitude, h.yop)
                    if pfllly not in self.pid_format_lang_localization_to_hits:
                        self.pid_format_lang_localization_to_hits[pfllly] = []
                    self.pid_format_lang_localization_to_hits[pfllly].append(h)

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

        @param hit: Hit ao qual o PID é atribuído
        """
        if not hit.pid:
            self._set_pid_from_pdf(hit)

        if not hit.pid:
            self._set_pid_as_issn(hit)

    def set_yop(self, hit: Hit):
        """
        Atribui o valor de ano de publicação (sigla yop em Inglês: Year of Publication) ao PID.

        @param hit: Hit ao qual o yop é atribuído
        """
        collection = self.get_collection(hit)
        hit.yop = self.pid_to_yop.get(collection, {}).get(hit.pid, {}).get('publication_year', '')

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
        collection = self.get_collection(hit)

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
            format_possible_langs = self.pid_to_format_lang.get(collection, {}).get(hit.pid, {}).get(hit.format, set())

            # Se idioma obtido da URL não é válido, atribui idioma padrão
            if hit.lang not in format_possible_langs:
                hit.lang = default_lang
        else:
            hit.lang = default_lang

    def get_collection(self, hit: Hit):
        """
        Obtém a coleção associada ao Hit (acesso)

        @param: um Hit
        """
        url_parsed = parse.urlparse(hit.action_name)
        return map_helper.DOMAINS.get(url_parsed.hostname, map_helper.DEFAULT_COLLECTION)

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
        elif self._article_content_is_reference_list(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_REFERENCE_LIST
        elif self._article_content_is_press_release(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_PRESS_RELEASE
        elif self._article_content_is_xml(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_XML
        elif self._article_content_is_pdf(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_PDF
        elif self._article_content_is_how_to_cite(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_HOW_TO_CITE
        elif self._article_content_is_download_citation(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_DOWNLOAD_CITATION
        elif self._article_content_is_translated(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_TRANSLATED
        elif self._article_content_is_pdf_request(hit):
            return map_helper.ARTICLE_CONTENT_TYPE_PDF_REQUEST
        else:
            return map_helper.ARTICLE_CONTENT_TYPE_UNDEFINED

    def get_issue_content_type(self, hit: Hit):
        """
        Obtém o tipo de conteúdo acessado relacionado a fascículo e com base na URL da ação

        @param hit: um objeto do tipo Hit
        @return: tipo de conteúdo acessado
        """
        if self._issue_content_is_toc(hit):
            return map_helper.ISSUE_CONTENT_TYPE_TOC
        elif self._issue_content_is_rss(hit):
            return map_helper.ISSUE_CONTENT_TYPE_RSS
        else:
            return map_helper.ISSUE_CONTENT_TYPE_UNDEFINED

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

    def _article_content_is_pdf_request(self, hit):
        if map_helper.ARTICLE_URL_PDF_REQUEST in hit.action_name:
            if hit.script == 'sci_pdf':
                return True

    def _article_content_is_pdf(self, hit):
        if map_helper.ARTICLE_URL_PDF in hit.action_name:
            return True
        if map_helper.ARTICLE_URL_PDF_FROM_EXTERNAL_TOOL in hit.action_name:
            return True

    def _article_content_is_how_to_cite(self, hit):
        if map_helper.ARTICLE_URL_HOW_TO_CITE in hit.action_name:
            if hit.script == 'sci_isoref':
                return True

    def _article_content_is_download_citation(self, hit):
        if map_helper.ARTICLE_URL_DOWNLOAD_CITATION in hit.action_name:
            return True

    def _article_content_is_full_text(self, hit):
        if map_helper.ARTICLE_URL_FULL_TEXT in hit.action_name:
            if hit.script == 'sci_arttext':
                return True

    def _article_content_is_full_text_plus(self, hit):
        if map_helper.ARTICLE_URL_FULL_TEXT_PLUS in hit.action_name:
            return True
        if map_helper.ARTICLE_URL_FULL_TEXT in hit.action_name:
            if hit.script == 'sci_arttext_plus':
                return True

    def _article_content_is_abstract(self, hit):
        if map_helper.ARTICLE_URL_ABSTRACT in hit.action_name:
            if hit.script == 'sci_abstract':
                return True

    def _article_content_is_reference_list(self, hit):
        if map_helper.ARTICLE_URL_REFERENCE_LIST in hit.action_name:
            return True

    def _article_content_is_press_release(self, hit):
        if map_helper.ARTICLE_URL_PRESS_RELEASE in hit.action_name:
            if hit.script == 'sci_arttext_pr':
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

    def _issue_content_is_rss(self, hit):
        if map_helper.ISSUE_URL_RSS in hit.action_name:
            return True

    def _issue_content_is_toc(self, hit):
        if map_helper.ISSUE_URL_MAIN_PAGE in hit.action_name:
            if hit.script == 'sci_issuetoc':
                return True
