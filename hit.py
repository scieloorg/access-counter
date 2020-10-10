import csv

from datetime import datetime
from socket import inet_ntoa
from urllib import parse
from utils import counter_tools, map_helper, pid_tools


class Hit:
    """
    Classe que representa o acesso a uma página (ação)
    """
    __slots__ = ['ip',
                 'server_time',
                 'browser_name',
                 'browser_version',
                 'action_name',
                 'action_params',
                 'session_id',
                 'pid',
                 'tlng',
                 'script',
                 'hit_type',
                 'content_type']

    def __init__(self, **kargs):
        # Endereço IP
        self.ip = kargs.get('ip', '')

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

        self.pid = self.action_params.get('pid', '')
        self.tlng = self.action_params.get('tlng', '')
        self.script = self.action_params.get('script', '')

        self.hit_type = ''
        self.content_type = ''

    def __str__(self):
        return '|'.join([self.session_id, self.server_time.strftime("%M:%S"), self.action_name])


class HitManager:
    """
    Classe que gerencia objetos `Hit`
    """
    __slots__ = ['session_to_actions', 'pid_to_hits', 'pdf_path_to_pid', 'issn_to_acronym']

    def __init__(self, path_pdf_to_pid, issn_to_acronym):
        self.session_to_actions = {}
        self.pid_to_hits = {}

        # Dicionários para tratamento de PID
        self.pdf_path_to_pid = path_pdf_to_pid
        self.issn_to_acronym = issn_to_acronym

    def create_hit_from_sql_data(self, row):
        """
        Cria objeto `Hit` a partir de dados extraídos diretamente o Matomo

        @param row: um objeto `LogLinkActionVisit`
        @return: um objeto `Hit`
        """
        dict_attrs = {
            'serverTime': row.server_time,
            'browserName': row.visit.config_browser_name,
            'browserVersion': row.visit.config_browser_version,
            'ip': inet_ntoa(row.visit.location_ip),
            'actionName': row.action.name
        }
        new_hit = Hit(**dict_attrs)

        if not new_hit.pid:
            self.set_pid_from_pdf(new_hit)

        self.set_hit_type(new_hit)
        self.set_content_type(new_hit)

        return new_hit

    def create_hit_from_log_line(self, **log_row):
        """
        Cria um `Hit` a partir de uma linha de log

        :param log_row: linha de log
        :return: `Hit` povoado com os dados da linha de log
        """
        new_hit = Hit(**log_row)

        if not new_hit.pid:
            self.set_pid_from_pdf(new_hit)

        self.set_hit_type(new_hit)
        self.set_content_type(new_hit)

        return new_hit

    def set_hits_from_log_file(self, log_file_name: str):
        """
        Cria objetos `Hit` a partir de dados de log previamente extraídos do Matomo

        :param log_file_name: nome do arquivo de log
        """
        with open(log_file_name) as f:
            csv_file = csv.DictReader(f, delimiter='\t')
            for log_row in csv_file:
                hit = self.create_hit_from_log_line(**log_row)
                self.add_hit(hit)

    def reset(self):
        """
        Limpa registros do `HitManager`
        """
        self.pid_to_hits = {}
        self.session_to_actions = {}

    def add_hit(self, hit: Hit):
        """
        Adiciona `Hit` em dicionário identificador de sessão -> ações

        :param hit: `Hit` ou acesso a ser adicionado no dicionário de sessões
        """
        if hit.session_id not in self.session_to_actions:
            self.session_to_actions[hit.session_id] = {}

        if hit.action_name not in self.session_to_actions[hit.session_id]:
            self.session_to_actions[hit.session_id][hit.action_name] = []

        self.session_to_actions[hit.session_id][hit.action_name].append(hit)

    def count_hits_by_pid(self):
        """
        Gera dicionário ``pid`` -> ``{tipo de url}`` -> ``[hits]``
        """
        counter = 0
        total = len(self.session_to_actions.keys())
        for session_id, actions_names in self.session_to_actions.items():
            counter += 1
            print('\r%d de %d' % (counter, total), end='')
            for action_name, hits in actions_names.items():
                for hit in hits:
                    # Caso PID esteja definido
                    if hit.pid:
                        if hit.pid not in self.pid_to_hits:
                            self.pid_to_hits[hit.pid] = []
                        self.pid_to_hits[hit.pid].append(hit)
                    # TODO: situação em que o PID não está definido (acessos à plataforma em geral)
                    else:
                        pass
        print()

    def remove_double_clicks(self, session_to_actions):
        """
        Remove cliques duplos

        :param session_to_actions: dicionário que mapeia sessão a ações
        """
        counter = 0
        total = len(self.session_to_actions.keys())
        for session, actions in session_to_actions.items():
            counter += 1
            print('\r%d de %d' % (counter, total), end='')
            for action_name, hits in actions.items():

                # Lista de hits sem duplos-cliques
                cleaned_hits = []

                # Caso haja mais de um Hit a uma mesma ação, dentro da sessão, remove os cliques-duplos
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

                # Troca lista de hits para a lista de cliques limpa (sem duplos-cliques)
                session_to_actions[session][action_name] = cleaned_hits
        print()

    def set_pid_from_pdf(self, hit: Hit):
        """
        Seta o PID de um artigo a partir de dicionário de path_pdf para PID

        :param hit: um Hit a arquivo PDF
        """
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, '')

        extracted_pid = self.pdf_path_to_pid.get(collection, {}).get(url_parsed.path)
        if extracted_pid:
            if len(extracted_pid) == 1:
                hit.pid = extracted_pid.pop()
            else:
                # TODO - À espera de um dicionário corrigido
                #   Como solução temporária, é considerado o primeiro PID da lista ordenada
                print('WARNNING:Há mais de um PID %s associado ao PDF %s' % (extracted_pid, url_parsed.path))
                hit.pid = sorted(extracted_pid)[0]

    def set_hit_type(self, hit: Hit):
        """
        Seta o tipo de item acessado (article, issue, journal ou platform) conforme o parâmetro ``pid`` de um hit

        :param hit: um Hit
        """
        hit.hit_type = pid_tools.get_pid_type(hit.pid)

    def set_content_type(self, hit: Hit):
        """
        Obtém o tipo de conteúdo acessado (para article, por exemplo, há os seguintes tipos de contéudo: texto
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

    def get_article_content_type(self, hit: Hit):
        """
        Obtém o tipo de conteúdo acessado com base na URL da ação

        :return: Tipo de conteúdo acessado
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
        return ''

    def get_journal_content_type(self, hit: Hit):
        """
        Obtém o tipo de conteúdo acessado com base na URL da ação

        :return: Tipo de conteúdo acessado
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
        else:
            return map_helper.JOURNAL_CONTENT_TYPE_UNDEFINED

    def get_platform_content_type(self, hit: Hit):
        return ''

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
        collection = map_helper.DOMAINS.get(url_parsed.hostname, '')
        acronym = self.issn_to_acronym.get(collection, {}).get(hit.pid, '')
        if acronym:
            if map_helper.JOURNAL_URL_ABOUT.format(acronym) in hit.action_name:
                return True

    def _journal_content_is_editorial_board(self, hit):
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, '')
        acronym = self.issn_to_acronym.get(collection, {}).get(hit.pid, '')
        if acronym:
            if map_helper.JOURNAL_URL_EDITORIAL_BOARD.format(acronym) in hit.action_name:
                return True

    def _journal_content_is_subscription(self, hit):
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, '')
        acronym = self.issn_to_acronym.get(collection, {}).get(hit.pid, '')
        if acronym:
            if map_helper.JOURNAL_URL_SUBSCRIPTION.format(acronym) in hit.action_name:
                return True

    def _journal_content_is_instructions(self, hit):
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, '')
        acronym = self.issn_to_acronym.get(collection, {}).get(hit.pid, '')
        if acronym:
            if map_helper.JOURNAL_URL_INSTRUCTIONS.format(acronym) in hit.action_name:
                return True
