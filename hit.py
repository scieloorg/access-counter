import csv

from datetime import datetime
from urllib import parse
from utils import counter_tools, map_helper


class Hit:
    """
    Modelo de dados que representa o acesso a uma página (ação).
    """
    def __init__(self, **kargs):
        # Endereço IP
        self.ip = kargs['ip']

        # Data e horário do acesso
        self.server_time = datetime.strptime(kargs['serverTime'], '%Y-%m-%d %H:%M:%S')

        # Nome do navegador utilizado
        self.browser_name = kargs['browserName'].lower()

        # Versão do navegador utilizado
        self.browser_version = kargs['browserVersion'].lower()

        # URL da ação
        self.action_name = kargs['actionName'].lower()

        # Extrai parâmetros da URL da ação
        self.action_params = self._extract_params_from_action_name(self.action_name)

        # Cria campos pid, tlng, script, hit_type e content_type a partir dos parâmetros da URL da ação
        self._create_attrs()

        # Gera um ID de sessão
        self.session_id = counter_tools.generate_session_id(self.ip,
                                                            self.browser_name,
                                                            self.browser_version,
                                                            self.server_time)

    def _extract_params_from_action_name(self, action_name: str):
        """
        Extrai parâmetros da URL e cria campos ``pid``, ``tlng`` e ``script``

        :param action_name: URL da ação
        :return: dicionário com os parâmetros extraídos da URL da ação
        """
        return dict(parse.parse_qsl(parse.urlsplit(action_name).query))

    def _extract_hit_type(self):
        """
        Obtém o tipo de item acessado (article, issue, journal ou platform) conforme o parâmetro ``pid`` de um hit

        :param pid: pid de um hit
        """
        if len(self.pid) == 9:
            return map_helper.HIT_TYPE_JOURNAL
        elif len(self.pid) == 17:
            return map_helper.HIT_TYPE_ISSUE
        elif len(self.pid) == 23:
            return map_helper.HIT_TYPE_ARTICLE
        else:
            return map_helper.HIT_TYPE_PLATFORM

    def _extract_content_type(self):
        """
        Obtém o tipo de conteúdo acessado (para article, por exemplo, há os seguintes tipos de contéudo: texto completo, página plus do texto completo, pdf, xml, resumo, como citar, texto completo traduzido)

        :return: tipo de conteúdo acessado
        """
        if self.hit_type == map_helper.HIT_TYPE_ARTICLE:
            return self.get_article_content_type()
        elif self.hit_type == map_helper.HIT_TYPE_ISSUE:
            return self.get_issue_content_type()
        elif self.hit_type == map_helper.HIT_TYPE_JOURNAL:
            return self.get_journal_content_type()
        elif self.hit_type == map_helper.HIT_TYPE_PLATFORM:
            return self.get_platform_content_type()

    def _create_attrs(self):
        """
        Cria campos ``pid``, ``tlng``, ``script``, ``hit_type`` e ``content_type`` a partir dos parâmetros da URL da ação
        """
        self.pid = self.action_params.get('pid', '')
        self.tlng = self.action_params.get('tlng', '')
        self.script = self.action_params.get('script', '')

        self.hit_type = self._extract_hit_type()
        self.content_type = self._extract_content_type()

    def get_article_content_type(self):
        """
        Obtém o tipo de conteúdo acessado com base na URL da ação

        :return: Tipo de conteúdo acessado
        """
        if self._article_content_is_full_text():
            return map_helper.ARTICLE_CONTENT_TYPE_FULL_TEXT
        elif self._article_content_is_full_text_plus():
            return map_helper.ARTICLE_CONTENT_TYPE_FULL_TEXT_PLUS
        elif self._article_content_is_abstract():
            return map_helper.ARTICLE_CONTENT_TYPE_ABSTRACT
        elif self._article_content_is_xml():
            return map_helper.ARTICLE_CONTENT_TYPE_XML
        elif self._article_content_is_pdf():
            return map_helper.ARTICLE_CONTENT_TYPE_PDF
        elif self._article_content_is_how_to_cite():
            return map_helper.ARTICLE_CONTENT_TYPE_HOW_TO_CITE
        elif self._article_content_is_translated():
            return map_helper.ARTICLE_CONTENT_TYPE_TRANSLATED
        else:
            return map_helper.ARTICLE_CONTENT_TYPE_UNDEFINED

    def get_issue_content_type(self):
        pass

    def get_journal_content_type(self):
        pass

    def get_platform_content_type(self):
        pass

    def _article_content_is_pdf(self):
        if map_helper.ARTICLE_URL_PDF in self.action_name:
            return True

    def _article_content_is_how_to_cite(self):
        if map_helper.ARTICLE_URL_HOW_TO_CITE in self.action_name:
            if self.script == 'sci_isoref':
                return True

    def _article_content_is_full_text(self):
        if map_helper.ARTICLE_URL_FULL_TEXT in self.action_name:
            if self.script == 'sci_arttext':
                return True

    def _article_content_is_full_text_plus(self):
        if map_helper.ARTICLE_URL_FULL_TEXT_PLUS in self.action_name:
            return True

    def _article_content_is_abstract(self):
        if map_helper.ARTICLE_URL_ABSTRACT in self.action_name:
            if self.script == 'sci_abstract':
                return True

    def _article_content_is_translated(self):
        if map_helper.ARTICLE_URL_TRANSLATED in self.action_name:
            return True

    def _article_content_is_xml(self):
        if map_helper.ARTICLE_URL_XML in self.action_name:
            return True


class HitManager:
    """
    Modelo de dados utilizado para gerenciar items de acesso.
    """
    def __init__(self, pdf_to_pid):
        self.hits = []
        self.session_to_actions = {}
        self.pid_to_hits = {}
        self.pdf_to_pid = pdf_to_pid

    def set_hits(self, log_file_name: str):
        """
        Carrega arquivo de log

        :param log_file_name: nome do arquivo de log
        """
        print('Lendo arquivo de log')
        counter = 0
        with open(log_file_name) as f:
            csv_file = csv.DictReader(f, delimiter='\t')
            for log_row in csv_file:
                hit = self.create_hit_from_log_row(**log_row)

                # Caso hit não tenha um PID na URL, pode ser um PDF de artigo
                if not hit.pid:
                    self.extract_pid_from_pdf(hit)

                self.hits.append(hit)

                self._update_session_to_action(hit)
                counter += 1
                print('\r%s' % str(counter), end='')
        print()

    def _update_session_to_action(self, hit: Hit):
        """
        Atualiza mapa identificador de sessão -> ações

        :param hit: acesso a ser atualizado no mapa de sessões
        """
        if hit.session_id not in self.session_to_actions:
            self.session_to_actions[hit.session_id] = {}

        if hit.action_name not in self.session_to_actions[hit.session_id]:
            self.session_to_actions[hit.session_id][hit.action_name] = []

        self.session_to_actions[hit.session_id][hit.action_name].append(hit)

    def count_hits_by_pid(self):
        """
        Gera mapa ``pid`` -> ``{tipo de url}`` -> ``[hits]``
        """
        for session_id, actions_names in self.session_to_actions.items():
            for action_name, hits in actions_names.items():
                for hit in hits:
                    if hit.pid:
                        if hit.pid not in self.pid_to_hits:
                            self.pid_to_hits[hit.pid] = []
                        self.pid_to_hits[hit.pid].append(hit)

    def create_hit_from_log_row(self, **log_row):
        """
        Cria um item de acesso a partir de uma linha de log

        :param log_row: linha de log
        :return: Hit povoado com os dados da linha de log
        """
        return Hit(**log_row)

    def clean_double_clicks(self, dict_session_values):
        """
        Remove cliques duplos

        :param dict_session_values: dicionário que mapeia sessão a valores de ação (``action_name``, ``pid``)
        """
        for session, actions in dict_session_values.items():
            for action_attr, hits in actions.items():
                if len(hits) > 1:
                    hits = sorted(hits, key=lambda x: x.server_time)
                    cleaned_hits = []

                    for i in range(len(hits) - 1):
                        past_hit = hits[i]
                        current_hit = hits[i + 1]

                        if not counter_tools.is_double_click(past_hit, current_hit):
                            cleaned_hits.append(past_hit)
                            if i + 2 == len(hits):
                                cleaned_hits.append(current_hit)
                        elif i + 2 == len(hits):
                            cleaned_hits.append(current_hit)

                    if cleaned_hits:
                        dict_session_values[session][action_attr] = cleaned_hits
                    else:
                        dict_session_values[session][action_attr] = hits

    def extract_pid_from_pdf(self, hit: Hit):
        """
        Extrai o PID de um artigo a partir de dicionário de path_pdf para PID

        :param hit: um objeto Hit
        """
        url_parsed = parse.urlparse(hit.action_name)
        collection = map_helper.DOMAINS.get(url_parsed.hostname, '')

        extracted_pid = self.pdf_to_pid.get(collection, {}).get(url_parsed.path)
        if extracted_pid:
            if len(extracted_pid) == 1:
                hit.pid = extracted_pid
            else:
                print('WARNNING:Há mais de um PID %s associado ao PDF %s' % (extracted_pid, url_parsed.path))
