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

        # ID atribuído a visita (pelo Matomo)
        self.visit_id = kargs['visitId'].lower()

        # ID atribuído ao visitante (pelo Matomo)
        self.visitor_id = kargs['visitorId'].lower()

        # ID da ação (na tabela do Matomo)
        self.action_id = kargs['actionId'].lower()

        # URL da ação
        self.action_name = kargs['actionName'].lower()

        # Extrai parâmetros da URL da ação
        self.action_params = self.extract_params_from_action_name(self.action_name)

        # Cria campos pid, tlng e script a partir dos parâmetros da URL da ação
        self.create_attrs_from_action_params()

        # Extrai tipo de URL
        self.create_item_type_from_pid()

        # Gera um ID de sessão
        self.session_id = counter_tools.generate_session_id(self.ip,
                                                            self.browser_name,
                                                            self.browser_version,
                                                            self.server_time)

    def extract_params_from_action_name(self, action_name: str):
        """
        Extrai parâmetros da URL e cria campos ``pid``, ``tlng`` e ``script``

        :param action_name: URL da ação
        :return: dicionário com os parâmetros extraídos da URL da ação
        """
        return dict(parse.parse_qsl(parse.urlsplit(action_name).query))

    def create_attrs_from_action_params(self):
        """
        Cria campos ``pid``, ``tlng`` e ``script`` a partir dos valores dos parâmetros da URL da ação
        """
        self.pid = self.action_params.get('pid', '')
        self.tlng = self.action_params.get('tlng', '')
        self.script = self.action_params.get('script', '')

    def create_item_type_from_pid(self):
        """
        Cria campo item_type conforme o tamanho do ``pid``

        """
        if len(self.pid) == 9:
            self.item_type = map_helper.HIT_TYPE_JOURNAL
        elif len(self.pid) == 17:
            self.item_type = map_helper.HIT_TYPE_ISSUE
        elif len(self.pid) == 23:
            self.item_type = map_helper.HIT_TYPE_ARTICLE
        else:
            self.item_type = map_helper.HIT_TYPE_UNDEFINED

    def __str__(self):
        return '|'.join([self.session_id, str(self.server_time), str(self.item_type), self.action_name])

    def __repr__(self):
        return '|'.join([self.session_id, str(self.server_time), str(self.item_type), self.action_name])

    def get_article_type(self):
        """
        Obtém o tipo de conteúdo acessado com base na URL da ação, se for artigo.
        :return: Tipo de conteúdo acessado
        """
        if self.item_type == map_helper.HIT_TYPE_ARTICLE:
            if self._article_is_full_text():
                return map_helper.ARTICLE_TYPE_FULL_TEXT
            elif self._article_is_full_text_plus():
                return map_helper.ARTICLE_TYPE_FULL_TEXT_PLUS
            elif self._article_is_abstract():
                return map_helper.ARTICLE_TYPE_ABSTRACT
            elif self._article_is_xml():
                return map_helper.ARTICLE_TYPE_XML
            elif self._article_is_pdf():
                return map_helper.ARTICLE_TYPE_PDF
            elif self._article_is_how_to_cite():
                return map_helper.ARTICLE_TYPE_HOW_TO_CITE
            elif self._article_is_translated():
                return map_helper.ARTICLE_TYPE_TRANSLATED
            else:
                return map_helper.ARTICLE_TYPE_UNDEFINED

    def _article_is_pdf(self):
        if map_helper.ARTICLE_URL_PDF in self.action_name:
            return True

    def _article_is_how_to_cite(self):
        if map_helper.ARTICLE_URL_HOW_TO_CITE in self.action_name:
            if self.script == 'sci_isoref':
                return True

    def _article_is_full_text(self):
        if map_helper.ARTICLE_URL_FULL_TEXT in self.action_name:
            if self.script == 'sci_arttext':
                return True

    def _article_is_full_text_plus(self):
        if map_helper.ARTICLE_URL_FULL_TEXT_PLUS in self.action_name:
            return True

    def _article_is_abstract(self):
        if map_helper.ARTICLE_URL_ABSTRACT in self.action_name:
            if self.script == 'sci_abstract':
                return True

    def _article_is_translated(self):
        if map_helper.ARTICLE_URL_TRANSLATED in self.action_name:
            return True

    def _article_is_xml(self):
        if map_helper.ARTICLE_URL_XML in self.action_name:
            return True


class HitManager:
    """
    Modelo de dados utilizado para gerenciar items de acesso.
    """
    def __init__(self):
        self.items = []
        self.session_to_actions = {}
        self.pid_to_hits = {}

    def set_items(self, raw_name: str):
        """
        Carrega arquivo de log

        :param raw_name: nome do arquivo de log
        """
        print('Lendo arquivo de log')
        counter = 0
        with open(raw_name) as f:
            csv_file = csv.DictReader(f, delimiter='\t')
            for log_row in csv_file:
                hit = self.create_hit_from_log_row(**log_row)
                self.items.append(hit)
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
        Gera mapa ``pid`` -> ``{tipo de conteúdo}`` -> ``[hits]``
        """
        for session_id in self.session_to_actions:
            actions_names = self.session_to_actions[session_id]
            for action_name in actions_names:
                hits = actions_names[action_name]
                for hit in hits:
                    if hit.item_type == map_helper.HIT_TYPE_ARTICLE:
                        if hit.pid not in self.pid_to_hits:
                            self.pid_to_hits[hit.pid] = {}

                        hit_article_type = hit.get_article_type()
                        if hit_article_type not in self.pid_to_hits[hit.pid]:
                            self.pid_to_hits[hit.pid][hit_article_type] = []

                        self.pid_to_hits[hit.pid][hit_article_type].append(hit)

    def get_item_type(self, pid):
        """
        Obtém o tipo de item acessado conforme o parâmetro ``pid``
        """
        if len(pid) == 9:
            return map_helper.HIT_TYPE_JOURNAL
        elif len(pid) == 17:
            return map_helper.HIT_TYPE_ISSUE
        elif len(pid) == 23:
            return map_helper.HIT_TYPE_ARTICLE

        return map_helper.HIT_TYPE_UNDEFINED

    def create_hit_from_log_row(self, **log_row):
        """
        Cria um item de acesso a partir de uma linha de log

        :param log_row: linha de log
        :return: Hit povoado com os dados da linha de log
        """
        return Hit(**log_row)

