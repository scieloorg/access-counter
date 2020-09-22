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
