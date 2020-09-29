import csv
import logging
import re

from datetime import datetime
from urllib import parse
from utils import counter_tools, map_helper


logging.basicConfig(filename='logs/hit_' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '.log', level=logging.DEBUG)


class Hit:
    """
    Classe que representa o acesso a uma página (ação).
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
    Classe que gerencia objetos Hit.
    """
    def __init__(self, path_pdf_to_pid):
        self.session_to_actions = {}
        self.pid_to_hits = {}
        self.pdf_path_to_pid = path_pdf_to_pid

    def set_hits(self, log_file_name: str):
        """
        Carrega arquivo de log

        :param log_file_name: nome do arquivo de log
        """
        with open(log_file_name) as f:
            csv_file = csv.DictReader(f, delimiter='\t')
            for log_row in csv_file:
                hit = self.create_hit_from_log_row(**log_row)
                self._update_session_to_action(hit)

                logging.debug('Hit.set_hits:importado:%s' % hit)

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
        new_hit = Hit(**log_row)

        if not new_hit.pid:
            self.set_pid_from_pdf(new_hit)

        self.set_hit_type(new_hit)
        self.set_content_type(new_hit)

        return new_hit

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
                                logging.debug('Hit.remove_double_clicks:adicionado:%s' % current_hit)
                        elif i + 2 == len(hits):
                            cleaned_hits.append(current_hit)
                            logging.debug('Hit.remove_double_clicks:adicionado:%s' % current_hit)
                else:
                    cleaned_hits.extend(hits)
                    logging.debug('Hit.remove_double_clicks:adicionado %s' % hits[0])

                    if cleaned_hits:
                        dict_session_values[session][action_attr] = cleaned_hits
                    else:
                        dict_session_values[session][action_attr] = hits

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
                print('WARNNING:Há mais de um PID %s associado ao PDF %s' % (extracted_pid, url_parsed.path))
                # Considera o PID mais antigo
                hit.pid = sorted(extracted_pid)[0]

    def set_hit_type(self, hit: Hit):
        """
        Seta o tipo de item acessado (article, issue, journal ou platform) conforme o parâmetro ``pid`` de um hit

        :param hit: um Hit
        """
        if re.match(map_helper.REGEX_ARTICLE_PID, hit.pid):
            hit.hit_type = map_helper.HIT_TYPE_ARTICLE
        elif re.match(map_helper.REGEX_ISSUE_PID, hit.pid):
            hit.hit_type = map_helper.HIT_TYPE_ISSUE
        elif re.match(map_helper.REGEX_JOURNAL_PID, hit.pid):
            hit.hit_type = map_helper.HIT_TYPE_JOURNAL
        else:
            hit.hit_type = map_helper.HIT_TYPE_PLATFORM

    def set_content_type(self, hit: Hit):
        """
        Obtém o tipo de conteúdo acessado (para article, por exemplo, há os seguintes tipos de contéudo: texto completo, página plus do texto completo, pdf, xml, resumo, como citar, texto completo traduzido)

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
        return ''

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
