from datetime import datetime


def _extract_user_agent(browser_name: str, browser_version: str):
    """
    Obtém o agente de usuário com base nas informações de nome e versão de navegador

    :param browser_name: nome do navegador
    :param browser_version: versão do navegador
    :return: agente do usuário
    """
    return '/'.join([browser_name, browser_version])


def _extract_slice(date: datetime):
    """
    Extrai uma fatia no formato YYYY-MM-DD|H

    :param date: um elemento datetime
    :return: uma str no formato YYYY-MM-DD|H
    """
    str_date = '-'.join([str(x) for x in [date.year, date.month, date.day]])
    str_hour_slice = str(date.hour)
    return '|'.join([str_date, str_hour_slice])


def generate_session_id(ip: str, browser_name: str, browser_version: str, date: datetime):
    """
    Gera um ID de sessão

    :param ip: endereço IP
    :param browser_name: nome do navegador
    :param browser_version: versão do navegador
    :param date: data e hora
    :return: uma str que representa um ID de sessão
    """
    date_slice = _extract_slice(date)
    user_agent = _extract_user_agent(browser_name, browser_version)
    return '|'.join([ip, user_agent, date_slice])


def is_double_click(group, past_hit, current_hit):
    """
    Verifica se a ação atual (current_action) é um duplo-clique. É condição necessária que ambos os Hits possuam
     a mesma estrutura de comparação. Por exemplo, a quadra (pid, format, content_type, language) serve para comparar
     o grupo 'article'.

    :param group: grupo de Hits (article | issue | journal | platform | others)
    :param past_hit: ação mais antiga
    :param current_hit: ação atual
    :return: True se for duplo-clique, False caso contrário
    """
    time_delta = current_hit.server_time - past_hit.server_time

    if group == 'article':
        if (past_hit.pid,
            past_hit.format,
            past_hit.content_type,
            past_hit.lang) == (current_hit.pid,
                               current_hit.format,
                               current_hit.content_type,
                               current_hit.lang):

            if time_delta.total_seconds() <= 30:
                return True

    if group == 'issue' or group == 'journal':
        if (past_hit.issn,
            past_hit.pid,
            past_hit.content_type,
            past_hit.action_name) == (current_hit.issn,
                                      current_hit.pid,
                                      current_hit.content_type,
                                      current_hit.action_name):
            if time_delta.total_seconds() <= 30:
                return True

    if group == 'journal' or group == 'platform':
        if past_hit.action_name == current_hit.action_name:
            if time_delta.total_seconds() <= 30:
                return True

    return False
