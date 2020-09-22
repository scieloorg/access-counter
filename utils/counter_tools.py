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
    Extrai uma fatia no formato ``YYYY-MM-DD|H``

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
