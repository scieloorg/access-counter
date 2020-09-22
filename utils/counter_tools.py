from datetime import datetime


def _extract_user_agent(browser_name: str, browser_version: str):
    """
    Obtém o agente de usuário com base nas informações de nome e versão de navegador

    :param browser_name: nome do navegador
    :param browser_version: versão do navegador
    :return: agente do usuário
    """
    return '/'.join([browser_name, browser_version])


