import re

from utils import map_helper


def article_to_issue(pid: str):
    """
    Obtém o código de fascículo de um artigo, a partir de PID

    :param pid: o PID de um artigo
    """
    if pid.startswith('S'):
        if len(pid) == 23:
            return pid[1:18]


def article_to_journal(pid: str):
    """
    Obtém o ISSN do periódico em que o artigo foi publicado, a partir de seu PID

    :param pid: o PID de um artigo
    """
    if pid.startswith('S'):
        if len(pid) == 23:
            return pid[1:10]


def issue_to_journal(pid: str):
    """
    Obtém o código de fascículo de um periódico, a partir de PID

    :param pid: o PID de um artigo
    """
    if not pid.startswith('S'):
        if len(pid) == 17:
            return pid[:10]


def get_pid_type(pid: str):
    """
    Obtém o tipo de hit conforme o PID

    :param pid: PID de artigo, fascículo ou periódico
    :return: o tipo de Hit (artigo, fascículo, periódico ou plataforma)
    """
    if re.match(map_helper.REGEX_ARTICLE_PID, pid):
        return map_helper.HIT_TYPE_ARTICLE
    elif re.match(map_helper.REGEX_ISSUE_PID, pid):
        return map_helper.HIT_TYPE_ISSUE
    elif re.match(map_helper.REGEX_JOURNAL_PID, pid):
        return map_helper.HIT_TYPE_JOURNAL
    else:
        return map_helper.HIT_TYPE_PLATFORM
