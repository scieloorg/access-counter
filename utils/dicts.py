import utils.map_actions as ma
import utils.map_metrics as mm


# Mapeia valor do parâmetro script ao respectivo código de conteúdo
script_to_hit_content = {
    'sci_abstract': ma.HIT_CONTENT_ARTICLE_ABSTRACT,
    'sci_alphabetic': ma.HIT_CONTENT_PLATFORM_LIST_JOURNALS_ALPHABETIC,
    'sci_arttext': ma.HIT_CONTENT_ARTICLE_FULL_TEXT,
    'sci_arttext_plus': ma.HIT_CONTENT_ARTICLE_FULL_TEXT_PLUS,
    'sci_arttext_pr': ma.HIT_CONTENT_ARTICLE_PRESS_RELEASE,
    'sci_home': ma.HIT_CONTENT_PLATFORM_HOME,
    'sci_isoref': ma.HIT_CONTENT_ARTICLE_ISOREF,
    'sci_issues': ma.HIT_CONTENT_JOURNAL_ISSUES,
    'sci_issuetoc': ma.HIT_CONTENT_ISSUE_TOC,
    'sci_pdf': ma.HIT_CONTENT_ARTICLE_REQUEST_PDF,
    'sci_serial': ma.HIT_CONTENT_JOURNAL_MAIN_PAGE,
    'sci_subject': ma.HIT_CONTENT_PLATFORM_LIST_JOURNALS_SUBJECT,
}

# Mapeia domínio ao acrônimo da respectiva coleção
domain_to_collection = {
    'scielo.org.ar': 'arg',
    'scielo.org.bo': 'bol',
    'scielo.cl': 'chi',
    'scielo.sa.cr': 'cri',
    'scielo.org.co': 'col',
    'scielo.sld.cu': 'cub',
    'scielo.senescyt.gob.ec': 'ecu',
    'scielo.isciii.es': 'esp',
    'scielo.org.mx': 'mex',
    'new.scielo.br': 'nbr',
    'scielo.org.pe': 'per',
    'scielo.mec.pt': 'prt',
    'scielo.iics.una.py': 'pry',
    'pepsic.bvsalud.org': 'psi',
    'revenf.bvs.br': 'rve',
    'scielo.br': 'scl',
    'scielosp.org': 'ssp',
    'socialsciences.scielo.org': 'sss',
    'scielo.org.za': 'sza',
    'scielo.edu.uy': 'ury',
    'scielo.org.ve': 'ven',
    'westindies.scielo.org': 'wid'
}

# Mapeia acrônimo de coleção ao domínio
collection_to_domain = {
    'arg': 'scielo.org.ar',
    'bol': 'scielo.org.bo',
    'chi': 'scielo.cl',
    'cri': 'scielo.sa.cr',
    'col': 'scielo.org.co',
    'cub': 'scielo.sld.cu',
    'ecu': 'scielo.senescyt.gob.ec',
    'esp': 'scielo.isciii.es',
    'mex': 'scielo.org.mx',
    'nbr': 'new.scielo.br',
    'per': 'scielo.org.pe',
    'prt': 'scielo.mec.pt',
    'pry': 'scielo.iics.una.py',
    'psi': 'pepsic.bvsalud.org',
    'rve': 'revenf.bvs.br',
    'scl': 'scielo.br',
    'ssp': 'scielosp.org',
    'sss': 'socialsciences.scielo.org',
    'sza': 'scielo.org.za',
    'ury': 'scielo.edu.uy',
    'ven': 'scielo.org.ve',
    'wid': 'westindies.scielo.org'
}

collection_to_default_language = {
    'arg': 'es',
    'bol': 'es',
    'chi': 'es',
    'cri': 'es',
    'col': 'es',
    'cub': 'es',
    'ecu': 'es',
    'esp': 'es',
    'mex': 'es',
    'nbr': 'pt',
    'per': 'es',
    'prt': 'es',
    'pry': 'es',
    'psi': 'pt',
    'rve': 'pt',
    'scl': 'pt',
    'ssp': 'pt',
    'sss': 'es',
    'sza': 'en',
    'ury': 'es',
    'ven': 'es',
    'wid': 'en'
}

# Mapeia acrônimo do idioma ao respectivo código numérico usado na base de dados
language_to_code = {
    'pt': 1,
    'es': 2,
    'en': 3,
    'fr': 4,
    'de': 5,
    'it': 6
}

# Mapeia acrônimo do formato do conteúdo ao respectivo código numérico na base de dados
format_to_code = {
    'html': 1,
    'pdf': 2
}

# Mapeia acrônimo da coleção ao respectivo código numérico na base de dados
collection_to_code = {
    'arg': 1,
    'bol': 2,
    'chl': 3,
    'cic': 4,
    'col': 5,
    'cri': 6,
    'cub': 7,
    'esp': 8,
    'mex': 9,
    'per': 10,
    'prt': 11,
    'pry': 12,
    'psi': 13,
    'rve': 14,
    'rvt': 15,
    'scl': 16,
    'spa': 17,
    'sss': 18,
    'sza': 19,
    'ury': 20,
    'ven': 21,
    'wid': 22
}

# mapeia nome do tipo de Hit ao respectivo código numérico
group_to_hit_type = {
    'article': ma.HIT_TYPE_ARTICLE,
    'issue': ma.HIT_TYPE_ISSUE,
    'journal': ma.HIT_TYPE_JOURNAL,
    'platform': ma.HIT_TYPE_PLATFORM
}

# mapeia nome do tipo de Hit a respectiva lista de métricas investigation
group_to_item_investigations = {
    'article': mm.COUNTER_ARTICLE_ITEM_INVESTIGATIONS,
    'issue': mm.COUNTER_ISSUE_ITEM_INVESTIGATIONS,
    'journal': mm.COUNTER_JOURNAL_ITEM_INVESTIGATIONS,
    'platform': mm.COUNTER_PLATFORM_ITEM_INVESTIGATIONS
}

# mapeia nome do tipo de Hit a respectiva lista de métricas request
group_to_item_requests = {
    'article': mm.COUNTER_ARTICLE_ITEM_REQUESTS,
    'issue': mm.COUNTER_ISSUE_ITEM_REQUESTS,
    'journal': mm.COUNTER_JOURNAL_ITEM_REQUESTS,
    'platform': mm.COUNTER_PLATFORM_ITEM_REQUESTS
}

# métricas COUNTER
counter_item_metrics = {
    'total_item_requests': 0,
    'unique_item_requests': 0,
    'total_item_investigations': 0,
    'unique_item_investigations': 0
}
