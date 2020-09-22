# Tipo de acesso a página não mapeada
HIT_TYPE_UNDEFINED = -1

# Acesso mapeado a artigo
HIT_TYPE_ARTICLE = 0

# Acesso mapeado a fascículo
HIT_TYPE_ISSUE = 1

# Acesso mapeado a periódico
HIT_TYPE_JOURNAL = 2

# Acesso mapeado às páginas da Plataforma
HIT_TYPE_PLATFORM = 3

# Página principal de artigo + script=sci_arttext, pid={}
ARTICLE_URL_FULL_TEXT = 'scielo.br/scielo.php?'

# Página principal de artigo, em formato plus + pid={}
ARTICLE_URL_FULL_TEXT_PLUS = 'scielo.br/article_plus.php?'

# Página de resumo de artigo + script=sci_abstract, pid={}
ARTICLE_URL_ABSTRACT = 'scielo.br/scielo.php?'

# Página sobre como citar artigo + script=sci_isoref, pid={}
ARTICLE_URL_HOW_TO_CITE = 'scielo.br/scielo.php?'

# Página de artigo em formato XML + pid={}
ARTICLE_URL_XML = 'scielo.br/scieloorg/php/articlexml.php?'

# Página para obter versão traduzida de artigo + pid={}
ARTICLE_URL_TRANSLATED = 'scielo.br/scieloorg/php/translate.php?'

# Arquivo PDF relacionado a artigo + nome_do_arquivo.pdf
ARTICLE_URL_PDF = 'scielo.br/pdf/'

# Tipo indefinido
ARTICLE_TYPE_UNDEFINED = -1

# Artigo completo
ARTICLE_TYPE_FULL_TEXT = 0

# Artigo completo em formato de página plus
ARTICLE_TYPE_FULL_TEXT_PLUS = 1

# Artigo em formato de resumo
ARTICLE_TYPE_ABSTRACT = 2

# Artigo em formato XML
ARTICLE_TYPE_XML = 3

# Artigo em formato PDF
ARTICLE_TYPE_PDF = 4

# Página de mais informações sobre como citar artigo
ARTICLE_TYPE_HOW_TO_CITE = 5

# Página de versão traduzida do artigo
ARTICLE_TYPE_TRANSLATED = 6

# Lista de URLs que contam para COUNTER Item Investigations
COUNTER_ARTICLE_ITEM_INVESTIGATIONS = [ARTICLE_TYPE_FULL_TEXT,
                                       ARTICLE_TYPE_FULL_TEXT_PLUS,
                                       ARTICLE_TYPE_ABSTRACT,
                                       ARTICLE_TYPE_HOW_TO_CITE,
                                       ARTICLE_TYPE_XML,
                                       ARTICLE_TYPE_TRANSLATED,
                                       ARTICLE_TYPE_PDF]

# Lista de URLs que são contabilizadas para COUNTER Item Requests
COUNTER_ARTICLE_ITEM_REQUESTS = [ARTICLE_TYPE_FULL_TEXT,
                                 ARTICLE_TYPE_FULL_TEXT_PLUS,
                                 ARTICLE_TYPE_XML,
                                 ARTICLE_TYPE_PDF]

# Página principal de issue + script=sci_issuetoc, pid={}
ISSUE_URL_MAIN_PAGE = 'scielo.br/scielo.php?'

# Página RSS de issue + pid={}
ISSUE_URL_RSS = 'scielo.br/rss.php?'

# Página principal de periódico + script=sci_serial, pid={}
JOURNAL_URL_MAIN_PAGE = 'scielo.br/scielo.php?'

# Página que lista todas as issues de periódico + script=sci_issues, pid={}
JOURNAL_URL_LIST = 'scielo.br/scielo.php?'

# Página sobre detalhes de periódico + revistas/{acrônimo}/...
JOURNAL_URL_ABOUT = 'scielo.br/revistas/{}/iaboutj.htm'

# Página sobre corpo editorial de periódico + revistas/{acrônimo}/...
JOURNAL_URL_EDITORIAL_BOARD = 'scielo.br/revistas/{}/iedboard.htm'

# Página sobre instruções de periódico + revistas/{acrônimo}/...
JOURNAL_URL_INSTRUCTIONS = 'scielo.br/revistas/{}/iinstruc.htm'

# Página sobre inscrição de periódico + revistas/{acrônimo}/...
JOURNAL_URL_SUBSCRIPTION = 'scielo.br/revistas/{}/isubscrp.htm'

# Página que lista todos os periódicos em ordem alfabética + script=sci_alphabetic
PLATFORM_URL_ALPHABETIC = 'scielo.br/scielo.php?'

# Página que lista todos os periódicos por assunto + script=sci_subject
PLATFORM_URL_SUBJECT = 'scielo.br/scielo.php?script=sci_subject'

# Página de busca
PLATFORM_URL_SEARCH = 'scielo.br/cgi-bin/wxis.exe/iah/?IsisScript=iah/iah.xis'

# Página principal
PLATFORM_URL_MAIN_PAGE = 'scielo.br'

# Páginas de avaliação
PLATFORM_URL_EVUALATION = 'scielo.br/avaliacao/'

# Página de equipe
PLATFORM_URL_TEAM = 'scielo.br/equipe/equipe_i.htm'
