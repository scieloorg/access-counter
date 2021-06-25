# Detecta PID de artigo
REGEX_ARTICLE_PID = r'^S[0-9]{4}-[0-9]{3}[0-9xX]\d{13}$'

# Detecta PID de fascículo
REGEX_ISSUE_PID = r'^[0-9]{4}-[0-9]{3}[0-9xX]\d{8}$'

# Detecta PID de periódico, isto é, representa um ISSN
REGEX_JOURNAL_PID = r'^[0-9]{4}-[0-9]{3}[0-9xX]$'

# Detecta ano de publicação (YOP) a partir do PID de artigo
REGEX_ARTICLE_PID_YOP = r'^S[0-9]{4}-[0-9]{3}[0-9xX](\d{4})\d{9}$'

# Detecta ISSN
REGEX_ISSN = r'[0-9]{4}-[0-9]{3}[0-9xX]'

# Detectam nome e/ou caminho de arquivo pdf
REGEX_PDF = r'.*\.pdf$'
REGEX_ARTICLE_PDF_PATH = r'.*(/pdf/.*/.*)'
REGEX_ARTICLE_PDF_FULL_PATH = r'(.*)(/pdf/.*/.*)'
REGEX_ARTICLE_PDF_ACRONYM = r'.*/pdf/(\w*)/(.*)/(.*)'  # grupo 1 = acrônimo, grupo 2 = fascículo, grupo 3 = arquivo

# Detectam acrônimo de periódico
REGEX_JOURNAL_IMG_REVISTAS = r'.*/img/revistas/(\w*)/.*'
REGEX_JOURNAL_REVISTAS = r'.*/revistas/(\w*)/.*'
REGEX_JOURNAL_FBPE = r'.*/img/fbpe/(\w*)/.*'
REGEX_JOURNAL_EDITORIAL_BOARD = r'.*/revistas/(\w*)/[e|i|p]edboard.htm'
REGEX_JOURNAL_ABOUT = r'.*/revistas/(\w*)/[e|i|p]aboutj.htm'
REGEX_JOURNAL_INSTRUCTIONS = r'.*/revistas/(\w*)/[e|i|p]instruc.htm'
REGEX_JOURNAL_SUBSCRIPTION = r'.*/revistas/(\w*)/[e|i|p]subscrp.htm'

# Detecta ISSN em URLs relacionadas periódico (serial_sci_serial e serial_sci_issues)
REGEX_JOURNAL_SCRIPT_SCI_SERIAL = r'.*script_sci_serial.*pid_(\d{4}-\d{3}[\d|x|X]).*'
REGEX_JOURNAL_SCRIPT_SCI_ISSUES = r'.*script_sci_issues.*pid_(\d{4}-\d{3}[\d|x|X]).*'

# Detectam URLs associadas à versão nova dos sites SciELO
REGEX_NEW_SCL_JOURNAL_ARTICLE_ABSTRACT = r'.*/j/(\w*)/a/(\w*)/abstract'  # grupo 1 = acrônimo, grupo 2 = PID
REGEX_NEW_SCL_JOURNAL_ARTICLE = r'.*/j/(\w*)/a/(\w*)'  # grupo 1 = acrônimo, grupo 2 = PID
REGEX_NEW_SCL_JOURNAL_FEED = r'.*/journal/(\w*)/feed'  # grupo 1 = acrônimo
REGEX_NEW_SCL_JOURNAL_GRID = r'.*/j/(\w*)/grid'  # grupo 1 = acrônimo
REGEX_NEW_SCL_JOURNAL_TOC = r'.*/j/(\w*)/i/([\w|\.]*)'  # grupo 1 = acrônimo, grupo 2 = fascículo
REGEX_NEW_SCL_JOURNAL_AOP = r'.*/j/(\w*)/aop'  # grupo 1 = acrônimo
REGEX_NEW_SCL_JOURNAL = r'.*/j/(\w*)/'  # grupo 1 = acrônimo
REGEX_NEW_SCL_JOURNALS_ALFAPHETIC = r'.*/journals/alpha'
REGEX_NEW_SCL_JOURNALS_THEMATIC = r'.*/journals/thematic'
REGEX_NEW_SCL_RAW = r'.*/article/ssm/content/raw/'
REGEX_NEW_SCL_RAW_DETAIL = r'.*/documentstore/([\w|-]*)/(\w*)/([\w|\.]*)' # grupo 1 = ISSN, grupo 2 = código ds, grupo 3 = insumo

# Detectam URLs associadas à versão nova do site SciELO Public Health
REGEX_SSP_JOURNAL_ARTICLE_HTML = r'.*/article/(\w*)/(.*)'
REGEX_SSP_JOURNAL_ARTICLE_HTML_DETAILS =r'.*/article/(\w*)/?([\d|\w|\.|\-]*)/?([\d|\w|\-]*)/?(\w*|)'
REGEX_SSP_JOURNAL_ARTICLE_PDF = r'.*/pdf/(\w*)/(.*)'
REGEX_SSP_JOURNAL_ARTICLE_PDF_DETAILS = r'.*/pdf/(\w*)/([\d|\w|\.|\-]*)/?([\d|\w|\-]*)/?(\w*|)'
REGEX_SSP_JOURNAL_ARTICLE_MEDIA_ASSETS = r'.*/media/assets/(\w*)/(.*)'
REGEX_SSP_JOURNAL_ARTICLE_MEDIA_ASSETS_DETAILS = r'.*/media/assets/(\w*)/?([\d|\w|\.|\-]*)/?([\d|\w|\-|\.|\_]*)'
REGEX_SSP_JOURNAL = r'.*/j/(\w*)/'
REGEX_SSP_JOURNAL_FEED = r'.*/journal/(\w*)/feed'
REGEX_SSP_JOURNAL_GRID = r'.*/j/(\w*)/grid'
REGEX_SSP_JOURNAL_ABOUT = r'.*/journal/(\w*)/about'
REGEX_SSP_JOURNAL_ISSUE = r'.*/j/(\w*)/i/([\d|\w|.]*)'
REGEX_SSP_JOURNAL_FEED_ISSUE = r'.*/feed/(\w*)/([\d|\w|.]*)'
REGEX_SSP_JOURNALS_ALPHABETIC = r'.*/journals/alpha'
REGEX_SSP_JOURNALS_THEMATIC = r'.*/journals/thematic'
REGEX_SSP_PLATFORM = r'scielosp\.org'
REGEX_SSP_PLATFORM_ABOUT = r'.*/about'
REGEX_SSP_JOURNAL_ARTICLE_YEAR = r':(\d{4})\.(.*):(.*)'

# Detectam URLs associadas à coleção Preprints
REGEX_PREPRINT_VIEW_ABSTRACT = r'preprint/view/(\d+)$'
REGEX_PREPRINT_DOCUMENT_ABSTRACT = r'documents/article/view/(\d+)$'
REGEX_PREPRINT_VERSION_ABSTRACT = r'preprint/view/(\d+)/version/(\d+)$'
REGEX_PREPRINT_VIEW_PDF = r'preprint/view/(\d+)/(\d+)'
REGEX_PREPRINT_DOWNLOAD_PDF = r'preprint/download/(\d+)/(\d+)'
REGEX_PREPRINT_DOCUMENT_DOWNLOAD_PDF = r'documents/article/download/(\d+)/(\d+)'
REGEX_PREPRINT_VERSION_DOWNLOAD_PDF = r'preprint/download/(\d+)/version/(\d+)/(\d+)'

# Detecta coleção e data de dicionário OPAC
REGEX_OPAC_DICTIONARY = r'opac-counter-dict-(\w{3})-\d{4}-\d{2}-\d{2}-p\d*.json'
REGEX_YEAR = r'1\d{3}|20\d{2}'
REGEX_PREPRINT_PID_PREFIX = r'oai:ops\.preprints\.scielo\.org:preprint\/(\d*)'
