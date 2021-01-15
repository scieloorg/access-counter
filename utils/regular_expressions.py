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
