from utils import map_actions as ma

# Conteúdos válidos para métrica Investigation de artigos
COUNTER_ARTICLE_ITEM_INVESTIGATIONS = [ma.HIT_CONTENT_ARTICLE_FULL_TEXT,
                                       ma.HIT_CONTENT_ARTICLE_FULL_TEXT_PLUS,
                                       ma.HIT_CONTENT_ARTICLE_ARTICLE_XML,
                                       ma.HIT_CONTENT_ARTICLE_PDF,
                                       ma.HIT_CONTENT_ARTICLE_PLUS,
                                       ma.HIT_CONTENT_ARTICLE_ABSTRACT,
                                       ma.HIT_CONTENT_ARTICLE_ISOREF,
                                       ma.HIT_CONTENT_ARTICLE_PRESS_RELEASE,
                                       ma.HIT_CONTENT_ARTICLE_REQUEST_PDF,
                                       ma.HIT_CONTENT_ARTICLE_CITEDSCIELO,
                                       ma.HIT_CONTENT_ARTICLE_REFERENCE_LIST,
                                       ma.HIT_CONTENT_ARTICLE_RELATED,
                                       ma.HIT_CONTENT_ARTICLE_TRANSLATE,
                                       ma.HIT_CONTENT_ARTICLE_DOWNLOAD_CITATION,
                                       ma.HIT_CONTENT_ARTICLE_EXTERNAL_PDF,
                                       ma.HIT_CONTENT_NEW_SCL_ARTICLE_HTML,
                                       ma.HIT_CONTENT_NEW_SCL_ARTICLE_ABSTRACT,
                                       ma.HIT_CONTENT_NEW_SCL_ARTICLE_XML,
                                       ma.HIT_CONTENT_NEW_SCL_ARTICLE_PDF,
                                       ma.HIT_CONTENT_NEW_SCL_ARTICLE_HOW_TO_CITE,
                                       ma.HIT_CONTENT_NEW_SCL_ARTICLE_REQUEST_PDF,
                                       ma.HIT_CONTENT_NEW_SCL_ARTICLE_TRANSLATE,
                                       ma.HIT_CONTENT_NEW_SCL_ARTICLE_AUTHORS,
                                       ma.HIT_CONTENT_NEW_SCL_ARTICLE_TABLES_AND_FIGURES,
                                       ma.HIT_CONTENT_TYPE_PREPRINT_ABSTRACT,
                                       ma.HIT_CONTENT_TYPE_PREPRINT_PDF]

# Conteúdos válidos para métrica Request de artigos
COUNTER_ARTICLE_ITEM_REQUESTS = [ma.HIT_CONTENT_ARTICLE_FULL_TEXT,
                                 ma.HIT_CONTENT_ARTICLE_FULL_TEXT_PLUS,
                                 ma.HIT_CONTENT_ARTICLE_ARTICLE_XML,
                                 ma.HIT_CONTENT_ARTICLE_PDF,
                                 ma.HIT_CONTENT_ARTICLE_EXTERNAL_PDF,
                                 ma.HIT_CONTENT_ARTICLE_PLUS,
                                 ma.HIT_CONTENT_NEW_SCL_ARTICLE_HTML,
                                 ma.HIT_CONTENT_NEW_SCL_ARTICLE_XML,
                                 ma.HIT_CONTENT_NEW_SCL_ARTICLE_PDF,
                                 ma.HIT_CONTENT_TYPE_PREPRINT_PDF]

# Conteúdos válidos para métrica Investigation de fascículos
COUNTER_ISSUE_ITEM_INVESTIGATIONS = [ma.HIT_CONTENT_ISSUE_TOC,
                                     ma.HIT_CONTENT_ISSUE_RSS]

# Conteúdos válidos para métrica Request de fascículos
COUNTER_ISSUE_ITEM_REQUESTS = [ma.HIT_CONTENT_ISSUE_TOC]

# Conteúdos válidos para métrica Investigation de periódicos
COUNTER_JOURNAL_ITEM_INVESTIGATIONS = [ma.HIT_CONTENT_JOURNAL_ISSUES,
                                       ma.HIT_CONTENT_JOURNAL_MAIN_PAGE,
                                       ma.HIT_CONTENT_JOURNAL_RSS,
                                       ma.HIT_CONTENT_JOURNAL_ABOUT,
                                       ma.HIT_CONTENT_JOURNAL_EDITORIAL,
                                       ma.HIT_CONTENT_JOURNAL_INSTRUCTIONS,
                                       ma.HIT_CONTENT_JOURNAL_SUBSCRIPTION,
                                       ma.HIT_CONTENT_JOURNAL_GOOGLE_METRICS,
                                       ma.HIT_CONTENT_JOURNAL_IMG_FBPE,
                                       ma.HIT_CONTENT_JOURNAL_IMG_REVISTAS,
                                       ma.HIT_CONTENT_JOURNAL_REVISTAS,
                                       ma.HIT_CONTENT_JOURNAL_SERIAL,
                                       ma.HIT_CONTENT_JOURNAL_STAT]

# Conteúdos válidos para métrica Request de periódicos
COUNTER_JOURNAL_ITEM_REQUESTS = [ma.HIT_CONTENT_JOURNAL_MAIN_PAGE]

# Conteúdos válidos para métrica Investigation de plataforma
COUNTER_PLATFORM_ITEM_INVESTIGATIONS = [ma.HIT_CONTENT_PLATFORM_LIST_JOURNALS_ALPHABETIC,
                                        ma.HIT_CONTENT_PLATFORM_HOME,
                                        ma.HIT_CONTENT_PLATFORM_MAIN_PAGE,
                                        ma.HIT_CONTENT_PLATFORM_LIST_JOURNALS_SUBJECT,
                                        ma.HIT_CONTENT_PLATFORM_EVALUATION,
                                        ma.HIT_CONTENT_PLATFORM_TEAM]

# Conteúdos válidos para métrica Request de plataforma
COUNTER_PLATFORM_ITEM_REQUESTS = [ma.HIT_CONTENT_PLATFORM_HOME,
                                  ma.HIT_CONTENT_PLATFORM_MAIN_PAGE]
