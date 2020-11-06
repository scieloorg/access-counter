# access-counter

Gera relatórios em formato COUNTER R5.

## Como instalar
1. Criar ambiente virtual para python 3.6+: `virtualenv -p python3.6 .venv`
2. Acessar ambiente virtual: `source .venv/bin/activate`
3. Instalar dependências:  `apt install libmysqlclient-dev`, `pip install -r requirements.txt`

## Como usar

__Criar tabelas__

`python create_tables.py -d mysql://user:pass@host:port/database --create_index_ip_on_matomo_log_visit --fix_fields_interactions`


__Popular tabela de periódicos__

`python populate_journals.py -d mysql://user:pass@host:port/database`

__Coletar dados do Matomo e gerar métricas COUNTER__

_Por meio de arquivos de log pré-processados_

`python main.py -p path_pdf_to_pid.data -a issn_to_acronym.data -p path_pdf_to_pid.data --langs pid_to_format_lang.data -u mysql://user:pass@host:port/database -l data/matomo-logs/ --idsite=1`

_Por meio de acesso direto ao banco de dados do Matomo_

`python main.py -p path_pdf_to_pid.data -a issn_to_acronym.data -p path_pdf_to_pid.data --langs pid_to_format_lang.data -u mysql://user:pass@host:port/database --period 2020-06-01,2020-06-07 --idsite=1`

## Insumos utilizados
a detalhar

__Dicionário de arquivos PDF__
- `path_pdf_to_pid.data`
 
__Dicionário de acrônimos de periódicos__
- `issn_to_acronym.data`

__Dicionário de idiomas__
- `pid_to_format_lang.data`

__Logs Matomo extraídos previamente__
- `data/matomo-logs/`


## Tabelas COUNTER
a detalhar

## Tabelas Matomo
a detalhar
