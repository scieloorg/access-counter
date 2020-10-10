# access-counter

Gera relatórios em formato COUNTER R5.

## Como instalar
1. Criar ambiente virtual para python 3.6+: `virtualenv -p python3.6 .venv`
2. Acessar ambiente virtual: `source .venv/bin/activate`
2. Instalar dependências: `pip install -r requirements`

## Como usar

__Criar tabelas__

`python create_tables.py -d mysql://user:pass@host:port/database`


__Popular tabela de periódicos__

`python populate_journals.py -d mysql://user:pass@host:port/database`

__Coletar dados do Matomo e gerar métricas COUNTER__

_Por meio de arquivos de log pré-processados_

`python main.py -p path_pdf_to_pid.data -a issn_to_acronym.data -u mysql://user:pass@host:port/database -l data/matomo-logs/`

_Por meio de acesso direto ao banco de dados do Matomo (preferível)_

`python main.py -p path_pdf_to_pid.data -a issn_to_acronym.data -u mysql://bn_matomo@172.18.0.3:3306/bitnami_matomo -d 2020-06-03`

## Insumos utilizados
a detalhar

__Dicionário de arquivos PDF --> PID__
- `path_pdf_to_pid.data`
 
__Dicionário de acrônimos --> ISSN__
- `issn_to_acronym.data`

__Logs Matomo extraídos previamente__
- `data/matomo-logs/`


## Tabelas COUNTER
a detalhar

## Tabelas Matomo
a detalhar
