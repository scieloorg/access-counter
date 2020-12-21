# access-counter

Gera relatórios em formato COUNTER R5.


## Como instalar
__Criar ambiente virtual para python 3.6+__ 
```bash
virtualenv -p python3.6 .venv
```

__Acessar ambiente virtual__
```bash
source .venv/bin/activate
```

__Instalar dependências__
```bash
apt install libmysqlclient-dev
pip install -r requirements.txt
```


## Como usar

__Criar tabelas__

```python
python create_tables.py -u {STRING DE CONEXÃO COM MATOMO-DB} --fix_matomo_db
```


__Popular tabela de periódicos__

```python
python populate_journals.py -u {STRING DE CONEXÃO COM MATOMO-DB}
```


__Coletar dados do Matomo e gerar métricas COUNTER__

_Por meio de arquivos de log pré-processados_

```python
python main.py --dict_pdf {DICIONÁRIO PDF-PID} --dict_acronym {DICIONÁRIO ISSN-ACRÔNIMO --dict_language {DICIONÁRIO PID-FORMATO-IDIOMA} -u {STRING DE CONEXÃO COM MATOMO-DB} --dir_pretables {DIRETÓRIO PRÉ-TABELAS MATOMO} --idsite {ID DO SITE}
```

_Por meio de acesso direto ao banco de dados do Matomo_

```python
python main.py --dict_pdf {DICIONÁRIO PDF-PID} --dict_acronym {DICIONÁRIO ISSN-ACRÔNIMO --dict_language {DICIONÁRIO PID-FORMATO-IDIOMA} -u {STRING DE CONEXÃO COM MATOMO-DB} --period {PERIÓDO} --idsite {ID DO SITE}
```


## Insumos utilizados

__Dicionário PDF-PID__
 
__Dicionário ISSN-ACRÔNIMO__

__Dicionário PID-FORMAT-IDIOMA__

__Diretório PRÉ-TABELAS MATOMO__

