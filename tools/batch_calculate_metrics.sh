#!/usr/bin/env bash

# Função para exibir ajuda do script

# Imprime a função do script
usage(){
  echo ""
  echo "Script para calcular métricas COUNTER em paralelo"
  echo "================================================="
  echo "Persiste em disco os resultados obtidos"
  echo "Arquivos r5_metrics_YYYYMMDD contêm os valores das métricas"
  echo "Arquivos r5_hits_YYYYMMDD contêm os acessos (hits) associados às métricas"
  echo "Sugere-se t = 5 para um computador com 16 GB de RAM e 6 cores"
  echo ""
}

usage_example(){
  echo "./batch_calculate.sh" \
       "-d testing-batch/" \
       "-u mysql://user:pass@localhost:3306/matomo" \
       "-s 1 -c scl" \
       "-p pdf-pid-2021-01-09.data" \
       "-a issn-acronym-2021-01-09.data" \
       "-l pid-format-lang-2021-01-09.data" \
       "-y pid-dates-2021-01-09.data" \
       "-t 4"
}

# Imprime uma ajuda sobre o script
help(){
  echo ""
  echo "Uso: $0 -d dir_pretables -u matomo_uri -s id_site -c collection -t threads -p dict_pdf -a dict_acronym -l dict_language -y dict_yop"
  echo -e "\t-d Diretório com as pré-tabelas"
  echo -e "\t-u String de conexão com o banco de dados Matomo"
  echo -e "\t-s ID do site no Matomo"
  echo -e "\t-c Acrônimo da coleção dos dados das pré-tabelas"
  echo -e "\t-t Número de processos a serem executados em paralelo"
  echo -e "\t-p Dicionário PDF --> PID"
  echo -e "\t-a Dicionário Acrônimo de Periódico --> ISSN"
  echo -e "\t-l Dicionário PID --> Idioma"
  echo -e "\t-y Dicionário PID --> Ano de Publicação"
  echo ""
  echo "Exemplo:"
  usage_example
  exit 1
}

# Imprime os parâmetros lidos
print_args(){
  echo ""
  echo -e "* Configuração realizada"
  echo -e "------------------------"
  echo ""
  echo -e "* dir_pretables:\t" "$dir_pretables"
  echo -e "* matomo_uri:\t " "$matomo_uri"
  echo -e "* id_site:\t " "$id_site"
  echo -e "* collection:\t " "$collection"
  echo -e "* threads:\t " "$threads"
  echo -e "* dict_pdf:\t " "$dict_pdf"
  echo -e "* dict_acronym:\t " "$dict_acronym"
  echo -e "* dict_language:\t " "$dict_language"
  echo -e "* dict_yop:\t " "$dict_yop"
  echo ""
}

# Função que cria um semáforo
open_sem(){
  mkfifo pipe-$$
  exec 3<>pipe-$$
  rm pipe-$$
  local i=$1
  for((;i>0;i--)); do
    printf %s 000 >&3
  done
}

# Função auxiliar de semárofo
run_with_lock(){
  local x
  read -u 3 -n 3 x && ((0==x)) || exit $x
  (
    ( "$@"; )
    printf '%.3d' $? >&3
  )&
}

# Imprime uso do scritp
usage

# Lê parâmetros de comando
while getopts d:u:s:c:t:p:a:l:y: flag
do
  case "${flag}" in
    d) dir_pretables=${OPTARG};;
    u) matomo_uri=${OPTARG};;
    s) id_site=${OPTARG};;
    c) collection=${OPTARG};;
    t) threads=${OPTARG};;
    p) dict_pdf=${OPTARG};;
    a) dict_acronym=${OPTARG};;
    l) dict_language=${OPTARG};;
    y) dict_yop=${OPTARG};;
    ? ) help;;
  esac
done

# Checa se argumentos foram preenchidos
if [ -z "$dir_pretables" ] || [ -z "$matomo_uri" ] || [ -z "$id_site" ] || [ -z "$collection" ] || [ -z "$threads" ] || [ -z "$dict_pdf" ] || [ -z "$dict_acronym" ] || [ -z "$dict_language" ] || [ -z "$dict_yop" ]
then
  echo ""
  echo "WARNING: Faltam argumentos";
  print_args
  help
fi

print_args

# Inicia execução do script
open_sem $threads
for f in "$dir_pretables"/*.tsv; do
  echo ""
  echo "* Iniciando para arquivo $f"
  echo ""
  run_with_lock .venv/bin/python calculate.py \
  -c $collection \
  -i $id_site \
  --dict_pdf $dict_pdf \
  --dict_acronym $dict_acronym \
  --dict_language $dict_language \
  --dict_yop $dict_yop -u $matomo_uri \
  --pretables "$f"
done
