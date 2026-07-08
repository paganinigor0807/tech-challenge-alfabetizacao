# Tech Challenge - Pipeline de Engenharia de Dados para Avaliação da Alfabetização

## Objetivo

Este projeto implementa uma pipeline de Engenharia de Dados para
extração, tratamento, monitoramento e disponibilização de dados da
Avaliação da Alfabetização. Os dados são extraídos do Google BigQuery e
organizados em uma arquitetura em camadas (Bronze, Silver e futuramente
Gold).

## Arquitetura

``` text
BigQuery
      │
      ▼
 Bronze Pipeline
      │
      ▼
Bronze (Parquet)
      │
      ▼
 Silver Pipeline
      │
      ▼
Silver (Parquet)
      │
      ▼
Gold (Em desenvolvimento)
```

## Estrutura do Projeto

``` text
credentials/
data/
 ├── bronze/
 ├── silver/
 └── gold/
docs/
logs/
src/
tests/
README.md
```

## Fonte dos Dados

Dataset: `basedosdados.br_inep_avaliacao_alfabetizacao`

Tabelas: - alunos - municipio - uf - meta_brasil - meta_municipio -
meta_uf

## Camada Bronze

-   Extração dos dados do BigQuery
-   Armazenamento em Parquet
-   Preservação dos dados originais
-   Monitoramento da execução

## Camada Silver

-   Padronização dos nomes das colunas
-   Remoção de espaços em branco
-   Remoção de duplicidades
-   Enriquecimento utilizando tabelas de domínio (CSV)

## Camada Gold

Em desenvolvimento.

## Monitoramento

As pipelines Bronze e Silver registram:

-   Horário de início e término
-   Tempo de execução
-   Quantidade de linhas
-   Quantidade de colunas
-   Throughput
-   Tamanho dos arquivos
-   Variação de armazenamento
-   Status da execução

Os registros são gravados em `logs/pipeline.log`.

## FinOps

Boas práticas implementadas:

-   Utilização do formato Parquet
-   Monitoramento do tamanho dos arquivos
-   Medição de throughput
-   Reutilização de DataFrames
-   Enriquecimento condicional
-   Histórico de execução

## Tratamento de Erros

-   Captura de exceções
-   Registro em log
-   Identificação da tabela que falhou

## Tecnologias

-   Python
-   Pandas
-   Google BigQuery
-   Parquet
-   Logging
-   Pathlib

## Como executar

### Instalar dependências

``` bash
pip install -r requirements.txt
```

### Executar Bronze

``` bash
python -m tests.test_bronze_pipeline
```

### Executar Silver

``` bash
python -m tests.test_silver_pipeline
```

## Próximas Evoluções

-   Camada Gold
-   Modelo dimensional
-   Dashboards analíticos
-   Indicadores de alfabetização
