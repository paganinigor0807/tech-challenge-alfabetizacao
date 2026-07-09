# Tech Challenge - Pipeline de Engenharia de Dados

## Objetivo

Este projeto implementa uma arquitetura de Engenharia de Dados utilizando processamento **Batch** e **Streaming**, organizados em uma arquitetura em camadas (**Bronze, Silver e Gold**).

O objetivo é construir um pipeline capaz de:

- Extrair dados de diferentes fontes;
- Padronizar e enriquecer os dados;
- Consolidar informações analíticas;
- Disponibilizar tabelas dimensionais e fatos para análise.

---

# Arquitetura

```
                +----------------+
                | Banco SQL      |
                +-------+--------+
                        |
                        v
                Bronze (Batch)
                        |
                        |
                        |
 Streaming Producer      |
(simulador_streaming)    |
        |               |
        v               |
temp_streaming_input    |
        |               |
        v               |
 Streaming Consumer      |
        |               |
        v               |
 Bronze (Streaming) -----+
                        |
                        v
                     Silver
                        |
                        v
                      Gold
```

---

# Estrutura do Projeto

```
src/
└── techchallenge/
    ├── batch.py
    ├── streaming.py
    ├── pipeline.py
    ├── batch_orchestrator.py
    ├── streaming_orchestrator.py
    │
    ├── extract/
    ├── transform/
    ├── load/
    ├── monitoring/
    ├── config/
    │
    └── streaming/
        ├── simulador_streaming.py
        └── streaming_consumer.py

data/
├── bronze/
│   ├── batch/
│   └── streaming/
├── silver/
├── gold/
├── temp_streaming_input/
└── temp_streaming_processed/
```

---

# Camadas

## Bronze

Responsável pela ingestão dos dados.

### Batch

Extrai dados diretamente do banco SQL e grava em formato Parquet.

### Streaming

Recebe eventos em formato JSON, realiza a ingestão e converte para Parquet.

Nenhuma transformação de negócio é realizada nesta camada.

---

## Silver

Responsável pelo tratamento dos dados.

Nesta etapa são realizados:

- Padronização de nomes de colunas;
- Remoção de espaços;
- Remoção de duplicidades;
- Enriquecimento através de tabelas de referência;
- Consolidação dos dados Batch + Streaming.

---

## Gold

Responsável pela modelagem analítica.

São produzidas:

### Dimensões

- Município
- UF
- Rede
- Tempo

### Tabelas Fato

- Alfabetização Município
- Alfabetização UF
- Alfabetização Brasil

As tabelas fato possuem indicadores como:

- alunos avaliados
- alunos alfabetizados
- alunos presentes
- provas preenchidas
- taxa de alfabetização
- taxa de presença
- taxa de preenchimento

---

# Fluxo Batch

```
Banco SQL
    │
    ▼
Bronze Batch
    ▼
Silver
    ▼
Gold
```

Execução:

```bash
python -m src.techchallenge.batch
```

---

# Fluxo Streaming

## 1. Iniciar o simulador

```bash
python -m src.techchallenge.streaming.simulador_streaming
```

O simulador gera continuamente arquivos JSON simulando a chegada de novos eventos.

---

## 2. Processar o Streaming

```bash
python -m src.techchallenge.streaming
```

Fluxo:

```
JSON
    │
    ▼
Streaming Consumer
    ▼
Bronze Streaming
    ▼
Silver
    ▼
Gold
```

---

# Monitoramento

Cada etapa do pipeline registra:

- tempo de execução;
- quantidade de registros de entrada;
- quantidade de registros de saída;
- quantidade de colunas processadas;
- arquivos produzidos.

Ao final da execução é apresentado um resumo da pipeline.

---

# Tecnologias

- Python
- Pandas
- SQLAlchemy
- Parquet
- PyArrow

---

# Modelo Dimensional

## Dimensões

- Dim Município
- Dim UF
- Dim Rede
- Dim Tempo

## Fatos

- Fato Alfabetização Município
- Fato Alfabetização UF
- Fato Alfabetização Brasil

---

# Execução

## Batch

```bash
python -m src.techchallenge.batch
```

## Simulador Streaming

```bash
python -m src.techchallenge.streaming.simulador_streaming
```

## Pipeline Streaming

```bash
python -m src.techchallenge.streaming
```

---

# Autor

Tech Challenge – Pós-graduação em Engenharia de Dados