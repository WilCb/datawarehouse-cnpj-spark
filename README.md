# 📊 Projeto Data Warehouse com PySpark e Arquitetura Medalhão

Este repositório contém a atividade prática desenvolvida como parte do treinamento do projeto de **Data Warehouse**, utilizando **Apache Spark 4.0.0**, **Delta Lake** e a **Arquitetura Medalhão**.

## 📁 Estrutura da Arquitetura Medalhão

A pipeline foi construída com base em 4 camadas principais de dados:

- `landing/` — Dados brutos coletados diretamente dos arquivos CSV da Receita Federal ou APIs públicas.
- `raw/` — Dados estruturados com schemas definidos e persistidos em Delta Lake.
- `trusted/` — Dados enriquecidos, com validações e joins entre dimensões e fatos.


---

## 🧠 Conceitos Estudados

- Arquitetura de Dados
  - Data Lakes e Data Warehouses
  - Arquitetura Medalhão
  - Tabelas Fato e Dimensão
- PySpark
  - Leitura e escrita com Delta Lake
  - Transformações, joins e tratamento de dados
- Delta Lake
  - Versionamento e `time travel`
- Consumo de Dados Públicos
  - API
  - Arquivos CSV do site da Receita Federal

---

## 🛠️ Tecnologias Utilizadas

- **Sistema Operacional:** Ubuntu via WSL (Windows Subsystem for Linux)
- **Python:** 3.12+
- **Apache Spark:** 4.0.0
- **Java:** 17
- **Delta Lake:** integrado via `delta-spark`
- **Notebooks:** JupyterLab / Jupyter Notebook

### Principais bibliotecas (requirements.txt)
```txt
pyspark==4.0.0
delta-spark==4.0.0
requests
jupyterlab
ipython
```

## 📓 Notebooks do Projeto

| Notebook                         | Descrição                                                                                      |
| -------------------------------- | ---------------------------------------------------------------------------------------------- |
| `01_coleta_dados.ipynb`          | Coleta dos dados públicos (arquivos CSV e APIs) e armazenamento na camada `landing/`.          |
| `02_preparacao_raw.ipynb`        | Limpeza inicial e escrita dos dados estruturados na camada `raw/`.                             |
| `03_transformacao_trusted.ipynb` | Joins com tabelas dimensão, enriquecimento e validação de dados. Escrita na camada `trusted/`. |
| `04_analise_resultados.ipynb`    | Análises exploratórias e exibição de versões com `DESCRIBE HISTORY`.                           |

## 🧩 Modelo Dimensional

A tabela fato é composta pelos dados das empresas (CNPJ), e relaciona-se com as seguintes dimensões:

| Tabela Fato | Chave                      | Tabela Dimensão   |
| ----------- | -------------------------- | ----------------- |
| CNPJ        | `codigo_cnae`              | CNAE              |
| CNPJ        | `codigo_municipio`         | Municípios (IBGE) |
| CNPJ        | `codigo_natureza_juridica` | Natureza Jurídica |
| CNPJ        | `cnpj`                     | Simples Nacional  |

## 🧪 Versionamento de Dados

- As camadas **RAW** e **TRUSTED** utilizam o formato Delta Lake.

- Foi realizado versionamento de dados com múltiplas escritas (append ou overwriteSchema) para fins de demonstração.

- Uso de **DESCRIBE HISTORY** para explorar alterações e reprocessamentos.

## 📂 Organização de Diretórios

```bash
.
├── LND/         # Dados brutos (JSON ou CSV)
├── RAW/             # Dados estruturados (Delta)
├── TRS/         # Dados tratados e confiáveis (Delta)
├── notebooks/
│   ├── 01_coleta_dados.ipynb
│   ├── 02_preparacao_raw.ipynb
│   ├── 03_transformacao_trusted.ipynb
│   └── 04_analise_resultados.ipynb
├── requirements.txt
└── README.md
```

## ✅ Requisitos para execução

Antes de rodar os notebooks:

1. Instale o Java 17 e o Apache Spark 4.0.0 com suporte a Delta Lake.

2. Configure seu ambiente Python com os pacotes listados em requirements.txt.

3. Utilize um terminal Linux (via WSL, se estiver no Windows) para evitar conflitos de dependência.

4. É necessário realizar o download manual dos arquivos CSV através do site do governo (link disponível no notebook `01_coleta_de_dados`.ipynb). Após o download, os arquivos devem ser salvos conforme a estrutura da camada RAW, criando as pastas necessárias conforme demonstrado.

## 👨‍💻 Autor

Este projeto foi desenvolvido por **Williams Araujo (WilCb)** como parte do **treinamento prático da Residência em TIC da UFAL/EASY em parceria com o SENAI/AL**, com foco em **Engenharia de Dados**.

O objetivo desta atividade é **nivelar o conhecimento técnico dos residentes**, preparando-os para o desenvolvimento do **projeto real** que será realizado nas etapas seguintes da residência.

