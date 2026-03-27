# 🚀 Ingestão e Transformação de Dados Serverless: GitHub API Pipeline

Este projeto implementa um pipeline de dados ponta a ponta (end-to-end), desacoplado e escalável, utilizando uma arquitetura de **Data Lakehouse Serverless** na AWS. O objetivo é extrair eventos públicos da API do GitHub, realizar transformações complexas e disponibilizar os dados para análise via SQL de alta performance.

A orquestração de todo o fluxo é gerenciada pelo **Apache Airflow**, rodando em contêineres Docker.

---

## 🏗️ Arquitetura do Projeto

A imagem abaixo ilustra o fluxo dos dados, desde a origem (GitHub API) até a camada de consumo (Amazon Athena):

![Arquitetura do Projeto](arquitetura.png)

---

## 🛠️ Destaques Técnicos e Boas Práticas (Visão Senior)

Este projeto foi desenhado focando nos seguintes pilares de Engenharia de Dados:

1.  **Orquestração e Linhagem de Dados (Airflow):**
    * Uso de DAG idempotente para garantir que reexecuções não dupliquem dados.
    * Dependência estrita entre tasks (`task_lambda >> task_glue`), garantindo integridade.

2.  **Arquitetura Serverless (Lambda & Glue):**
    * **Lambda (Ingestão):** Processamento leve e rápido para download de dados semi-estruturados (JSON).
    * **Glue (Transformação):** Cluster Spark gerenciado para lidar com transformações complexas (flattening e conversão de tipos) em escala utilizando PySpark.

3.  **Otimização de Custos e Performance (Parquet):**
    * Conversão do JSON bruto (Bronze Layer) para **Parquet** (Silver Layer) com compressão Snappy.
    * O formato colunar reduz drasticamente o tamanho de armazenamento e o custo de escaneamento de dados no Athena.

4.  **Desacoplamento de Computação e Armazenamento (Athena):**
    * Uso do Amazon Athena como motor de query ad-hoc (Presto/Trino).
    * Separação total entre a ferramenta de processamento e a ferramenta de consulta, garantindo flexibilidade para ferramentas de BI.

---

## 📂 Estrutura do Data Lake (S3)

O bucket do S3 (`marcos-data-glue-lab-2026`) está organizado seguindo o conceito de arquitetura medalhão:

* **`raw/`**: (Bronze Layer) Arquivos JSON brutos.
* **`transformed/github_events/`**: (Silver Layer) Dados otimizados em formato Parquet.

---

## 🖥️ Como Consultar os Dados (Athena)

Execute o seguinte comando SQL no editor do Athena para criar a tabela de consumo:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS default.github_events_gold (
  event_id string,
  event_type string,
  created_at_timestamp timestamp,
  actor_username string,
  repo_fullname string,
  repo_url string
)
STORED AS PARQUET
LOCATION 's3://marcos-data-glue-lab-2026/transformed/github_events/';
