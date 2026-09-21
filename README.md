
# ⚓ LH Nautical — Data Platform End-to-End 🚀

Este repositório contém a infraestrutura como código (IaC) e os scripts de engenharia de dados necessários para orquestrar e executar a **plataforma de dados completa** da **LH Nautical**, desde a ingestão automatizada na **Camada Bronze** até a transformação em **dbt**, visualização em **Power BI**, análise preditiva e um **assistente de IA baseado em RAG**.

A solução utiliza **PySpark** para processamento distribuído, **Databricks Asset Bundles (DABs)** para gerenciamento de recursos em nuvem, **dbt** para transformações nas camadas Silver e Gold, **Bitbucket Pipelines** para entrega contínua (CI/CD), **Power BI** para visualização de dados, **MLflow** para modelagem preditiva e **RAG (Retrieval-Augmented Generation)** para o assistente de IA.

---

## 🏗️ Arquitetura e Componentes do Projeto

A plataforma de dados da LH Nautical segue a arquitetura **Medallion** (Bronze → Silver → Gold) e engloba os seguintes blocos:

| Camada / Componente | Tecnologia | Descrição |
| :--- | :--- | :--- |
| **Bronze** | PySpark + DABs | Ingestão automatizada de dados do SQL Server e API do Banco Central |
| **Silver** | dbt (Databricks adapter) | Limpeza, padronização e enriquecimento dos dados |
| **Gold** | dbt (Databricks adapter) | Modelagem dimensional voltada para análise e consumo |
| **Orquestração** | Databricks Workflows (DABs) | Agendamento e encadeamento de tarefas |
| **Qualidade de Dados** | PySpark + Testes automatizados | Barreira de segurança entre camadas |
| **Visualização** | Power BI | Dashboards interativos para o negócio |
| **Análise Preditiva** | Databricks Notebooks + PySpark | Modelos de forecast para demanda e receita |
| **Assistente de IA** | RAG (Retrieval-Augmented Generation) | Chatbot que responde perguntas sobre os dados da plataforma |

O ecossistema de ingestão é dividido em duas fontes principais de dados, ambas configuradas para salvar os dados no formato Delta com suporte a evolução de esquema (`mergeSchema`).

---

## 🛠️ Detalhamento dos Pipelines de Ingestão

### 1. Ingestão de Câmbio (API Olinda - Bacen)
* **Arquivo:** `src/bronze/ingestao_cambio.py`
* **Tipo de Carga:** Incremental via **Smart Watermark**.
* **Mecanismo de Escrita:** Delta Merge (Upsert).

O script consome a API do Banco Central para capturar as cotações diárias do dólar. Caso nenhuma data seja fornecida via parâmetro, ele executa uma consulta automatizada (`SELECT MAX(dataHoraCotacao)`) na tabela destino `bronze.taxas_cambio` para identificar o último registro processado e baixa apenas o incremento até o dia atual. O comando `Merge` garante a idempotência do pipeline, evitando duplicidade de registros.

### 2. Ingestão Dinâmica de Tabelas Relacionais (SQL Server)
* **Arquivo:** `src/bronze/ingestao_tabelas.py`
* **Tipo de Carga:** Carga Total (Full Load) programada.
* **Mecanismo de Escrita:** Delta Overwrite com `mergeSchema`.

O script foi desenhado com foco em **metadados dinâmicos**. Ele se conecta ao banco de dados de origem via JDBC e consulta o `INFORMATION_SCHEMA.TABLES` para descobrir ativamente as tabelas existentes. Ele adiciona a coluna técnica `ingestion_timestamp` em cada linha antes de sobrescrever as tabelas na camada Bronze.

---

## 🚀 Orquestração com Databricks Asset Bundles (DABs)

O arquivo `databricks.yml` declara o workflow e os recursos necessários para o ambiente de Desenvolvimento (`dev`), utilizando computação totalmente gerenciada (**Serverless**).

### Detalhes do Workflow: `[Grupo 2] Pipeline Ingestao Bronze JDBC`

* **Agendamento (Cron):** Executado diariamente às **01:00 AM** (Horário de Brasília - `America/Sao_Paulo`).
* **Observabilidade:** Alertas de e-mail automatizados configurados para disparar **em caso de falha** (`on_failure`) para o time de engenharia responsável:
  * `yasmim.monteiro@indicium.ai`
  * `caua.braga@indicium.ai`
  * `iasmin.santiago@indicium.ai`

### Sequência de Tarefas (Tasks)

| Ordem | Nome da Tarefa | Script Origem | Parâmetros / Configuração |
| :--- | :--- | :--- | :--- |
| **1** | `rodar_ingestao_sql_server` | `ingestao_tabelas.py` | Recebe as credenciais via Databricks Secrets (`grupo2_cofre`) e injeta uma lista restrita de **24 tabelas** de negócio para controle de FinOps e Governança. |
| **2** | `rodar_ingestao_cambio_bacen` | `ingestao_cambio.py` | Executado sem parâmetros de data para forçar a inicialização automática do *Smart Watermark*. |
| **3** | `rodar_testes_qualidade_bronze` | `test_bronze_quality.py` | Orquestração centralizada com abordagem *Fail-Late*. Avalia a presença, estrutura (linhagem) e volumetria de todas as tabelas geradas nas etapas anteriores, agindo como barreira de segurança para as camadas subsequentes. |

> 🔒 **Segurança:** O script intercepta automaticamente strings que utilizam o padrão `{{secrets/scope/key}}` e resolve as credenciais em tempo de execução via `dbutils.secrets`, omitindo exibições de senhas nos logs do cluster.

---
## 🧪 Framework de Qualidade de Dados (Data Quality)

O projeto conta com um framework automatizado de **Data Quality** desenvolvido em PySpark. Ele executa validações físicas, estruturais e de governança logo após as ingestões, atuando como uma barreira de segurança para garantir a integridade dos dados antes que fiquem disponíveis para as camadas subsequentes (*Silver* e *Gold*).

* **Arquivo:** `tests/test_bronze_quality.py` (ou o caminho correspondente no seu repositório)
* **Mecanismo de Descoberta:** Totalmente dinâmico via Metastore (`spark.catalog.listTables`). O motor identifica automaticamente as tabelas ingeridas e persistidas na camada Bronze em tempo de execução, eliminando a necessidade de manutenção ou atualização manual do arquivo de testes ao adicionar novas origens.

### Detalhamento da Suíte de Testes (Asserts)

1. **Presença de Tabelas no Catálogo (`test_presenca_de_tabelas`)**
   * **Utilidade:** Valida se existem objetos físicos gerados dentro do esquema alvo.
   * **O que previne:** Mitiga falsos positivos de sucesso em processos de ETL que falham de forma silenciosa e terminam sem gerar nenhuma tabela. Evita quebras em cascata por tentativa de leitura de esquemas inexistentes.

2. **Estrutura e Linhagem Técnica (`test_estrutura_e_auditoria`)**
   * **Utilidade:** Inspeciona o esquema das tabelas em busca de colunas obrigatórias de governança (como `ingestion_timestamp`, `dh_ingestao` ou `ingestao_timestamp`).
   * **O que previne:** Degradação da governança de dados (*Data Lineage*). Garante que nenhum dado seja exposto no Lakehouse sem o carimbo temporal que identifica exatamente quando o registro foi extraído da origem.

3. **Volumetria de Dados e Carga Zerada (`test_volumetria_dados`)**
   * **Utilidade:** Avalia a integridade física através de uma contagem de linhas (`count()`), validando se os conectores (JDBC/APIs) realmente trouxeram registros válidos.
   * **O que previne:** Evita que falhas de rede, quebras de conectores ou aplicação de filtros incorretos façam com que dados históricos consolidados sejam sobrescritos por tabelas vazias (volumetria zerada).

### 🏗️ Estratégia de Resiliência e FinOps

* **Abordagem *Fail-Late* (Falha Tardia):** Cada tabela é auditada de forma isolada dentro de blocos `try/except`. Caso uma tabela apresente inconsistências, a falha é registrada internamente, mas o script continua validando as tabelas remanescentes. Isso garante um relatório completo de erros em uma única execução do Job.
* **Bloqueio Downstream:** Ao final da validação, se houver qualquer inconformidade no relatório consolidado, o script gera um relatório de falhas e força um encerramento sistêmico via SystemExit. Isso carimba o erro real no terminal, altera o status do Job do Databricks para FAILED e ativa o gatilho de e-mails de alerta para o time, impedindo automaticamente que os dados inconsistentes avancem no pipeline.
* **Práticas de FinOps:** O script utiliza o método `SparkSession.builder.getOrCreate()`, o que permite o reaproveitamento otimizado do cluster Serverless ativo, reduzindo o tempo de inicialização e o consumo desnecessário de créditos computacionais.

## 🔄 Esteira de Integração e Entrega Contínuas (CI/CD)

A automação de deploy é gerenciada pelo **Bitbucket Pipelines** (`bitbucket-pipelines.yml`). Sempre que modificações são consolidadas na branch **`main`**, o pipeline executa os seguintes passos dentro de um ambiente isolado com a imagem `python:3.11-slim`:

1. **Preparação do Ambiente:** Atualiza o contêiner e garante a instalação do `curl` e `unzip`.
2. **Instalação da CLI:** Faz o download e instala a versão oficial do **Databricks CLI**.
3. **Validação Técnica (`databricks bundle validate`):** Valida a integridade do arquivo `databricks.yml`, checando a sintaxe e as permissões antes de subir qualquer alteração.
4. **Implantação Automatizada (`databricks bundle deploy`):** Publica os novos códigos e atualiza o agendamento do Job diretamente no workspace do Databricks de destino.

---

## 🥈 Transformação de Dados com dbt (Camadas Silver e Gold)

Após a ingestão na Camada Bronze, os dados são transformados e modelados utilizando **dbt** com o adaptador do Databricks (`dbt-databricks`). As transformações são versionadas, testadas e documentadas como código.

### Camada Silver — Limpeza e Padronização

* **Arquivo base:** `dbt_lh_nautical/models/silver/`
* **Objetivo:** Aplicar regras de limpeza, padronização de tipos, remoção de duplicidades e normalização de nomes de colunas.
* **Características:**
  * Conversão de tipos de dados para formatos canônicos.
  * Tratamento de nulos e valores sentinelas.
  * Padronização de nomenclatura (`snake_case`).
  * Deduplicação via `row_number()` ou `qualify`.

### Camada Gold — Modelagem Dimensional

* **Arquivo base:** `dbt_lh_nautical/models/gold/`
* **Objetivo:** Construir o modelo dimensional (Star Schema) voltado para consumo analítico e BI.
* **Características:**
  * Criação de tabelas dimensão (`dim_*`) e fato (`fct_*`).
  * Aplicação de regras de negócio e métricas derivadas.
  * Materialização como tabelas Delta otimizadas no Databricks.

### Testes e Documentação dbt

* **Schema tests:** `unique`, `not_null`, `relationships` e testes customizados em Python/YAML.
* **Documentação:** `dbt docs generate` gera a documentação automática do lineage e das descrições de cada modelo.
* **Execução:**
  ```bash
  dbt deps
  dbt run --profiles-dir .
  dbt test --profiles-dir .
  dbt docs generate --profiles-dir .
  ```

---

## 📊 Visualização de Dados no Power BI

A camada Gold é consumida diretamente pelo **Power BI** através do conector nativo do Databricks (ou via Unity Catalog), disponibilizando dashboards interativos para a área de negócio.

### Dashboards Construídos

* **Visão Geral de Vendas:** KPIs de faturamento, volume de pedidos e ticket médio ao longo do tempo.
* **Análise de Câmbio:** Cotações do dólar com filtros por período e comparação de médias móveis.
* **Desempenho Operacional:** Indicadores de eficiência por região, categoria e canal de venda.
* **Governança e Qualidade:** Painel de monitoramento da volumetria das tabelas e status dos pipelines de ingestão.

### Características Técnicas

* **Atualização:** Agendamento de refresh automático sincronizado com o término do pipeline Gold.
* **Segurança:** Row-Level Security (RLS) configurada via Unity Catalog para garantir acesso granular por perfil de usuário.
* **Conexão:** DirectQuery ou Import Mode, conforme requisito de latência de cada dashboard.

---

## 📈 Análise Preditiva

A plataforma conta com modelos de **análise preditiva** desenvolvidos em PySpark e rastreados via **MLflow**, integrados ao pipeline de dados para gerar previsões automatizadas.

### Modelos Desenvolvidos

| Modelo | Finalidade | Algoritmo |
| :--- | :--- | :--- |
| **Forecast de Receita** | Previsão de faturamento mensal/trimestral | Séries temporais / Regressão |
| **Previsão de Demanda** | Estimativa de volume de pedidos por categoria | Regressão / Gradient Boosting |

### Fluxo do Pipeline Preditivo

1. **Extração de Features:** Leitura das tabelas Gold via Spark DataFrame.
2. **Treinamento:** Treino do modelo e logging de parâmetros, métricas e artefatos no MLflow.
3. **Inferência:** Aplicação do modelo em lote (`batch inference`) e escrita dos resultados em uma tabela de previsões (`gold.previsoes_*`).
4. **Monitoramento:** Versionamento de modelos no MLflow Model Registry e comparação de versões.

### Arquivos Relevantes

* `src/ml/forecast_receita.py` — Treinamento e inferência do forecast de receita.
* `src/ml/previsao_demanda.py` — Treinamento e inferência do modelo de demanda.
* `src/ml/utils.py` — Funções de feature engineering e métricas de avaliação.

---

## 🤖 Assistente de IA com RAG (Retrieval-Augmented Generation)

A solução inclui um **assistente de IA baseado em RAG** que permite aos usuários do negócio fazer perguntas em linguagem natural sobre os dados da plataforma e receber respostas contextualizadas.

### Arquitetura do RAG

1. **Indexação:** Os metadados das tabelas Gold (esquemas, descrições de colunas, dicionário de dados e dashboards) são indexados em um **Vector Search Index** no Databricks.
2. **Embeddings:** Geração de embeddings a partir da documentação semântica dos dados, utilizando modelos de embedding do Databricks Foundation Model APIs.
3. **Retrieval:** Quando o usuário faz uma pergunta, o sistema converte a query em embedding e recupera os trechos mais relevantes do índice via busca de similaridade.
4. **Geração:** Os trechos recuperados são injetados no prompt de um LLM (Foundation Model) que gera a resposta final em linguagem natural.

### Capacidades do Assistente

* 📋 **Q&A sobre dados:** Responde perguntas como "Qual foi o faturamento do último trimestre?" ou "Qual a categoria com maior volume de pedidos?".
* 🔍 **Exploração de esquema:** Descreve tabelas, colunas e relacionamentos disponíveis na camada Gold.
* 📊 **Geração de SQL:** Pode sugerir queries SQL com base na pergunta do usuário e no esquema das tabelas.
* 📈 **Interpretação de métricas:** Explica KPIs e métricas calculadas nos dashboards.

### Arquivos Relevantes

* `src/rag/indexacao_vector_search.py` — Criação e atualização do índice Vector Search.
* `src/rag/assistant_rag.py` — Lógica de retrieval + geração de respostas.
* `src/rag/config.yaml` — Configuração de modelos de embedding, LLM e parâmetros de busca.

---

## 🗺️ Visão Geral do Fluxo de Dados

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌──────────────┐     ┌───────────────┐
│   Fontes     │     │   Bronze     │     │   Silver     │     │    Gold      │     │  Consumo      │
│             │     │             │     │             │     │             │     │               │
│ SQL Server  │────▶│ Ingestão    │────▶│ dbt: limpeza│────▶│ dbt: star   │────▶│ Power BI      │
│ API Bacen   │────▶│ PySpark     │     │ padronização│     │ schema      │     │ Análise       │
│             │     │ DABs        │     │             │     │             │     │ Preditiva     │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘     │ Assistente    │
                                                                   │            │ IA (RAG)      │
                                                                   └────────────▶└───────────────┘
```

---

## 📋 Pré-requisitos para Execução Local / Desenvolvimento

Caso queira debugar os scripts localmente através do seu ambiente de desenvolvimento ou terminal, garanta os seguintes requisitos:

* Interpretador Python versão `3.11.x` instalado.
* Instalação do PySpark (`pip install pyspark`).
* Driver JDBC do SQL Server configurado no ambiente de execução do Spark.
* CLI do Databricks autenticada localmente para testes com Bundles.
* **dbt** instalado com o adaptador do Databricks: `pip install dbt-databricks`.
* **Power BI Desktop** (para edição e publicação de relatórios locais).
* **MLflow** instalado (`pip install mlflow`) para rastreamento de modelos preditivos.
* Acesso ao **Vector Search** e às **Foundation Model APIs** do Databricks para o assistente RAG.

---

## 📁 Estrutura de Diretórios

```
lh-nautical/
├── src/
│   ├── bronze/
│   │   ├── ingestao_cambio.py
│   │   └── ingestao_tabelas.py
│   ├── ml/
│   │   ├── forecast_receita.py
│   │   ├── previsao_demanda.py
│   │   └── utils.py
│   └── rag/
│       ├── indexacao_vector_search.py
│       ├── assistant_rag.py
│       └── config.yaml
├── dbt_lh_nautical/
│   ├── models/
│   │   ├── silver/
│   │   └── gold/
│   ├── dbt_project.yml
│   └── profiles.yml
├── tests/
│   └── test_bronze_quality.py
├── databricks.yml
├── bitbucket-pipelines.yml
└── README.md
```

---

## 🚀 Roadmap e Próximos Passos

* [ ] Expansão das fontes de ingestão (APIs de terceiros, SFTP).
* [ ] Modelos preditivos adicionais (churn, otimização de estoque).
* [ ] Integração do assistente RAG com canais de Teams/Slack.
* [ ] Governança automatizada de dados com Unity Catalog tags e linhagem end-to-end.