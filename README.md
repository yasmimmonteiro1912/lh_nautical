# ⚓ LH Nautical — Plataforma de Dados e IA End-to-End

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![XGBoost](https://img.shields.io/badge/XGBoost-006600?style=flat)
![LangChain](https://img.shields.io/badge/LangChain-1C3C3C?style=flat&logo=langchain&logoColor=white)
![MLflow](https://img.shields.io/badge/MLflow-0194E2?style=flat&logo=mlflow&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat&logo=powerbi&logoColor=black)

Plataforma de dados completa para a **LH Nautical**, varejista de equipamentos náuticos com lojas físicas e armazéns. O projeto cobre todo o ciclo do dado:

**ingestão → qualidade → modelagem dimensional → dashboard → previsão de demanda com ML → assistente de IA com RAG**

> Projeto desenvolvido em grupo (Grupo 2) no programa **Lighthouse da Indicium** (2026). Veja [Minha contribuição](#-minha-contribuição).

---

## 🗺️ Arquitetura

```
 Fontes                Bronze                 Silver / Gold (dbt)          Consumo
┌────────────┐   ┌──────────────────┐   ┌──────────────────────────┐   ┌──────────────────────┐
│ SQL Server │──▶│ PySpark (JDBC)   │   │ staging → intermediate   │──▶│ Power BI             │
│ API Bacen  │──▶│ PySpark (API)    │──▶│ → marts (star schema)    │──▶│ Forecast (XGBoost)   │
└────────────┘   │ Testes de DQ     │   └──────────────────────────┘   │ Chatbot RAG (LLM)    │
                 └──────────────────┘                                  └──────────────────────┘
        Orquestração: Databricks Asset Bundles (Serverless)  ·  CI/CD: Bitbucket Pipelines
```

| Componente | Tecnologia | Pasta |
| :--- | :--- | :--- |
| Ingestão (Bronze) | PySpark, Delta Lake | `lh_nautical_bronze/src/` |
| Qualidade de dados | PySpark | `lh_nautical_bronze/tests/` |
| Transformação (Silver/Gold) | dbt-databricks | `lh_nautical_dbt/` |
| Orquestração | Databricks Asset Bundles | `databricks.yml` |
| CI/CD | Bitbucket Pipelines | `bitbucket-pipelines.yml` |
| Dashboard | Power BI | `lh_nautical_dashboard/` |
| Previsão de demanda | Pandas, scikit-learn, XGBoost, statsmodels | `lh_nautical_predictive_modeling/` |
| Assistente de IA (RAG) | LangChain, Databricks Vector Search, Llama 3.3 70B, MLflow | `rag/` |
| App do chatbot | Gradio + Databricks Apps | `lh_nautical_chatbot/` |

---

## 🤖 Assistente de IA com RAG

Chatbot que responde, em português, perguntas de negócio a partir dos relatórios em PDF do projeto. Exemplos: previsão de demanda, crescimento dos centros de distribuição, desempenho financeiro.

**Pipeline:**

1. **Ingestão incremental de documentos** (`rag/incremental_pdf_to_docs.ipynb`): lê os PDFs de um Volume do Unity Catalog com `pdfplumber` e processa só os arquivos novos, com controle na tabela `docs_track`. Divide o texto com `RecursiveCharacterTextSplitter` (chunks de 1.000 caracteres, overlap de 200).
2. **Armazenamento** (`rag/create_needed_tables.ipynb`): tabelas Delta com Change Data Feed habilitado, para sincronizar com o índice vetorial.
3. **Indexação e busca:** embeddings com `databricks-qwen3-embedding-0-6b` e índice no **Databricks Vector Search**.
4. **Geração** (`rag/rag_chatbot.ipynb`): `RetrievalQA` (LangChain) com o LLM `databricks-meta-llama-3-3-70b-instruct` e um prompt de sistema que restringe as respostas ao contexto recuperado.
5. **Deploy:** a chain é registrada no **MLflow / Unity Catalog** e servida no **Databricks Model Serving**. A interface em **Gradio** (`lh_nautical_chatbot/app.py`) roda no **Databricks Apps** e trata retry e timeout do endpoint.

Instruções de uso do app em [`lh_nautical_chatbot/README.md`](lh_nautical_chatbot/README.md).

---

## 📈 Previsão de Demanda (Machine Learning)

Notebook: `lh_nautical_predictive_modeling/`

**Problema:** prever a demanda mensal por **produto × local** (1.009 produtos × 6 locais, 2020–2026) para apoiar reposição de estoque e a decisão de expansão dos centros de distribuição.

**Metodologia:**

- **Diagnóstico da série:** granularidade, completude e classificação de intermitência **Syntetos-Boylan**. A demanda é majoritariamente intermitente.
- **Grade calendário com zero-fill:** meses sem venda passam a ser zero, o que é necessário para lags e médias móveis corretos.
- **Features:** lags (1, 2, 3 e 12 meses), médias móveis (3 e 6 meses) e calendário (mês, trimestre).
- **Modelos comparados:** baseline de média móvel (SMA-3 / SMA-6), Random Forest, Random Forest + categoria e **XGBoost**.
- **Validação walk-forward:** 9 janelas de 3 meses entre 2022 e 2026, cobrindo alta e baixa temporada.
- **Previsão recursiva** de 3 meses (set–nov/2026) e análise de força de sazonalidade com `seasonal_decompose`.

**Resultados (walk-forward):**

| Métrica | Baseline (SMA-3) | XGBoost |
| :--- | :---: | :---: |
| WAPE médio | 1,605 | **1,440** (~10% melhor) |
| Desvio padrão do WAPE | 0,400 | **0,219** |
| Janelas vencidas (de 9) | 1 | **8** |

O XGBoost foi escolhido como modelo final. O ganho se concentra na baixa temporada (redução de 15–22% no WAPE). A feature de categoria foi testada e descartada por não trazer ganho preditivo.

---

## 🥉 Ingestão — Camada Bronze

| Pipeline | Arquivo | Estratégia |
| :--- | :--- | :--- |
| Câmbio (API Olinda / Bacen) | `ingestao_cambio.py` | Incremental com watermark automático (`MAX(dataHoraCotacao)`) e `MERGE` idempotente |
| Tabelas relacionais (SQL Server) | `ingestao_tabelas.py` | Full load via JDBC, descoberta dinâmica pelo `INFORMATION_SCHEMA`, coluna `ingestion_timestamp`, `mergeSchema` |

As credenciais ficam no **Databricks Secrets** e são resolvidas em tempo de execução, sem aparecer nos logs.

### Qualidade de dados (`test_bronze_quality.py`)

Testes de presença de tabelas, colunas de auditoria e volumetria. As tabelas são descobertas dinamicamente no catálogo. A abordagem é **fail-late**: todas as tabelas são validadas, o script gera um relatório consolidado e, se houver qualquer falha, marca o job como `FAILED` e impede que o dbt rode.

---

## 🥈🥇 Transformação — dbt (Silver e Gold)

| Camada | Modelos | Materialização | Schema |
| :--- | :---: | :--- | :--- |
| `staging` | 25 | view | silver |
| `intermediate` | 11 | view | silver |
| `marts` (dim, fct, agg) | 18 | table (Delta) | gold |

- **Star schema** com dimensões (`dim_customers`, `dim_products`, `dim_dates`, `dim_location`…), fatos (`fct_sales_items`, `fct_inventory_movements`, `fct_purchase_orders`…) e agregações (`agg_customer_rfm`, `agg_inventory_idle_stock`, `agg_sales_channel_monthly`…).
- Conversão cambial de pedidos de compra com a cotação do Bacen.
- Testes genéricos (`unique`, `not_null`, `relationships`…) e testes singulares de regra de negócio, como receita líquida ≤ receita bruta e coerência do valor de estoque.
- Seeds para correção e validação de nomes de produtos. Pacotes `dbt_utils` e `dbt_date`. Lint com `sqlfluff`.

```bash
cd lh_nautical_dbt
dbt deps
dbt build
```

---

## ⚙️ Orquestração e CI/CD

**Job `[Grupo 2] Pipeline End-to-End Nautical`** (`databricks.yml`), em ambiente Serverless, roda diariamente à 01:00 (America/Sao_Paulo) e envia alerta por e-mail em caso de falha:

1. `rodar_ingestao_sql_server` e `rodar_ingestao_cambio_bacen`, em paralelo
2. `rodar_testes_qualidade_bronze`
3. `rodar_dbt_silver_gold`

**CI/CD:** a cada push na `main`, o Bitbucket Pipelines instala o Databricks CLI e executa `databricks bundle validate` e depois `databricks bundle deploy`.

---

## 📊 Dashboard Power BI

O relatório consome a camada Gold e é dividido em visões **Estratégica** (C-Level), **Tática** (gestores) e **Operacional** (rotina), com navegação por botões, bookmarks e drill-through. Detalhes em [`lh_nautical_dashboard/README.md`](lh_nautical_dashboard/README.md).

---

## 👩‍💻 Minha contribuição

**Yasmim Monteiro:**

- **Tech lead da etapa de modelagem de dados:** conduzi as decisões de modelagem e desenvolvi o projeto dbt (staging, intermediate e marts) que alimenta o dashboard e os modelos de ML.
- **Ingestão Bronze:** pipeline de ingestão via JDBC do SQL Server.
- **Dashboard:** desenvolvi a visão **Operacional**, com os KPIs de ruptura de estoque, pedidos sem receita, cadastros pendentes e divergências de pagamento.
- **Previsão de demanda:** desenvolvi o notebook de modelagem preditiva (EDA, features, comparação de modelos, walk-forward e previsão).
- **RAG:** participei do desenvolvimento do assistente de IA.

---

## 📁 Estrutura do repositório

```
lh_nautical/
├── databricks.yml                     # Bundle: job end-to-end (Bronze → dbt)
├── bitbucket-pipelines.yml            # CI/CD: validate + deploy do bundle
├── resources/                         # Recursos extras do bundle (app do chatbot)
├── lh_nautical_bronze/
│   ├── src/                           # ingestao_tabelas.py, ingestao_cambio.py
│   └── tests/                         # test_bronze_quality.py
├── lh_nautical_dbt/
│   ├── models/{staging,intermediate,marts}/
│   ├── seeds/  tests/  macros/
│   └── dbt_project.yml
├── lh_nautical_predictive_modeling/   # Notebook de previsão de demanda
├── rag/                               # Notebooks do pipeline RAG
├── lh_nautical_chatbot/               # App Gradio (Databricks Apps)
└── lh_nautical_dashboard/             # Relatório Power BI (.pbix)
```

---

## 🚀 Próximos passos

- Registrar o modelo de demanda no MLflow e gerar previsões em lote numa tabela Gold.
- Avaliar o RAG com um conjunto de perguntas de referência (relevância e fidelidade das respostas).
- Expandir o assistente para gerar SQL sobre a camada Gold.
