
# ⚓ Pipeline de Ingestão LH Nautical - Camada Bronze 🚀

Este repositório contém a infraestrutura como código (IaC) e os scripts de engenharia de dados necessários para orquestrar e executar a ingestão automatizada de dados para a **Camada Bronze** do Delta Lake da **LH Nautical**. 

O projeto utiliza **PySpark** para processamento distribuído, **Databricks Asset Bundles (DABs)** para gerenciamento de recursos em nuvem e **Bitbucket Pipelines** para garantir a entrega contínua (CI/CD).

---

## 🏗️ Arquitetura e Componentes do Projeto

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
  * `erivan.oliveira@indicium.ai`
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
* **Bloqueio Downstream:** Ao final da validação, se houver qualquer inconformidade no relatório consolidado, o script dispara uma interrupção crítica (`RuntimeError`). Isso altera o status do Job do Databricks para **FAILED**, impedindo automaticamente que os dados inconsistentes avancem no pipeline.
* **Práticas de FinOps:** O script utiliza o método `SparkSession.builder.getOrCreate()`, o que permite o reaproveitamento otimizado do cluster Serverless ativo, reduzindo o tempo de inicialização e o consumo desnecessário de créditos computacionais.

## 🔄 Esteira de Integração e Entrega Contínuas (CI/CD)

A automação de deploy é gerenciada pelo **Bitbucket Pipelines** (`bitbucket-pipelines.yml`). Sempre que modificações são consolidadas na branch **`main`**, o pipeline executa os seguintes passos dentro de um ambiente isolado com a imagem `python:3.11-slim`:

1. **Preparação do Ambiente:** Atualiza o contêiner e garante a instalação do `curl` e `unzip`.
2. **Instalação da CLI:** Faz o download e instala a versão oficial do **Databricks CLI**.
3. **Validação Técnica (`databricks bundle validate`):** Valida a integridade do arquivo `databricks.yml`, checando a sintaxe e as permissões antes de subir qualquer alteração.
4. **Implantação Automatizada (`databricks bundle deploy`):** Publica os novos códigos e atualiza o agendamento do Job diretamente no workspace do Databricks de destino.

---

## 📋 Pré-requisitos para Execução Local / Desenvolvimento

Caso queira debugar os scripts localmente através do seu ambiente de desenvolvimento ou terminal, garanta os seguintes requisitos:

* Interpretador Python versão `3.11.x` instalado.
* Instalação do PySpark (`pip install pyspark`).
* Driver JDBC do SQL Server configurado no ambiente de execução do Spark.
* CLI do Databricks autenticada localmente para testes com Bundles.