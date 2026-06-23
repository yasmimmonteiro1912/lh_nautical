
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

> 🔒 **Segurança:** O script intercepta automaticamente strings que utilizam o padrão `{{secrets/scope/key}}` e resolve as credenciais em tempo de execução via `dbutils.secrets`, omitindo exibições de senhas nos logs do cluster.

---

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