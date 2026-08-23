# ⚓ LH NAUTICAL — Dashboard Gerencial & Solução Analítica (2026)

![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=sql&logoColor=white)

> Solução analítica end-to-end desenvolvida para o setor náutico, integrando a camada Gold do Databricks ao Power BI para prover inteligência de negócios do nível C-Level ao operacional.

---

## 📌 Visão Geral do Projeto

A **LH Nautical** é uma varejista de equipamentos e acessórios náuticos operando em modelo multi-filial (lojas físicas e e-commerce). Este projeto resolveu três dores centrais de gestão através de um Data Lakehouse e um Dashboard Executivo:

1. **Inconsistência Cadastral:** Monitoramento ativo de cadastros pendentes por fornecedor/marca.
2. **Cegueira Financeira:** Conciliação automática de divergências de pagamento e faturamento real vs. emitido.
3. **Gestão de Inventário:** Identificação de capital imobilizado em estoque parado (+90 dias) e alertas de ruptura.

---

## 🏗️ Arquitetura da Solução

[ Databricks Lakehouse ] ➔ (dbt / SQL Gold Layer) ➔ [ Power BI VertiPaq ] ➔ [ Relatório Executivo ]


* **Camada Gold (Databricks / dbt):** Regras de negócio complexas, modelagem Star Schema e tabelas agregadas (`agg_*`) resolvidas na transformação de dados.
* **Modo de Carga (Import):** Otimização de performance local com navegação na casa dos milissegundos.
* **Governança de Timestamp (refresh):** Captura de data/hora da carga via Power Query M (`DateTime.LocalNow()`), eliminando distorções de recálculo ao salvar o arquivo localmente.

---

## 📊 Estrutura do Dashboard (Storytelling Macro to Micro)

O relatório adota a técnica de **Progressive Disclosure**, dividindo-se em 4 visões integradas com navegação customizada por botões:

### 1. Overview (Portal de Entrada)
Landing page interativa para direcionamento de perfil de usuário e atalhos de navegação.

### 2. Visão Estratégica (C-Level)
* **Foco:** Performance global da empresa, margens e crescimento.
* **KPIs:** Faturamento Líquido (R$), Crescimento YoY (%), % Receita Líquida/Bruta, Ticket Médio e Margem Bruta (R$ e %).
* **Visuais:** Faturamento Mensal 2026 vs. 2025, Margem por Categoria e Capital Parado por Filial.

### 3. Visão Tática (Gestores de Área)
* **Foco:** Investigação de causas-raiz e comportamento do consumidor.
* **KPIs:** Ticket Médio, Taxa de Devoluções (%), Clientes Ativos e Total em Descontos.
* **Visuais:** Segmentação RFM (Clientes Ativos vs. Em Risco), Análise de Sazonalidade por Categoria e Motivos de Devolução.

### 4. Visão Operacional (Analistas & Rotina)
* **Foco:** Listas de ação diária e mitigação de gargalos.
* **KPIs:** Ruptura de Estoque, Pedidos Sem Receita, Cadastros Pendentes e Pedidos Divergentes.
* **Funcionalidades:** Alternância de estados via Bookmarks (*Estoque Parado* vs. *Estoque Quebrado*) e Drill-through para detalhamento individual de divergências financeiras.

---

## 🎨 UI/UX & Design System

* **Paleta de Cores:** Estilizada via arquivo de tema customizado (`lh_nautical_theme.json`).
* **Layout:** Padrão de leitura em Z (*Z-Pattern*), priorizando *Big Numbers* no topo.
* **Navegação:** Ocultação de abas nativas e uso de **Blank Buttons** com ações de navegação de página para garantir uma experiência de aplicação web.

---

## 🛠️ Tecnologias Utilizadas

* **Power BI Desktop** (Visualização e DAX)
* **Databricks** (SQL Warehouse)
* **dbt (data build tool)** (Modelagem de dados na camada Marts/Gold)

---

## 👥 Equipe do Projeto

| Papel | Responsável |
| :--- | :--- |
| **Responsável Técnica & UI/UX** | Iasmin Santiago |
| **Negócios & Storytelling** | Aline Noce |
| **Engenharia & Analytics** | Yasmim Monteiro, Cauã Braga |

---

## 📜 Licença e Governança

Projeto desenvolvido como parte do programa de formação corporativa da **Indicium AI** (LH2026.05)