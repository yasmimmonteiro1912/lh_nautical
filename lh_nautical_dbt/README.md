# LH Nautical dbt Core

## Visão Geral

O projeto `lh_nautical_dbt` é responsável pela modelagem analítica e pela camada de transformação de dados da LH Nautical. Ele organiza os dados em uma arquitetura medallion, promovendo qualidade, testabilidade e conformidade com os padrões de Analytics Engineering da Indicium.

Principais características:
- Materializações pré-definidas para Databricks/Delta Lake
- Governança via **SQLFluff**, testes dbt e documentação YAML
- Dependências gerenciadas por `dbt` e `dbt_utils`
- Suporte a **Databricks** com uso esperado de runtime `dbutils`

---

## Orquestração, Deploy e CI/CD

A orquestração do pipeline diário (fuso `America/Sao_Paulo`) e os recursos do Databricks são gerenciados via `databricks.yml`, localizado na **raiz do repositório** (fora da pasta `lh_nautical_dbt`).

A pasta `lh_nautical_dbt` é isolada e dedicada exclusivamente aos modelos, testes e documentação de transformação dbt.

O deploy automatizado é executado pelo `bitbucket-pipelines.yml`, também na raiz, que valida e publica o bundle no Databricks a cada atualização na branch `main`.

---

## Estrutura do Projeto

```text
lh_nautical_dbt/
├── .gitignore
├── .sqlfluff
├── dbt_project.yml
├── package-lock.yml
├── packages.yml
├── analyses
├── macros/
│   └── generate_schema_name.sql
├── models/
│   ├── staging/                 # Limpeza e padronização dos dados brutos
│   │   ├── stg_lh_nautical_addresses.sql
│   │   ├── stg_lh_nautical_customers.sql
│   │   └── ...
│   ├── intermediate/            # Lógica de negócio intermediária e regras sanitizadas
│   │   ├── int_customers_enriched.sql
│   │   ├── int_orders_with_items.sql
│   │   └── ...
│   └── marts/                   # Modelos Star Schema consumíveis pelo negócio
│       ├── dim_customers.sql
│       ├── fct_sales_items.sql
│       ├── agg_customer_rfm.sql
│       └── ...
├── seeds/
│   └── seed_movement_types.csv
├── snapshots/
└── tests/                       # Testes singulares (Disjuntores de Negócio)
    ├── test_net_revenue_less_than_or_equal_gross.sql
    └── test_stock_value_coherence.sql

```

---
## Arquitetura de Modelagem (Medallion)

A transformação é estruturada em três camadas com responsabilidades, schemas e materializações bem definidas no dbt_project.yml:

| Camada | Papel e Responsabilidade | Materialização | Schema / Target |
| :--- | :--- | :--- | :--- |
| **Staging** (`stg_`) | Limpeza inicial, padronização de tipos e renomeação de colunas dos dados brutos. | `view` | `silver` |
| **Intermediate** (`int_`) | Aplicação de regras de negócio complexas, joins intermediários e desnormalização/higienização. | `view` | `silver` |
| **Marts** (`dim_`, `fct_`, `agg_`) | Modelagem final em Star Schema (Fatos, Dimensões e Agregados como RFM) para consumo de BI e Analytics. | `table` (Format Delta) | `gold` |


### Estrutura dos Marts
- `dim_*`: Dimensões (clientes, produtos, localização, fornecedores, datas).
- `fct_*`: Fatos operacionais e financeiros (vendas, estoque, pedidos, movimentos).
- `agg_*`: Agregados analíticos (RFM, performance por canal, saúde de inventário).

---

## Governança e Qualidade

### SQLFluff
A validação está configurada em `.sqlfluff` com:
- Dialeto: databricks (templater dbt)
- Padrão: Palavras-chave e identificadores em minúsculas, vírgulas leading.

### Boas práticas de SQL
- `select` organizado em CTEs para legibilidade
- uso explícito de `cast(... as <tipo>)` para coerção de tipos
- joins com chaves tipadas claramente
- nomeação consistente de modelos e colunas

### Tipagem rigorosa
- conversões de tipo explícitas em joins e cálculos
- precisão de datas e timestamps antes de funções temporais

### Testes de qualidade
- **Genéricos (YAML)**: unique, not_null, relationships, accepted_values.

- **Singulares (Disjuntores em tests/)**:
	- test_net_revenue_less_than_or_equal_gross.sql: Bloqueia o pipeline se a receita líquida superar a bruta.
	- test_stock_value_coherence.sql: Garante que produtos sem estoque em quantidade também tenham seu valor em R$ zerado.

---

## Dependências do Projeto

### Pacotes dbt
Gerenciados em `packages.yml`:
- `dbt-labs/dbt_utils` `1.3.0`
- `calogica/dbt_date` `>=0.9.0, <1.0.0`
- `dbt-msft/tsql_utils` `>=1.2.0, <2.0.0`

### Macros customizadas
- `macros/generate_schema_name.sql`

### Seeds
- `seeds/seed_movement_types.csv`

### Ambiente Databricks
O projeto é voltado para execução em Databricks e permite o uso de `dbutils` em runtime e scripts auxiliares.

---
## Comandos de Execução Local

Use estes comandos na raiz de `lh_nautical_dbt`:

```bash
# validar a conexão e configuração do profile
dbt debug --profiles-dir ~/.dbt

# instalar dependências do projeto
dbt deps

# carregar seeds de lookup
dbt seed --profiles-dir ~/.dbt

# executar os modelos
dbt run --profiles-dir ~/.dbt

# executar a suíte de testes
dbt test --profiles-dir ~/.dbt

# compilar e rodar tudo em sequência
dbt build --profiles-dir ~/.dbt
```

### Dicas de execução
- rode `dbt debug` antes de `dbt run` para garantir conexões e credenciais
- use `dbt seed` sempre que `seed_movement_types.csv` for atualizado
- em CI, prefira `dbt build` para validar geração e testes juntos

---

## Documentação e Observabilidade

Gerar a documentação do projeto:

```bash
dbt docs generate --profiles-dir ~/.dbt
dbt docs serve --profiles-dir ~/.dbt
```

No ambiente local, `dbt docs serve` abre uma interface interativa com:
- árvore de modelos
- descrições de tabelas e colunas
- dependências e lineage
- resultados de testes

---

## Padrões de Contribuição

- Mantenha nomes de modelos e colunas em minúsculas
- Atualize ou crie o arquivo `.yml` correspondente a cada modelo novo
- Adicione `description` em modelos e colunas para documentação automática
- Inclua testes adicionais ao criar nova regra de negócio
- Não commite credenciais em `profiles.yml`

---

## Exemplo de `profiles.yml`

```yaml
lh_nautical_dbt:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: <YOUR_CATALOG>
      schema: <YOUR_SCHEMA>
      host: <DATABRICKS_HOST>
      http_path: <HTTP_PATH>
      token: <DATABRICKS_TOKEN>
```

> Use variáveis de ambiente ou secrets para credenciais, e nunca versionar tokens.

---

## Observações Finais

Este projeto entrega uma camada analítica orientada a governança, com:
- modelagem em camadas medallion
- testes funcionais e de integridade financeira
- validação de estilo via SQLFluff
- documentação dbt gerada automaticamente

Mantenha o foco em qualidade, legibilidade e idempotência em todas as alterações.



