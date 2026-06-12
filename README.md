# ⚓ Lighthouse - Pipeline de Dados Nautical (Grupo 2)

Bem-vindo ao repositório oficial do **Grupo 2** para o desafio Lighthouse da Indicium.

## 🏗️ Arquitetura do Projeto
Este projeto utiliza a **Arquitetura Medallion** dentro da plataforma Databricks, estruturado da seguinte forma:
* `src/bronze/`: Scripts de ingestão de dados via PySpark e conexão JDBC.
* `src/silver/`: Processamento, limpeza e padronização dos dados.
* `src/gold/`: Agregações e tabelas prontas para consumo de Analytics.
* `tests/`: Scripts de validação e testes de fumaça da infraestrutura.

## 🚀 Como Executar o Teste de Sincronização
O arquivo de validação inicial está localizado em `tests/test_sync.py` e serve para garantir o funcionamento do espelhamento Bitbucket ➔ Databricks Workspaces.
