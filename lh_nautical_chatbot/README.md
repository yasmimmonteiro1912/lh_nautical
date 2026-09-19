# LH Nautical Chatbot

Chatbot de inteligência preditiva da LH Nautical, construído com RAG (Retrieval-Augmented Generation) sobre documentos PDF da empresa. O chatbot responde perguntas sobre previsão de demanda, centros de distribuição, logística e desempenho financeiro.

## Como acessar o chatbot

### 1. Acesse a URL do app

O chatbot está disponível no seguinte endereço:

```
https://lh-nautical-chatbot-7474654687781800.aws.databricksapps.com
```

Abra esta URL em qualquer navegador moderno (Chrome, Firefox, Edge, Safari).

### 2. Faça login no Databricks

Ao acessar a URL, você será redirecionado para a tela de login do Databricks. Autentique-se com suas credenciais corporativas. Após o login, você será automaticamente redirecionado para a interface do chatbot.

> **Importante:** Você precisa ter acesso ao workspace do Databricks da LH Nautical para utilizar o chatbot.

### 3. Use o chatbot

A interface do chatbot exibe:

* **Título:** "LH Nautical Chatbot"
* **Caixa de texto:** Digite sua pergunta no campo "Pode perguntar!"
* **Botão de envio:** Pressione Enter ou clique no botão de enviar
* **Exemplos:** Clique em uma das perguntas de exemplo para testar o chatbot rapidamente

### Perguntas de exemplo

Você pode fazer perguntas como:

* "Qual é a previsão de demanda para os próximos 3 meses?"
* "Qual Centro de Distribuição apresentará o maior crescimento?"
* "Devemos abrir novos Centros de Distribuição? Se sim, onde?"
* "Qual o desempenho financeiro atual da empresa?"
* "Qual a taxa de crescimento do Armazém PR?"

## Dicas de uso

* **Seja específico:** Perguntas específicas geram respostas mais precisas
* **Idioma:** O chatbot responde sempre em português (PT-BR)
* **Contexto:** O chatbot usa apenas os documentos PDF da LH Nautical como base de conhecimento. Se a pergunta não estiver relacionada ao escopo da empresa, o chatbot informará que não tem informações suficientes
* **Primeira pergunta após inatividade:** Se o chatbot esteve inativo por um tempo, a primeira pergunta pode demorar até 2 minutos para responder (o modelo está reiniciando). As perguntas seguintes serão rápidas

## Solução de problemas

| Problema | Solução |
| --- | --- |
| Tela em branco | Aguarde 30 segundos e recarregue a página |
| Mensagem "O modelo está iniciando" | Aguarde alguns segundos e envie a pergunta novamente |
| Mensagem "Erro ao consultar o modelo" | Tente novamente em alguns segundos. O endpoint pode estar reiniciando |
| Não consegue fazer login | Verifique se você tem acesso ao workspace do Databricks da LH Nautical |

## Arquitetura

O chatbot é composto por:

1. **App Gradio** (`app.py`): Interface web que recebe as perguntas do usuário
2. **Endpoint de serving** (`lh_nautical_chatbot`): Modelo RAG deployado no Databricks Model Serving
3. **Vector Search Index** (`workspace.rag.docs_idx`): Índice de busca vetorial sobre os PDFs
4. **Tabela de documentos** (`workspace.rag.docs_text`): Texto extraído dos PDFs e dividido em chunks

### Fluxo de funcionamento

```
Usuário faz pergunta
        |
        v
   App Gradio
        |
        v
   Endpoint lh_nautical_chatbot (Model Serving)
        |
        v
   Vector Search (busca contexto nos PDFs)
        |
        v
   LLM gera resposta com o contexto encontrado
        |
        v
   Resposta exibida no chatbot
```

## Gerenciamento do app

### Iniciar o app

```bash
databricks apps start lh-nautical-chatbot
```

### Parar o app

```bash
databricks apps stop lh-nautical-chatbot
```

### Verificar status

```bash
databricks apps get lh-nautical-chatbot
```

### Deployar nova versão do código

```bash
databricks apps deploy lh-nautical-chatbot --source-code-path /Workspace/Users/cauabrgal11@alu.ufc.br/grupo_2/rag/chatbot_app
```

## Estrutura de arquivos

```
grupo_2/
│   ├── lh_nautical_chatbot
│   │   ├── app.py              # Código do app Gradio
│   │   ├── app.yaml            # Configuração do app
│   │   └── requirements.txt    # Dependências do app
│       └── README.md           # Este arquivo                   
│   ├── rag
        ├── chatbot_gui.ipynb   # Interface gráfica do chatbot
│   │   ├── create_needed_tables.ipynb  # Criação de tabelas necessárias
│   │   └── incremental_pdf_to_docs.ipynb   # Conversão de PDF em
│       └── rag_chatbot.ipynb         #Configuração do modelo e endpoint           
└── databricks.yml              # Configuração do bundle (DABs)
```

## Responsáveis

* **Desenvolvimento:** Grupo 2 - LH Nautical
* **Workspace:** Databricks - LH Nautical
* **Contato:** cauabrgal11@alu.ufc.br; yasmimmonteiro1912@gmail.com; iasmin.tenorio@gmail.com