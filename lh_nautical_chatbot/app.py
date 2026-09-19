import os
import requests
import time
import gradio as gr
from gradio.themes import Soft
from databricks.sdk import WorkspaceClient

# Get workspace URL and auth token at startup
_w = WorkspaceClient()
HOST = _w.config.host
TOKEN = _w.config.authenticate()["Authorization"].split(" ", 1)[1]
ENDPOINT = "lh_nautical_chatbot"
API_URL = f"{HOST}/serving-endpoints/{ENDPOINT}/invocations"

def respond(message, chat_history):
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"inputs": [{"query": message}]}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, headers=headers, json=payload, timeout=120)
            response.raise_for_status()
            result = response.json()
            predictions = result.get("predictions", [])
            if predictions and isinstance(predictions, list):
                answer = predictions[0]
                if isinstance(answer, dict) and "output" in answer:
                    return answer["output"]
                return str(answer)
            return "Não consegui obter uma resposta. Tente novamente."
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 500 and attempt < max_retries - 1:
                time.sleep(10)
                continue
            return f"Erro ao consultar o modelo. Tente novamente em alguns segundos."
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep(10)
                continue
            return f"O modelo está iniciando. Tente novamente em alguns segundos."
        except Exception:
            return f"Erro ao processar a solicitação. Tente novamente."
    return "Não consegui obter uma resposta após várias tentativas. Tente novamente."

theme = Soft(
    text_size="sm",
    radius_size="sm",
    spacing_size="sm",
)

demo = gr.ChatInterface(
    respond,
    chatbot=gr.Chatbot(show_label=False, container=False, show_copy_button=True, bubble_full_width=True),
    title="LH Nautical Chatbot",
    description="""&gt; **👋 Olá! Sou o assistente virtual de inteligência preditiva da LH Nautical.**

&gt; Estou aqui para te ajudar a navegar de forma rápida e precisa pelos nossos dados estratégicos. Você pode me perguntar sobre:

* 📈 **Previsão de Demanda:** Qual a expectativa de vendas por produto e loja para os próximos 3 meses?
* 🚚 **Logística &amp; CDs:** Qual Centro de Distribuição terá o maior crescimento ou se devemos abrir novos pontos?
* 📊 **Desempenho Financeiro:** Resultados do relatório gerencial, faturamento e margens.
&gt; **Como posso te ajudar hoje? Digite sua dúvida abaixo!**""",
    theme=theme,
    examples=["Qual é a previsão de demanda para os próximos 3 meses?", "Qual Centro de Distribuição apresentará o maior crescimento?", "Devemos abrir novos Centros de Distribuição? Se sim, onde?"],
    textbox=gr.Textbox(placeholder="Pode perguntar!", scale=7),
    retry_btn=None,
    undo_btn=None,
    clear_btn="Clear",
)

demo.launch(server_name="0.0.0.0", server_port=8000)