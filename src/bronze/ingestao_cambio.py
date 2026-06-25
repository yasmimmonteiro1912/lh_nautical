import argparse
import logging
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# Configuração global de Logging para auditoria e observabilidade do pipeline
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("🚀 Iniciando Carga Bronze de Câmbio via API Bacen...")


def get_args():
    """Captura argumentos de linha de comando para execuções parametrizadas.

    Permite que o time de dados execute reprocessamentos retroativos (backfills) 
    manualmente se necessário.

    Returns:
        argparse.Namespace: Objeto contendo os argumentos de data inicial e final.
    """
    parser = argparse.ArgumentParser(description="Pipeline de Extração Incremental - Cotação de Câmbio")

    # Parâmetros opcionais para manter a flexibilidade de rodar um período na mão se precisar
    parser.add_argument("--start_date", required=False, default=None, help="Data inicial da janela (MM-DD-YYYY)")
    parser.add_argument("--end_date", required=False, default=None, help="Data final da janela (MM-DD-YYYY)")
    return parser.parse_args()


def read_api_bacen(start_date, end_date):
    """Consome a API de dados abertos do Banco Central do Brasil (Bacen).

    Busca as cotações diárias do dólar PTAX para o período estipulado.

    Args:
        start_date (str): Data de início no formato 'MM-DD-YYYY'.
        end_date (str): Data de fim no formato 'MM-DD-YYYY'.

    Returns:
        list: Lista de dicionários contendo os registros de cotação retornados pela API.
        
    Raises:
        HTTPError: Se a requisição HTTP falhar ou retornar status impeditivo.
    """
    logger.info(f"🌐 Solicitando cotações ao Banco Central de {start_date} até {end_date}")

    base_url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"
    endpoint = "CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    
    # Parâmetros de consulta via OData com escape de strings necessário para a API
    query_params = (
        f"?@dataInicial=%27{start_date}%27"
        f"&@dataFinalCotacao=%27{end_date}%27"
        f"&$format=json"
    )
    
    url_completa = f"{base_url}/{endpoint}{query_params}"

    # Headers mimetizando navegador para evitar bloqueios de requisições automatizadas
    headers = {
        "Host": "olinda.bcb.gov.br",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json;odata.metadata=minimal"
    }

    response = requests.get(url_completa, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json().get("value", [])


def save_bronze_incremental(spark, df, table_name):
    """Grava os dados de câmbio na camada Bronze usando a estratégia de Upsert (Merge).

    Garante a idempotência do pipeline: atualiza registros existentes se houver
    sobreposição e insere os novos, evitando a duplicação de cotações.

    Args:
        spark (SparkSession): Instância ativa do Spark.
        df (DataFrame): DataFrame contendo os novos dados coletados da API.
        table_name (str): Nome da tabela de destino.
    """
    nome_tabela_completo = f"bronze.{table_name}"
    logger.info(f"💾 Gravando de forma incremental em {nome_tabela_completo}")

    if not spark.catalog.tableExists(nome_tabela_completo):
        logger.info(f"✨ Tabela {nome_tabela_completo} não encontrada. Criando carga inicial...")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(nome_tabela_completo)
        )
    else:
        logger.info(f"🔄 Aplicando Delta Merge para atualizar registros e evitar duplicidades...")
        tabela_destino = DeltaTable.forName(spark, nome_tabela_completo)
        (
            tabela_destino.alias("target")
            .merge(
                df.alias("source"),
                "target.dataHoraCotacao = source.dataHoraCotacao"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


def main():
    """Função principal que orquestra o ciclo de vida da ingestão de câmbio."""
    args = get_args()
    spark = SparkSession.builder.getOrCreate()
    
    nome_tabela = "taxas_cambio"
    nome_tabela_completo = f"bronze.{nome_tabela}"

    # Instancia o banco de dados/schema caso o ambiente seja novo
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    # ==============================================================================
    # LÓGICA SMART WATERMARK: Identificação inteligente do delta de dados
    # ==============================================================================
    if not args.start_date or not args.end_date:
        logger.info("🔍 Parâmetros ausentes. Investigando última data de cotação via Smart Watermark...")
        
        if spark.catalog.tableExists(nome_tabela_completo):
           # Coleta o maior valor do registro temporal diretamente dos metadados do Delta Lake
            max_value = spark.sql(f"SELECT MAX(dataHoraCotacao) FROM {nome_tabela_completo}").collect()[0][0]
            
            if max_value:
                # Tratamento defensivo de tipos para o retorno do Spark SQL
                if isinstance(max_value, str):
                    dt_ultima = datetime.strptime(max_value[:10], "%Y-%m-%d")
                else:
                    dt_ultima = max_value
                
                # O Watermark define a data de início como o ponto exato da última cotação salva
                start_date = dt_ultima.strftime("%m-%d-%Y")
                logger.info(f"📈 Último registro encontrado no banco: {start_date}. Coletando incremento...")
            else:
                start_date = "01-01-2000"
                logger.info("⚠️ Tabela existe mas está vazia. Aplicando data padrão: 01-01-2000")
        else:
            start_date = "01-01-2000"
            logger.info("✨ Primeira execução do pipeline. Aplicando data padrão histórica: 01-01-2000")
        
       # O teto da janela de execução automatizada será sempre o dia corrente
        end_date = datetime.now().strftime("%m-%d-%Y")
    else:
       # Sobrescrita manual via parâmetros passados pelo orquestrador (Databricks)
        start_date = args.start_date
        end_date = args.end_date
        logger.info("⏳ Executando carga forçada via parâmetros manuais...")

    # ==============================================================================
    # Processamento, Enriquecimento e Persistência
    # ==============================================================================
    try:
        # Consumo da Origem
        lista_cotacoes = read_api_bacen(start_date, end_date)

        if not lista_cotacoes:
            logger.warning(f"⚠️ Nenhuma nova cotação emitida pelo Bacen no período de {start_date} a {end_date}.")
            return
        
        # Conversão e volumetria
        df = spark.createDataFrame(lista_cotacoes)
        logger.info(f"📊 Dados extraídos. Quantidade de registros na janela: {df.count()}")

        # Auditoria: Adição de carimbo de data/hora do processamento
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        # Persistência na camada Bronze
        save_bronze_incremental(spark, df, nome_tabela)
        logger.info(f"✅ {nome_tabela_completo} sincronizada com sucesso!")

    except Exception as e:
        logger.error(f"❌ Erro crítico no processamento do câmbio: {str(e)}")


if __name__ == "__main__":
    main()