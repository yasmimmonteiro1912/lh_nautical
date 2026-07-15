import argparse
import logging
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, substring, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from delta.tables import DeltaTable

# Configuração global de Logging para auditoria e observabilidade do pipeline
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("🚀 Iniciando Carga Bronze de Câmbio (USD & EUR) via API Bacen...")


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


def read_api_bacen_usd(start_date, end_date):
    """Consome a API do Banco Central para buscar cotações diárias do Dólar."""
    logger.info(f"💵 Solicitando cotações do Dólar ao Bacen de {start_date} até {end_date}")

    base_url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"
    endpoint = "CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    
    query_params = (
        f"?@dataInicial=%27{start_date}%27"
        f"&@dataFinalCotacao=%27{end_date}%27"
        f"&$format=json"
    )
    
    url_completa = f"{base_url}/{endpoint}{query_params}"

    headers = {
        "Host": "olinda.bcb.gov.br",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json;odata.metadata=minimal"
    }

    response = requests.get(url_completa, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json().get("value", [])


def read_api_bacen_eur(start_date, end_date):
    """Consome a API do Banco Central para buscar cotações diárias do Euro."""
    logger.info(f"💶 Solicitando cotações do Euro ao Bacen de {start_date} até {end_date}")

    base_url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"
    endpoint = "CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    
    query_params = (
        f"?@moeda=%27EUR%27"
        f"&@dataInicial=%27{start_date}%27"
        f"&@dataFinalCotacao=%27{end_date}%27"
        f"&$format=json"
    )
    
    url_completa = f"{base_url}/{endpoint}{query_params}"

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
                
                # O Watermark define a data de início como o ponto exato da última cotação de fechamento salva
                start_date = dt_ultima.strftime("%m-%d-%Y")
                logger.info(f"📈 Último fechamento encontrado no banco: {start_date}. Coletando incremento...")
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
        # Consumo da Origem (Dólar e Euro)
        lista_usd = read_api_bacen_usd(start_date, end_date)
        lista_eur = read_api_bacen_eur(start_date, end_date)

        if not lista_usd and not lista_eur:
            logger.warning(f"⚠️ Nenhuma nova cotação emitida pelo Bacen no período de {start_date} a {end_date}.")
            return
        
        # Definição de Schemas Estritos para prevenção de quebras de tipagem em listas vazias
        schema_usd = StructType([
            StructField("cotacaoCompra", DoubleType(), True),
            StructField("cotacaoVenda", DoubleType(), True),
            StructField("dataHoraCotacao", StringType(), True)
        ])

        schema_eur = StructType([
            StructField("cotacaoCompra", DoubleType(), True),
            StructField("cotacaoVenda", DoubleType(), True),
            StructField("dataHoraCotacao", StringType(), True),
            StructField("tipoBoletim", StringType(), True)
        ])

        # Criação dos DataFrames Spark estruturados
        df_usd_raw = spark.createDataFrame(lista_usd, schema=schema_usd) if lista_usd else spark.createDataFrame([], schema=schema_usd)
        df_eur_raw = spark.createDataFrame(lista_eur, schema=schema_eur) if lista_eur else spark.createDataFrame([], schema=schema_eur)

        # 1. Filtro estrito do Euro: Garante que APENAS boletins de Fechamento entrem no pipeline (Blindagem do incremental!)
        df_eur_clean = df_eur_raw.filter(col("tipoBoletim") == "Fechamento")

        # 2. Criação de chave temporal curta (YYYY-MM-DD) para join seguro contra milissegundos
        df_usd_prep = df_usd_raw.withColumn("data_ref", substring(col("dataHoraCotacao"), 1, 10))
        df_eur_prep = df_eur_clean.withColumn("data_ref", substring(col("dataHoraCotacao"), 1, 10))

        # 3. Isolamento e renomeação de colunas para evitar conflitos de nomes no join
        df_eur_prep = df_eur_prep.select(
            col("data_ref").alias("eur_data_ref"),
            col("dataHoraCotacao").alias("dataHoraCotacaoEuro"),
            col("cotacaoCompra").alias("cotacaoCompraEuro"),
            col("cotacaoVenda").alias("cotacaoVendaEuro")
        )

        # 4. Consolidação das Moedas via Outer Join baseado na data de referência
        df_joined = df_usd_prep.join(df_eur_prep, df_usd_prep["data_ref"] == df_eur_prep["eur_data_ref"], how="outer")

        # 5. Tratamento de Timestamp Unificado (Coalesce de segurança)
        df_joined = df_joined.withColumn(
            "final_dataHoraCotacao",
            coalesce(col("dataHoraCotacao"), col("dataHoraCotacaoEuro"))
        )

        # 6. Seleção final unificada (5 Colunas de Negócio + 1 Técnica)
        df_final = df_joined.select(
            col("final_dataHoraCotacao").alias("dataHoraCotacao"), # 1. Chave temporal consolidada
            col("cotacaoCompra"),                                   # 2. Compra Dólar
            col("cotacaoVenda"),                                    # 3. Venda Dólar
            col("cotacaoCompraEuro"),                               # 4. Compra Euro
            col("cotacaoVendaEuro")                                 # 5. Venda Euro
        ).withColumn("ingestion_timestamp", current_timestamp())

        # Validação final de volume antes de acionar a gravação física
        df_final_count = df_final.count()
        if df_final_count == 0:
            logger.warning("⚠️ O DataFrame final consolidado está vazio. Nenhum dado novo gravado.")
            return

        logger.info(f"📊 Dados extraídos e unificados. Quantidade de registros na janela: {df_final_count}")
        
        # Persistência na camada Bronze
        save_bronze_incremental(spark, df_final, nome_tabela)
        logger.info(f"✅ {nome_tabela_completo} sincronizada com sucesso!")

    except Exception as e:
        logger.error(f"❌ Erro crítico no processamento do câmbio: {str(e)}")


if __name__ == "__main__":
    main()