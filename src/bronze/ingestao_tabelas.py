import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# ==========================================================
# Mudança: Adição de logging
# Motivo principal: Melhor rastreabilidade e monitoramento
# ==========================================================



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

logger.info("🚀 Iniciando Carga Bronze via DAB...")

# ==========================================================
# Mudança: Criação de função para capturar argumentos
# Motivo principal: Melhor organização e manutenção
# ==========================================================

def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)

    return parser.parse_args()


# ==========================================================
# Mudança: Criação de função para leitura JDBC
# Motivo principal: Reutilização e separação de responsabilidades
# ==========================================================

def read_table(spark, jdbc_url, connection_properties, table_name):

    logger.info(f"📥 Lendo tabela dbo.{table_name}")

    return spark.read.jdbc(
        url=jdbc_url,
        table=f"dbo.{table_name}",
        properties=connection_properties
    )


# ==========================================================
# Mudança: Criação de função para gravação Bronze
# Motivo principal: Centralizar regras de escrita
# ==========================================================

def save_bronze(df, table_name):

    logger.info(f"💾 Gravando bronze.{table_name}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"bronze.{table_name}")
    )


# ==========================================================
# Mudança: Criação de função para processar tabela
# Motivo principal: Código mais modular
# ==========================================================

def process_table(spark, jdbc_url, connection_properties, table_name):

    try:

        df = read_table(
            spark,
            jdbc_url,
            connection_properties,
            table_name
        )

        logger.info("🔥 LEITURA JDBC CONCLUIDA")

        row_count = df.count()

        logger.info(
            f"📊 Tabela {table_name}: {row_count} registros encontrados"
        )


        # ======================================================
        # Mudança: Inclusão de coluna técnica de ingestão
        # Motivo principal: Rastreabilidade dos dados
        # ======================================================

        df = df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )

        # ======================================================
        # Mudança: Captura de quantidade de registros
        # Motivo principal: Auditoria e validação da carga
        # ======================================================

        save_bronze(df, table_name)

        
        logger.info(
            f"✅ bronze.{table_name} carregada com sucesso"
        )

    except Exception as e:

        logger.error(
            f"❌ Erro ao processar tabela {table_name}: {str(e)}"
        )


def main():

    args = get_args()

    logger.info(f"HOST={args.host}")
    logger.info(f"PORT={args.port}")
    logger.info(f"DATABASE={args.database}")
    logger.info(f"USER={args.user}")

    spark = SparkSession.builder.getOrCreate()

    spark.sql(
        "CREATE DATABASE IF NOT EXISTS bronze"
    )

    jdbc_url = (
        f"jdbc:sqlserver://{args.host}:{args.port};"
        f"database={args.database};"
        f"encrypt=true;"
        f"trustServerCertificate=true;"
    )

    connection_properties = {
        "user": args.user,
        "password": args.password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # ==========================================================
    # Mudança: Lista isolada em variável única
    # Motivo principal: Facilitar futura externalização
    # para YAML/JSON
    # ==========================================================

    tabelas = [
        "addresses",
        "attributes",
        "brands",
        "categories",
        "customers",
        "employees",
        "fiscal_invoices",
        "goods_receipt_items",
        "goods_receipts",
        "locations",
        "order_items",
        "orders",
        "payments",
        "product_suppliers",
        "product_variants",
        "products",
        "purchase_order_items",
        "purchase_orders",
        "return_items",
        "returns",
        "stock_levels",
        "stock_movements",
        "suppliers",
        "variant_attribute_values"
    ]

    logger.info(
        f"📋 Total de tabelas a processar: {len(tabelas)}"
    )

    for tabela in tabelas:

        process_table(
            spark,
            jdbc_url,
            connection_properties,
            tabela
        )

    logger.info("🔥 TESTE YASMIM - VERSAO NOVA")


if __name__ == "__main__":
    main()

    
