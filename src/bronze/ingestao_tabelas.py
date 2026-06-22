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

    spark = SparkSession.builder.getOrCreate()

    def resolver_secret(texto_param):
        if texto_param and texto_param.startswith("{{secrets/"):
            clean = texto_param.replace("{{secrets/", "").replace("}}", "")
            scope, key = clean.split("/")
            from pyspark.dbutils import DBUtils
            return DBUtils(spark).secrets.get(scope=scope, key=key)
        return texto_param

    # Resolvendo os parâmetros (se forem chaves do cofre, viram os dados reais na memória)
    db_host = resolver_secret(args.host)
    db_port = resolver_secret(args.port)
    db_name = resolver_secret(args.database)
    db_user = resolver_secret(args.user)
    db_pass = resolver_secret(args.password)

    # Logs estruturados com os dados já resolvidos (com a senha protegida)
    logger.info(f"HOST={db_host}")
    logger.info(f"PORT={db_port}")
    logger.info(f"DATABASE={db_name}")
    logger.info(f"USER={db_user}")

    spark.sql(
        "CREATE DATABASE IF NOT EXISTS bronze"
    )

    jdbc_url = (
        f"jdbc:sqlserver://{db_host}:{db_port};"
        f"database={db_name};"
        f"encrypt=true;"
        f"trustServerCertificate=true;"
    )

    connection_properties = {
        "user": db_user,
        "password": db_pass,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # ==========================================================================
    # TESTE IASMIN - MAPEAMENTO DINÂMICO DE METADADOS
    # ==========================================================================
    # MAPEAMENTO DINÂMICO: Consulta o catálogo da origem para descobrir e ingerir 
    # novas tabelas automaticamente, eliminando a manutenção manual do código.
    logger.info("🔍 Consultando o INFORMATION_SCHEMA do SQL Server...")
    query_tabelas = "(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE') as t"
    
    df_metadados = spark.read.jdbc(url=jdbc_url, table=query_tabelas, properties=connection_properties)
    
    tabelas = [row['TABLE_NAME'] for row in df_metadados.collect()]
    logger.info(f"📋 [TESTE IASMIN] Descobertas {len(tabelas)} tabelas prontas para processamento.")
    # ==========================================================================

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

    logger.info("🔥 TESTE YASMIM - VERSAO NOVA COM SECRETS RESOLVIDOS!")


if __name__ == "__main__":
    main()

    
