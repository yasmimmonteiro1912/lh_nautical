import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Configuração do Logger para rastreabilidade do pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

logger.info("🚀 Iniciando Carga Bronze via DAB (Databricks Asset Bundles)...")

def get_args():
    """Captura e faz o parse dos argumentos de linha de comando para o pipeline.

    Returns:
        argparse.Namespace: Objeto contendo os argumentos de conexão e execução.
    """
    parser = argparse.ArgumentParser(description="Pipeline de Ingestão de Dados - Camada Bronze")

    parser.add_argument("--host", required=True, help="Host do banco de dados (aceita padrão {{secrets/scope/key}})")
    parser.add_argument("--port", required=True, help="Porta de conexão do banco de dados")
    parser.add_argument("--database", required=True, help="Nome do banco de dados de origem" )
    parser.add_argument("--user", required=True, help="Usuário de autenticação")
    parser.add_argument("--password", required=True, help="Senha de autenticação (aceita padrão {{secrets/scope/key}})")
    parser.add_argument("--target_tables", required=False, default=None, help="Lista de tabelas separadas por vírgula (opcional)")

    return parser.parse_args()

def read_table(spark, jdbc_url, connection_properties, table_name):
    """Realiza a leitura de uma tabela específica na origem via JDBC.

    Args:
        spark (SparkSession): Instância ativa do Spark.
        jdbc_url (str): String de conexão com o banco de dados.
        connection_properties (dict): Propriedades de conexão (usuário, senha, driver).
        table_name (str): Nome da tabela a ser lida no esquema 'dbo'.

    Returns:
        DataFrame: PySpark DataFrame com os dados extraídos da origem.
    """
    logger.info(f"📥 Lendo tabela fonte: dbo.{table_name}")

    return spark.read.jdbc(
        url=jdbc_url,
        table=f"dbo.{table_name}",
        properties=connection_properties
    )

def save_bronze(df, table_name):
    """Grava o DataFrame na camada Bronze utilizando o formato Delta Lake.

    Aplica a estratégia de overwrite e permite evolução de esquema (mergeSchema).

    Args:
        df (DataFrame): DataFrame do PySpark contendo os dados a serem salvos.
        table_name (str): Nome final da tabela na camada Bronze.
    """

    logger.info(f"💾 Gravando dados na tabela: bronze.{table_name}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"bronze.{table_name}")
    )


def process_table(spark, jdbc_url, connection_properties, table_name):
    """Executa a orquestração do processamento de uma tabela (Leitura, Transformação e Escrita).

    Inclui tratamento de exceções para garantir o isolamento de falhas por tabela.

    Args:
        spark (SparkSession): Instância ativa do Spark.
        jdbc_url (str): String de conexão JDBC.
        connection_properties (dict): Dicionário com credenciais de acesso.
        table_name (str): Nome da tabela a ser processada.
    """
    try:
        #Extração
        df = read_table(
            spark,
            jdbc_url,
            connection_properties,
            table_name
        )

        logger.info(f"🔥 Leitura JDBC concluída para a tabela {table_name}.")

        row_count = df.count()

        logger.info(
            f"📊 Tabela {table_name}: {row_count} registros encontrados na origem."
        )

        # Engenharia de Atributos: Adição de metadados técnicos de controle (Governança)
        df = df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )

        #Carga
        save_bronze(df, table_name)

        
        logger.info(
            f"✅ Tabela bronze.{table_name} carregada e indexada com sucesso."
        )

    except Exception as e:

        logger.error(
            f"❌ Erro crítico ao processar a tabela {table_name}: {str(e)}"
        )


def main():
    """Função principal para execução do pipeline de ingestão dinâmica."""
    args = get_args()

    spark = SparkSession.builder.getOrCreate()

    def resolver_secret(texto_param):
        """Resolve chaves do Databricks Secrets de forma dinâmica em tempo de execução.

        Args:
            texto_param (str): Valor do argumento que pode ou não ser um ponteiro de secret.

        Returns:
            str: O valor real da credencial descriptografado em memória.
        """
        if texto_param and texto_param.startswith("{{secrets/"):
            clean = texto_param.replace("{{secrets/", "").replace("}}", "")
            scope, key = clean.split("/")
            from pyspark.dbutils import DBUtils
            return DBUtils(spark).secrets.get(scope=scope, key=key)
        return texto_param

    # Resolução das credenciais em memória (Escopo Seguro)
    db_host = resolver_secret(args.host)
    db_port = resolver_secret(args.port)
    db_name = resolver_secret(args.database)
    db_user = resolver_secret(args.user)
    db_pass = resolver_secret(args.password)

    # evitando a exposição de IPs, usuários ou nomes de bancos reais no log de execução.
    logger.info(f"Configuração de Conexão -> HOST_REF={args.host} | PORT={args.port} | DB_REF={args.database} | USER_REF={args.user}")

    # Garante a existência do catálogo/schema de destino
    spark.sql(
        "CREATE DATABASE IF NOT EXISTS bronze"
    )

    # Construção dos objetos de conexão JDBC
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

    # Mapeamento Dinâmico de Metadados (Descoberta Automática de Schema)
    # Evita manutenção manual consultando diretamente o INFORMATION_SCHEMA da origem.
    logger.info("🔍 Consultando o INFORMATION_SCHEMA do SQL Server para mapeamento dinâmico...")
    query_tabelas = "(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE') as t"
    
    df_metadados = spark.read.jdbc(url=jdbc_url, table=query_tabelas, properties=connection_properties)
    
    tabelas = [row['TABLE_NAME'] for row in df_metadados.collect()]
    logger.info(f"📋 Mapeamento concluído. Descobertas {len(tabelas)} tabelas disponíveis na origem.")


    # Aplicação de regras de FinOps e Governança (Filtro se houver carga escopada)
    if args.target_tables:
        lista_permitida = [t.strip() for t in args.target_tables.split(",")]
        tabelas = [t for t in tabelas if t in lista_permitida]
        logger.info(f"🎯 Filtro FinOps aplicado. Escopo reduzido para {len(tabelas)} tabelas alvo.")

    logger.info(
        f"🚀 Iniciando processamento em lote para {len(tabelas)} tabelas."
    )

    # Iteração sequencial sobre as tabelas mapeadas
    for tabela in tabelas:
        process_table(
            spark,
            jdbc_url,
            connection_properties,
            # Passando a string para o script final de logs mascarados
            tabela
        )

    logger.info("🏁 Pipeline executado por completo. Camada Bronze atualizada.")


if __name__ == "__main__":
    main()