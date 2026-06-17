import argparse
from pyspark.sql import SparkSession

print("🚀 Iniciando Carga Bronze via DAB...")

# 1. Captura as variáveis injetadas de forma segura pelo DAB
parser = argparse.ArgumentParser()
parser.add_argument("--host", required=True)
parser.add_argument("--port", required=True)
parser.add_argument("--database", required=True)
parser.add_argument("--user", required=True)
parser.add_argument("--password", required=True)
args = parser.parse_args()

# 2. Inicializa o ambiente Spark e garante o Schema Bronze
spark = SparkSession.builder.getOrCreate()
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# 3. Configura a string de conexão JDBC
jdbc_url = f"jdbc:sqlserver://{args.host}:{args.port};database={args.database};encrypt=true;trustServerCertificate=true;"
connection_properties = {
    "user": args.user,
    "password": args.password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# 4. Lista consolidada das 24 tabelas extraídas do DBeaver
tabelas = [
    "addresses", "attributes", "brands", "categories", "customers", 
    "employees", "fiscal_invoices", "goods_receipt_items", "goods_receipts", 
    "locations", "order_items", "orders", "payments", "product_suppliers", 
    "product_variants", "products", "purchase_order_items", "purchase_orders", 
    "return_items", "returns", "stock_levels", "stock_movements", 
    "suppliers", "variant_attribute_values"
]

# 5. Loop de Processamento em Lote (Overwrite)
for t in tabelas:
    try:
        print(f"📥 Lendo tabela: dbo.{t}")
        df = spark.read.jdbc(url=jdbc_url, table=f"dbo.{t}", properties=connection_properties)
        
        print(f"💾 Salvando no Metastore como: bronze.{t}")
        df.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{t}")
        print(f"✅ Sucesso!")
    except Exception as e:
        print(f"❌ Erro na tabela {t}: {e}")

print("🏁 Carga concluída com sucesso!")