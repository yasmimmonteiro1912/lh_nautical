"""
Módulo de Configuração e Inicialização do Engine de Testes.

Este bloco é responsável por preparar o ambiente de execução no Databricks,
ativando o sistema de logs estruturados (diário de bordo) e instanciando
a sessão do Apache Spark necessária para a manipulação dos DataFrames.
"""

import logging
import time
from pyspark.sql import SparkSession

# ==============================================================================
# CONFIGURAÇÃO DE LOGGING E CRIAÇÃO DO LOGGER GLOBAL
# ==============================================================================
# Liga e configura o gravador de avisos do código.
# Cada linha exibida na tela terá o formato: [Data/Hora] | [Tipo de Aviso] | [Mensagem].
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("🔎 Inicializando Esteira de Qualidade Dinâmica - Camada Bronze...")


# ==============================================================================
# INICIALIZAÇÃO DA SESSÃO DO APACHE SPARK
# ==============================================================================
# Liga os motores do Spark. O método 'getOrCreate()' reaproveita o cluster ativo 
# do Databricks se ele já existir, otimizando o tempo de execução e FinOps.
spark = SparkSession.builder.getOrCreate()


# ==============================================================================
# FUNÇÃO AUXILIAR: BUSCA DINÂMICA DE TABELAS
# ==============================================================================
def obter_tabelas_do_catalogo(spark_session: SparkSession, schema_name: str) -> list:
    """""Consulta o Metastore do Databricks para mapear as tabelas existentes.
    Este método elimina a necessidade de manutenção manual: o próprio Spark
    identifica quais tabelas foram ingeridas e as envia para a fila de testes.
    Assim, não é necessário atualizar este arquivo caso haja
    uma tabela nova do SQL Server ou de outra API.
    Args:
        spark_session (SparkSession): Conexão ativa com o motor do Spark.
        schema_name (str): Nome do banco de dados alvo (ex: 'bronze').
    Returns:
        list: Uma lista contendo os nomes de todas as tabelas encontradas.
    """
    if not spark_session.catalog.databaseExists(schema_name):
        raise AssertionError(f"❌ O banco de dados '{schema_name}' não existe!")
        
    df_tabelas = spark_session.catalog.listTables(schema_name)
    return [tabela.name for tabela in df_tabelas]


# ==============================================================================
# SUÍTE DE TESTES UNITÁRIOS DE DADO (DATA QUALITY ASSERTS)
# # ==============================================================================

def test_presenca_de_tabelas(lista_tabelas: list, schema_name: str) -> None:
    """[TESTE 1] - Validação de Existência do Banco de Dados.
    
    UTILIDADE: Garante que o pipeline de ingestão realmente gerou dados no catálogo
    e impede falsos positivos de sucesso caso o processo falhe sem gerar tabelas.
    
    PREVINE: Quebras em cascata nas camadas Silver/Gold por tentar ler um schema 
    completamente vazio ou inexistente.
    """
    logger.info(f"🧪 [TESTE 1] - Validando se existem tabelas no catálogo '{schema_name}'...")
    assert len(lista_tabelas) > 0, f"❌ Erro Crítico: Nenhuma tabela encontrada no schema '{schema_name}'!"
    logger.info(f"✅ Sucesso: {len(lista_tabelas)} tabelas encontradas para auditoria.")


def test_estrutura_e_auditoria(spark_session: SparkSession, schema_name: str, table_name: str) -> None:
    """[TESTE 2] - Validação de Metadados e Linhagem Técnica (Governança).
    O Teste do Carimbo de Data (Linhagem).

    O QUE FAZ: Entra em cada tabela e procura pela coluna que diz o dia e horário 
    que o dado foi extraído (como a 'ingestion_timestamp').
    
    O QUE PREVINE: Perda de controle. Se alguém criar uma tabela nova no futuro 
    e esquecer de colocar o carimbo de data da carga, esse teste barra o dado 
    antes que ele misture com o resto do banco sem identificação.
    """
    caminho_tabela = f"{schema_name}.{table_name}"
    df = spark_session.table(caminho_tabela)
    
    # Lista de nomes comuns que o time costuma usar para o carimbo de data
    colunas_validas = ["dh_ingestao", "ingestion_timestamp", "ingestao_timestamp"]
    tem_coluna = any(col in df.columns for col in colunas_validas)
    
    assert tem_coluna, f"❌ Falha em {caminho_tabela}: Nenhuma coluna de auditoria técnica foi encontrada!"


def test_volumetria_dados(spark_session: SparkSession, schema_name: str, table_name: str) -> int:
    """[TESTE 3] - Validação de Volumetria e Carga Zerada.
    
    UTILIDADE: Avalia a integridade física dos dados através de uma contagem de linhas,
    garantindo que os conectores (JDBC/APIs) realmente trouxeram registros.
    
    PREVINE: O pior cenário de produção: Sobrescrever dados históricos válidos por 
    tabelas vazias (0 registros) causadas por falhas de rede ou filtros incorretos.
    """
    caminho_tabela = f"{schema_name}.{table_name}"
    df = spark_session.table(caminho_tabela)
    
    total_registros = df.count()
    assert total_registros > 0, f"❌ Falha em {caminho_tabela}: A tabela foi gravada vazia (0 registros)!"
    
    return total_registros


# ==============================================================================
# O GERENTE DO PROCESSO (A ORQUESTRAÇÃO)
# ==============================================================================
def construir_e_rodar_suite():
    """Gerencia e orquestra a execução de toda a suíte de testes.
    
    LÓGICA TOLERANTE A FALHAS: O orquestrador executa os testes em todas as tabelas
    até o final da lista. Se uma tabela falhar, ele registra o erro mas continua testando
    as demais. Isso garante um diagnóstico completo da infraestrutura no fim do Job.
    """
    schema_alvo = "bronze"
    tempo_inicio = time.time()
    tabelas_com_erro = []
    
    try:
        # 1. Detetive descobre as tabelas de forma automática
        tabelas_para_testar = obter_tabelas_do_catalogo(spark, schema_alvo)
        
        # 2. Roda o teste de catálogo global (Teste 1)
        test_presenca_de_tabelas(tabelas_para_testar, schema_alvo)
        
        # 3. Roda o fiscal de Carimbo e o fiscal de Linhas para cada tabela da lista
        for tabela in tabelas_para_testar:
            try:
                # Testa se tem a coluna de data (Teste 2)
                test_estrutura_e_auditoria(spark, schema_alvo, tabela)
                
                # Testa se tem linhas dentro (Teste 3)
                linhas = test_volumetria_dados(spark, schema_alvo, tabela)
                
                logger.info(f"  ↳ ✅ {schema_alvo}.{tabela}: Estrutura OK | Volumetria: {linhas} registros")
                
            except AssertionError as error_validacao:
                # Se der ruim, anota o nome da culpada e segue o baile para a próxima
                logger.error(str(error_validacao))
                tabelas_com_erro.append(tabela)
        
        # 4. Encerramento e cálculo de tempo
        duracao = time.time() - tempo_inicio
        logger.info(f"⏱️ Tempo total de vistoria dos dados: {duracao:.2f} segundos.")

        # Se sobrou algum nome na listinha de problemas, o pipeline explode aqui 
        # para acender a luz vermelha no painel e mandar o e-mail de aviso do grupo.
        if tabelas_com_erro:
            raise RuntimeError(f"❌ O framework de testes encontrou falhas de qualidade em: {tabelas_com_erro}")
            
        logger.info("🏁 SUCESSO: Todas as tabelas estão saudáveis e prontas!")
        
    except Exception as e:
        logger.error(f"❌ A suíte de testes quebrou: {str(e)}")
        raise e


if __name__ == "__main__":
    # Dá a partida no gerente
    construir_e_rodar_suite()