"""
Módulo de Configuração e Execução do Framework de Data Quality.

Este script automatiza as validações de integridade, estrutura e volumetria
para as tabelas pertencentes à camada Bronze no ambiente Databricks, utilizando
o Apache Spark como mecanismo central de processamento distribuído.
"""

import logging
import time
from pyspark.sql import SparkSession

# ==============================================================================
# CONFIGURAÇÃO DE LOGGING E CRIAÇÃO DO LOGGER GLOBAL
# ==============================================================================
# Configuração do manipulador de logs estruturados para auditoria.
# Liga e configura o gravador de avisos do código.
# Padrão de saída definido: [Data/Hora] | [Nível de Alerta] | [Mensagem].
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("🔎 Inicializando Pipeline de Qualidade de Dados (Data Quality) - Camada Bronze...")


# ==============================================================================
# INICIALIZAÇÃO DA SESSÃO DO APACHE SPARK
# ==============================================================================
# Instanciação da SparkSession, ligando o motor de processamento do Apache Spark.
# Utiliza o método getOrCreate() para otimização
# de recursos computacionais e reaproveitamento do cluster ativo (Práticas de FinOps).
# O método 'getOrCreate()' reaproveita o cluster ativo 
# do Databricks se ele já existir.
spark = SparkSession.builder.getOrCreate()


# ==============================================================================
# FUNÇÃO AUXILIAR: BUSCA DINÂMICA DE TABELAS
# ==============================================================================
def obter_tabelas_do_catalogo(spark_session: SparkSession, schema_name: str) -> list:
    """Consulta o Metastore do Databricks para mapear as tabelas existentes.

    Elimina a necessidade de manutenção manual do catálogo de testes. O motor
    do Spark identifica, em tempo de execução, quais tabelas foram ingeridas e persistidas no
    esquema alvo, enviando-as para a fila de testes sem necessidade de refatoração.
    Assim, não é necessário atualizar este arquivo caso haja uma nova tabela 
    vinda do SQL Server ou de outra API.

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
# ==============================================================================

def test_presenca_de_tabelas(lista_tabelas: list, schema_name: str) -> None:
    """[TESTE 1] - Validação de Existência do Banco de Dados.
    
    UTILIDADE: Garante que o pipeline de ingestão realmente gerou objetos no catálogo
    e mitiga falsos positivos de sucesso em processos de ETL vazios, que falhem em gerar tabelas.
    
    PREVINE: Quebras em cascata nas camadas subsequentes (Silver/Gold)
    decorrentes da tentativa de leitura de um esquema inexistente.
    """
    logger.info(f"🧪 [TESTE 1] - Validando se existem tabelas no catálogo '{schema_name}'...")
    assert len(lista_tabelas) > 0, f"❌ Erro Crítico: Nenhuma tabela encontrada no schema '{schema_name}'!"
    logger.info(f"✅ Sucesso: {len(lista_tabelas)} tabelas encontradas para auditoria.")


def test_estrutura_e_auditoria(spark_session: SparkSession, schema_name: str, table_name: str) -> None:
    """[TESTE 2] - Validação de Metadados e Linhagem Técnica (Governança).

    O QUE FAZ: Inspeciona o esquema físico do objeto em busca de atributos de
    auditoria obrigatórios (como 'ingestion_timestamp'), garantindo a 
    rastreabilidade temporal e identificação exata do momento da extração da carga.
    Isso garante que nenhum dado seja gravado sem sabermos quando ele foi coletado.
    
    O QUE PREVINE: Degradação da governança de dados e perda de controle sobre o ciclo de
    vida da informação (Data Lineage), impedindo que novos objetos sejam integrados
    ao ecossistema sem os devidos carimbos de auditoria e linhagem técnica.
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
    garantindo que os conectores (JDBC/APIs) realmente extraíram registros da origem.
    Se a tabela foi gravada com zero registros, sinaliza uma falha na coleta do sistema de origem.
    
    PREVINE: A geração de cargas corrompidas por falhas de rede, quebras em conectores 
    ou aplicação de filtros incorretos. Assim, evita-se que dados históricos válidos 
    sejam sobrescritos por tabelas vazias (0 registros/volumetria zerada).

    """
    caminho_tabela = f"{schema_name}.{table_name}"
    df = spark_session.table(caminho_tabela)
    
    total_registros = df.count()
    assert total_registros > 0, f"❌ Falha em {caminho_tabela}: A tabela foi gravada vazia (0 registros, volumetria zerada)!"
    
    return total_registros


# ==============================================================================
# O GERENTE DO PROCESSO (A ORQUESTRAÇÃO)
# ==============================================================================
def construir_e_rodar_suite():
    """Gerencia e orquestra a execução da suíte de qualidade de dados, de ponta a ponta.

    Abordagem Fail-Late (Falha Tardia):
    Cada objeto é validado de forma independente dentro de blocos try/except isolados. 
    Caso uma tabela apresente inconformidades nas asserções, a exceção é registrada internamente 
    e a execução prossegue para as tabelas remanescentes.
    Isso evita que o script pare no primeiro erro e permite testar tudo de uma vez.

    Consolidação e Bloqueio Downstream:
    No final, o código mostra um relatório com todas as tabelas que falharam.
    Se houver qualquer erro, o processo dispara uma interrupção (RuntimeError), 
    alterando o status do Job para FAILED, para impedir
    que dados inconsistentes ou vazios sigam para as camadas subsequentes do Lakehouse (Silver/Gold).
    """
    schema_alvo = "bronze"
    tempo_inicio = time.time()
    tabelas_com_erro = []
    
    try:
        # 1. Mapeamento dinâmico dos metadados do esquema
        tabelas_para_testar = obter_tabelas_do_catalogo(spark, schema_alvo)
        
        # 2. Execução da asserção global de catálogo (Teste 1)
        test_presenca_de_tabelas(tabelas_para_testar, schema_alvo)
        
        # 3. Execução das asserções estruturais e volumétricas por objeto (Testes 2 e 3)
        for tabela in tabelas_para_testar:
            logger.info(f"🔄 Auditando tabela: {schema_alvo}.{tabela}")
            try:
                # Execução e log isolado do Teste 2 (Estrutura)
                test_estrutura_e_auditoria(spark, schema_alvo, tabela)
                logger.info(f"  ↳ 🏷️ [TESTE 2] Estrutura e Carimbo de Auditoria: OK")
                
                # Execução e log isolado do Teste 3 (Volumetria)
                linhas = test_volumetria_dados(spark, schema_alvo, tabela)
                logger.info(f"  ↳ 📊 [TESTE 3] Volumetria Validada: {linhas} registros")
                
            except AssertionError as error_validacao:
                # Captura falhas de asserção individuais e armazena o objeto para o relatório final
                logger.error(f"  ❌ Falha de Qualidade na tabela {tabela}: {str(error_validacao)}")
                tabelas_com_erro.append(tabela)
        
        # 4. Consolidação de métricas e encerramento do processo com cálculo de tempo
        duracao = time.time() - tempo_inicio
        logger.info(f"⏱️ Tempo total de vistoria dos dados: {duracao:.2f} segundos.")

        # Se houver erros, desenha um relatório resumido na tela antes de quebrar
        if tabelas_com_erro:
            logger.warning("==================================================")
            logger.warning("⚠️  RELATÓRIO DE FALHAS DE QUALIDADE (DATA QUALITY)")
            logger.warning("==================================================")
            for errada in tabelas_com_erro:
                logger.warning(f" 🔴 Tabela com problemas: {schema_alvo}.{errada}")
            logger.warning("==================================================")
            
            raise RuntimeError(f"❌ O framework de testes encontrou falhas de qualidade em: {tabelas_com_erro}")
            
        logger.info("🏁 Todos os objetos auditados estão íntegros e disponíveis para consumo!")
        
    except Exception as e:
        logger.error(f"❌ A suíte de testes quebrou: {str(e)}")
        raise e


if __name__ == "__main__":
    import sys 

    try:
        # Inicialização do orquestrador central do framework
        construir_e_rodar_suite()
        
    except Exception as e:
        """
        ==============================================================================
        GESTÃO DE EXCEÇÕES, OBSERVABILIDADE E DISPARO DE ALERTAS NO WORKFLOWS
        ==============================================================================
        CONCEITO ARQUITETURAL (Fail-Late & Explicit Exit Code):
        O framework utiliza uma estratégia de falha tardia para consolidar todas as 
        inconformidades de dados em um relatório final.
        
        POR QUE COMBINAR LOGGER.CRITICAL + SYSTEMEXIT COM MENSAGEM?
        1. Rastreabilidade na UI (Databricks Web): O 'logger.critical' com 'exc_info=True' 
           salva o traceback completo e estruturado do erro de qualidade no Standard Error (stderr) 
           do cluster, essencial para auditorias futuras na tela do Databricks.
           
        2. Visibilidade no CLI (Terminal Local): Como o CLI 'databricks bundle run' oculta 
           logs de erros comuns ao falhar, usamos 'raise SystemExit(mensagem)'. Isso 
           carimba o tipo do erro (ex: RuntimeError) e a lista de tabelas afetadas 
           diretamente na saída principal do terminal, evitando buscas cegas nos logs.
           
        3. Automação de Alertas (E-mail): O SystemExit força o fechamento do processo e 
            garante o código de saída correto para o Databricks Workflows, que assume o status 'FAILED', 
            percebe o colapso da tarefa  e dispara as notificações de e-mail do 'databricks.yml' 
            sem violar a documentação do README.
        ==============================================================================
        """
        # 1. Registra a falha estruturada com o traceback completo para a interface web
        logger.critical("💥 Falha crítica de Data Quality detectada na Camada Bronze:", exc_info=True)
        
        # 2. Formata a mensagem combinando o tipo do erro (ex: RuntimeError) com o texto real
        mensagem_customizada = f"{type(e).__name__}: {str(e)}"
        
        # 3. Encerra o sistema com erro de forma controlada: força o CLI a printar a mensagem no terminal e disparar o e-mail
        raise SystemExit(mensagem_customizada) from None
    
    
