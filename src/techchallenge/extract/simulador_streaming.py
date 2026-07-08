import json
import random
import time
import threading
from pathlib import Path
import os

# 1. Limpa o mapa ruim do Java (se existir) para forçar o Spark a usar o Java bom do seu terminal
if 'JAVA_HOME' in os.environ:
    del os.environ['JAVA_HOME']

# 2. Configura os tradutores do Windows (Hadoop)
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'

from pyspark.sql import SparkSession
import pyspark.sql.types

# ==========================================
# 1. FUNÇÃO GERADORA (PRODUTOR)
# ==========================================
def gerar_dados_streaming(pasta_destino: str):
    """
    Simula a chegada contínua de resultados de avaliação de alunos 
    e salva cada registro como um arquivo JSON.
    """
    Path(pasta_destino).mkdir(parents=True, exist_ok=True)
    id_aluno_seq = 1000
    
    print(f"🔄 Iniciando gerador de streaming na pasta: {pasta_destino}...")
    
    while True:
        aluno = {
            "ano": 2024,
            "id_municipio": random.choice([3550308, 3304557, 3106200]),
            "id_escola": f"ESC-{random.randint(100, 999)}",
            "id_aluno": id_aluno_seq,
            "caderno": random.choice(["CAD-A", "CAD-B", "CAD-C"]),
            "serie": random.choice([2, 3, 5, 9]),
            "rede": random.choice(["Estadual", "Municipal", "Privada"]),
            "presenca": 1,
            "preenchimento_caderno": 1,
            "proficiencia": round(random.uniform(150.0, 350.0), 2),
            "peso_aluno": round(random.uniform(0.8, 1.2), 2)
        }
        
        # Define se é alfabetizado baseado na nota
        aluno["alfabetizado"] = 1 if aluno["proficiencia"] >= 200.0 else 0
        
        nome_arquivo = f"{pasta_destino}/aluno_{id_aluno_seq}.json"
        
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            json.dump(aluno, f, ensure_ascii=False)
            
        print(f"📝 [{time.strftime('%H:%M:%S')}] Arquivo gerado: aluno_{id_aluno_seq}.json")
        
        id_aluno_seq += 1
        time.sleep(2)

# ==========================================
# 2. FUNÇÃO CONSUMIDORA (PYSPARK)
# ==========================================
def consumir_dados_streaming(pasta_origem: str, pasta_destino: str):
    """
    Consome os arquivos JSON contínuos e os salva na camada Bronze em formato Parquet.
    """
    print("🚀 Iniciando o PySpark...")
    spark = SparkSession.builder \
        .appName("IngestaoStreamingBronze") \
        .getOrCreate()

    # Ocultar os logs extensos do Spark no terminal (opcional, deixa mais limpo)
    spark.sparkContext.setLogLevel("WARN")

    esquema_oficial = pyspark.sql.types.StructType([
        pyspark.sql.types.StructField("ano", pyspark.sql.types.IntegerType(), True),
        pyspark.sql.types.StructField("id_municipio", pyspark.sql.types.IntegerType(), True),
        pyspark.sql.types.StructField("id_escola", pyspark.sql.types.StringType(), True),
        pyspark.sql.types.StructField("id_aluno", pyspark.sql.types.IntegerType(), True),
        pyspark.sql.types.StructField("caderno", pyspark.sql.types.StringType(), True),
        pyspark.sql.types.StructField("serie", pyspark.sql.types.IntegerType(), True),
        pyspark.sql.types.StructField("rede", pyspark.sql.types.StringType(), True),
        pyspark.sql.types.StructField("presenca", pyspark.sql.types.IntegerType(), True),
        pyspark.sql.types.StructField("preenchimento_caderno", pyspark.sql.types.IntegerType(), True),
        pyspark.sql.types.StructField("proficiencia", pyspark.sql.types.DoubleType(), True),
        pyspark.sql.types.StructField("peso_aluno", pyspark.sql.types.DoubleType(), True),
        pyspark.sql.types.StructField("alfabetizado", pyspark.sql.types.IntegerType(), True)
    ])

    print(f"👀 Monitorando novos arquivos na pasta: {pasta_origem}...")

    df_streaming = spark.readStream \
        .schema(esquema_oficial) \
        .json(pasta_origem)

    # Salvando em formato otimizado (Parquet)
    query = df_streaming.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", f"{pasta_destino}/_checkpoints") \
        .start(pasta_destino)

    return query

# ==========================================
# 3. BLOCO PRINCIPAL DE EXECUÇÃO
# ==========================================
if __name__ == "__main__":
    pasta_entrada = "data/temp_streaming_input"
    pasta_bronze = "data/bronze/streaming/alunos"
    
    # 1. Gerador rodando em segundo plano (AGORA SEM O #)
    thread = threading.Thread(target=gerar_dados_streaming, args=(pasta_entrada,))
    thread.daemon = True
    thread.start()
    
    # 2. Inicia o PySpark (Consumidor)
    print("🔍 Iniciando teste completo do Spark...")
    query = consumir_dados_streaming(pasta_entrada, pasta_bronze)
    
    # 3. Mantém o código rodando ativamente
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Streaming interrompido com sucesso pelo usuário!")