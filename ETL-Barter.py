# Databricks notebook source
from LDCDataAccessLayerPy import databricks_init, KeyVaultManager, data_lake

# SETUP DEVELOPMENT  - DataLake
databricks_init('SHAREDBUSINESSSERVICES')
# Initialize the DataLakeManager with default parameters or specify the necessary connection details
kv = KeyVaultManager()

# COMMAND ----------

conn_string = kv.get_key_vault_secret("adls-crr-store-gen2")
dl_dwh = data_lake.DataLakeManager(connection_string=conn_string, file_system="dwh")
dl_rep = data_lake.DataLakeManager(connection_string=conn_string, file_system="rep")

# COMMAND ----------

from LDCDataAccessLayerPy import utils

cfg = utils.parse_connection_string(conn_string)

for key, value in {
    f"fs.azure.account.auth.type.{cfg['AccountName']}.dfs.core.windows.net": "OAuth",
    f"fs.azure.account.oauth.provider.type.{cfg['AccountName']}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{cfg['AccountName']}.dfs.core.windows.net": cfg['ApplicationId'],
    f"fs.azure.account.oauth2.client.secret.{cfg['AccountName']}.dfs.core.windows.net": cfg['SecretKey'],
    f"fs.azure.account.oauth2.client.endpoint.{cfg['AccountName']}.dfs.core.windows.net": f"https://login.microsoftonline.com/{cfg['TenantId']}/oauth2/token"}.items():
    spark.conf.set(key, value)

# COMMAND ----------

# Forma recomendade de leitura de arquivos parquet para dataframes Spark...

file_path = "public/SharedBusinessServices/SBS - Origination/ETL_Defensivos.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
DF_Defensivos = spark.read.parquet(url)

file_path = "public/SharedBusinessServices/SBS - Origination/ETL_Fertilizantes.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
DF_Fertilizantes = spark.read.parquet(url)

file_path = "public/SharedBusinessServices/SBS - Origination/ETL_Material.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
ETL_Material = spark.read.parquet(url)

file_path = "public/SharedBusinessServices/SBS - Origination/ETL_Pedidos.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
ETL_Pedidos = spark.read.parquet(url)

file_path = "public/SharedBusinessServices/SBS - Origination/Pedidos.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
Pedidos = spark.read.parquet(url)

file_path = "public/SharedBusinessServices/SBS - Origination/Barter.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
Barter = spark.read.parquet(url)

# COMMAND ----------

DF_Defensivos.createOrReplaceTempView("DF_Defensivos")
DF_Fertilizantes.createOrReplaceTempView("DF_Fertilizantes")
ETL_Material.createOrReplaceTempView("ETL_Material")
ETL_Pedidos.createOrReplaceTempView("ETL_Pedidos")
Pedidos.createOrReplaceTempView("Pedidos")
Barter.createOrReplaceTempView("Barter")

# COMMAND ----------

from pyspark.sql.functions import col, lit, round

ETL_Pedidos = ETL_Pedidos.withColumn(
    "montante_MI",
    round(
        col("montante_dolar") * col("cambio_sec") * lit(-1)
    )
)

ETL_Pedidos = ETL_Pedidos.dropDuplicates(["num_documento"])
#display(ETL_Pedidos)

ETL_Pedidos.createOrReplaceTempView("ETL_Pedidos")

# COMMAND ----------

# ============================================================
# SEÇÃO 1: PREPARAÇÃO E NORMALIZAÇÃO DAS TABELAS
# ============================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

# ------------------------------------------------------------
# 1️⃣ Normalização da tabela BARTER (recibos)
# ------------------------------------------------------------

Barter_df = (
    spark.table("Barter")
    # Normaliza CPF/CNPJ removendo pontuação
    .withColumn("CPF_CNPJ_NORM", regexp_replace(col("CPF_CNPJ"), r"[.\-\/\s]", ""))
    # Converte data
    .withColumn("DATA_PAGAMENTO_DT", to_date(col("DATA_PREVISAO").cast(StringType()), "yyyyMMdd"))
    # Garante tipos numéricos para cálculos
    .withColumn("VALOR_PAGO", col("VALOR_PAGO").cast(DoubleType()))
    .withColumn("PTAX_PAGO", col("PTAX_PAGO").cast(DoubleType()))
    # Cria chave única para controle
    .withColumn("RECIBO_ID", monotonically_increasing_id())
)

# ------------------------------------------------------------
# 2️⃣ Normalização da tabela PEDIDOS
# ------------------------------------------------------------

Pedidos_df = (
    spark.table("Pedidos")
    .withColumnRenamed("CONTRATO", "CONTRATO_PEDIDO")
    .withColumnRenamed("PEDIDO", "NUM_PEDIDO")
)

# ------------------------------------------------------------
# 3️⃣ Normalização da tabela ETL_PEDIDOS (SAP)
# ------------------------------------------------------------

ETL_Pedidos_df = (
    spark.table("ETL_Pedidos")
    # Normaliza CPF e CNPJ para comparação com o Barter
    .withColumn("cpf_norm", regexp_replace(col("cpf").cast(StringType()), r"[.\-\/\s]", ""))
    .withColumn("cnpj_norm", regexp_replace(col("cnpj").cast(StringType()), r"[.\-\/\s]", ""))
    # Garante tipos numéricos
    .withColumn("montante_dolar", col("montante_dolar").cast(DoubleType()))
    .withColumn("montante_MI", col("montante_MI").cast(DoubleType()))
)

# ------------------------------------------------------------
# 4️⃣ Normalização da tabela ETL_MATERIAL
# ------------------------------------------------------------

ETL_Material_df = (
    spark.table("ETL_Material")
    .withColumnRenamed("Doc.vendas", "DOC_VENDAS")
    .withColumnRenamed("Setor atividade", "SETOR_ATIVIDADE")
)

# ------------------------------------------------------------
# 5️⃣ Normalização DF_DEFENSIVOS e DF_FERTILIZANTES
#     Remove sufixos do contrato (-10, -20, -120, etc.)
# ------------------------------------------------------------

def normalize_contrato_col(df):
    return df.withColumn("CONTRATO_NORM", regexp_replace(col("CONTRATO"), r"-\d+$", ""))

DF_Defensivos_df = normalize_contrato_col(spark.table("DF_Defensivos"))
DF_Fertilizantes_df = normalize_contrato_col(spark.table("DF_Fertilizantes"))

# ------------------------------------------------------------
# 6️⃣ Registra as tabelas normalizadas como temp views
# ------------------------------------------------------------

Barter_df.createOrReplaceTempView("vw_Barter_Norm")
Pedidos_df.createOrReplaceTempView("vw_Pedidos_Norm")
ETL_Pedidos_df.createOrReplaceTempView("vw_ETL_Pedidos_Norm")
ETL_Material_df.createOrReplaceTempView("vw_ETL_Material_Norm")
DF_Defensivos_df.createOrReplaceTempView("vw_DF_Defensivos_Norm")
DF_Fertilizantes_df.createOrReplaceTempView("vw_DF_Fertilizantes_Norm")


# COMMAND ----------

# ============================================================
# SEÇÃO 2: CONCILIAÇÃO AUTOMÁTICA
# ============================================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ------------------------------------------------------------
# 1️⃣ Agrupamento inicial de recibos (por regra de conciliação)
# ------------------------------------------------------------
# Agrupa por chaves de conciliação para tentar casar múltiplos recibos
recibos_grouped = (
    spark.table("vw_Barter_Norm")
    .groupBy("CONTRATO_RECIBO", "PRODUTO", "CPF_CNPJ_NORM", "SEQUENCIA_CC", "MOEDA_CC", "PTAX_PAGO") #arrumar CONTRATO_RECIBO para CONTRATO_CC
    .agg(
        collect_list("RECIBO").alias("LISTA_RECIBOS"),
        collect_list("VALOR_PAGO").alias("LISTA_VALORES"),
        collect_list("DATA_PAGAMENTO_DT").alias("LISTA_DATAS"),
        sum("VALOR_PAGO").alias("SOMA_VALOR_PAGO"),
        count("*").alias("QTD_RECIBOS")
    )
    .withColumn("AGRUPAMENTO", monotonically_increasing_id())  # cria ID interno de grupo
)

pedidos_grouped = spark.table("vw_Pedidos_Norm").groupBy("NUM_PEDIDO", "CONTRATO_PEDIDO", "SEQUENCIA_CC").agg(collect_list("RECIBO").alias("RECIBOS"))

recibos_pedidos = recibos_grouped.join(
    pedidos_grouped.select("CONTRATO_PEDIDO", "NUM_PEDIDO", "SEQUENCIA_CC"),
    (recibos_grouped["CONTRATO_RECIBO"] == pedidos_grouped["CONTRATO_PEDIDO"]) &
    (recibos_grouped["SEQUENCIA_CC"] == pedidos_grouped["SEQUENCIA_CC"]),
    how="inner"
)

DF_Defensivos_df.withColumn("CONTRATO", regexp_replace(col("CONTRATO"), r"-\d+$", ""))
DF_Fertilizantes_df.withColumn("CONTRATO", regexp_replace(col("CONTRATO"), r"-\d+$", ""))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, sum as _sum, when, array, struct, collect_list,
    explode, row_number, concat, lit, lpad
)
from pyspark.sql.types import *
from pyspark.sql.functions import size, array_repeat, arrays_zip, expr, array, array_contains

# Carregar os CSVs
df_material = ETL_Material_df
df_pedidos = ETL_Pedidos_df
df_recibos = recibos_pedidos

# Preparar df_pedidos: adicionar coluna formatada para num_documento com zeros à esquerda
df_pedidos = df_pedidos.withColumn(
    "num_documento_formatted",
    lpad(col("num_documento").cast("string"), 10, "0")
)

# Preparar df_recibos: calcular valor alvo baseado na moeda
# Agrupar por AGRUPAMENTO para consolidar informações e coletar todos os NUM_PEDIDO
df_recibos_agrupado = df_recibos.groupBy("AGRUPAMENTO").agg(
    collect_list("NUM_PEDIDO").alias("LISTA_NUM_PEDIDOS"),
    _sum(lit(1)).alias("QTD_PEDIDOS"),
    # Pegar valores únicos (são os mesmos para todas as linhas do agrupamento)
    first("CPF_CNPJ_NORM").alias("CPF_CNPJ_NORM"),
    first("MOEDA_CC").alias("MOEDA_CC"),
    first("SOMA_VALOR_PAGO").alias("SOMA_VALOR_PAGO"),
    first("PTAX_PAGO").alias("PTAX_PAGO")
)

df_recibos_agrupado = df_recibos_agrupado.withColumn(
    "VALOR_ALVO",
    when(col("MOEDA_CC") == "US$CO", col("SOMA_VALOR_PAGO") / col("PTAX_PAGO"))
    .otherwise(col("SOMA_VALOR_PAGO"))
).withColumn(
    "CAMPO_COMPARACAO",
    when(col("MOEDA_CC") == "US$CO", lit("montante_dolar"))
    .otherwise(lit("montante_MI"))
)

# Explodir a lista de NUM_PEDIDO para fazer join com pedidos
df_recibos_exploded = df_recibos_agrupado.select(
    col("AGRUPAMENTO"),
    col("CPF_CNPJ_NORM"),
    col("MOEDA_CC"),
    col("SOMA_VALOR_PAGO"),
    col("PTAX_PAGO"),
    col("VALOR_ALVO"),
    col("CAMPO_COMPARACAO"),
    col("QTD_PEDIDOS"),
    explode("LISTA_NUM_PEDIDOS").alias("NUM_PEDIDO")
)

# Join: recibos com pedidos usando NUM_PEDIDO = ctr_barter
df_joined = df_recibos_exploded.join(
    df_pedidos,
    df_recibos_exploded.NUM_PEDIDO == df_pedidos.ctr_barter,
    "inner"
)

# Criar colunas auxiliares para comparação de raiz de CNPJ (8 primeiros dígitos)
df_joined = df_joined.withColumn(
    "CPF_CNPJ_RAIZ",
    when(
        col("CPF_CNPJ_NORM").cast("string").substr(1, 8).rlike("^[0-9]{8}$"),
        col("CPF_CNPJ_NORM").cast("string").substr(1, 8)
    ).otherwise(col("CPF_CNPJ_NORM").cast("string"))
).withColumn(
    "cpf_norm_str",
    col("cpf_norm").cast("string")
).withColumn(
    "cnpj_norm_raiz",
    when(
        col("cnpj_norm").cast("string").substr(1, 8).rlike("^[0-9]{8}$"),
        col("cnpj_norm").cast("string").substr(1, 8)
    ).otherwise(col("cnpj_norm").cast("string"))
)

# Validar CPF_CNPJ_NORM: 
# - Se for CPF (11 dígitos): comparação exata com cpf_norm
# - Se for CNPJ (14 dígitos): comparação da raiz (8 primeiros dígitos) com cnpj_norm
df_joined = df_joined.withColumn(
    "CPF_CNPJ_VALIDO",
    when(
        # Validação de CPF (comparação exata)
        (col("CPF_CNPJ_NORM").cast("string") == col("cpf_norm_str")),
        lit(1)
    ).when(
        # Validação de CNPJ (comparação de raiz - 8 primeiros dígitos)
        (col("CPF_CNPJ_RAIZ") == col("cnpj_norm_raiz")),
        lit(1)
    ).otherwise(lit(0))
)

# Identificar agrupamentos que possuem ALGUM pedido com CPF/CNPJ inválido
# Esses agrupamentos vão para tratativa manual COMPLETAMENTE
df_agrupamentos_cpf_invalido = df_joined.filter(col("CPF_CNPJ_VALIDO") == 0).select(
    "AGRUPAMENTO"
).distinct()

# Criar DataFrame de tratativa manual com informações detalhadas
df_tratativa_manual_cpf = df_joined.join(
    df_agrupamentos_cpf_invalido,
    "AGRUPAMENTO",
    "inner"
).select(
    "AGRUPAMENTO",
    "NUM_PEDIDO",
    "CPF_CNPJ_NORM",
    "CPF_CNPJ_RAIZ",
    "cpf_norm",
    "cnpj_norm",
    "cnpj_norm_raiz",
    "MOEDA_CC",
    "SOMA_VALOR_PAGO",
    "CPF_CNPJ_VALIDO"
).distinct().withColumn(
    "MOTIVO_MANUAL",
    lit("CPF/CNPJ NAO CORRESPONDE")
)

# Filtrar apenas AGRUPAMENTOS onde TODOS os pedidos têm CPF/CNPJ válido
df_joined_valido = df_joined.join(
    df_agrupamentos_cpf_invalido,
    "AGRUPAMENTO",
    "left_anti"  # Mantém apenas agrupamentos que NÃO estão na lista de inválidos
)

# Selecionar colunas relevantes e criar coluna de valor para comparação
df_joined = df_joined.select(
    col("AGRUPAMENTO"),
    col("NUM_PEDIDO"),
    col("MOEDA_CC"),
    col("VALOR_ALVO"),
    col("CAMPO_COMPARACAO"),
    col("num_documento"),
    col("num_documento_formatted"),
    col("data_documento"),
    col("montante_MI"),
    col("montante_dolar")
).withColumn(
    "VALOR_DOCUMENTO",
    when(col("CAMPO_COMPARACAO") == "montante_dolar", col("montante_dolar"))
    .otherwise(col("montante_MI"))
)

# Processar agrupamentos em ordem para garantir exclusividade de documentos
# Ordenar agrupamentos (menor para maior)
agrupamentos_ordenados = df_joined.select("AGRUPAMENTO").distinct().orderBy("AGRUPAMENTO").collect()

# Lista para armazenar resultados de cada agrupamento
resultados_lista = []

# Set para rastrear documentos já utilizados
documentos_usados = set()

# Processar cada agrupamento sequencialmente
for row in agrupamentos_ordenados:
    agrupamento_atual = row["AGRUPAMENTO"]
    
    # Filtrar dados do agrupamento atual
    df_agrupamento = df_joined.filter(col("AGRUPAMENTO") == agrupamento_atual)
    
    # Filtrar documentos que ainda não foram usados
    df_disponiveis = df_agrupamento.filter(
        ~col("num_documento").isin(list(documentos_usados)) if documentos_usados else lit(True)
    )
    
    # Se não há documentos disponíveis, pular este agrupamento
    if df_disponiveis.count() == 0:
        continue
    
    # Ordenar documentos disponíveis por VALOR_DOCUMENTO e data_documento
    window_spec = Window.orderBy(
        col("VALOR_DOCUMENTO").asc(),
        col("data_documento").asc()
    )
    
    df_ordered = df_disponiveis.withColumn("ordem", row_number().over(window_spec))
    
    # Calcular soma acumulada
    window_cumsum = Window.orderBy("ordem").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    
    df_cumulative = df_ordered.withColumn(
        "SOMA_ACUMULADA",
        _sum("VALOR_DOCUMENTO").over(window_cumsum)
    )
    
    # Criar coluna com a soma acumulada anterior
    window_prev_sum = Window.orderBy("ordem").rowsBetween(
        Window.unboundedPreceding, -1
    )
    
    df_with_prev = df_cumulative.withColumn(
        "SOMA_ANTERIOR",
        when(
            _sum("VALOR_DOCUMENTO").over(window_prev_sum).isNull(),
            lit(0)
        ).otherwise(_sum("VALOR_DOCUMENTO").over(window_prev_sum))
    )
    
    # Identificar documentos que devem ser utilizados
    df_selecionados = df_with_prev.filter(col("SOMA_ANTERIOR") < col("VALOR_ALVO"))
    
    # Se nenhum documento foi selecionado (primeiro documento já excede o valor),
    # selecionar pelo menos o primeiro
    if df_selecionados.count() == 0:
        df_selecionados = df_with_prev.filter(col("ordem") == 1)
    
    # Adicionar à lista de resultados
    resultados_lista.append(df_selecionados)
    
    # Atualizar set de documentos usados
    docs_selecionados = df_selecionados.select("num_documento").rdd.flatMap(lambda x: x).collect()
    documentos_usados.update(docs_selecionados)

# Unir todos os resultados
if resultados_lista:
    df_resultado = resultados_lista[0]
    for df in resultados_lista[1:]:
        df_resultado = df_resultado.union(df)
else:
    # Se não há resultados, criar DataFrame vazio com schema correto
    df_resultado = spark.createDataFrame([], df_joined.schema)

# Agrupar resultado por AGRUPAMENTO para criar lista de documentos
df_final = df_resultado.groupBy(
    "AGRUPAMENTO",
    "NUM_PEDIDO",
    "MOEDA_CC",
    "VALOR_ALVO"
).agg(
    collect_list("num_documento_formatted").alias("LISTA_NUM_DOCUMENTOS"),
    _sum("VALOR_DOCUMENTO").alias("SOMA_VALORES_SELECIONADOS"),
    _sum(lit(1)).alias("QTD_DOCUMENTOS_SELECIONADOS")
).orderBy("AGRUPAMENTO")

# Mostrar resultado
#print("=== RESULTADO FINAL - BAIXA POR DOCUMENTOS ===")
#df_final.show(truncate=False)

# Mostrar detalhes por agrupamento
#print("\n=== DETALHES POR AGRUPAMENTO - BAIXA POR DOCUMENTOS ===")
#df_resultado.select(
#    "AGRUPAMENTO",
#    "NUM_PEDIDO",
#    "num_documento_formatted",
#    "data_documento",
#    "VALOR_DOCUMENTO",
#    "SOMA_ACUMULADA",
#    "VALOR_ALVO",
#    "ordem"
#).orderBy("AGRUPAMENTO", "ordem").show(100, truncate=False)

# ========================================================================
# FLUXO 2: TRATAR AGRUPAMENTOS NÃO ENCONTRADOS NO ETL_PEDIDOS
# ========================================================================

# Identificar agrupamentos que foram processados (encontrados no ETL_Pedidos)
agrupamentos_processados = df_resultado.select("AGRUPAMENTO").distinct()

# Identificar agrupamentos não encontrados
# Voltar ao df_recibos_agrupado (sem explosão)
df_nao_encontrados = df_recibos_agrupado.join(
    agrupamentos_processados,
    "AGRUPAMENTO",
    "left_anti"  # Retorna apenas registros da esquerda que não têm match na direita
)

#print("\n=== AGRUPAMENTOS NÃO ENCONTRADOS NO ETL_PEDIDOS ===")
#df_nao_encontrados.select("AGRUPAMENTO", "LISTA_NUM_PEDIDOS", "SOMA_VALOR_PAGO").show(truncate=False)

# Explodir lista de pedidos para buscar no ETL_material
df_nao_encontrados_exploded = df_nao_encontrados.select(
    col("AGRUPAMENTO"),
    col("CPF_CNPJ_NORM"),
    col("MOEDA_CC"),
    col("SOMA_VALOR_PAGO"),
    col("PTAX_PAGO"),
    col("QTD_PEDIDOS"),
    explode("LISTA_NUM_PEDIDOS").alias("NUM_PEDIDO")
)

# Join com ETL_material para buscar SETOR_ATIVIDADE
df_com_setor = df_nao_encontrados_exploded.join(
    df_material.select("DOC_VENDAS", "SETOR_ATIVIDADE").distinct(),
    df_nao_encontrados_exploded.NUM_PEDIDO == df_material.DOC_VENDAS,
    "left"
)

# Preparar DF_Fertilizantes: Agrupar por CONTRATO_NORM e somar valores
# Incluir CNPJ/CPF Cliente Emissor para validação
df_fertilizantes_norm = DF_Fertilizantes_df.groupBy("CONTRATO_NORM", "MOEDA").agg(
    _sum("VALOR ($) A FATURAR").alias("VALOR_TOTAL_CONTRATO"),
    first("CNPJ/CPF Cliente Emissor").alias("CPF_CNPJ_CLIENTE")
)

# Preparar DF_Defensivos: Agrupar por CONTRATO_NORM e somar valores
# Incluir CNPJ/CPF Cliente Emissor para validação
df_defensivos_norm = (
    DF_Defensivos_df.groupBy("CONTRATO_NORM", "Moeda2")
    .agg(
        _sum("VALOR ($) A FATURAR").alias("VALOR_TOTAL_CONTRATO"),
        first("CNPJ/CPF Cliente Emissor").alias("CPF_CNPJ_CLIENTE")
    )
    .withColumnRenamed("Moeda2", "MOEDA")
    .select("CONTRATO_NORM", "MOEDA", "VALOR_TOTAL_CONTRATO", "CPF_CNPJ_CLIENTE")
)

# Preparar valores do recibo para comparação
# SOMA_VALOR_PAGO sempre está em BRL
# Converter para USD quando necessário
# Agrupar por AGRUPAMENTO para consolidar antes de fazer as comparações
df_com_setor_agrupado = df_com_setor.groupBy(
    "AGRUPAMENTO",
    "CPF_CNPJ_NORM",
    "MOEDA_CC",
    "SOMA_VALOR_PAGO",
    "PTAX_PAGO"
).agg(
    collect_list("NUM_PEDIDO").alias("LISTA_NUM_PEDIDOS_SETOR"),
    first("SETOR_ATIVIDADE").alias("SETOR_ATIVIDADE")  # Assumindo mesmo setor para todos os pedidos
)

df_com_setor_agrupado = df_com_setor_agrupado.withColumn(
    "VALOR_RECIBO_BRL",
    col("SOMA_VALOR_PAGO")  # Sempre em BRL
).withColumn(
    "VALOR_RECIBO_USD",
    when(
        (col("PTAX_PAGO").isNotNull()) & (col("PTAX_PAGO") > 0),
        col("SOMA_VALOR_PAGO") / col("PTAX_PAGO")
    ).otherwise(lit(None))  # Converter BRL para USD
)

# Explodir novamente para fazer join com contratos
df_com_setor_exp = df_com_setor_agrupado.select(
    col("AGRUPAMENTO"),
    col("CPF_CNPJ_NORM"),
    col("MOEDA_CC"),
    col("SOMA_VALOR_PAGO"),
    col("PTAX_PAGO"),
    col("VALOR_RECIBO_BRL"),
    col("VALOR_RECIBO_USD"),
    col("SETOR_ATIVIDADE"),
    explode("LISTA_NUM_PEDIDOS_SETOR").alias("NUM_PEDIDO")
)

# Verificar em df_defensivos_norm para SETOR_ATIVIDADE = DF
# Buscar todos os contratos dos pedidos do agrupamento e somar
df_defensivos_check_temp = df_com_setor_exp.filter(col("SETOR_ATIVIDADE") == "DF").join(
    df_defensivos_norm,
    df_com_setor_exp.NUM_PEDIDO == df_defensivos_norm.CONTRATO_NORM,
    "left"
)

# Agrupar por AGRUPAMENTO para somar todos os contratos
df_defensivos_check = df_defensivos_check_temp.groupBy(
    "AGRUPAMENTO",
    "MOEDA_CC",
    "SOMA_VALOR_PAGO",
    "PTAX_PAGO",
    "SETOR_ATIVIDADE"
).agg(
    # Somar valores por moeda (considerando possíveis NULL)
    _sum(when(col("MOEDA") == "USD", col("VALOR_TOTAL_CONTRATO")).otherwise(0)).alias("VALOR_TOTAL_USD"),
    _sum(when(col("MOEDA") == "BRL", col("VALOR_TOTAL_CONTRATO")).otherwise(0)).alias("VALOR_TOTAL_BRL"),
    collect_list(when(col("CONTRATO_NORM").isNotNull(), col("NUM_PEDIDO"))).alias("LISTA_NUM_PEDIDOS_ENCONTRADOS"),
    collect_list(when(col("CONTRATO_NORM").isNotNull(), col("CPF_CNPJ_CLIENTE").cast("string"))).alias("CPF_CNPJ_CLIENTE"),
    collect_list(when(col("CONTRATO_NORM").isNotNull(), col("CPF_CNPJ_NORM"))).alias("CPF_CNPJ_NORM")
).withColumn(
    "CPF_CNPJ_NORM",
    array_repeat(
        col("CPF_CNPJ_NORM")[0],
        size(col("CPF_CNPJ_CLIENTE"))
    )
).withColumn(
    # Recalcular valores do recibo aqui
    "VALOR_RECIBO_BRL",
    col("SOMA_VALOR_PAGO")
).withColumn(
    "VALOR_RECIBO_USD",
    when(
        (col("PTAX_PAGO").isNotNull()) & (col("PTAX_PAGO") > 0),
        col("SOMA_VALOR_PAGO") / col("PTAX_PAGO")
    ).otherwise(lit(None))
).withColumn(
    "ENCONTRADO_CONTRATO",
    when((col("VALOR_TOTAL_USD") > 0) | (col("VALOR_TOTAL_BRL") > 0), lit("SIM")).otherwise(lit("NAO"))
).withColumn(
    # Verificar qual moeda tem valor (pode ter ambas ou apenas uma)
    # Se tiver valores em ambas as moedas, precisamos somar tudo na mesma moeda
    "MOEDA",
    when(
        (col("VALOR_TOTAL_USD") > 0) & (col("VALOR_TOTAL_BRL") > 0),
        # Se tem ambas, converter BRL para USD e somar
        lit("USD")
    ).when(col("VALOR_TOTAL_USD") > 0, lit("USD"))
    .when(col("VALOR_TOTAL_BRL") > 0, lit("BRL"))
    .otherwise(lit(None))
).withColumn(
    # Calcular valor total considerando conversão se necessário
    "VALOR_TOTAL_CONTRATO",
    when(
        (col("VALOR_TOTAL_USD") > 0) & (col("VALOR_TOTAL_BRL") > 0),
        # Converter BRL para USD e somar
        col("VALOR_TOTAL_USD") + (col("VALOR_TOTAL_BRL") / col("PTAX_PAGO"))
    ).when(col("MOEDA") == "USD", col("VALOR_TOTAL_USD"))
    .otherwise(col("VALOR_TOTAL_BRL"))
).withColumn(
    # Comparar na moeda do contrato
    "DIFERENCA_VALOR",
    when(
        col("MOEDA") == "USD",
        col("VALOR_RECIBO_USD") - col("VALOR_TOTAL_CONTRATO")
    ).otherwise(
        col("VALOR_RECIBO_BRL") - col("VALOR_TOTAL_CONTRATO")
    )
).withColumn(
    "VALOR_VALIDO",
    when(
        col("ENCONTRADO_CONTRATO") == "NAO",
        lit(0)
    ).when(
        col("DIFERENCA_VALOR").isNull(),
        lit(0)
    ).when(
        col("DIFERENCA_VALOR") <= 0.01,
        lit(1)
    ).otherwise(lit(0))
).withColumn(
    "TIPO_BAIXA",
    when(
        expr("array_except(CPF_CNPJ_CLIENTE, CPF_CNPJ_NORM) != array()"),
        lit("CPF/CNPJ NAO CORRESPONDE")
    ).when(
        (col("ENCONTRADO_CONTRATO") == "SIM") & (col("VALOR_VALIDO") == 1),
        lit("ADIANTAMENTO")
    ).when(
        (col("ENCONTRADO_CONTRATO") == "SIM") & (col("VALOR_VALIDO") == 0),
        lit("DIVERGENCIA_VALOR")
    ).otherwise(lit("NAO_ENCONTRADO"))
)

# Verificar em df_fertilizantes_norm para SETOR_ATIVIDADE = FT
df_fertilizantes_check_temp = df_com_setor_exp.filter(col("SETOR_ATIVIDADE") == "FT").join(
    df_fertilizantes_norm,
    df_com_setor_exp.NUM_PEDIDO == df_fertilizantes_norm.CONTRATO_NORM,
    "left"
)

# Agrupar por AGRUPAMENTO para somar todos os contratos
df_fertilizantes_check = df_fertilizantes_check_temp.groupBy(
    "AGRUPAMENTO",
    "MOEDA_CC",
    "SOMA_VALOR_PAGO",
    "PTAX_PAGO",
    "SETOR_ATIVIDADE"
).agg(
    # Somar valores por moeda (considerando possíveis NULL)
    _sum(when(col("MOEDA") == "USD", col("VALOR_TOTAL_CONTRATO")).otherwise(0)).alias("VALOR_TOTAL_USD"),
    _sum(when(col("MOEDA") == "BRL", col("VALOR_TOTAL_CONTRATO")).otherwise(0)).alias("VALOR_TOTAL_BRL"),
    collect_list(when(col("CONTRATO_NORM").isNotNull(), col("NUM_PEDIDO"))).alias("LISTA_NUM_PEDIDOS_ENCONTRADOS"),
    collect_list(when(col("CONTRATO_NORM").isNotNull(), col("CPF_CNPJ_CLIENTE").cast("string"))).alias("CPF_CNPJ_CLIENTE"),
    collect_list(when(col("CONTRATO_NORM").isNotNull(), col("CPF_CNPJ_NORM"))).alias("CPF_CNPJ_NORM")
).withColumn(
    "CPF_CNPJ_NORM",
    array_repeat(
        col("CPF_CNPJ_NORM")[0],
        size(col("CPF_CNPJ_CLIENTE"))
    )
).withColumn(
    # Recalcular valores do recibo aqui
    "VALOR_RECIBO_BRL",
    col("SOMA_VALOR_PAGO")
).withColumn(
    "VALOR_RECIBO_USD",
    when(
        (col("PTAX_PAGO").isNotNull()) & (col("PTAX_PAGO") > 0),
        col("SOMA_VALOR_PAGO") / col("PTAX_PAGO")
    ).otherwise(lit(None))
).withColumn(
    "ENCONTRADO_CONTRATO",
    when((col("VALOR_TOTAL_USD") > 0) | (col("VALOR_TOTAL_BRL") > 0), lit("SIM")).otherwise(lit("NAO"))
).withColumn(
    # Verificar qual moeda tem valor
    "MOEDA",
    when(
        (col("VALOR_TOTAL_USD") > 0) & (col("VALOR_TOTAL_BRL") > 0),
        # Se tem ambas, converter BRL para USD e somar
        lit("USD")
    ).when(col("VALOR_TOTAL_USD") > 0, lit("USD"))
    .when(col("VALOR_TOTAL_BRL") > 0, lit("BRL"))
    .otherwise(lit(None))
).withColumn(
    # Calcular valor total considerando conversão se necessário
    "VALOR_TOTAL_CONTRATO",
    when(
        (col("VALOR_TOTAL_USD") > 0) & (col("VALOR_TOTAL_BRL") > 0),
        # Converter BRL para USD e somar
        col("VALOR_TOTAL_USD") + (col("VALOR_TOTAL_BRL") / col("PTAX_PAGO"))
    ).when(col("MOEDA") == "USD", col("VALOR_TOTAL_USD"))
    .otherwise(col("VALOR_TOTAL_BRL"))
).withColumn(
    # Comparar na moeda do contrato
    "DIFERENCA_VALOR",
    when(
        col("MOEDA") == "USD",
        col("VALOR_RECIBO_USD") - col("VALOR_TOTAL_CONTRATO")
    ).otherwise(
        col("VALOR_RECIBO_BRL") - col("VALOR_TOTAL_CONTRATO")
    )
).withColumn(
    "VALOR_VALIDO",
    when(
        col("ENCONTRADO_CONTRATO") == "NAO",
        lit(0)
    ).when(
        col("DIFERENCA_VALOR").isNull(),
        lit(0)
    ).when(
        col("DIFERENCA_VALOR") <= 0.01,
        lit(1)
    ).otherwise(lit(0))
).withColumn(
    "TIPO_BAIXA",
    when(
        expr("array_except(CPF_CNPJ_CLIENTE, CPF_CNPJ_NORM) != array()"),
        lit("CPF/CNPJ NAO CORRESPONDE")
    ).when(
        (col("ENCONTRADO_CONTRATO") == "SIM") & (col("VALOR_VALIDO") == 1),
        lit("ADIANTAMENTO")
    ).when(
        (col("ENCONTRADO_CONTRATO") == "SIM") & (col("VALOR_VALIDO") == 0),
        lit("DIVERGENCIA_VALOR")
    ).otherwise(lit("NAO_ENCONTRADO"))
)

# Tratar casos onde SETOR_ATIVIDADE não é DF nem FT
df_outros_setores = df_com_setor_agrupado.filter(
    (col("SETOR_ATIVIDADE").isNull()) | 
    ((col("SETOR_ATIVIDADE") != "DF") & (col("SETOR_ATIVIDADE") != "FT"))
).withColumn(
    "TIPO_BAIXA",
    lit("SETOR_NAO_IDENTIFICADO")
).withColumn(
    "DIFERENCA_VALOR",
    lit(None).cast("double")
).withColumn(
    "MOEDA",
    lit(None).cast("string")
).withColumn(
    "VALOR_TOTAL_CONTRATO",
    lit(None).cast("double")
)

# Unir todos os resultados de baixa por adiantamento
df_baixa_adiantamento = df_defensivos_check.select(
    "AGRUPAMENTO",
    "MOEDA_CC",
    "SOMA_VALOR_PAGO",
    "VALOR_RECIBO_BRL",
    "VALOR_RECIBO_USD",
    "MOEDA",
    "VALOR_TOTAL_CONTRATO",
    "DIFERENCA_VALOR",
    "SETOR_ATIVIDADE",
    "TIPO_BAIXA"
).union(
    df_fertilizantes_check.select(
        "AGRUPAMENTO",
        "MOEDA_CC",
        "SOMA_VALOR_PAGO",
        "VALOR_RECIBO_BRL",
        "VALOR_RECIBO_USD",
        "MOEDA",
        "VALOR_TOTAL_CONTRATO",
        "DIFERENCA_VALOR",
        "SETOR_ATIVIDADE",
        "TIPO_BAIXA"
    )
).union(
    df_outros_setores.select(
        "AGRUPAMENTO",
        "MOEDA_CC",
        "SOMA_VALOR_PAGO",
        "VALOR_RECIBO_BRL",
        "VALOR_RECIBO_USD",
        "MOEDA",
        "VALOR_TOTAL_CONTRATO",
        "DIFERENCA_VALOR",
        "SETOR_ATIVIDADE",
        "TIPO_BAIXA"
    )
)

#print("\n=== RESULTADO FINAL - BAIXA POR ADIANTAMENTO ===")
#df_baixa_adiantamento.orderBy("AGRUPAMENTO").show(truncate=False)

# Separar casos de divergência de valor para tratativa manual
df_tratativa_manual_valor = df_baixa_adiantamento.filter(
    col("TIPO_BAIXA") == "DIVERGENCIA_VALOR"
).withColumn(
    "MOTIVO_MANUAL",
    lit("DIVERGENCIA DE VALOR")
)

# Atualizar df_baixa_adiantamento para conter apenas adiantamentos válidos
df_baixa_adiantamento_valido = df_baixa_adiantamento.filter(
    col("TIPO_BAIXA") == "ADIANTAMENTO"
)


# ========================================================================
# TRATATIVA MANUAL - CONSOLIDAR TODOS OS CASOS -- Alterar aqui para incluir o fluxo 2
# ========================================================================

# Filtrar apenas agrupamentos de CPF/CNPJ inválido que foram encontrados no ETL_Pedidos
# (Não incluir agrupamentos que foram para o FLUXO 2)
agrupamentos_fluxo2 = df_baixa_adiantamento.select("AGRUPAMENTO").distinct()

df_tratativa_manual_cpf_final = df_tratativa_manual_cpf.join(
    agrupamentos_fluxo2,
    "AGRUPAMENTO",
    "left_anti"  # Mantém apenas os que NÃO estão no FLUXO 2
)

#print("\n=== AGRUPAMENTOS PARA TRATATIVA MANUAL ===")
#print("\n--- CPF/CNPJ NÃO CORRESPONDE ---")
#df_tratativa_manual_cpf_final.show(truncate=False)

#print("\n--- DIVERGÊNCIA DE VALOR ---")
#df_tratativa_manual_valor.show(truncate=False)

# Consolidar todos os casos de tratativa manual
df_tratativa_manual_consolidado = df_tratativa_manual_cpf_final.select(
    "AGRUPAMENTO",
    "NUM_PEDIDO",
    "MOEDA_CC",
    "SOMA_VALOR_PAGO",
    "MOTIVO_MANUAL"
).union(
    df_tratativa_manual_valor.select(
        "AGRUPAMENTO",
        lit(None).cast("string").alias("NUM_PEDIDO"),
        "MOEDA_CC",
        "SOMA_VALOR_PAGO",
        "MOTIVO_MANUAL"
    )
)

"""
print("\n--- CONSOLIDADO DE TRATATIVA MANUAL ---")
df_tratativa_manual_consolidado.orderBy("AGRUPAMENTO").show(truncate=False)

print(f"\nTotal de agrupamentos para tratativa manual (CPF/CNPJ): {df_tratativa_manual_cpf_final.count()}")
print(f"Total de agrupamentos para tratativa manual (DIVERGÊNCIA VALOR): {df_tratativa_manual_valor.count()}")
print(f"Total geral de tratativa manual: {df_tratativa_manual_consolidado.count()}")


# Criar resumo consolidado
print("\n=== RESUMO CONSOLIDADO ===")
print(f"Total de agrupamentos processados com documentos: {df_final.count()}")
print(f"Total de agrupamentos com baixa por adiantamento: {df_baixa_adiantamento_valido.count()}")
print(f"Total de agrupamentos não identificados: {df_baixa_adiantamento.filter(col('TIPO_BAIXA') != 'ADIANTAMENTO').filter(col('TIPO_BAIXA') != 'DIVERGENCIA_VALOR').count()}")
print(f"Total de agrupamentos para tratativa manual: {df_tratativa_manual_consolidado.count()}")
"""

# COMMAND ----------

def salvar_parquet_datalake(df, nome_arquivo, base_path):
    """
    Salva DataFrame como arquivo parquet único no Data Lake
    
    Args:
        df: DataFrame do Spark
        nome_arquivo: Nome do arquivo sem extensão (ex: 'fato_projetos')
        base_path: Caminho base no Data Lake (ex: 'public/SharedBusinessServices/SBS - Origination/PowerBI')
    """
    try:
        temp_dir = f"{base_path}/{nome_arquivo}"
        arquivo_final = f"{base_path}/{nome_arquivo}.parquet"
        
        # 1. Salva em 1 arquivo parquet no diretório temporário
        df.coalesce(1).write.mode("overwrite").format("parquet").save(
            f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{temp_dir}"
        )
        
        # 2. Lista os arquivos dentro do diretório temporário
        arquivos_temp = dl_rep.ls(temp_dir)
        
        # 3. Filtra para pegar o arquivo parquet gerado
        arquivo_parquet = [f for f in arquivos_temp if "parquet" in f][0]
        
        # 4. Verifica se o arquivo final já existe antes de removê-lo
        if dl_rep.exists(arquivo_final):
            dl_rep.rm(arquivo_final, recursive=False)
        
        # 5. Move o novo parquet para o diretório final
        dl_rep.mv(arquivo_parquet, arquivo_final)
        
        # 6. Remove o diretório temporário
        dl_rep.rm(temp_dir, recursive=True)
        
        print(f"✓ {nome_arquivo}.parquet salvo com sucesso!")
        
    except Exception as e:
        print(f"✗ Erro ao salvar {nome_arquivo}.parquet: {str(e)}")
        raise

# Configuração do caminho base (AJUSTAR CONFORME SEU AMBIENTE)
BASE_PATH = "public/SharedBusinessServices/SBS - Origination/Barter"

# Lista de DataFrames para exportar
dataframes_export = [
    (df_resultado, "df_resultado"), #Baixa por documento
    (df_baixa_adiantamento, "df_baixa_adiantamento"), #Baixa por adiantamento
    (df_tratativa_manual_cpf_final, "df_tratativa_manual_cpf_final"), #Tratativa manual CNP/CPF
    (df_tratativa_manual_valor, "df_tratativa_manual_valor"), #Tratativa manual valor
    (recibos_pedidos.drop(pedidos_grouped["SEQUENCIA_CC"]), "recibos_pedidos") #Agrupamentos
]

# Exportar todos os DataFrames
for df, nome in dataframes_export:
    salvar_parquet_datalake(df, nome, BASE_PATH)

# COMMAND ----------

# MAGIC %skip
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql import Window
# MAGIC from pyspark.sql.functions import (
# MAGIC     col, sum as _sum, when, array, struct, collect_list,
# MAGIC     explode, row_number, concat, lit, lpad
# MAGIC )
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC # Carregar os CSVs
# MAGIC df_material = ETL_Material_df
# MAGIC df_pedidos = ETL_Pedidos_df
# MAGIC df_recibos = recibos_pedidos
# MAGIC
# MAGIC # Preparar df_pedidos: adicionar coluna formatada para num_documento com zeros à esquerda
# MAGIC df_pedidos = df_pedidos.withColumn(
# MAGIC     "num_documento_formatted",
# MAGIC     lpad(col("num_documento").cast("string"), 10, "0")
# MAGIC )
# MAGIC
# MAGIC # Preparar df_recibos: calcular valor alvo baseado na moeda
# MAGIC # Agrupar por AGRUPAMENTO para consolidar informações e coletar todos os NUM_PEDIDO
# MAGIC df_recibos_agrupado = df_recibos.groupBy("AGRUPAMENTO").agg(
# MAGIC     collect_list("NUM_PEDIDO").alias("LISTA_NUM_PEDIDOS"),
# MAGIC     _sum(lit(1)).alias("QTD_PEDIDOS"),
# MAGIC     # Pegar valores únicos (são os mesmos para todas as linhas do agrupamento)
# MAGIC     first("CPF_CNPJ_NORM").alias("CPF_CNPJ_NORM"),
# MAGIC     first("MOEDA_CC").alias("MOEDA_CC"),
# MAGIC     first("SOMA_VALOR_PAGO").alias("SOMA_VALOR_PAGO"),
# MAGIC     first("PTAX_PAGO").alias("PTAX_PAGO")
# MAGIC )
# MAGIC
# MAGIC df_recibos_agrupado = df_recibos_agrupado.withColumn(
# MAGIC     "VALOR_ALVO",
# MAGIC     when(col("MOEDA_CC") == "US$CO", col("SOMA_VALOR_PAGO") / col("PTAX_PAGO"))
# MAGIC     .otherwise(col("SOMA_VALOR_PAGO"))
# MAGIC ).withColumn(
# MAGIC     "CAMPO_COMPARACAO",
# MAGIC     when(col("MOEDA_CC") == "US$CO", lit("montante_dolar"))
# MAGIC     .otherwise(lit("montante_MI"))
# MAGIC )
# MAGIC
# MAGIC # Explodir a lista de NUM_PEDIDO para fazer join com pedidos
# MAGIC df_recibos_exploded = df_recibos_agrupado.select(
# MAGIC     col("AGRUPAMENTO"),
# MAGIC     col("CPF_CNPJ_NORM"),
# MAGIC     col("MOEDA_CC"),
# MAGIC     col("SOMA_VALOR_PAGO"),
# MAGIC     col("PTAX_PAGO"),
# MAGIC     col("VALOR_ALVO"),
# MAGIC     col("CAMPO_COMPARACAO"),
# MAGIC     col("QTD_PEDIDOS"),
# MAGIC     explode("LISTA_NUM_PEDIDOS").alias("NUM_PEDIDO")
# MAGIC )
# MAGIC
# MAGIC # Join: recibos com pedidos usando NUM_PEDIDO = ctr_barter
# MAGIC df_joined = df_recibos_exploded.join(
# MAGIC     df_pedidos,
# MAGIC     df_recibos_exploded.NUM_PEDIDO == df_pedidos.ctr_barter,
# MAGIC     "inner"
# MAGIC )
# MAGIC
# MAGIC # Criar colunas auxiliares para comparação de raiz de CNPJ (8 primeiros dígitos)
# MAGIC df_joined = df_joined.withColumn(
# MAGIC     "CPF_CNPJ_RAIZ",
# MAGIC     when(
# MAGIC         col("CPF_CNPJ_NORM").cast("string").substr(1, 8).rlike("^[0-9]{8}$"),
# MAGIC         col("CPF_CNPJ_NORM").cast("string").substr(1, 8)
# MAGIC     ).otherwise(col("CPF_CNPJ_NORM").cast("string"))
# MAGIC ).withColumn(
# MAGIC     "cpf_norm_str",
# MAGIC     col("cpf_norm").cast("string")
# MAGIC ).withColumn(
# MAGIC     "cnpj_norm_raiz",
# MAGIC     when(
# MAGIC         col("cnpj_norm").cast("string").substr(1, 8).rlike("^[0-9]{8}$"),
# MAGIC         col("cnpj_norm").cast("string").substr(1, 8)
# MAGIC     ).otherwise(col("cnpj_norm").cast("string"))
# MAGIC )
# MAGIC
# MAGIC # Validar CPF_CNPJ_NORM: 
# MAGIC # - Se for CPF (11 dígitos): comparação exata com cpf_norm
# MAGIC # - Se for CNPJ (14 dígitos): comparação da raiz (8 primeiros dígitos) com cnpj_norm
# MAGIC df_joined = df_joined.withColumn(
# MAGIC     "CPF_CNPJ_VALIDO",
# MAGIC     when(
# MAGIC         # Validação de CPF (comparação exata)
# MAGIC         (col("CPF_CNPJ_NORM").cast("string") == col("cpf_norm_str")),
# MAGIC         lit(1)
# MAGIC     ).when(
# MAGIC         # Validação de CNPJ (comparação de raiz - 8 primeiros dígitos)
# MAGIC         (col("CPF_CNPJ_RAIZ") == col("cnpj_norm_raiz")),
# MAGIC         lit(1)
# MAGIC     ).otherwise(lit(0))
# MAGIC )
# MAGIC
# MAGIC # Identificar agrupamentos que possuem ALGUM pedido com CPF/CNPJ inválido
# MAGIC # Esses agrupamentos vão para tratativa manual COMPLETAMENTE
# MAGIC df_agrupamentos_cpf_invalido = df_joined.filter(col("CPF_CNPJ_VALIDO") == 0).select(
# MAGIC     "AGRUPAMENTO"
# MAGIC ).distinct()
# MAGIC
# MAGIC # Criar DataFrame de tratativa manual com informações detalhadas
# MAGIC df_tratativa_manual_cpf = df_joined.join(
# MAGIC     df_agrupamentos_cpf_invalido,
# MAGIC     "AGRUPAMENTO",
# MAGIC     "inner"
# MAGIC ).select(
# MAGIC     "AGRUPAMENTO",
# MAGIC     "NUM_PEDIDO",
# MAGIC     "CPF_CNPJ_NORM",
# MAGIC     "CPF_CNPJ_RAIZ",
# MAGIC     "cpf_norm",
# MAGIC     "cnpj_norm",
# MAGIC     "cnpj_norm_raiz",
# MAGIC     "MOEDA_CC",
# MAGIC     "SOMA_VALOR_PAGO",
# MAGIC     "CPF_CNPJ_VALIDO"
# MAGIC ).distinct().withColumn(
# MAGIC     "MOTIVO_MANUAL",
# MAGIC     lit("CPF/CNPJ NAO CORRESPONDE")
# MAGIC )
# MAGIC
# MAGIC # Filtrar apenas AGRUPAMENTOS onde TODOS os pedidos têm CPF/CNPJ válido
# MAGIC df_joined_valido = df_joined.join(
# MAGIC     df_agrupamentos_cpf_invalido,
# MAGIC     "AGRUPAMENTO",
# MAGIC     "left_anti"  # Mantém apenas agrupamentos que NÃO estão na lista de inválidos
# MAGIC )
# MAGIC
# MAGIC # Selecionar colunas relevantes e criar coluna de valor para comparação
# MAGIC df_joined = df_joined.select(
# MAGIC     col("AGRUPAMENTO"),
# MAGIC     col("NUM_PEDIDO"),
# MAGIC     col("MOEDA_CC"),
# MAGIC     col("VALOR_ALVO"),
# MAGIC     col("CAMPO_COMPARACAO"),
# MAGIC     col("num_documento"),
# MAGIC     col("num_documento_formatted"),
# MAGIC     col("data_documento"),
# MAGIC     col("montante_MI"),
# MAGIC     col("montante_dolar")
# MAGIC ).withColumn(
# MAGIC     "VALOR_DOCUMENTO",
# MAGIC     when(col("CAMPO_COMPARACAO") == "montante_dolar", col("montante_dolar"))
# MAGIC     .otherwise(col("montante_MI"))
# MAGIC )
# MAGIC
# MAGIC # Processar agrupamentos em ordem para garantir exclusividade de documentos
# MAGIC # Ordenar agrupamentos (menor para maior)
# MAGIC agrupamentos_ordenados = df_joined.select("AGRUPAMENTO").distinct().orderBy("AGRUPAMENTO").collect()
# MAGIC
# MAGIC # Lista para armazenar resultados de cada agrupamento
# MAGIC resultados_lista = []
# MAGIC
# MAGIC # Set para rastrear documentos já utilizados
# MAGIC documentos_usados = set()
# MAGIC
# MAGIC # Processar cada agrupamento sequencialmente
# MAGIC for row in agrupamentos_ordenados:
# MAGIC     agrupamento_atual = row["AGRUPAMENTO"]
# MAGIC     
# MAGIC     # Filtrar dados do agrupamento atual
# MAGIC     df_agrupamento = df_joined.filter(col("AGRUPAMENTO") == agrupamento_atual)
# MAGIC     
# MAGIC     # Filtrar documentos que ainda não foram usados
# MAGIC     df_disponiveis = df_agrupamento.filter(
# MAGIC         ~col("num_documento").isin(list(documentos_usados)) if documentos_usados else lit(True)
# MAGIC     )
# MAGIC     
# MAGIC     # Se não há documentos disponíveis, pular este agrupamento
# MAGIC     if df_disponiveis.count() == 0:
# MAGIC         continue
# MAGIC     
# MAGIC     # Ordenar documentos disponíveis por VALOR_DOCUMENTO e data_documento
# MAGIC     window_spec = Window.orderBy(
# MAGIC         col("VALOR_DOCUMENTO").asc(),
# MAGIC         col("data_documento").asc()
# MAGIC     )
# MAGIC     
# MAGIC     df_ordered = df_disponiveis.withColumn("ordem", row_number().over(window_spec))
# MAGIC     
# MAGIC     # Calcular soma acumulada
# MAGIC     window_cumsum = Window.orderBy("ordem").rowsBetween(
# MAGIC         Window.unboundedPreceding, Window.currentRow
# MAGIC     )
# MAGIC     
# MAGIC     df_cumulative = df_ordered.withColumn(
# MAGIC         "SOMA_ACUMULADA",
# MAGIC         _sum("VALOR_DOCUMENTO").over(window_cumsum)
# MAGIC     )
# MAGIC     
# MAGIC     # Criar coluna com a soma acumulada anterior
# MAGIC     window_prev_sum = Window.orderBy("ordem").rowsBetween(
# MAGIC         Window.unboundedPreceding, -1
# MAGIC     )
# MAGIC     
# MAGIC     df_with_prev = df_cumulative.withColumn(
# MAGIC         "SOMA_ANTERIOR",
# MAGIC         when(
# MAGIC             _sum("VALOR_DOCUMENTO").over(window_prev_sum).isNull(),
# MAGIC             lit(0)
# MAGIC         ).otherwise(_sum("VALOR_DOCUMENTO").over(window_prev_sum))
# MAGIC     )
# MAGIC     
# MAGIC     # Identificar documentos que devem ser utilizados
# MAGIC     df_selecionados = df_with_prev.filter(col("SOMA_ANTERIOR") < col("VALOR_ALVO"))
# MAGIC     
# MAGIC     # Se nenhum documento foi selecionado (primeiro documento já excede o valor),
# MAGIC     # selecionar pelo menos o primeiro
# MAGIC     if df_selecionados.count() == 0:
# MAGIC         df_selecionados = df_with_prev.filter(col("ordem") == 1)
# MAGIC     
# MAGIC     # Adicionar à lista de resultados
# MAGIC     resultados_lista.append(df_selecionados)
# MAGIC     
# MAGIC     # Atualizar set de documentos usados
# MAGIC     docs_selecionados = df_selecionados.select("num_documento").rdd.flatMap(lambda x: x).collect()
# MAGIC     documentos_usados.update(docs_selecionados)
# MAGIC
# MAGIC # Unir todos os resultados
# MAGIC if resultados_lista:
# MAGIC     df_resultado = resultados_lista[0]
# MAGIC     for df in resultados_lista[1:]:
# MAGIC         df_resultado = df_resultado.union(df)
# MAGIC else:
# MAGIC     # Se não há resultados, criar DataFrame vazio com schema correto
# MAGIC     df_resultado = spark.createDataFrame([], df_joined.schema)
# MAGIC
# MAGIC # Agrupar resultado por AGRUPAMENTO para criar lista de documentos
# MAGIC df_final = df_resultado.groupBy(
# MAGIC     "AGRUPAMENTO",
# MAGIC     "NUM_PEDIDO",
# MAGIC     "MOEDA_CC",
# MAGIC     "VALOR_ALVO"
# MAGIC ).agg(
# MAGIC     collect_list("num_documento_formatted").alias("LISTA_NUM_DOCUMENTOS"),
# MAGIC     _sum("VALOR_DOCUMENTO").alias("SOMA_VALORES_SELECIONADOS"),
# MAGIC     _sum(lit(1)).alias("QTD_DOCUMENTOS_SELECIONADOS")
# MAGIC ).orderBy("AGRUPAMENTO")
# MAGIC
# MAGIC # Mostrar resultado
# MAGIC #print("=== RESULTADO FINAL - BAIXA POR DOCUMENTOS ===")
# MAGIC #df_final.show(truncate=False)
# MAGIC
# MAGIC # Mostrar detalhes por agrupamento
# MAGIC #print("\n=== DETALHES POR AGRUPAMENTO - BAIXA POR DOCUMENTOS ===")
# MAGIC #df_resultado.select(
# MAGIC #    "AGRUPAMENTO",
# MAGIC #    "NUM_PEDIDO",
# MAGIC #    "num_documento_formatted",
# MAGIC #    "data_documento",
# MAGIC #    "VALOR_DOCUMENTO",
# MAGIC #    "SOMA_ACUMULADA",
# MAGIC #    "VALOR_ALVO",
# MAGIC #    "ordem"
# MAGIC #).orderBy("AGRUPAMENTO", "ordem").show(100, truncate=False)
# MAGIC
# MAGIC # ========================================================================
# MAGIC # FLUXO 2: TRATAR AGRUPAMENTOS NÃO ENCONTRADOS NO ETL_PEDIDOS
# MAGIC # ========================================================================
# MAGIC
# MAGIC # Identificar agrupamentos que foram processados (encontrados no ETL_Pedidos)
# MAGIC agrupamentos_processados = df_resultado.select("AGRUPAMENTO").distinct()
# MAGIC
# MAGIC # Identificar agrupamentos não encontrados
# MAGIC # Voltar ao df_recibos_agrupado (sem explosão)
# MAGIC df_nao_encontrados = df_recibos_agrupado.join(
# MAGIC     agrupamentos_processados,
# MAGIC     "AGRUPAMENTO",
# MAGIC     "left_anti"  # Retorna apenas registros da esquerda que não têm match na direita
# MAGIC )
# MAGIC
# MAGIC #print("\n=== AGRUPAMENTOS NÃO ENCONTRADOS NO ETL_PEDIDOS ===")
# MAGIC #df_nao_encontrados.select("AGRUPAMENTO", "LISTA_NUM_PEDIDOS", "SOMA_VALOR_PAGO").show(truncate=False)
# MAGIC
# MAGIC # Explodir lista de pedidos para buscar no ETL_material
# MAGIC df_nao_encontrados_exploded = df_nao_encontrados.select(
# MAGIC     col("AGRUPAMENTO"),
# MAGIC     col("CPF_CNPJ_NORM"),
# MAGIC     col("MOEDA_CC"),
# MAGIC     col("SOMA_VALOR_PAGO"),
# MAGIC     col("PTAX_PAGO"),
# MAGIC     col("QTD_PEDIDOS"),
# MAGIC     explode("LISTA_NUM_PEDIDOS").alias("NUM_PEDIDO")
# MAGIC )
# MAGIC
# MAGIC # Join com ETL_material para buscar SETOR_ATIVIDADE
# MAGIC df_com_setor = df_nao_encontrados_exploded.join(
# MAGIC     df_material.select("DOC_VENDAS", "SETOR_ATIVIDADE").distinct(),
# MAGIC     df_nao_encontrados_exploded.NUM_PEDIDO == df_material.DOC_VENDAS,
# MAGIC     "left"
# MAGIC )
# MAGIC
# MAGIC # Preparar DF_Fertilizantes: Agrupar por CONTRATO_NORM e somar valores
# MAGIC df_fertilizantes_norm = DF_Fertilizantes_df.groupBy("CONTRATO_NORM", "MOEDA").agg(
# MAGIC     _sum("VALOR ($) A FATURAR").alias("VALOR_TOTAL_CONTRATO")
# MAGIC )
# MAGIC
# MAGIC # Preparar DF_Defensivos: Agrupar por CONTRATO_NORM e somar valores
# MAGIC df_defensivos_norm = (
# MAGIC     DF_Defensivos_df.groupBy("CONTRATO_NORM", "Moeda2")
# MAGIC     .agg(_sum("VALOR ($) A FATURAR").alias("VALOR_TOTAL_CONTRATO"))
# MAGIC     .withColumnRenamed("Moeda2", "MOEDA")
# MAGIC     .select("CONTRATO_NORM", "MOEDA", "VALOR_TOTAL_CONTRATO")
# MAGIC )
# MAGIC
# MAGIC # Preparar valores do recibo para comparação
# MAGIC # SOMA_VALOR_PAGO sempre está em BRL
# MAGIC # Converter para USD quando necessário
# MAGIC # Agrupar por AGRUPAMENTO para consolidar antes de fazer as comparações
# MAGIC df_com_setor_agrupado = df_com_setor.groupBy(
# MAGIC     "AGRUPAMENTO",
# MAGIC     "CPF_CNPJ_NORM",
# MAGIC     "MOEDA_CC",
# MAGIC     "SOMA_VALOR_PAGO",
# MAGIC     "PTAX_PAGO"
# MAGIC ).agg(
# MAGIC     collect_list("NUM_PEDIDO").alias("LISTA_NUM_PEDIDOS_SETOR"),
# MAGIC     first("SETOR_ATIVIDADE").alias("SETOR_ATIVIDADE")  # Assumindo mesmo setor para todos os pedidos
# MAGIC )
# MAGIC
# MAGIC df_com_setor_agrupado = df_com_setor_agrupado.withColumn(
# MAGIC     "VALOR_RECIBO_BRL",
# MAGIC     col("SOMA_VALOR_PAGO")  # Sempre em BRL
# MAGIC ).withColumn(
# MAGIC     "VALOR_RECIBO_USD",
# MAGIC     when(
# MAGIC         (col("PTAX_PAGO").isNotNull()) & (col("PTAX_PAGO") > 0),
# MAGIC         col("SOMA_VALOR_PAGO") / col("PTAX_PAGO")
# MAGIC     ).otherwise(lit(None))  # Converter BRL para USD
# MAGIC )
# MAGIC
# MAGIC # Explodir novamente para fazer join com contratos
# MAGIC df_com_setor_exp = df_com_setor_agrupado.select(
# MAGIC     col("AGRUPAMENTO"),
# MAGIC     col("CPF_CNPJ_NORM"),
# MAGIC     col("MOEDA_CC"),
# MAGIC     col("SOMA_VALOR_PAGO"),
# MAGIC     col("PTAX_PAGO"),
# MAGIC     col("VALOR_RECIBO_BRL"),
# MAGIC     col("VALOR_RECIBO_USD"),
# MAGIC     col("SETOR_ATIVIDADE"),
# MAGIC     explode("LISTA_NUM_PEDIDOS_SETOR").alias("NUM_PEDIDO")
# MAGIC )
# MAGIC
# MAGIC # Verificar em df_defensivos_norm para SETOR_ATIVIDADE = DF
# MAGIC # Buscar todos os contratos dos pedidos do agrupamento e somar
# MAGIC df_defensivos_check_temp = df_com_setor_exp.filter(col("SETOR_ATIVIDADE") == "DF").join(
# MAGIC     df_defensivos_norm,
# MAGIC     df_com_setor_exp.NUM_PEDIDO == df_defensivos_norm.CONTRATO_NORM,
# MAGIC     "left"
# MAGIC )
# MAGIC
# MAGIC # Agrupar por AGRUPAMENTO para somar todos os contratos
# MAGIC df_defensivos_check = df_defensivos_check_temp.groupBy(
# MAGIC     "AGRUPAMENTO",
# MAGIC     "CPF_CNPJ_NORM",
# MAGIC     "MOEDA_CC",
# MAGIC     "SOMA_VALOR_PAGO",
# MAGIC     "PTAX_PAGO",
# MAGIC     "SETOR_ATIVIDADE"
# MAGIC ).agg(
# MAGIC     # Somar valores por moeda (considerando possíveis NULL)
# MAGIC     _sum(when(col("MOEDA") == "USD", col("VALOR_TOTAL_CONTRATO")).otherwise(0)).alias("VALOR_TOTAL_USD"),
# MAGIC     _sum(when(col("MOEDA") == "BRL", col("VALOR_TOTAL_CONTRATO")).otherwise(0)).alias("VALOR_TOTAL_BRL"),
# MAGIC     collect_list(when(col("CONTRATO_NORM").isNotNull(), col("NUM_PEDIDO"))).alias("LISTA_NUM_PEDIDOS_ENCONTRADOS")
# MAGIC ).withColumn(
# MAGIC     # Recalcular valores do recibo aqui
# MAGIC     "VALOR_RECIBO_BRL",
# MAGIC     col("SOMA_VALOR_PAGO")
# MAGIC ).withColumn(
# MAGIC     "VALOR_RECIBO_USD",
# MAGIC     when(
# MAGIC         (col("PTAX_PAGO").isNotNull()) & (col("PTAX_PAGO") > 0),
# MAGIC         col("SOMA_VALOR_PAGO") / col("PTAX_PAGO")
# MAGIC     ).otherwise(lit(None))
# MAGIC ).withColumn(
# MAGIC     "ENCONTRADO_CONTRATO",
# MAGIC     when((col("VALOR_TOTAL_USD") > 0) | (col("VALOR_TOTAL_BRL") > 0), lit("SIM")).otherwise(lit("NAO"))
# MAGIC ).withColumn(
# MAGIC     # Verificar qual moeda tem valor (pode ter ambas ou apenas uma)
# MAGIC     # Se tiver valores em ambas as moedas, precisamos somar tudo na mesma moeda
# MAGIC     "MOEDA",
# MAGIC     when(
# MAGIC         (col("VALOR_TOTAL_USD") > 0) & (col("VALOR_TOTAL_BRL") > 0),
# MAGIC         # Se tem ambas, converter BRL para USD e somar
# MAGIC         lit("USD")
# MAGIC     ).when(col("VALOR_TOTAL_USD") > 0, lit("USD"))
# MAGIC     .when(col("VALOR_TOTAL_BRL") > 0, lit("BRL"))
# MAGIC     .otherwise(lit(None))
# MAGIC ).withColumn(
# MAGIC     # Calcular valor total considerando conversão se necessário
# MAGIC     "VALOR_TOTAL_CONTRATO",
# MAGIC     when(
# MAGIC         (col("VALOR_TOTAL_USD") > 0) & (col("VALOR_TOTAL_BRL") > 0),
# MAGIC         # Converter BRL para USD e somar
# MAGIC         col("VALOR_TOTAL_USD") + (col("VALOR_TOTAL_BRL") / col("PTAX_PAGO"))
# MAGIC     ).when(col("MOEDA") == "USD", col("VALOR_TOTAL_USD"))
# MAGIC     .otherwise(col("VALOR_TOTAL_BRL"))
# MAGIC ).withColumn(
# MAGIC     # Comparar na moeda do contrato
# MAGIC     "DIFERENCA_VALOR",
# MAGIC     when(
# MAGIC         col("MOEDA") == "USD",
# MAGIC         col("VALOR_RECIBO_USD") - col("VALOR_TOTAL_CONTRATO")
# MAGIC     ).otherwise(
# MAGIC         col("VALOR_RECIBO_BRL") - col("VALOR_TOTAL_CONTRATO")
# MAGIC     )
# MAGIC ).withColumn(
# MAGIC     "VALOR_VALIDO",
# MAGIC     when(
# MAGIC         col("ENCONTRADO_CONTRATO") == "NAO",
# MAGIC         lit(0)
# MAGIC     ).when(
# MAGIC         col("DIFERENCA_VALOR").isNull(),
# MAGIC         lit(0)
# MAGIC     ).when(
# MAGIC         col("DIFERENCA_VALOR") <= 0.01,
# MAGIC         lit(1)
# MAGIC     ).otherwise(lit(0))
# MAGIC ).withColumn(
# MAGIC     "TIPO_BAIXA",
# MAGIC     when(
# MAGIC         (col("ENCONTRADO_CONTRATO") == "SIM") & (col("VALOR_VALIDO") == 1),
# MAGIC         lit("ADIANTAMENTO")
# MAGIC     ).when(
# MAGIC         (col("ENCONTRADO_CONTRATO") == "SIM") & (col("VALOR_VALIDO") == 0),
# MAGIC         lit("DIVERGENCIA_VALOR")
# MAGIC     ).otherwise(lit("NAO_ENCONTRADO"))
# MAGIC )
# MAGIC
# MAGIC # Verificar em df_fertilizantes_norm para SETOR_ATIVIDADE = FT
# MAGIC df_fertilizantes_check_temp = df_com_setor_exp.filter(col("SETOR_ATIVIDADE") == "FT").join(
# MAGIC     df_fertilizantes_norm,
# MAGIC     df_com_setor_exp.NUM_PEDIDO == df_fertilizantes_norm.CONTRATO_NORM,
# MAGIC     "left"
# MAGIC )
# MAGIC
# MAGIC # Agrupar por AGRUPAMENTO para somar todos os contratos
# MAGIC df_fertilizantes_check = df_fertilizantes_check_temp.groupBy(
# MAGIC     "AGRUPAMENTO",
# MAGIC     "CPF_CNPJ_NORM",
# MAGIC     "MOEDA_CC",
# MAGIC     "SOMA_VALOR_PAGO",
# MAGIC     "PTAX_PAGO",
# MAGIC     "SETOR_ATIVIDADE"
# MAGIC ).agg(
# MAGIC     # Somar valores por moeda (considerando possíveis NULL)
# MAGIC     _sum(when(col("MOEDA") == "USD", col("VALOR_TOTAL_CONTRATO")).otherwise(0)).alias("VALOR_TOTAL_USD"),
# MAGIC     _sum(when(col("MOEDA") == "BRL", col("VALOR_TOTAL_CONTRATO")).otherwise(0)).alias("VALOR_TOTAL_BRL"),
# MAGIC     collect_list(when(col("CONTRATO_NORM").isNotNull(), col("NUM_PEDIDO"))).alias("LISTA_NUM_PEDIDOS_ENCONTRADOS")
# MAGIC ).withColumn(
# MAGIC     # Recalcular valores do recibo aqui
# MAGIC     "VALOR_RECIBO_BRL",
# MAGIC     col("SOMA_VALOR_PAGO")
# MAGIC ).withColumn(
# MAGIC     "VALOR_RECIBO_USD",
# MAGIC     when(
# MAGIC         (col("PTAX_PAGO").isNotNull()) & (col("PTAX_PAGO") > 0),
# MAGIC         col("SOMA_VALOR_PAGO") / col("PTAX_PAGO")
# MAGIC     ).otherwise(lit(None))
# MAGIC ).withColumn(
# MAGIC     "ENCONTRADO_CONTRATO",
# MAGIC     when((col("VALOR_TOTAL_USD") > 0) | (col("VALOR_TOTAL_BRL") > 0), lit("SIM")).otherwise(lit("NAO"))
# MAGIC ).withColumn(
# MAGIC     # Verificar qual moeda tem valor
# MAGIC     "MOEDA",
# MAGIC     when(
# MAGIC         (col("VALOR_TOTAL_USD") > 0) & (col("VALOR_TOTAL_BRL") > 0),
# MAGIC         # Se tem ambas, converter BRL para USD e somar
# MAGIC         lit("USD")
# MAGIC     ).when(col("VALOR_TOTAL_USD") > 0, lit("USD"))
# MAGIC     .when(col("VALOR_TOTAL_BRL") > 0, lit("BRL"))
# MAGIC     .otherwise(lit(None))
# MAGIC ).withColumn(
# MAGIC     # Calcular valor total considerando conversão se necessário
# MAGIC     "VALOR_TOTAL_CONTRATO",
# MAGIC     when(
# MAGIC         (col("VALOR_TOTAL_USD") > 0) & (col("VALOR_TOTAL_BRL") > 0),
# MAGIC         # Converter BRL para USD e somar
# MAGIC         col("VALOR_TOTAL_USD") + (col("VALOR_TOTAL_BRL") / col("PTAX_PAGO"))
# MAGIC     ).when(col("MOEDA") == "USD", col("VALOR_TOTAL_USD"))
# MAGIC     .otherwise(col("VALOR_TOTAL_BRL"))
# MAGIC ).withColumn(
# MAGIC     # Comparar na moeda do contrato
# MAGIC     "DIFERENCA_VALOR",
# MAGIC     when(
# MAGIC         col("MOEDA") == "USD",
# MAGIC         col("VALOR_RECIBO_USD") - col("VALOR_TOTAL_CONTRATO")
# MAGIC     ).otherwise(
# MAGIC         col("VALOR_RECIBO_BRL") - col("VALOR_TOTAL_CONTRATO")
# MAGIC     )
# MAGIC ).withColumn(
# MAGIC     "VALOR_VALIDO",
# MAGIC     when(
# MAGIC         col("ENCONTRADO_CONTRATO") == "NAO",
# MAGIC         lit(0)
# MAGIC     ).when(
# MAGIC         col("DIFERENCA_VALOR").isNull(),
# MAGIC         lit(0)
# MAGIC     ).when(
# MAGIC         col("DIFERENCA_VALOR") <= 0.01,
# MAGIC         lit(1)
# MAGIC     ).otherwise(lit(0))
# MAGIC ).withColumn(
# MAGIC     "TIPO_BAIXA",
# MAGIC     when(
# MAGIC         (col("ENCONTRADO_CONTRATO") == "SIM") & (col("VALOR_VALIDO") == 1),
# MAGIC         lit("ADIANTAMENTO")
# MAGIC     ).when(
# MAGIC         (col("ENCONTRADO_CONTRATO") == "SIM") & (col("VALOR_VALIDO") == 0),
# MAGIC         lit("DIVERGENCIA_VALOR")
# MAGIC     ).otherwise(lit("NAO_ENCONTRADO"))
# MAGIC )
# MAGIC
# MAGIC # Tratar casos onde SETOR_ATIVIDADE não é DF nem FT
# MAGIC df_outros_setores = df_com_setor_agrupado.filter(
# MAGIC     (col("SETOR_ATIVIDADE").isNull()) | 
# MAGIC     ((col("SETOR_ATIVIDADE") != "DF") & (col("SETOR_ATIVIDADE") != "FT"))
# MAGIC ).withColumn(
# MAGIC     "TIPO_BAIXA",
# MAGIC     lit("SETOR_NAO_IDENTIFICADO")
# MAGIC ).withColumn(
# MAGIC     "DIFERENCA_VALOR",
# MAGIC     lit(None).cast("double")
# MAGIC ).withColumn(
# MAGIC     "MOEDA",
# MAGIC     lit(None).cast("string")
# MAGIC ).withColumn(
# MAGIC     "VALOR_TOTAL_CONTRATO",
# MAGIC     lit(None).cast("double")
# MAGIC )
# MAGIC
# MAGIC # Unir todos os resultados de baixa por adiantamento
# MAGIC df_baixa_adiantamento = df_defensivos_check.select(
# MAGIC     "AGRUPAMENTO",
# MAGIC     "MOEDA_CC",
# MAGIC     "SOMA_VALOR_PAGO",
# MAGIC     "VALOR_RECIBO_BRL",
# MAGIC     "VALOR_RECIBO_USD",
# MAGIC     "MOEDA",
# MAGIC     "VALOR_TOTAL_CONTRATO",
# MAGIC     "DIFERENCA_VALOR",
# MAGIC     "SETOR_ATIVIDADE",
# MAGIC     "TIPO_BAIXA"
# MAGIC ).union(
# MAGIC     df_fertilizantes_check.select(
# MAGIC         "AGRUPAMENTO",
# MAGIC         "MOEDA_CC",
# MAGIC         "SOMA_VALOR_PAGO",
# MAGIC         "VALOR_RECIBO_BRL",
# MAGIC         "VALOR_RECIBO_USD",
# MAGIC         "MOEDA",
# MAGIC         "VALOR_TOTAL_CONTRATO",
# MAGIC         "DIFERENCA_VALOR",
# MAGIC         "SETOR_ATIVIDADE",
# MAGIC         "TIPO_BAIXA"
# MAGIC     )
# MAGIC ).union(
# MAGIC     df_outros_setores.select(
# MAGIC         "AGRUPAMENTO",
# MAGIC         "MOEDA_CC",
# MAGIC         "SOMA_VALOR_PAGO",
# MAGIC         "VALOR_RECIBO_BRL",
# MAGIC         "VALOR_RECIBO_USD",
# MAGIC         "MOEDA",
# MAGIC         "VALOR_TOTAL_CONTRATO",
# MAGIC         "DIFERENCA_VALOR",
# MAGIC         "SETOR_ATIVIDADE",
# MAGIC         "TIPO_BAIXA"
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC #print("\n=== RESULTADO FINAL - BAIXA POR ADIANTAMENTO ===")
# MAGIC #df_baixa_adiantamento.orderBy("AGRUPAMENTO").show(truncate=False)
# MAGIC
# MAGIC # Separar casos de divergência de valor para tratativa manual
# MAGIC df_tratativa_manual_valor = df_baixa_adiantamento.filter(
# MAGIC     col("TIPO_BAIXA") == "DIVERGENCIA_VALOR"
# MAGIC ).withColumn(
# MAGIC     "MOTIVO_MANUAL",
# MAGIC     lit("DIVERGENCIA DE VALOR")
# MAGIC )
# MAGIC
# MAGIC # Atualizar df_baixa_adiantamento para conter apenas adiantamentos válidos
# MAGIC df_baixa_adiantamento_valido = df_baixa_adiantamento.filter(
# MAGIC     col("TIPO_BAIXA") == "ADIANTAMENTO"
# MAGIC )
# MAGIC
# MAGIC # Criar resumo consolidado
# MAGIC #print("\n=== RESUMO CONSOLIDADO ===")
# MAGIC #print(f"Total de agrupamentos processados com documentos: {df_final.count()}")
# MAGIC #print(f"Total de agrupamentos com baixa por adiantamento: {df_baixa_adiantamento_valido.count()}")
# MAGIC #print(f"Total de agrupamentos não identificados: {df_baixa_adiantamento.filter(col('TIPO_BAIXA') != 'ADIANTAMENTO').filter(col('TIPO_BAIXA') != 'DIVERGENCIA_VALOR').count()}")
# MAGIC #print(f"Total de agrupamentos para tratativa manual: {df_tratativa_manual_consolidado.count()}")
# MAGIC
# MAGIC # ========================================================================
# MAGIC # TRATATIVA MANUAL - CONSOLIDAR TODOS OS CASOS
# MAGIC # ========================================================================
# MAGIC
# MAGIC #print("\n=== AGRUPAMENTOS PARA TRATATIVA MANUAL ===")
# MAGIC #print("\n--- CPF/CNPJ NÃO CORRESPONDE ---")
# MAGIC #df_tratativa_manual_cpf.show(truncate=False)
# MAGIC
# MAGIC #print("\n--- DIVERGÊNCIA DE VALOR ---")
# MAGIC #df_tratativa_manual_valor.show(truncate=False)