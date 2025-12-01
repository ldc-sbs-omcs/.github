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

file_path = "BASE/SAPECC/LATEST/VBAK"
url = f"abfss://{dl_dwh.file_system}@{dl_dwh.adls_client.primary_hostname}/{file_path}"
T1 = spark.read.parquet(url)

file_path = "BASE/SAPECC/LATEST/VBAP"
url = f"abfss://{dl_dwh.file_system}@{dl_dwh.adls_client.primary_hostname}/{file_path}"
T2 = spark.read.parquet(url)

file_path = "BASE/SAPECC/LATEST/MAKT.parquet"
url = f"abfss://{dl_dwh.file_system}@{dl_dwh.adls_client.primary_hostname}/{file_path}"
T3 = spark.read.parquet(url)

file_path = "BASE/SAPECC/LATEST/BSEG"
url = f"abfss://{dl_dwh.file_system}@{dl_dwh.adls_client.primary_hostname}/{file_path}"
T4 = spark.read.parquet(url)

file_path = "BASE/SAPECC/LATEST/BKPF"
url = f"abfss://{dl_dwh.file_system}@{dl_dwh.adls_client.primary_hostname}/{file_path}"
T5 = spark.read.parquet(url)

file_path = "BASE/SAPECC/LATEST/BSID.parquet"
url = f"abfss://{dl_dwh.file_system}@{dl_dwh.adls_client.primary_hostname}/{file_path}"
T6 = spark.read.parquet(url)

file_path = "BASE/SAPECC/LATEST/KNA1.parquet"
url = f"abfss://{dl_dwh.file_system}@{dl_dwh.adls_client.primary_hostname}/{file_path}"
T7 = spark.read.parquet(url)

# COMMAND ----------

T1.createOrReplaceTempView("VBAK")
T2.createOrReplaceTempView("VBAP")
T3.createOrReplaceTempView("MAKT")
T4.createOrReplaceTempView("BSEG")
T5.createOrReplaceTempView("BKPF")
T6.createOrReplaceTempView("BSID")
T7.createOrReplaceTempView("KNA1")

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

def extrai_ctrs_v5(texto):
    """
    Extrai todos os contratos (CTRs) válidos de uma string de texto.

    Regras:
    - Aceita números iniciando com 40, com até dois zeros antes (ex: 0040123456 → 40123456).
    - Aceita formato com sufixos curtos (ex: 4012/3456 → 40123456).
    """
    if not texto:
        return []

    texto = texto.upper()
    resultado = set()

    # 1. Captura CTRs completos com até 2 zeros antes de '40'
    # Ex: 0040123456 → 40123456
    ctrs_completos = re.findall(r"\b0{0,2}(40\d{6})\b", texto)
    for ctr in ctrs_completos:
        resultado.add(ctr)

    # 2. Captura padrão de base/sufixos: 4012/3456, 4012/456
    padrao_com_sufixos = re.findall(r"\b(40\d{2})(?:\d{2})?/(\d{3,4})\b", texto)
    for base_parcial, sufixo in padrao_com_sufixos:
        novo_ctr = base_parcial + sufixo
        if len(novo_ctr) == 8 and novo_ctr.startswith("40"):
            resultado.add(novo_ctr)

    return list(resultado)

# COMMAND ----------

# Registra como UDF
extrai_ctrs_udf_v5 = F.udf(extrai_ctrs_v5, ArrayType(StringType()))

# Aplica ao DataFrame
df_bsid = spark.sql("SELECT BELNR AS numero_documento, SGTXT AS texto_documento FROM BSID")

df_ctrs = (
    df_bsid.withColumn("ctr_list", extrai_ctrs_udf_v5(F.col("texto_documento")))
           .withColumn("ctr", F.explode("ctr_list"))
           .select("numero_documento", "ctr")
)

# Cria a view temporária para usar no JOIN
df_ctrs.createOrReplaceTempView("ctrs_extraidos")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     bseg.BUKRS         AS empresa,
# MAGIC     bseg.BELNR         AS num_documento,
# MAGIC     bseg.SEGMENT       AS segmento,
# MAGIC     bseg.ZZBUPLA       AS local_negocio,
# MAGIC     bseg.DMBTR         AS montante_MI,
# MAGIC     bsid.DMBE2         AS montante_dolar,
# MAGIC     bseg.REBZG,
# MAGIC
# MAGIC     bkpf.BLART         AS tipo_documento,
# MAGIC     bsid.ZFBDT         AS data_documento,
# MAGIC     bkpf.XBLNR         AS referencia,
# MAGIC     bkpf.WAERS         AS moeda,
# MAGIC     bkpf.HWAER         AS moeda_orig,
# MAGIC     bkpf.HWAE2         AS moeda_sec,
# MAGIC     bkpf.KURS2         AS cambio_sec,
# MAGIC
# MAGIC     bsid.KUNNR         AS codigo_cliente,
# MAGIC
# MAGIC     kna1.NAME1         AS nome_cliente,
# MAGIC     kna1.STCD1         AS cnpj,
# MAGIC     kna1.STCD2         AS cpf,
# MAGIC
# MAGIC     bsid.SGTXT         AS texto,
# MAGIC     ctrs.ctr           AS ctr_barter
# MAGIC
# MAGIC
# MAGIC FROM 
# MAGIC     BSEG bseg
# MAGIC LEFT JOIN 
# MAGIC     BKPF bkpf ON bseg.BELNR = bkpf.BELNR
# MAGIC RIGHT JOIN 
# MAGIC     BSID bsid ON bseg.BELNR = bsid.BELNR
# MAGIC LEFT JOIN 
# MAGIC     KNA1 kna1 ON bsid.KUNNR = kna1.KUNNR
# MAGIC LEFT JOIN 
# MAGIC     ctrs_extraidos ctrs ON bseg.BELNR = ctrs.numero_documento
# MAGIC
# MAGIC WHERE 
# MAGIC    -- bseg.BELNR = 0096402614  --AND 
# MAGIC    -- bseg.REBZG <> '' AND 
# MAGIC     bseg.SEGMENT in ('0000000015', '0000000051', '0000000052') AND
# MAGIC     bkpf.BLART in ('C1', 'D5', 'D6', 'RV') AND
# MAGIC     ctrs.ctr <> '' AND
# MAGIC     --ctrs.ctr in ('40099421', '40097031', '40136119', '40110529' ,'40136943')
# MAGIC     ctrs.ctr in ('40117863', '40117884', '40097031', '40132243', '40117879', '40134203', '40117874', '40124022', '40117871', '40105653', '40108318', '40108323', '40128781', '40114038', '40105319', '40108317', '40108319', '40132245', '40131510', '40128896', '40128780', '40105318', '40136061', '40135594')
# MAGIC   
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY bseg.BELNR ORDER BY bseg.BELNR) = 1

# COMMAND ----------

ETL_Pedidos = _sqldf

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC
# MAGIC (VBAK.VBELN * 1) AS `Doc.vendas`, 
# MAGIC  VBAP.POSNR AS `Item Contr`,
# MAGIC  (VBAP.MATNR * 1) AS `Material`,
# MAGIC  MAKT.MAKTX AS `Descr. Material`,
# MAGIC  VBAP.NETWR AS `Valor Bruto`,
# MAGIC  VBAP.WAERK AS `Moeda`,
# MAGIC  VBAK.SPART AS `Setor atividade` -- DF Defensivos e FT Fertilizantes
# MAGIC
# MAGIC  FROM VBAK AS VBAK 
# MAGIC
# MAGIC LEFT JOIN VBAP AS VBAP
# MAGIC ON VBAK.VBELN = VBAP.VBELN
# MAGIC LEFT JOIN MAKT AS MAKT
# MAGIC ON VBAP.MATNR = MAKT.MATNR 
# MAGIC
# MAGIC WHERE 
# MAGIC MAKT.SPRAS = 'P' AND (VBAK.SPART = 'DF' OR VBAK.SPART = 'FT') AND (VBAK.VBELN * 1) in (40117863, 40117884, 40097031, 40132243, 40117879, 40134203, 40117874, 40124022, 40117871, 40105653, 40108318, 40108323, 40128781, 40114038, 40105319, 40108317, 40108319, 40132245, 40131510, 40128896, 40128780, 40105318, 40136061, 40135594)

# COMMAND ----------

ETL_Material = _sqldf

# COMMAND ----------

# 1. Salva a nova_view em 1 arquivo parquet no diretório temporário
ETL_Pedidos.coalesce(1).write.mode("overwrite").format("parquet").save(f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/public/SharedBusinessServices/SBS - Origination/ETL_Pedidos")

# 2. Lista os arquivos dentro do diretório temporário
arquivos_temp = dl_rep.ls("public/SharedBusinessServices/SBS - Origination/ETL_Pedidos")

# 3. Filtra para pegar o arquivo parquet gerado
arquivo_parquet = [f for f in arquivos_temp if "parquet" in f][0]

# 4. Verifica se o arquivo final já existe antes de removê-lo
arquivo_final = "public/SharedBusinessServices/SBS - Origination/ETL_Pedidos.parquet"
if dl_rep.exists(arquivo_final):
    dl_rep.rm(arquivo_final, recursive=False)

# 5. Move o novo parquet para o diretório final
dl_rep.mv(arquivo_parquet, arquivo_final)

# 6. Remove o diretório temporário
dl_rep.rm("public/SharedBusinessServices/SBS - Origination/ETL_Pedidos", recursive=True)

# COMMAND ----------

# 1. Salva a nova_view em 1 arquivo parquet no diretório temporário
ETL_Material.coalesce(1).write.mode("overwrite").format("parquet").save(f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/public/SharedBusinessServices/SBS - Origination/ETL_Material")

# 2. Lista os arquivos dentro do diretório temporário
arquivos_temp = dl_rep.ls("public/SharedBusinessServices/SBS - Origination/ETL_Material")

# 3. Filtra para pegar o arquivo parquet gerado
arquivo_parquet = [f for f in arquivos_temp if "parquet" in f][0]

# 4. Verifica se o arquivo final já existe antes de removê-lo
arquivo_final = "public/SharedBusinessServices/SBS - Origination/ETL_Material.parquet"
if dl_rep.exists(arquivo_final):
    dl_rep.rm(arquivo_final, recursive=False)

# 5. Move o novo parquet para o diretório final
dl_rep.mv(arquivo_parquet, arquivo_final)

# 6. Remove o diretório temporário
dl_rep.rm("public/SharedBusinessServices/SBS - Origination/ETL_Material", recursive=True)