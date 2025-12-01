# Databricks notebook source
from LDCDataAccessLayerPy import databricks_init, KeyVaultManager, data_lake, SharePointManager
from datetime import datetime
import pandas as pd
%pip install pyxlsb

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

# Dicionário para converter número do mês em abreviação
meses_abrev = {
    1: "01-JAN", 2: "02-FEV", 3: "03-MAR", 4: "04-ABR",
    5: "05-MAI", 6: "06-JUN", 7: "07-JUL", 8: "08-AGO",
    9: "09-SET", 10: "10-OUT", 11: "11 NOV", 12: "12-DEZ"
}

# Data atual
hoje = datetime.today()
ano, mes, dia = hoje.year, hoje.month, hoje.day
mes_abrev = meses_abrev[mes]

# Monta o caminho dinâmico
#arquivo = f"CARTEIRA_FERTILIZANTES_NLT_{dia:02d}_{mes:02d}_{ano}.xlsm"
arquivo = f"CARTEIRA_FERTILIZANTES_NLT_12_11_2025.xlsm"
#folder = f"sites/plataforma-fertilizantes/planejamento/PLANEJAMENTO/04_CARTEIRA/01_CARTEIRA/{ano}/{mes_abrev}"
folder = f"sites/plataforma-fertilizantes/planejamento/PLANEJAMENTO/04_CARTEIRA/01_CARTEIRA/2025/11 NOV/"
file_path = f"{folder}/{arquivo}"

# URL base
url = "https://ldcom365.sharepoint.com"

# Leitura do arquivo e seleção de colunas de interesse
if __name__ == '__main__':
    sp = SharePointManager()
    with sp.open(f"{url}/{file_path}") as f:
        # Lê a planilha, usando a linha 3 como cabeçalho (índice 3)
        DF_Fertilizantes = pd.read_excel(f, sheet_name="Carteira Fertilizantes SAP", header=3)

# COMMAND ----------

DF_Fertilizantes.columns = DF_Fertilizantes.iloc[0]  # define a primeira linha como cabeçalho
DF_Fertilizantes = DF_Fertilizantes[1:]              # remove a linha do cabeçalho do corpo

# Colunas desejadas
colunas_interesse = [
    "CLIENTE EMISSOR",
    "CLIENTE RECEBEDOR",
    "CONTRATO",
    "VALOR ($) A FATURAR",
    "MOEDA",
    "CNPJ/CPF Cliente Emissor",
    "CNPJ/CPF Cliente Recebedor"
]

# Filtra apenas as colunas de interesse (sem erro se faltar alguma)
DF_Fertilizantes = DF_Fertilizantes[[c for c in colunas_interesse if c in DF_Fertilizantes.columns]]
if "CONTRATO" in DF_Fertilizantes.columns:
    DF_Fertilizantes["CONTRATO"] = DF_Fertilizantes["CONTRATO"].astype(str).str.replace(r"-10$", "", regex=True)

# COMMAND ----------

DF_Fertilizantes

# COMMAND ----------

from datetime import datetime

# Carteira
if __name__ == '__main__':
    url = "https://ldcom365.sharepoint.com"
    sp = SharePointManager()

    # Gera a data atual no formato dd.mm.yyyy
    data_hoje = datetime.today().strftime("%d.%m.%Y")

    #folder = "sites/GRP-CROP-DEFENSIVOS/Shared%20Documents/Relat%C3%B3rios/OPEN%20SALES%20-%20GESTORES/NOVEMBRO"
    folder = "sites/GRP-CROP-DEFENSIVOS/Shared%20Documents/Relat%C3%B3rios/OPEN%20SALES%20-%20GESTORES/NOVEMBRO"
    #file_path = f"{folder}/CARTEIRA_DEFENSIVOS_{data_hoje}.xlsb"
    file_path = f"{folder}/CARTEIRA_DEFENSIVOS_12.11.2025.xlsb"

    with sp.open(f"{url}/{file_path}") as f:
        DF_Defensivos = pd.read_excel(f, sheet_name="Carteira Crop SAP", header=3)

# COMMAND ----------

DF_Defensivos.columns = DF_Defensivos.iloc[0]  # DF_Defensivosine a primeira linha como cabeçalho
DF_Defensivos = DF_Defensivos[1:]              # remove a linha do cabeçalho do corpo

# Colunas desejadas
colunas_interesse = [
    "CLIENTE ",
    "CONTRATO",
    "Moeda2",
    "SALDO À ETG",
    "Valor Unitário Produto",
    "CNPJ/CPF Cliente Emissor"

]

# Filtra apenas as colunas de interesse (sem erro se faltar alguma)
DF_Defensivos = DF_Defensivos[[c for c in colunas_interesse if c in DF_Defensivos.columns]]

# Remove qualquer sufixo no formato "-00" (ex: -10, -20, -35, etc.)
if "CONTRATO" in DF_Defensivos.columns:
    DF_Defensivos["CONTRATO"] = DF_Defensivos["CONTRATO"].astype(str).str.replace(r"-\d{2}$", "", regex=True)


# COMMAND ----------

DF_Defensivos["VALOR ($) A FATURAR"] = (
    DF_Defensivos["SALDO À ETG"] * DF_Defensivos["Valor Unitário Produto"]
)
display(DF_Defensivos)

# COMMAND ----------

# Corrige apenas a coluna problemática
DF_Defensivos["VALOR ($) A FATURAR"] = (
    DF_Defensivos["VALOR ($) A FATURAR"]
    .astype(str)                         # Garante que tudo é string
    .str.replace(r"[^\d,.-]", "", regex=True)  # Remove símbolos não numéricos
    .str.replace(",", ".", regex=False)  # Troca vírgula por ponto (se houver)
)

# Converte para numérico, forçando erros a NaN
DF_Defensivos["VALOR ($) A FATURAR"] = pd.to_numeric(
    DF_Defensivos["VALOR ($) A FATURAR"], errors="coerce"
)

# COMMAND ----------

DF_Fertilizantes = spark.createDataFrame(DF_Fertilizantes)
DF_Defensivos = spark.createDataFrame(DF_Defensivos)

# COMMAND ----------

DF_Defensivos = DF_Defensivos.drop("SALDO À ETG", "Valor Unitário Produto")
display(DF_Defensivos)

# COMMAND ----------

# 1. Salva a nova_view em 1 arquivo parquet no diretório temporário
DF_Fertilizantes.coalesce(1).write.mode("overwrite").format("parquet").save(f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/public/SharedBusinessServices/SBS - Origination/ETL_Fertilizantes")

# 2. Lista os arquivos dentro do diretório temporário
arquivos_temp = dl_rep.ls("public/SharedBusinessServices/SBS - Origination/ETL_Fertilizantes")

# 3. Filtra para pegar o arquivo parquet gerado
arquivo_parquet = [f for f in arquivos_temp if "parquet" in f][0]

# 4. Verifica se o arquivo final já existe antes de removê-lo
arquivo_final = "public/SharedBusinessServices/SBS - Origination/ETL_Fertilizantes.parquet"
if dl_rep.exists(arquivo_final):
    dl_rep.rm(arquivo_final, recursive=False)

# 5. Move o novo parquet para o diretório final
dl_rep.mv(arquivo_parquet, arquivo_final)

# 6. Remove o diretório temporário
dl_rep.rm("public/SharedBusinessServices/SBS - Origination/ETL_Fertilizantes", recursive=True)

# COMMAND ----------

# 1. Salva a nova_view em 1 arquivo parquet no diretório temporário
DF_Defensivos.coalesce(1).write.mode("overwrite").format("parquet").save(f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/public/SharedBusinessServices/SBS - Origination/ETL_Defensivos")

# 2. Lista os arquivos dentro do diretório temporário
arquivos_temp = dl_rep.ls("public/SharedBusinessServices/SBS - Origination/ETL_Defensivos")

# 3. Filtra para pegar o arquivo parquet gerado
arquivo_parquet = [f for f in arquivos_temp if "parquet" in f][0]

# 4. Verifica se o arquivo final já existe antes de removê-lo
arquivo_final = "public/SharedBusinessServices/SBS - Origination/ETL_Defensivos.parquet"
if dl_rep.exists(arquivo_final):
    dl_rep.rm(arquivo_final, recursive=False)

# 5. Move o novo parquet para o diretório final
dl_rep.mv(arquivo_parquet, arquivo_final)

# 6. Remove o diretório temporário
dl_rep.rm("public/SharedBusinessServices/SBS - Origination/ETL_Defensivos", recursive=True)