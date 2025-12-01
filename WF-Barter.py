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

file_path = "public/SharedBusinessServices/SBS - Origination/Barter/df_baixa_adiantamento.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
df_baixa_adiantamento = spark.read.parquet(url)

file_path = "public/SharedBusinessServices/SBS - Origination/Barter/df_resultado.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
df_resultado = spark.read.parquet(url)

file_path = "public/SharedBusinessServices/SBS - Origination/Barter/df_tratativa_manual_cpf_final.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
df_tratativa_manual_cpf_final = spark.read.parquet(url)

file_path = "public/SharedBusinessServices/SBS - Origination/Barter/df_tratativa_manual_valor.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
df_tratativa_manual_valor = spark.read.parquet(url)

file_path = "public/SharedBusinessServices/SBS - Origination/Barter/recibos_pedidos.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
recibos_pedidos = spark.read.parquet(url)

file_path = "public/SharedBusinessServices/SBS - Origination/ETL_Pedidos.parquet"
url = f"abfss://{dl_rep.file_system}@{dl_rep.adls_client.primary_hostname}/{file_path}"
ETL_Pedidos = spark.read.parquet(url)

file_path = "BASE/SAPECC/LATEST/KNA1.parquet"
url = f"abfss://{dl_dwh.file_system}@{dl_dwh.adls_client.primary_hostname}/{file_path}"
KNA1 = spark.read.parquet(url)

# COMMAND ----------

df_baixa_adiantamento.createOrReplaceTempView("df_baixa_adiantamento")
df_resultado.createOrReplaceTempView("df_resultado")
df_tratativa_manual_cpf_final.createOrReplaceTempView("df_tratativa_manual_cpf_final")
df_tratativa_manual_valor.createOrReplaceTempView("df_tratativa_manual_valor")
recibos_pedidos.createOrReplaceTempView("recibos_pedidos")
ETL_Pedidos.createOrReplaceTempView("ETL_Pedidos")
KNA1.createOrReplaceTempView("KNA1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC df_resultado.AGRUPAMENTO,
# MAGIC df_resultado.ordem,
# MAGIC
# MAGIC CASE 
# MAGIC   WHEN segmento = 0000000052 THEN '52 - Fertilizantes'
# MAGIC   ELSE '51 - Defensivos'
# MAGIC END AS Segmento,
# MAGIC
# MAGIC codigo_cliente AS `Cód. Cliente`,
# MAGIC nome_cliente AS `Nome do Cliente`,
# MAGIC nome_cliente  AS `Depositante`,
# MAGIC ctr_barter AS `CTR FERT`,
# MAGIC referencia  AS `Referência (N° da NF)`,
# MAGIC df_resultado.num_documento AS `Nº do Documento`,
# MAGIC
# MAGIC CASE 
# MAGIC   WHEN (SOMA_ACUMULADA <= VALOR_ALVO) THEN VALOR_DOCUMENTO
# MAGIC   ELSE round(VALOR_DOCUMENTO - (SOMA_ACUMULADA - VALOR_ALVO),2)
# MAGIC END AS `VALOR_DOC`,
# MAGIC
# MAGIC `VALOR_DOC` / VALOR_DOCUMENTO AS `razao`,
# MAGIC
# MAGIC round(df_resultado.montante_MI * `razao`,2) AS `Montante MI`,
# MAGIC round(df_resultado.montante_dolar * `razao`,2) AS `Valor USD`,
# MAGIC
# MAGIC
# MAGIC ROUND(SOMA_ACUMULADA, 2) AS SOMA_ACUMULADA,
# MAGIC ROUND(VALOR_ALVO, 2) AS VALOR_ALVO,
# MAGIC
# MAGIC CASE 
# MAGIC   WHEN SOMA_ACUMULADA <= VALOR_ALVO THEN 'LIQUIDAÇÃO TOTAL'
# MAGIC   ELSE 'CTR ' || ctr_barter || 'NF ' || referencia || ' LIQUIDAÇÃO PARCIAL'
# MAGIC END AS `Justificativa`
# MAGIC
# MAGIC
# MAGIC from df_resultado
# MAGIC
# MAGIC left join ETL_Pedidos
# MAGIC   on df_resultado.num_documento = ETL_Pedidos.num_documento
# MAGIC
# MAGIC left join df_tratativa_manual_cpf_final
# MAGIC  on df_tratativa_manual_cpf_final.AGRUPAMENTO = df_resultado.AGRUPAMENTO
# MAGIC
# MAGIC left join df_tratativa_manual_valor
# MAGIC  on df_tratativa_manual_valor.AGRUPAMENTO = df_resultado.AGRUPAMENTO
# MAGIC
# MAGIC
# MAGIC where df_tratativa_manual_cpf_final.MOTIVO_MANUAL is null and df_tratativa_manual_valor.MOTIVO_MANUAL is null
# MAGIC
# MAGIC order by df_resultado.AGRUPAMENTO, df_resultado.ordem

# COMMAND ----------

df_documento_T1 = _sqldf
df_documento_T1.createOrReplaceTempView("df_documento_T1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   df_documento_T1.AGRUPAMENTO,
# MAGIC   array_agg(distinct cast(NUM_PEDIDO as string)) as NUM_PEDIDO_ARRAY
# MAGIC from df_documento_T1
# MAGIC left join recibos_pedidos
# MAGIC   on recibos_pedidos.agrupamento = df_documento_T1.AGRUPAMENTO
# MAGIC group by df_documento_T1.AGRUPAMENTO

# COMMAND ----------

joined_df = df_documento_T1.join(_sqldf, on="AGRUPAMENTO").orderBy("AGRUPAMENTO", "ordem")
display(joined_df)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

df = joined_df  # seu dataframe

# 1) Janela para pegar o último registro de cada agrupamento (maior ordem)
w = Window.partitionBy("AGRUPAMENTO").orderBy(F.desc("ordem"))

df_last = (
    df
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")  # último registro do agrupamento
    .filter(F.col("Justificativa") == "LIQUIDAÇÃO TOTAL")
)

# 2) Calcular diferença
df_last = df_last.withColumn(
    "VALOR_FALTANTE",
    F.round(F.col("VALOR_ALVO") - F.col("SOMA_ACUMULADA"), 2)
)

# 3) Apenas casos onde a diferença é > 1
df_last = df_last.filter(F.col("VALOR_FALTANTE") > 1)

# 4) Criar nova linha duplicada, ajustando ordem/valor
df_insert = (
    df_last
        .withColumn("ordem", F.col("ordem") + 1)
        .withColumn("Referência (N° da NF)", F.concat(F.lit("CTR FERT "), F.array_join(F.col("NUM_PEDIDO_ARRAY"), ", ")))
        .withColumn("Nº do Documento", F.lit("-"))
        .withColumn("VALOR_DOC", F.col("VALOR_FALTANTE"))
        .withColumn("Montante MI", F.when(F.col("Montante MI") > 0, F.col("VALOR_FALTANTE")).otherwise(F.lit(0)))
        .withColumn("Valor USD", F.when(F.col("Montante MI") == 0, F.col("VALOR_FALTANTE")).otherwise(F.lit(0)))
        .withColumn("SOMA_ACUMULADA", F.col("SOMA_ACUMULADA") + F.col("VALOR_FALTANTE"))
        .withColumn(
            "Justificativa",
            F.when(
                (F.col("VALOR_DOC") / F.col("VALOR_ALVO")) <= 0.2,
                F.concat(F.lit("ADTO BARTER - CTR FERT "), F.array_join(F.col("NUM_PEDIDO_ARRAY"), ", "))
            ).otherwise(F.lit("adto 20%"))
        )
        .drop("rn", "VALOR_FALTANTE")
)

# 5) Dataframe final
df_final = df.unionByName(df_insert)

# opcional: ordenar novamente
df_final = df_final.orderBy("AGRUPAMENTO", "ordem")

display(df_final)


# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC     SELECT 
# MAGIC         r.AGRUPAMENTO,
# MAGIC         zipped.LISTA_RECIBOS       AS Recibo,
# MAGIC         r.CONTRATO_RECIBO          AS Contrato,
# MAGIC         nvl(round((zipped.LISTA_VALORES / r.PTAX_PAGO),2), 0) AS `Valor USD`,
# MAGIC         r.PTAX_PAGO                AS Ptax,
# MAGIC         zipped.LISTA_VALORES          AS `Valor BRL`,
# MAGIC         zipped.LISTA_DATAS         AS `Data Pagamento`,
# MAGIC         r.NUM_PEDIDO,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY 
# MAGIC                 r.AGRUPAMENTO,
# MAGIC                 zipped.LISTA_RECIBOS,
# MAGIC                 r.CONTRATO_RECIBO,
# MAGIC                 zipped.LISTA_VALORES,
# MAGIC                 zipped.LISTA_DATAS
# MAGIC             ORDER BY r.NUM_PEDIDO
# MAGIC         ) AS rn
# MAGIC     FROM recibos_pedidos r
# MAGIC     LEFT JOIN df_tratativa_manual_cpf_final 
# MAGIC         ON df_tratativa_manual_cpf_final.AGRUPAMENTO = r.AGRUPAMENTO
# MAGIC     LEFT JOIN df_tratativa_manual_valor
# MAGIC         ON df_tratativa_manual_valor.AGRUPAMENTO = r.AGRUPAMENTO
# MAGIC     LEFT JOIN df_baixa_adiantamento
# MAGIC         ON df_baixa_adiantamento.AGRUPAMENTO = r.AGRUPAMENTO
# MAGIC     LATERAL VIEW posexplode(arrays_zip(
# MAGIC         r.LISTA_RECIBOS,
# MAGIC         r.LISTA_DATAS,
# MAGIC         r.LISTA_VALORES
# MAGIC     )) AS idx, zipped
# MAGIC     WHERE 
# MAGIC         df_tratativa_manual_cpf_final.MOTIVO_MANUAL IS NULL
# MAGIC         AND df_tratativa_manual_valor.MOTIVO_MANUAL IS NULL
# MAGIC         AND df_baixa_adiantamento.TIPO_BAIXA IS NULL
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM base
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df_documento_T2 = _sqldf

# COMMAND ----------

# MAGIC %skip
# MAGIC import pandas as pd
# MAGIC from openpyxl import load_workbook
# MAGIC from io import BytesIO
# MAGIC from copy import copy
# MAGIC import os
# MAGIC import ast
# MAGIC
# MAGIC # ----------------------------
# MAGIC # Copiar formatação de uma linha
# MAGIC # ----------------------------
# MAGIC def copiar_formatacao_linha(ws, origem, destino):
# MAGIC     max_col = ws.max_column
# MAGIC     for col in range(1, max_col + 1):
# MAGIC         cell_origem = ws.cell(row=origem, column=col)
# MAGIC         cell_destino = ws.cell(row=destino, column=col)
# MAGIC
# MAGIC         # copia _style (quando existir)
# MAGIC         if getattr(cell_origem, "has_style", False):
# MAGIC             try:
# MAGIC                 cell_destino._style = copy(cell_origem._style)
# MAGIC             except Exception:
# MAGIC                 pass
# MAGIC
# MAGIC         if cell_origem.border:
# MAGIC             cell_destino.border = copy(cell_origem.border)
# MAGIC         if cell_origem.fill:
# MAGIC             cell_destino.fill = copy(cell_origem.fill)
# MAGIC         if cell_origem.font:
# MAGIC             cell_destino.font = copy(cell_origem.font)
# MAGIC         if cell_origem.alignment:
# MAGIC             cell_destino.alignment = copy(cell_origem.alignment)
# MAGIC
# MAGIC         cell_destino.number_format = cell_origem.number_format
# MAGIC
# MAGIC
# MAGIC # -------------------------------------------------------
# MAGIC # Função principal: gerar arquivos Baixa por Documento
# MAGIC # -------------------------------------------------------
# MAGIC def gerar_baixa_por_documento(df_T1, df_T2, sp, url_modelo_local, pasta_saida_sharepoint):
# MAGIC     """
# MAGIC     df_T1: df_documento_T1 (pyspark or pandas)
# MAGIC     df_T2: df_documento_T2 (pyspark or pandas)
# MAGIC     sp: SharePointManager instance
# MAGIC     url_modelo_local: caminho local/URL do modelo (ex: "/mnt/data/Y25M011_modelo.xlsx")
# MAGIC     pasta_saida_sharepoint: destino no SharePoint (ex: "sites/.../16.%20Databricks")
# MAGIC     """
# MAGIC
# MAGIC     # Conversão Spark -> pandas se necessário
# MAGIC     if not isinstance(df_T1, pd.DataFrame):
# MAGIC         df_T1 = df_T1.toPandas()
# MAGIC     if not isinstance(df_T2, pd.DataFrame):
# MAGIC         df_T2 = df_T2.toPandas()
# MAGIC
# MAGIC     # Normalizar tipos e valores
# MAGIC     df_T1 = df_T1.fillna("")
# MAGIC     df_T2 = df_T2.fillna("")
# MAGIC
# MAGIC     # Garantir colunas relevantes existam
# MAGIC     required_t1 = {"AGRUPAMENTO", "ordem", "Segmento", "Cód. Cliente", "Nome do Cliente",
# MAGIC                    "Depositante", "CTR FERT", "Referência (N° da NF)", "Nº do Documento",
# MAGIC                    "VALOR_DOC", "Justificativa", "NUM_PEDIDO_ARRAY"}
# MAGIC     required_t2 = {"AGRUPAMENTO", "Recibo", "Contrato", "Valor USD", "Ptax",
# MAGIC                    "Valor BRL", "Data Pagamento", "NUM_PEDIDO"}
# MAGIC
# MAGIC     missing_t1 = required_t1 - set(df_T1.columns)
# MAGIC     missing_t2 = required_t2 - set(df_T2.columns)
# MAGIC     if missing_t1:
# MAGIC         raise ValueError(f"Faltam colunas em df_T1: {missing_t1}")
# MAGIC     if missing_t2:
# MAGIC         raise ValueError(f"Faltam colunas em df_T2: {missing_t2}")
# MAGIC
# MAGIC     # Itera por agrupamento (arquivo por agrupamento)
# MAGIC     agrupamentos = df_T1["AGRUPAMENTO"].unique()
# MAGIC
# MAGIC     for ag in agrupamentos:
# MAGIC         t1_ag = df_T1[df_T1["AGRUPAMENTO"] == ag].copy()
# MAGIC         t2_ag = df_T2[df_T2["AGRUPAMENTO"] == ag].copy()
# MAGIC
# MAGIC         if t1_ag.shape[0] == 0 and t2_ag.shape[0] == 0:
# MAGIC             continue
# MAGIC
# MAGIC         # ordena ordens por coluna 'ordem' (numérica se possível)
# MAGIC         try:
# MAGIC             t1_ag["ordem"] = pd.to_numeric(t1_ag["ordem"])
# MAGIC         except Exception:
# MAGIC             pass
# MAGIC         t1_ag = t1_ag.sort_values("ordem").reset_index(drop=True)
# MAGIC
# MAGIC         # carrega workbook do modelo (usando caminho local/SharePoint)
# MAGIC         # Observação: se usar sp.open com url_modelo_local, mantenha como antes.
# MAGIC         # Aqui fazemos o fluxo usando sp.open para manter consistência com uploads anteriores.
# MAGIC         with sp.open(url_modelo_local) as f:
# MAGIC             modelo_bytes = f.read()
# MAGIC
# MAGIC         wb = load_workbook(BytesIO(modelo_bytes))
# MAGIC         ws = wb.active
# MAGIC
# MAGIC         # ---------- Tabela 1 (linha base = 4) ----------
# MAGIC         base_t1 = 4
# MAGIC         # Quantas ordens teremos
# MAGIC         ordens = t1_ag["ordem"].tolist()
# MAGIC         n_ordens = len(ordens)
# MAGIC
# MAGIC         # Preenche/insere linhas para cada ordem
# MAGIC         # A primeira ordem cabe na linha base; demais exigem inserção de linha abaixo
# MAGIC         for idx, (_, ordem_row) in enumerate(t1_ag.reset_index().iterrows()):
# MAGIC             linha = base_t1 + idx
# MAGIC             if idx == 0:
# MAGIC                 # usa a linha base (não insere)
# MAGIC                 pass
# MAGIC             else:
# MAGIC                 # inserir nova linha na posição 'linha' (empurra tudo para baixo)
# MAGIC                 ws.insert_rows(linha)
# MAGIC                 # copiar formatação da linha base original (linha 4)
# MAGIC                 copiar_formatacao_linha(ws, origem=base_t1, destino=linha)
# MAGIC
# MAGIC             # preencher campos da Tabela 1 na linha calculada
# MAGIC             ws[f"A{linha}"].value = "MacroFértil"
# MAGIC             ws[f"B{linha}"].value = "1001 - 442 - LDC NOVA MUTUM"
# MAGIC             ws[f"C{linha}"].value = ordem_row["Segmento"]
# MAGIC             ws[f"D{linha}"].value = str(ordem_row["Cód. Cliente"])
# MAGIC             ws[f"E{linha}"].value = ordem_row["Nome do Cliente"]
# MAGIC             # F <- Referência (N° da NF)
# MAGIC             ws[f"F{linha}"].value = ordem_row["Referência (N° da NF)"]
# MAGIC             # G <- Nº do Documento
# MAGIC             ws[f"G{linha}"].value = ordem_row["Nº do Documento"]
# MAGIC             # H <- Montante MI
# MAGIC             ws[f"H{linha}"].value = ordem_row["Montante MI"]
# MAGIC             # I <- Valor USD
# MAGIC             ws[f"I{linha}"].value = ordem_row["Valor USD"]
# MAGIC             # J fixo (se aplicável)
# MAGIC             ws[f"J{linha}"].value = "24310008"
# MAGIC             # K <- Justificativa
# MAGIC             ws[f"K{linha}"].value = ordem_row["Justificativa"]
# MAGIC             # Guardar depositante para usar na tabela 2 por esta ordem
# MAGIC             # (mantemos em memória; T1 também tem coluna Depositante)
# MAGIC             # armazenar via dicionário: ordem_num -> depositante
# MAGIC         # construir map ordem -> depositante / outros campos
# MAGIC         ordem_depositante = {}
# MAGIC         ordem_info = {}
# MAGIC         for _, r in t1_ag.iterrows():
# MAGIC             ordem_key = r["ordem"]
# MAGIC             ordem_depositante[ordem_key] = r["Depositante"]
# MAGIC             ordem_info[ordem_key] = {
# MAGIC                 "Depositante": r["Depositante"],
# MAGIC                 "Segmento": r["Segmento"],
# MAGIC                 "Cód. Cliente": r["Cód. Cliente"],
# MAGIC                 "Nome do Cliente": r["Nome do Cliente"],
# MAGIC                 "Referência (N° da NF)": r["Referência (N° da NF)"],
# MAGIC                 "Nº do Documento": r["Nº do Documento"],
# MAGIC                 "VALOR_DOC": r["VALOR_DOC"],
# MAGIC                 "Justificativa": r["Justificativa"],
# MAGIC                 # parse NUM_PEDIDO_ARRAY (json-like string) para lista de pedidos
# MAGIC                 "NUM_PEDIDOS": []
# MAGIC             }
# MAGIC             # parse NUM_PEDIDO_ARRAY, que pode ser string como '["40097031"]'
# MAGIC             try:
# MAGIC                 arr = r["NUM_PEDIDO_ARRAY"]
# MAGIC                 if isinstance(arr, str) and arr.strip() != "":
# MAGIC                     # tenta interpretar como lista
# MAGIC                     pedidos = ast.literal_eval(arr)
# MAGIC                     # garantir strings e strip
# MAGIC                     pedidos = [str(p).strip() for p in pedidos]
# MAGIC                 elif isinstance(arr, (list, tuple)):
# MAGIC                     pedidos = [str(p).strip() for p in arr]
# MAGIC                 else:
# MAGIC                     pedidos = []
# MAGIC             except Exception:
# MAGIC                 pedidos = []
# MAGIC             ordem_info[ordem_key]["NUM_PEDIDOS"] = pedidos
# MAGIC
# MAGIC         # ---------- Após inserir linhas em T1, a posição da T2 base desloca ----------
# MAGIC         # original t2_base = 13, mas se inserimos (n_ordens - 1) linhas acima, ajusta:
# MAGIC         extra_t1 = max(0, n_ordens - 1)
# MAGIC         base_t2 = 13 + extra_t1
# MAGIC
# MAGIC         # A linha que serve de referência de formatação para T2 é 'base_t2' (a qual contém o template original)
# MAGIC         linha_base_t2_para_copiar = base_t2
# MAGIC
# MAGIC         # ---------- Tabela 2: para cada ordem, buscar os recibos correspondentes ----------
# MAGIC         # Começamos preenchendo a primeira linha de T2 em base_t2.
# MAGIC         current_t2_row_offset = 0  # usado para calcular onde escrever a próxima linha de T2
# MAGIC
# MAGIC         # Para cada ordem, na ordem definida em t1_ag (ordens ordenadas)
# MAGIC         for ordem_value in t1_ag["ordem"].tolist():
# MAGIC             # obter lista de NUM_PEDIDOS para essa ordem
# MAGIC             pedidos_ordem = ordem_info.get(ordem_value, {}).get("NUM_PEDIDOS", [])
# MAGIC
# MAGIC             if len(pedidos_ordem) == 0:
# MAGIC                 # se não houver pedidos para a ordem, não haverá linhas em T2 para essa ordem; pular
# MAGIC                 continue
# MAGIC
# MAGIC             # selecionar linhas t2 correspondentes a quaisquer NUM_PEDIDO que estejam na lista
# MAGIC             t2_para_ordem = t2_ag[t2_ag["NUM_PEDIDO"].astype(str).isin([str(x) for x in pedidos_ordem])].copy()
# MAGIC
# MAGIC             if t2_para_ordem.shape[0] == 0:
# MAGIC                 continue
# MAGIC
# MAGIC             # iterar linhas de t2_para_ordem em ordem original (se quiser ordenar, pode ordenar por rn)
# MAGIC             try:
# MAGIC                 t2_para_ordem = t2_para_ordem.sort_values("rn")
# MAGIC             except Exception:
# MAGIC                 pass
# MAGIC
# MAGIC             for idx_row, (_, rec_row) in enumerate(t2_para_ordem.iterrows()):
# MAGIC                 # calcula a linha no Excel onde vamos escrever
# MAGIC                 excel_row = base_t2 + current_t2_row_offset
# MAGIC
# MAGIC                 if current_t2_row_offset == 0:
# MAGIC                     # primeira linha de T2 escreve diretamente na base_t2 (não insere)
# MAGIC                     pass
# MAGIC                 else:
# MAGIC                     # para as demais, inserimos na posição excel_row (empurra linhas pra baixo)
# MAGIC                     ws.insert_rows(excel_row)
# MAGIC                     # copiar formatação da linha base (ajustada)
# MAGIC                     copiar_formatacao_linha(ws, origem=linha_base_t2_para_copiar, destino=excel_row)
# MAGIC
# MAGIC                 # Preencher colunas de T2 (B..H)
# MAGIC                 ws[f"B{excel_row}"].value = rec_row["Recibo"]
# MAGIC                 ws[f"C{excel_row}"].value = rec_row["Contrato"]
# MAGIC
# MAGIC                 # Valores numéricos
# MAGIC                 try:
# MAGIC                     ws[f"D{excel_row}"].value = float(rec_row["Valor USD"]) if rec_row["Valor USD"] != "" else 0.0
# MAGIC                 except Exception:
# MAGIC                     ws[f"D{excel_row}"].value = rec_row["Valor USD"]
# MAGIC
# MAGIC                 try:
# MAGIC                     ws[f"E{excel_row}"].value = float(rec_row["Ptax"]) if rec_row["Ptax"] != "" else 0.0
# MAGIC                 except Exception:
# MAGIC                     ws[f"E{excel_row}"].value = rec_row["Ptax"]
# MAGIC
# MAGIC                 try:
# MAGIC                     ws[f"F{excel_row}"].value = float(rec_row["Valor BRL"]) if rec_row["Valor BRL"] != "" else 0.0
# MAGIC                 except Exception:
# MAGIC                     ws[f"F{excel_row}"].value = rec_row["Valor BRL"]
# MAGIC
# MAGIC                 if rec_row["Data Pagamento"] != "":
# MAGIC                     try:
# MAGIC                         ws[f"G{excel_row}"].value = pd.to_datetime(rec_row["Data Pagamento"]).date()
# MAGIC                     except Exception:
# MAGIC                         ws[f"G{excel_row}"].value = rec_row["Data Pagamento"]
# MAGIC
# MAGIC                 # H (Depositante) - conforme instrução: IGNORAR H da entrada; usar Depositante vindo da T1 (ordem)
# MAGIC                 depositante_para_ordem = ordem_depositante.get(ordem_value, "")
# MAGIC                 ws[f"H{excel_row}"].value = depositante_para_ordem
# MAGIC
# MAGIC                 # avançar offset para próxima linha T2
# MAGIC                 current_t2_row_offset += 1
# MAGIC
# MAGIC         # ---------- nome do arquivo e salvar/upload ----------
# MAGIC         depositante_arquivo = (
# MAGIC             t1_ag.iloc[0]["Depositante"] if t1_ag.shape[0] > 0 else "NO_DEPOSITANTE"
# MAGIC         )
# MAGIC         depositante_arquivo = str(depositante_arquivo).replace(" ", "_")
# MAGIC         nome_final = f"Y25M011_{ag}_{depositante_arquivo}.xlsx"
# MAGIC
# MAGIC         temp_path = f"/tmp/{nome_final}"
# MAGIC         wb.save(temp_path)
# MAGIC
# MAGIC         url_saida = f"{pasta_saida_sharepoint}/{nome_final}"
# MAGIC         sp.upload_file(url_saida, temp_path, overwrite=True)
# MAGIC
# MAGIC         # opcional: remover arquivo temporário
# MAGIC         try:
# MAGIC             os.remove(temp_path)
# MAGIC         except Exception:
# MAGIC             pass
# MAGIC
# MAGIC         print(f"✔ Arquivo BAIXA gerado e enviado: {url_saida}")
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC from LDCDataAccessLayerPy import SharePointManager
# MAGIC import pandas as pd
# MAGIC
# MAGIC sp = SharePointManager()
# MAGIC
# MAGIC url_base = "https://ldcom365.sharepoint.com"
# MAGIC
# MAGIC # Caminho do seu modelo no SharePoint
# MAGIC url_modelo = (
# MAGIC     f"{url_base}/sites/GRP-Originao-CSC/Documentos%20Compartilhados/General/16.%20Databricks/Barter/Y25M011_modelo.xlsx"
# MAGIC )
# MAGIC
# MAGIC # Local de saída
# MAGIC pasta_saida = (
# MAGIC     f"{url_base}/sites/GRP-Originao-CSC/Documentos%20Compartilhados/General/16.%20Databricks/Barter"
# MAGIC )
# MAGIC
# MAGIC gerar_baixa_por_documento(df_final, df_documento_T2, sp, url_modelo, pasta_saida)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   df_baixa_adiantamento.AGRUPAMENTO,
# MAGIC   CASE 
# MAGIC     WHEN df_baixa_adiantamento.SETOR_ATIVIDADE = 'FT' THEN '52 - Fertilizantes'
# MAGIC     ELSE '51 - Defensivos'
# MAGIC   END AS Segmento,
# MAGIC   KUNNR AS `Cód. Cliente`,
# MAGIC   NAME1 AS `Nome do Cliente`,
# MAGIC   NAME1 AS `Depositante`,
# MAGIC   'CTR FERT ' || NUM_PEDIDO AS `Referência (N° da NF)`,
# MAGIC   CASE WHEN MOEDA <> 'USD' THEN VALOR_TOTAL_CONTRATO ELSE '-' END AS `Valor MI`,
# MAGIC   'ADTO BARTER - CTR FERT ' || NUM_PEDIDO AS `Justificativa`,
# MAGIC   Recibo,
# MAGIC   CONTRATO_RECIBO AS Contrato,
# MAGIC   VALOR_RECIBO_USD AS `Valor USD`,
# MAGIC   PTAX_PAGO AS Ptax,
# MAGIC   recibos_pedidos.SOMA_VALOR_PAGO AS `Valor BRL`,
# MAGIC   `Data Pagamento`
# MAGIC
# MAGIC
# MAGIC FROM 
# MAGIC   df_baixa_adiantamento
# MAGIC LEFT JOIN recibos_pedidos
# MAGIC   ON df_baixa_adiantamento.agrupamento = recibos_pedidos.agrupamento
# MAGIC LEFT JOIN KNA1
# MAGIC   ON recibos_pedidos.CPF_CNPJ_NORM = kna1.STCD1
# MAGIC   OR recibos_pedidos.CPF_CNPJ_NORM = kna1.STCD2
# MAGIC LATERAL VIEW EXPLODE(LISTA_RECIBOS) AS Recibo
# MAGIC LATERAL VIEW EXPLODE(LISTA_DATAS) AS `Data Pagamento`
# MAGIC WHERE 
# MAGIC   df_baixa_adiantamento.TIPO_BAIXA = "ADIANTAMENTO"

# COMMAND ----------

display(df_tratativa_manual_cpf_final)

# COMMAND ----------

# MAGIC %skip
# MAGIC import pandas as pd
# MAGIC from openpyxl import load_workbook
# MAGIC from io import BytesIO
# MAGIC from copy import copy
# MAGIC import os
# MAGIC
# MAGIC
# MAGIC # -------------------------------------------------------
# MAGIC # Copiar formatação da linha 13 para nova linha inserida
# MAGIC # -------------------------------------------------------
# MAGIC def copiar_formatacao_linha(ws, origem, destino):
# MAGIC     max_col = ws.max_column
# MAGIC     for col in range(1, max_col + 1):
# MAGIC         cell_origem = ws.cell(row=origem, column=col)
# MAGIC         cell_destino = ws.cell(row=destino, column=col)
# MAGIC
# MAGIC         if cell_origem.has_style:
# MAGIC             cell_destino._style = copy(cell_origem._style)
# MAGIC
# MAGIC         if cell_origem.border:
# MAGIC             cell_destino.border = copy(cell_origem.border)
# MAGIC
# MAGIC         if cell_origem.fill:
# MAGIC             cell_destino.fill = copy(cell_origem.fill)
# MAGIC
# MAGIC         if cell_origem.font:
# MAGIC             cell_destino.font = copy(cell_origem.font)
# MAGIC
# MAGIC         if cell_origem.alignment:
# MAGIC             cell_destino.alignment = copy(cell_origem.alignment)
# MAGIC
# MAGIC         cell_destino.number_format = cell_origem.number_format
# MAGIC
# MAGIC
# MAGIC
# MAGIC # -------------------------------------------------------
# MAGIC # FUNÇÃO PRINCIPAL — GERAR ARQUIVOS DE ADIANTAMENTO
# MAGIC # -------------------------------------------------------
# MAGIC def gerar_arquivos_adiantamento(df_adiantamento, sp, url_modelo, pasta_saida_sharepoint):
# MAGIC
# MAGIC     # df original é pyspark → converter para pandas
# MAGIC     df_adiantamento = df_adiantamento.toPandas()
# MAGIC
# MAGIC     agrupamentos = df_adiantamento["AGRUPAMENTO"].unique()
# MAGIC
# MAGIC     for ag in agrupamentos:
# MAGIC
# MAGIC         df_g = df_adiantamento[df_adiantamento["AGRUPAMENTO"] == ag]
# MAGIC         registro = df_g.iloc[0]
# MAGIC
# MAGIC         # ----------------------------------------------
# MAGIC         # Carregar modelo do SharePoint
# MAGIC         # ----------------------------------------------
# MAGIC         with sp.open(url_modelo) as f:
# MAGIC             modelo_bytes = f.read()
# MAGIC
# MAGIC         wb = load_workbook(BytesIO(modelo_bytes))
# MAGIC         ws = wb.active
# MAGIC
# MAGIC
# MAGIC         # ----------------------------------------------
# MAGIC         # TABELA 1 — Linha 4
# MAGIC         # ----------------------------------------------
# MAGIC         ws["A4"].value = "MacroFértil"
# MAGIC         ws["B4"].value = "1001 - 442 - LDC NOVA MUTUM"
# MAGIC         ws["C4"].value = registro["Segmento"]
# MAGIC         ws["D4"].value = str(registro["Cód. Cliente"])
# MAGIC         ws["E4"].value = registro["Nome do Cliente"]
# MAGIC         ws["F4"].value = registro["Referência (N° da NF)"]
# MAGIC         ws["G4"].value = "-"
# MAGIC         ws["H4"].value = float(registro["Valor MI"].replace(",", ".").replace("-", "0").strip())
# MAGIC         ws["J4"].value = "24310008"
# MAGIC         ws["K4"].value = registro["Justificativa"]
# MAGIC         # I4, L4, M4 possuem fórmulas
# MAGIC
# MAGIC
# MAGIC         # ----------------------------------------------
# MAGIC         # TABELA 2 — Dinâmica (linha 13 + novas linhas)
# MAGIC         # ----------------------------------------------
# MAGIC         lin_base = 13  
# MAGIC         row_excel = lin_base
# MAGIC
# MAGIC         # ---- Preenche o primeiro registro na própria linha 13 ----
# MAGIC         first = df_g.iloc[0]
# MAGIC
# MAGIC         ws[f"B{row_excel}"].value = first["Recibo"]
# MAGIC         ws[f"C{row_excel}"].value = first["Contrato"]
# MAGIC         ws[f"D{row_excel}"].value = float(first["Valor USD"])
# MAGIC         ws[f"E{row_excel}"].value = float(first["Ptax"])
# MAGIC         ws[f"F{row_excel}"].value = float(first["Valor BRL"])
# MAGIC
# MAGIC         if not pd.isna(first["Data Pagamento"]):
# MAGIC             ws[f"G{row_excel}"].value = pd.to_datetime(first["Data Pagamento"]).date()
# MAGIC
# MAGIC         ws[f"H{row_excel}"].value = first["Depositante"]
# MAGIC
# MAGIC
# MAGIC         # ---- Agora insere novas linhas para os demais registros ----
# MAGIC         for idx in range(1, len(df_g)):
# MAGIC
# MAGIC             nova_linha = row_excel + idx
# MAGIC
# MAGIC             # Insere linha abaixo
# MAGIC             ws.insert_rows(nova_linha)
# MAGIC
# MAGIC             # Copia formatação da linha 13 ORIGINAL
# MAGIC             copiar_formatacao_linha(ws, origem=lin_base, destino=nova_linha)
# MAGIC
# MAGIC             row = df_g.iloc[idx]
# MAGIC
# MAGIC             ws[f"B{nova_linha}"].value = row["Recibo"]
# MAGIC             ws[f"C{nova_linha}"].value = row["Contrato"]
# MAGIC             ws[f"D{nova_linha}"].value = float(row["Valor USD"])
# MAGIC             ws[f"E{nova_linha}"].value = float(row["Ptax"])
# MAGIC             ws[f"F{nova_linha}"].value = float(row["Valor BRL"])
# MAGIC
# MAGIC             if not pd.isna(row["Data Pagamento"]):
# MAGIC                 ws[f"G{nova_linha}"].value = pd.to_datetime(row["Data Pagamento"]).date()
# MAGIC
# MAGIC             ws[f"H{nova_linha}"].value = row["Depositante"]
# MAGIC
# MAGIC
# MAGIC         # ----------------------------------------------
# MAGIC         # Nome final do arquivo
# MAGIC         # ----------------------------------------------
# MAGIC         depositante = registro["Depositante"].replace(" ", "_")
# MAGIC         nome_final = f"Y25M011_{ag}_{depositante}.xlsx"
# MAGIC
# MAGIC
# MAGIC         # ----------------------------------------------
# MAGIC         # Salvar localmente e enviar ao SharePoint
# MAGIC         # ----------------------------------------------
# MAGIC         temp_path = f"/tmp/{nome_final}"
# MAGIC         wb.save(temp_path)
# MAGIC
# MAGIC         url_saida = f"{pasta_saida_sharepoint}/{nome_final}"
# MAGIC         sp.upload_file(url_saida, temp_path, overwrite=True)
# MAGIC
# MAGIC         print(f"✔ Arquivo enviado ao SharePoint: {url_saida}")
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC from LDCDataAccessLayerPy import SharePointManager
# MAGIC import pandas as pd
# MAGIC
# MAGIC sp = SharePointManager()
# MAGIC
# MAGIC url_base = "https://ldcom365.sharepoint.com"
# MAGIC
# MAGIC # Caminho do seu modelo no SharePoint
# MAGIC url_modelo = (
# MAGIC     f"{url_base}/sites/GRP-Originao-CSC/Documentos%20Compartilhados/General/16.%20Databricks/Barter/Y25M011_modelo.xlsx"
# MAGIC )
# MAGIC
# MAGIC # Local de saída
# MAGIC pasta_saida = (
# MAGIC     f"{url_base}/sites/GRP-Originao-CSC/Documentos%20Compartilhados/General/16.%20Databricks/Barter"
# MAGIC )
# MAGIC
# MAGIC df_adiantamento = _sqldf
# MAGIC
# MAGIC gerar_arquivos_adiantamento(df_adiantamento, sp, url_modelo, pasta_saida)
# MAGIC