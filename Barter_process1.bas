Attribute VB_Name = "Módulo1"
Option Explicit
' Declarações de variáveis para conexão SAP
Dim SapGuiAuto As Object
Dim application As Object
Dim connection As Object
Dim session As Object

' Variável de conexão global
Public conn As Object

' ===== Conectar no banco via ODBC =====
Sub ConectarBanco()
    Dim strConn As String
    Dim usuario As String, senha As String
    
    ' >> Defina aqui o usuário e senha <<
    usuario = "santev"
    senha = "Yt7#Bp5$Xv3%Lm9&Qw4@"
    
    ' String de conexão ODBC
    strConn = "DSN=BRCOISP1;UID=" & usuario & ";PWD=" & senha & ";"
    
    ' Cria objeto de conexão
    Set conn = CreateObject("ADODB.Connection")
    conn.Open strConn
    
    'MsgBox "Conexão estabelecida com sucesso!", vbInformation
End Sub

' ===== Desconectar do banco =====
Sub DesconectarBanco()
    On Error Resume Next
    If Not conn Is Nothing Then
        If conn.State = 1 Then conn.Close
        Set conn = Nothing
    End If
    'MsgBox "Conexão encerrada.", vbInformation
End Sub


Sub SAP_Connection()
' Cria/pega instância SAP
    On Error Resume Next
    Set SapGuiAuto = GetObject("SAPGUI") ' Já aberta
    If SapGuiAuto Is Nothing Then
        MsgBox "SAP GUI não está aberto. Abra o SAP e tente novamente.", vbCritical
        Exit Sub
    End If
    On Error GoTo 0
    
    Set application = SapGuiAuto.GetScriptingEngine
    Set connection = application.Children(0)
    Set session = connection.Children(0)


End Sub

Sub Entrar_WF()

    Dim Layout As Boolean
    Dim rlayout As Long
    Dim vrow As Integer
    
    vrow = 2
    Layout = False
    rlayout = 0

    Call SAP_Connection
    
    ' --- A partir daqui é a automação SAP ---
    
    ' Entra na transação do WF
    session.findById("wnd[0]/tbar[0]/okcd").Text = "SBWP"
    session.findById("wnd[0]/tbar[0]/btn[0]").press
    
    session.findById("wnd[0]/usr/cntlSINWP_CONTAINER/shellcont/shell/shellcont[0]/shell").selectedNode = "          2"
    session.findById("wnd[0]/usr/cntlSINWP_CONTAINER/shellcont/shell/shellcont[0]/shell").selectedNode = "          5"
    
    session.findById("wnd[0]/usr/cntlSINWP_CONTAINER/shellcont/shell/shellcont[1]/shell/shellcont[0]/shell").pressToolbarContextButton "&MB_VARIANT"
    session.findById("wnd[0]/usr/cntlSINWP_CONTAINER/shellcont/shell/shellcont[1]/shell/shellcont[0]/shell").selectContextMenuItem "&LOAD"
    
    
    ' Pesquisa o layout com nome /AWORKBARTER
    Do While Layout = False

        If session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").GetCellValue(rlayout, "VARIANT") = "/AWORKBARTER" Then

            Layout = True
            'msgbox session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").GetCellValue(rlayout, "VARIANT")
            session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").setCurrentCell rlayout, "TEXT"
            session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").selectedRows = rlayout
            session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").clickCurrentCell

        Else

            rlayout = rlayout + 1
    
        End If

    Loop
    
End Sub

Sub ExportarDocSAP()
    Dim table As Object
    Dim i As Long
    Dim rowCount As Long
    Dim ws As Worksheet
    Dim tbl As ListObject
    Dim rng As Range

    ' Conecta SAP
    Call SAP_Connection

    ' Aba de destino
    On Error Resume Next
    Set ws = ThisWorkbook.Sheets("TabelaSAP")
    If ws Is Nothing Then
        Set ws = ThisWorkbook.Sheets.Add
        ws.Name = "TabelaSAP"
    End If
    On Error GoTo 0

    ' Captura a tabela SAP
    Set table = session.findById("wnd[0]/usr/cntlSINWP_CONTAINER/shellcont/shell/shellcont[1]/shell/shellcont[0]/shell")

    ' Número de linhas
    rowCount = table.rowCount

    ' Verifica se a tabela "T_DOC_SAP" já existe
    On Error Resume Next
    Set tbl = ws.ListObjects("T_DOC_SAP")
    On Error GoTo 0

    ' Se a tabela ainda não existir, cria
    If tbl Is Nothing Then
        ws.Cells(1, 1).Value = "DOC_SAP"
        Set rng = ws.Range("A1:A" & rowCount + 1)
        Set tbl = ws.ListObjects.Add(xlSrcRange, rng, , xlYes)
        tbl.Name = "T_DOC_SAP"
    Else
        ' Se a tabela já existir, limpa apenas os dados
        If tbl.ListRows.Count > 0 Then tbl.DataBodyRange.ClearContents
    End If

    ' Exportar apenas a 4ª coluna (índice 3 pois começa em zero)
    For i = 0 To rowCount - 1
        ws.Cells(i + 2, 1).Value = table.GetCellValue(i, table.ColumnOrder(3))
    Next i

    ' Atualiza o intervalo da tabela (expande se necessário)
    Set rng = ws.Range("A1:A" & rowCount + 1)


    'MsgBox "Exportação concluída com sucesso!", vbInformation
End Sub


Function ConverterDataSAP(dataSAP As String) As String
    ' Converte "03.11.2025" ? "20251103"
    If Len(dataSAP) = 10 And InStr(dataSAP, ".") = 3 Then
        ConverterDataSAP = Mid(dataSAP, 7, 4) & Mid(dataSAP, 4, 2) & Left(dataSAP, 2)
    Else
        ConverterDataSAP = dataSAP ' retorna original se formato inesperado
    End If
End Function




Sub ValidarQueriesEExportarResultados()
    Dim sqlA As String, sqlB As String
    Dim rsMain As Object, rsPedido As Object ' ADODB.Recordset (late binding)
    Dim dictPedidos As Object ' Scripting.Dictionary
    Dim wb As Workbook, wsSQL As Worksheet
    Dim wsOut As Worksheet
    Dim rowOut As Long
    Dim fld As Object
    Dim i As Long

    
    ' -----------------------
    ' VARIÁVEIS QUE VÊM DA TELA SAP
    ' -----------------------
    ' Preencha manualmente aqui OU leia de células (ex.: Sheets("TelaSAP").Range("B2"))
    Dim SAP_Recibo As String
 
    ' -----------------------
    
    Set wb = ThisWorkbook
    On Error GoTo ErrHandler
    Set wsSQL = wb.Worksheets("SQL")
    
    sqlA = Trim(wsSQL.Range("A1").Value)
    sqlB = Trim(wsSQL.Range("B1").Value)
    If sqlA = "" Or sqlB = "" Then
        MsgBox "As células SQL!A1 e/ou SQL!B1 estão vazias. Verifique.", vbExclamation
        Exit Sub
    End If
    
    
    
    ' Conecta (usa sua rotina)
    ConectarBanco
    
    ' Verifica se conn existe e está aberta
    If conn Is Nothing Then
        MsgBox "A conexão 'conn' não foi inicializada. Verifique ConectarBanco.", vbCritical
        GoTo CleanUp
    End If
    If Not (TypeName(conn) Like "*Connection*") Then
        MsgBox "A variável 'conn' não é um objeto de conexão válido.", vbCritical
        GoTo CleanUp
    End If
    If conn.State <> 1 Then ' adStateOpen = 1
        MsgBox "A conexão com o banco não está aberta.", vbCritical
        GoTo CleanUp
    End If
    
    ' Executa query principal (A1)
    Set rsMain = CreateObject("ADODB.Recordset")
    rsMain.CursorLocation = 3 ' adUseClient
    rsMain.Open sqlA, conn, 0, 1 ' adOpenForwardOnly, adLockReadOnly
    
    ' Executa query de pedidos (B1)
    Set rsPedido = CreateObject("ADODB.Recordset")
    rsPedido.CursorLocation = 3
    rsPedido.Open sqlB, conn, 0, 1
    
    ' Monta dicionário DOC_SAP -> coleção de pedidos (string concatenada)
    Set dictPedidos = CreateObject("Scripting.Dictionary")
    If Not rsPedido.EOF Then
        rsPedido.MoveFirst
        Do While Not rsPedido.EOF
            Dim keyDoc As String, pedidoVal As String
            keyDoc = NzString(rsPedido.Fields("DOC_SAP").Value)
            pedidoVal = NzString(rsPedido.Fields("PEDIDO").Value)
            If dictPedidos.Exists(keyDoc) Then
                dictPedidos(keyDoc) = dictPedidos(keyDoc) & "||" & pedidoVal
            Else
                dictPedidos.Add keyDoc, pedidoVal
            End If
            rsPedido.MoveNext
        Loop
    End If
    
    ' Prepara planilha de saída
    On Error Resume Next
    Set wsOut = wb.Worksheets("Resultado_Validação")
    If Not wsOut Is Nothing Then
        application.DisplayAlerts = False
        wsOut.Delete
        application.DisplayAlerts = True
    End If
    On Error GoTo ErrHandler
    Set wsOut = wb.Worksheets.Add(After:=wb.Worksheets(wb.Worksheets.Count))
    wsOut.Name = "Resultado_Validação"
    
    ' Cabeçalho: copia campos do rsMain + colunas de validação
    rowOut = 1
    Dim colIndex As Long
    colIndex = 1
    If Not rsMain.EOF Then
        For Each fld In rsMain.Fields
            wsOut.Cells(rowOut, colIndex).Value = fld.Name
            colIndex = colIndex + 1
        Next fld
    End If
    ' adiciona colunas de validação
    wsOut.Cells(rowOut, colIndex).Value = "Recibo_match"
    wsOut.Cells(rowOut, colIndex + 1).Value = "Status_is_21"
    wsOut.Cells(rowOut, colIndex + 2).Value = "Contrato_match"
    wsOut.Cells(rowOut, colIndex + 3).Value = "Has_Pedido_40"
    wsOut.Cells(rowOut, colIndex + 4).Value = "FormaPagamento_W"
    wsOut.Cells(rowOut, colIndex + 5).Value = "DataPrevisao_match"
    wsOut.Cells(rowOut, colIndex + 6).Value = "ValorPago_leq_ValorCC"
    wsOut.Cells(rowOut, colIndex + 7).Value = "ValorPago_match_SAP"
    wsOut.Cells(rowOut, colIndex + 8).Value = "ResumoFalhas"
    
    ' Processa cada linha de rsMain
    rowOut = 2
    If Not rsMain.EOF Then
        rsMain.MoveFirst
        Do While Not rsMain.EOF
            Dim docSAP As String
            Dim recibo As String
            Dim statusVal As String
            Dim contratoCC As String
            Dim contratoRecibo As String
            Dim formaPag As String
            Dim dataPrevRaw As Variant
            Dim dataPrevDate As Variant
            Dim valorCCRaw As Variant, valorPagoRaw As Variant
            Dim valorCC As Double, valorPago As Double
            
            Dim table As Object
            Dim SAP_DataPrevisao As String
            Dim SAP_ValorPago As String
            
            
            ' leia campos conforme aliases que você informou nas queries
            recibo = NzString(rsMain.Fields("RECIBO").Value)
            docSAP = NzString(rsMain.Fields("DOC_SAP").Value)
            statusVal = NzString(rsMain.Fields("STATUS").Value)
            contratoCC = NzString(rsMain.Fields("CONTRATO_CC").Value)
            contratoRecibo = NzString(rsMain.Fields("CONTRATO_Recibo").Value) ' observe o alias exato
            formaPag = NzString(rsMain.Fields("FORMA_PAGAMENTO").Value)
            dataPrevRaw = rsMain.Fields("DATA_PREVISAO").Value
            valorCCRaw = rsMain.Fields("Valor_CC").Value
            valorPagoRaw = rsMain.Fields("Valor_Pago").Value
            
            ' normaliza valores numéricos (remove R$, pontos, transformar vírgula em ponto)
            valorCC = (valorCCRaw * 1)
            valorPago = (valorPagoRaw * 1)
            
            ' converte data previsão (aceita YYYYMMDD ou data nativa)
            dataPrevDate = ParsePossibleYYYYMMDDToDate(dataPrevRaw)
            
            ' inicia array de resultados de validação
            Dim passRecibo As Boolean, passStatus As Boolean, passContrato As Boolean
            Dim passPedido40 As Boolean, passFormaPag As Boolean, passDataPrev As Boolean
            Dim passValorLeq As Boolean, passValorEqSAP As Boolean
            Dim messages As String
            Dim txtData As Object
            Dim txtValor As Object
            
            messages = ""
            
            
            
            
           ' Conecta ao SAP (usa sua rotina existente)
    Call SAP_Connection
    
    ' Captura a tabela SAP
    Set table = session.findById("wnd[0]/usr/cntlSINWP_CONTAINER/shellcont/shell/shellcont[1]/shell/shellcont[0]/shell")
    
    ' Seleciona e abre o documento (linha da tabela)
    table.currentCellRow = CStr(rowOut - 2)
    table.selectedRows = CStr(rowOut - 2)
   ' Excel.application.Wait (Now + TimeValue("0:00:02"))
    
    table.doubleClickCurrentCell
    
    ' Espera a tela abrir
    'Excel.application.Wait (Now + TimeValue("0:00:02"))
    
    ' Captura os campos desejados
    Set txtData = session.findById("wnd[0]/usr/ctxtBSEG-ZFBDT")
    SAP_DataPrevisao = txtData.Text
    
    
    Set txtValor = session.findById("wnd[0]/usr/txtBSEG-WRBTR")
    SAP_ValorPago = txtValor.Text

    
    ' Limpeza de formatação
    SAP_DataPrevisao = Trim(SAP_DataPrevisao)
    SAP_ValorPago = Replace(Replace(SAP_ValorPago, ".", ""), ",", ".") ' transforma para formato numérico
    
    ' Fecha o documento e retorna à tabela
    session.findById("wnd[0]/tbar[0]/btn[3]").press
    
    ' Espera a tela abrir
    'Excel.application.Wait (Now + TimeValue("0:00:02"))
    
    table.currentCellRow = CStr(rowOut - 2)
    table.selectedRows = CStr(rowOut - 2)
    session.findById("wnd[0]/usr/cntlSINWP_CONTAINER/shellcont/shell/shellcont[1]/shell/shellcont[0]/shell").pressToolbarButton "ABCK"
    
    ' Espera a tela abrir
    'Excel.application.Wait (Now + TimeValue("0:00:02"))
    
    
    SAP_DataPrevisao = ConverterDataSAP(SAP_DataPrevisao)
    
                              
            
            ' Validação 1: RECIBO precisa coincidir com a variável SAP_Recibo (se SAP_Recibo for preenchida)
            If Trim(SAP_Recibo) <> "" Then
                passRecibo = (Trim(UCase(recibo)) = Trim(UCase(SAP_Recibo)))
                If Not passRecibo Then messages = messages & "RECIBO diferente; "
            Else
                passRecibo = True
            End If
            
            ' Validação 2: STATUS = 21
            passStatus = (Trim(statusVal) = "21")
            If Not passStatus Then messages = messages & "STATUS <> 21; "
            
            ' Validação 3: CONTRATO_CC = CONTRATO_RECIBO
            passContrato = (Trim(contratoCC) = Trim(contratoRecibo))
            If Not passContrato Then messages = messages & "CONTRATO_CC <> CONTRATO_RECIBO; "
            
            ' Validação 4: Pelo menos 1 PEDIDO que comece com "40" (procura em dictPedidos por DOC_SAP)
            If dictPedidos.Exists(docSAP) Then
                Dim allPedidos As String
                allPedidos = dictPedidos(docSAP) ' string concatenada com || entre valores
                passPedido40 = PedidoListHasPrefix40(allPedidos)
                If Not passPedido40 Then messages = messages & "Nenhum PEDIDO com prefixo '40'; "
            Else
                passPedido40 = False
                messages = messages & "Nenhum PEDIDO encontrado na Query B1; "
            End If
            
            ' Validação 5: FORMA_PAGAMENTO = "W"
            passFormaPag = (Trim(UCase(formaPag)) = "W")
            If Not passFormaPag Then messages = messages & "FORMA_PAGAMENTO <> 'W'; "
            
            ' Validação 6: DATA_PREVISAO precisa coincidir com SAP_DataPrevisao (se variável SAP preenchida)
            If Not IsEmpty(SAP_DataPrevisao) And Trim(NzString(SAP_DataPrevisao)) <> "" Then
                Dim sapDate As Variant
                sapDate = ParsePossibleYYYYMMDDToDate(SAP_DataPrevisao)
                If IsDate(sapDate) And IsDate(dataPrevDate) Then
                    passDataPrev = (CDate(dataPrevDate) = CDate(sapDate))
                    If Not passDataPrev Then messages = messages & "DATA_PREVISAO divergente; "
                Else
                    passDataPrev = (Trim(NzString(dataPrevRaw)) = Trim(NzString(SAP_DataPrevisao)))
                    If Not passDataPrev Then messages = messages & "DATA_PREVISAO divergente (comparação string); "
                End If
            Else
                passDataPrev = True
            End If
            
            ' Validação 7: Valor_Pago não pode ser maior do que Valor_CC
            passValorLeq = (valorPago <= valorCC)
            If Not passValorLeq Then messages = messages & "VALOR_PAGO > VALOR_CC; "
            
            ' Validação 8: VALOR_PAGO precisa ser igual ao valor que vem da tela do SAP (se informado)
            If Not IsEmpty(SAP_ValorPago) And Trim(NzString(SAP_ValorPago)) <> "" Then
                Dim sapValor As Double
                sapValor = val(NzString(SAP_ValorPago))
                passValorEqSAP = (Abs(valorPago - sapValor) < 0.005) ' tolerância centavos
                If Not passValorEqSAP Then messages = messages & "VALOR_PAGO difere do SAP (" & sapValor & "); "
            Else
                passValorEqSAP = True
            End If
            
            ' Escreve saída: todas as colunas do recordset
            colIndex = 1
            For Each fld In rsMain.Fields
                wsOut.Cells(rowOut, colIndex).Value = rsMain.Fields(fld.Name).Value
                colIndex = colIndex + 1
            Next fld
            
            ' Escreve colunas de validação
            wsOut.Cells(rowOut, colIndex).Value = IIf(passRecibo, "OK", "ERRO")
            wsOut.Cells(rowOut, colIndex + 1).Value = IIf(passStatus, "OK", "ERRO")
            wsOut.Cells(rowOut, colIndex + 2).Value = IIf(passContrato, "OK", "ERRO")
            wsOut.Cells(rowOut, colIndex + 3).Value = IIf(passPedido40, "OK", "ERRO")
            wsOut.Cells(rowOut, colIndex + 4).Value = IIf(passFormaPag, "OK", "ERRO")
            wsOut.Cells(rowOut, colIndex + 5).Value = IIf(passDataPrev, "OK", "ERRO")
            wsOut.Cells(rowOut, colIndex + 6).Value = IIf(passValorLeq, "OK", "ERRO")
            wsOut.Cells(rowOut, colIndex + 7).Value = IIf(passValorEqSAP, "OK", "ERRO")
            wsOut.Cells(rowOut, colIndex + 8).Value = IIf(messages = "", "Todos OK", messages)
            
            rowOut = rowOut + 1
            rsMain.MoveNext
            
            
        Loop
    End If
    
    
    ' formata cabeçalho em negrito
    wsOut.Rows(1).Font.Bold = True
    wsOut.Columns.AutoFit
    
   ' MsgBox "Processamento concluído. Verifique a planilha 'Resultado_Validação'.", vbInformation

CleanUp:
    On Error Resume Next
    If Not rsMain Is Nothing Then
        If rsMain.State = 1 Then rsMain.Close
    End If
    If Not rsPedido Is Nothing Then
        If rsPedido.State = 1 Then rsPedido.Close
    End If
    Set rsMain = Nothing
    Set rsPedido = Nothing
    Set dictPedidos = Nothing
    
    ' Desconecta
    On Error Resume Next
    DesconectarBanco
    Exit Sub

ErrHandler:
    MsgBox "Erro: " & Err.Number & " - " & Err.Description, vbCritical
    Resume CleanUp
End Sub


Sub ExportarQueryPedidos()
    On Error GoTo TrataErro
    Dim ws As Worksheet
    Dim sql As String
    Dim rs As Object
    Dim col As Long, row As Long
    Dim nomeAba As String
    
    nomeAba = "Pedidos_Query"
    
    ' === Lê o SQL da aba "SQL" célula B1 ===
    sql = Trim(ThisWorkbook.Sheets("SQL").Range("B1").Value)
    If sql = "" Then
        MsgBox "A célula B1 na aba 'SQL' está vazia. Insira a query e tente novamente.", vbExclamation
        Exit Sub
    End If
    
    ' === Conecta no banco ===
    Call ConectarBanco
    
    ' === Executa a query ===
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn
    
    If rs.EOF Then
        MsgBox "A query não retornou resultados.", vbInformation
        GoTo Limpar
    End If
    
    ' === Cria (ou substitui) a aba de destino ===
    On Error Resume Next
    application.DisplayAlerts = False
    ThisWorkbook.Sheets(nomeAba).Delete
    application.DisplayAlerts = True
    On Error GoTo TrataErro
    
    Set ws = ThisWorkbook.Sheets.Add
    ws.Name = nomeAba
    
    ' === Cabeçalhos ===
    For col = 0 To rs.Fields.Count - 1
        ws.Cells(1, col + 1).Value = rs.Fields(col).Name
        ws.Cells(1, col + 1).Font.Bold = True
        ws.Cells(1, col + 1).Interior.Color = RGB(220, 230, 241)
    Next col
    
    ' === Dados ===
    row = 2
    Do Until rs.EOF
        For col = 0 To rs.Fields.Count - 1
            ws.Cells(row, col + 1).Value = rs.Fields(col).Value
        Next col
        rs.MoveNext
        row = row + 1
    Loop
    
    ' === Ajustes visuais ===
   ' ws.Columns.AutoFit
   ' ws.Rows(1).FreezePanes = True
    
   ' MsgBox "Consulta concluída com sucesso!" & vbCrLf & _
    '       "A aba '" & nomeAba & "' foi criada com " & (row - 2) & " registros.", vbInformation
           
Limpar:
    ' === Fecha objetos e desconecta ===
    If Not rs Is Nothing Then
        If rs.State = 1 Then rs.Close
        Set rs = Nothing
    End If
    Call DesconectarBanco
    Exit Sub

TrataErro:
    MsgBox "Erro ao executar a query de pedidos:" & vbCrLf & Err.Description, vbCritical
    Resume Limpar
End Sub


' ----------------------
' Funções utilitárias
' ----------------------

' Converte "R$ 21.600,00" ou "21.600,00" para Double (Brasil)
Private Function ParseBrazilCurrencyToDouble(s As String) As Double
    Dim t As String
    t = Trim(s)
    If t = "" Then
        ParseBrazilCurrencyToDouble = 0
        Exit Function
    End If
    t = Replace(t, "R$", "")
    t = Replace(t, "$", "")
    t = Replace(t, " ", "")
    ' remover pontos de milhar e trocar vírgula por ponto decimal
    t = Replace(t, ".", "")
    t = Replace(t, ",", ".")
    t = Replace(t, """", "")
    On Error Resume Next
    ParseBrazilCurrencyToDouble = CDbl(t)
    If Err.Number <> 0 Then
        ParseBrazilCurrencyToDouble = 0
        Err.Clear
    End If
    On Error GoTo 0
End Function

' Se valor for YYYYMMDD numérico (ex: 20251001) converte para Date, senão tenta CDate, senão retorna Null
Private Function ParsePossibleYYYYMMDDToDate(v As Variant) As Variant
    On Error GoTo ErrF
    If IsNull(v) Then
        ParsePossibleYYYYMMDDToDate = Null
        Exit Function
    End If
    Dim s As String
    s = Trim(NzString(v))
    If Len(s) = 8 And IsNumeric(s) Then
        Dim yyyy As Integer, mm As Integer, dd As Integer
        yyyy = CInt(Left(s, 4))
        mm = CInt(Mid(s, 5, 2))
        dd = CInt(Right(s, 2))
        ParsePossibleYYYYMMDDToDate = DateSerial(yyyy, mm, dd)
        Exit Function
    End If
    If IsDate(s) Then
        ParsePossibleYYYYMMDDToDate = CDate(s)
        Exit Function
    End If
    ParsePossibleYYYYMMDDToDate = Null
    Exit Function
ErrF:
    ParsePossibleYYYYMMDDToDate = Null
End Function

' Testa se em uma lista concatenada por "||" existe pedido que comece com 40 (ignora "R$" e pontuações)
Private Function PedidoListHasPrefix40(allPedidos As String) As Boolean
    Dim parts() As String
    If Trim(allPedidos) = "" Then
        PedidoListHasPrefix40 = False
        Exit Function
    End If
    parts = Split(allPedidos, "||")
    Dim i As Long, p As String
    For i = LBound(parts) To UBound(parts)
        p = Trim(parts(i))
        p = Replace(p, "R$", "")
        p = Replace(p, "$", "")
        p = Replace(p, ".", "")
        p = Replace(p, ",", "")
        p = Replace(p, " ", "")
        If Len(p) >= 2 Then
            If Left(p, 2) = "40" Then
                PedidoListHasPrefix40 = True
                Exit Function
            End If
        End If
    Next i
    PedidoListHasPrefix40 = False
End Function

' Substitui Null por string vazia
Private Function NzString(v As Variant) As String
    If IsNull(v) Then NzString = "" Else NzString = CStr(v)
End Function

Sub Aprovacao_Barter()

Call Entrar_WF

Call ExportarDocSAP

Call ValidarQueriesEExportarResultados



End Sub

