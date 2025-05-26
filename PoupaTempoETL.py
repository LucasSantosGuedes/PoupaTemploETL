import streamlit as st
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import re
from datetime import datetime
import io
from ydata_profiling import ProfileReport
import base64

st.set_page_config(page_title="Analisador Avançado de Excel para ETL", layout="wide")

# CSS customizado com as cores especificadas
st.markdown("""
<style>
    /* Cores principais */
    :root {
        --cor-principal: #23476f;     /* Pantone 7693 C - Azul escuro */
        --cor-destaque: #e00d23;      /* Pantone 2035 C - Vermelho */
        --cor-secundaria: #a0a7b0;    /* Pantone 429 C - Cinza */
        --cor-fundo: #f8f9fa;
        --cor-texto: #2c3e50;
    }

    /* Header principal */
    .main-header {
        background: linear-gradient(135deg, var(--cor-principal) 0%, #2a5282 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    .main-header h1 {
        color: white !important;
        font-size: 2.5rem !important;
        font-weight: 700 !important;
        margin-bottom: 0.5rem !important;
        text-align: center;
    }
    
    .main-header p {
        color: #e2e8f0 !important;
        font-size: 1.1rem !important;
        text-align: center;
        margin: 0 !important;
    }

    /* Sidebar styling */
    .css-1d391kg {
        background-color: var(--cor-fundo);
    }
    
    /* Métricas */
    [data-testid="metric-container"] {
        background: white;
        border: 2px solid var(--cor-secundaria);
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    
    [data-testid="metric-container"] [data-testid="metric-value"] {
        color: var(--cor-principal) !important;
        font-weight: 700 !important;
    }
    
    [data-testid="metric-container"] [data-testid="metric-label"] {
        color: var(--cor-texto) !important;
    }

    /* Botões */
    .stButton button {
        background: linear-gradient(135deg, var(--cor-principal) 0%, #2a5282 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.5rem 2rem !important;
        font-weight: 600 !important;
        transition: all 0.3s ease !important;
    }
    
    .stButton button:hover {
        background: linear-gradient(135deg, #1e3a5f 0%, var(--cor-principal) 100%) !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 4px 12px rgba(35, 71, 111, 0.3) !important;
    }

    /* Botão de download especial */
    .stDownloadButton button {
        background: linear-gradient(135deg, var(--cor-destaque) 0%, #c70920 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.5rem 2rem !important;
        font-weight: 600 !important;
    }
    
    .stDownloadButton button:hover {
        background: linear-gradient(135deg, #c70920 0%, var(--cor-destaque) 100%) !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 4px 12px rgba(224, 13, 35, 0.3) !important;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: var(--cor-fundo);
        padding: 0.5rem;
        border-radius: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: white;
        border: 2px solid var(--cor-secundaria);
        border-radius: 8px;
        color: var(--cor-texto);
        font-weight: 600;
        padding: 0.5rem 1rem;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, var(--cor-principal) 0%, #2a5282 100%) !important;
        color: white !important;
        border-color: var(--cor-principal) !important;
    }

    /* Alertas e mensagens */
    .stAlert > div {
        border-radius: 8px;
        border-left: 4px solid var(--cor-destaque);
    }
    
    .stSuccess > div {
        background-color: #d4edda;
        border-left-color: #28a745;
        color: #155724;
    }
    
    .stError > div {
        background-color: #f8d7da;
        border-left-color: var(--cor-destaque);
        color: #721c24;
    }
    
    .stWarning > div {
        background-color: #fff3cd;
        border-left-color: #ffc107;
        color: #856404;
    }
    
    .stInfo > div {
        background-color: #cce7ff;
        border-left-color: var(--cor-principal);
        color: #0c5aa6;
    }

    /* Expanders */
    .streamlit-expanderHeader {
        background-color: var(--cor-fundo) !important;
        border: 2px solid var(--cor-secundaria) !important;
        border-radius: 8px !important;
        color: var(--cor-texto) !important;
        font-weight: 600 !important;
    }
    
    .streamlit-expanderContent {
        border: 2px solid var(--cor-secundaria) !important;
        border-top: none !important;
        border-radius: 0 0 8px 8px !important;
        background-color: white !important;
    }

    /* Dataframes */
    .stDataFrame {
        border: 2px solid var(--cor-secundaria);
        border-radius: 8px;
        overflow: hidden;
    }

    /* File uploader */
    .stFileUploader > div {
        border: 2px dashed var(--cor-secundaria) !important;
        border-radius: 8px !important;
        background-color: var(--cor-fundo) !important;
    }
    
    .stFileUploader label {
        color: var(--cor-principal) !important;
        font-weight: 600 !important;
    }

    /* Code blocks */
    .stCodeBlock {
        border: 2px solid var(--cor-secundaria);
        border-radius: 8px;
        background-color: #f8f9fa;
    }

    /* Checkbox */
    .stCheckbox label {
        color: var(--cor-texto) !important;
        font-weight: 500 !important;
    }

    /* Subheaders */
    h2, h3 {
        color: var(--cor-principal) !important;
        border-bottom: 2px solid var(--cor-secundaria);
        padding-bottom: 0.5rem;
    }

    /* Status containers */
    .status-container {
        background: white;
        border: 2px solid var(--cor-secundaria);
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    
    .problem-card {
        background: linear-gradient(135deg, #fff5f5 0%, #fed7d7 100%);
        border: 2px solid var(--cor-destaque);
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    
    .solution-card {
        background: linear-gradient(135deg, #f0f9ff 0%, #dbeafe 100%);
        border: 2px solid var(--cor-principal);
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }

    /* Spinner customizado */
    .stSpinner > div {
        border-top-color: var(--cor-principal) !important;
    }
</style>
""", unsafe_allow_html=True)

# Header principal customizado
st.markdown("""
<div class="main-header">
    <h1>🚀 Bem-vindo(a) ao Poupa Tempo ETL</h1>
    <p>Análise avançada de arquivos Excel com sugestões específicas para Apache NiFi e código Groovy</p>
</div>
""", unsafe_allow_html=True)

# Funções de análise (mantendo as originais)
def verificar_valores_nulos(df: pd.DataFrame) -> Dict:
    """Identifica colunas com valores nulos/vazios"""
    resultado = {}
    for coluna in df.columns:
        nulos = df[coluna].isnull().sum()
        vazios = (df[coluna].astype(str).str.strip() == '').sum()
        total_problemas = nulos + vazios
        if total_problemas > 0:
            resultado[coluna] = {
                "nulos": nulos,
                "vazios": vazios,
                "total": total_problemas,
                "percentual": round(total_problemas / len(df) * 100, 2)
            }
    return resultado

def verificar_tipos_inconsistentes(df: pd.DataFrame) -> Dict:
    """Verifica inconsistências nos tipos de dados"""
    resultado = {}
    for coluna in df.columns:
        tipos_encontrados = set()
        for valor in df[coluna].dropna():
            try:
                if isinstance(valor, (int, float)):
                    tipos_encontrados.add('numérico')
                elif pd.to_datetime(valor, errors='ignore') != valor:
                    tipos_encontrados.add('data')
                else:
                    tipos_encontrados.add('texto')
            except:
                tipos_encontrados.add('texto')
        
        if len(tipos_encontrados) > 1:
            resultado[coluna] = list(tipos_encontrados)
    return resultado

def verificar_duplicatas(df: pd.DataFrame) -> Dict:
    """Identifica registros duplicados"""
    duplicatas_completas = df.duplicated().sum()
    resultado = {
        "registros_duplicados": duplicatas_completas,
        "percentual": round(duplicatas_completas / len(df) * 100, 2)
    }
    
    # Verifica duplicatas por coluna
    colunas_duplicatas = {}
    for coluna in df.columns:
        dup_coluna = df[coluna].duplicated().sum()
        if dup_coluna > 0:
            colunas_duplicatas[coluna] = {
                "duplicatas": dup_coluna,
                "percentual": round(dup_coluna / len(df) * 100, 2)
            }
    
    resultado["por_coluna"] = colunas_duplicatas
    return resultado

def verificar_caracteres_especiais(df: pd.DataFrame) -> Dict:
    """Identifica caracteres especiais problemáticos"""
    resultado = {}
    caracteres_problematicos = r'[^\w\s\d\.,@-]'
    
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            valores_com_caracteres = 0
            exemplos = []
            for valor in df[coluna].dropna().astype(str):
                if re.search(caracteres_problematicos, valor):
                    valores_com_caracteres += 1
                    if len(exemplos) < 3:
                        exemplos.append(valor[:50])
            
            if valores_com_caracteres > 0:
                resultado[coluna] = {
                    "count": valores_com_caracteres,
                    "exemplos": exemplos
                }
    return resultado

def verificar_espacos_extras(df: pd.DataFrame) -> Dict:
    """Identifica espaços em branco extras"""
    resultado = {}
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            espacos_inicio_fim = 0
            espacos_multiplos = 0
            
            for valor in df[coluna].dropna().astype(str):
                if valor != valor.strip():
                    espacos_inicio_fim += 1
                if '  ' in valor:
                    espacos_multiplos += 1
            
            if espacos_inicio_fim > 0 or espacos_multiplos > 0:
                resultado[coluna] = {
                    "espacos_inicio_fim": espacos_inicio_fim,
                    "espacos_multiplos": espacos_multiplos
                }
    return resultado

def verificar_formatos_data(df: pd.DataFrame) -> Dict:
    """Identifica diferentes formatos de data"""
    resultado = {}
    formatos_data = [
        '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d',
        '%d-%m-%Y', '%m-%d-%Y', '%d.%m.%Y', '%Y.%m.%d'
    ]
    
    for coluna in df.columns:
        formatos_encontrados = {}
        if df[coluna].dtype == 'object':
            for valor in df[coluna].dropna().astype(str):
                for formato in formatos_data:
                    try:
                        datetime.strptime(valor, formato)
                        if formato not in formatos_encontrados:
                            formatos_encontrados[formato] = 0
                        formatos_encontrados[formato] += 1
                        break
                    except:
                        continue
            
            if len(formatos_encontrados) > 1:
                resultado[coluna] = formatos_encontrados
    return resultado

def analisar_nomes_colunas(df: pd.DataFrame) -> Dict:
    """Analisa problemas nos nomes das colunas"""
    problemas = []
    
    for coluna in df.columns:
        issues = []
        if ' ' in coluna:
            issues.append("contém espaços")
        if re.search(r'[^\w\s]', coluna):
            issues.append("contém caracteres especiais")
        if re.match(r'^\d', coluna):
            issues.append("começa com número")
        if len(coluna) > 50:
            issues.append("nome muito longo")
        
        if issues:
            problemas.append({
                "coluna": coluna,
                "problemas": issues
            })
    
    return problemas

def gerar_sugestoes_groovy(analise: Dict) -> Dict:
    """Gera sugestões para correção dos problemas encontrados em Groovy para NiFi"""
    sugestoes = {}
    
    # Sugestões para valores nulos
    if analise.get("valores_nulos") and len(analise["valores_nulos"]) > 0:
        sugestoes["valores_nulos"] = {
            "problema": "Valores nulos ou vazios encontrados",
            "sugestoes": [
                "Preencher com valores padrão",
                "Remover registros com valores nulos",
                "Criar categoria 'Desconhecido' para dados categóricos"
            ],
            "codigo_exemplo": """
// Groovy para Apache NiFi - ExecuteScript
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream ->
    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def lines = content.split('\\n')
    def writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    
    lines.eachWithIndex { line, index ->
        if (index == 0) {
            // Header
            writer.writeLine(line)
        } else {
            def columns = line.split(',')
            // Tratar nulos e vazios
            columns = columns.collect { col -> 
                (col == null || col.trim().isEmpty()) ? 'DESCONHECIDO' : col 
            }
            writer.writeLine(columns.join(','))
        }
    }
    writer.flush()
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
"""
        }
    
    # Sugestões para tipos inconsistentes
    if analise.get("tipos_inconsistentes") and len(analise["tipos_inconsistentes"]) > 0:
        sugestoes["tipos_inconsistentes"] = {
            "problema": "Tipos de dados inconsistentes",
            "sugestoes": [
                "Padronizar tipos de dados",
                "Usar conversão segura com tratamento de erros",
                "Separar dados em colunas diferentes se necessário"
            ],
            "codigo_exemplo": """
// Groovy para Apache NiFi - ExecuteScript
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (!flowFile) return

def processarTipos(String valor) {
    if (valor == null || valor.trim().isEmpty()) return ''
    
    // Tenta converter para número
    if (valor.matches('^-?\\\\d+\\\\.?\\\\d*$')) {
        try {
            return valor.toBigDecimal()
        } catch (Exception e) {
            // Se falhar, mantém como string
        }
    }
    
    // Tenta converter para data
    def dateFormats = ['yyyy-MM-dd', 'dd/MM/yyyy', 'MM/dd/yyyy']
    for (formato in dateFormats) {
        try {
            return Date.parse(formato, valor)
        } catch (Exception e) {
            // Continua tentando outros formatos
        }
    }
    
    // Se não for número nem data, retorna como string
    return valor
}

flowFile = session.write(flowFile, { inputStream, outputStream ->
    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def lines = content.split('\\n')
    def writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    
    lines.eachWithIndex { line, index ->
        if (index == 0) {
            writer.writeLine(line)
        } else {
            def columns = line.split(',')
            columns = columns.collect { col -> processarTipos(col).toString() }
            writer.writeLine(columns.join(','))
        }
    }
    writer.flush()
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
"""
        }
    
    # Sugestões para duplicatas
    if analise.get("duplicatas") and analise["duplicatas"]["registros_duplicados"] > 0:
        sugestoes["duplicatas"] = {
            "problema": "Registros duplicados encontrados",
            "sugestoes": [
                "Remover duplicatas completas",
                "Manter apenas o primeiro registro",
                "Agregar duplicatas com funções de agregação"
            ],
            "codigo_exemplo": """
// Groovy para Apache NiFi - ExecuteScript
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream ->
    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def lines = content.split('\\n')
    def uniqueRecords = [] as Set
    def writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    
    lines.eachWithIndex { line, index ->
        if (index == 0) {
            // Mantém o header
            writer.writeLine(line)
        } else if (!line.trim().isEmpty()) {
            // Remove duplicatas baseado no conteúdo completo da linha
            if (!uniqueRecords.contains(line)) {
                uniqueRecords.add(line)
                writer.writeLine(line)
            }
        }
    }
    writer.flush()
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
"""
        }
    
    # Sugestões para caracteres especiais
    if analise.get("caracteres_especiais") and len(analise["caracteres_especiais"]) > 0:
        sugestoes["caracteres_especiais"] = {
            "problema": "Caracteres especiais encontrados",
            "sugestoes": [
                "Remover caracteres especiais",
                "Substituir por equivalentes ASCII",
                "Normalizar texto com transliteração"
            ],
            "codigo_exemplo": """
// Groovy para Apache NiFi - ExecuteScript
import java.text.Normalizer
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (!flowFile) return

def limparTexto(String texto) {
    if (!texto) return ""
    
    // Remove acentos
    texto = Normalizer.normalize(texto, Normalizer.Form.NFD)
    texto = texto.replaceAll("\\\\p{M}", "")
    
    // Remove caracteres especiais, mantendo apenas letras, números, espaços e alguns pontuação
    texto = texto.replaceAll(/[^\\\\w\\\\s.,@-]/, "")
    
    // Substitui múltiplos espaços por um único
    texto = texto.replaceAll(/\\\\s+/, " ").trim()
    
    return texto
}

flowFile = session.write(flowFile, { inputStream, outputStream ->
    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def lines = content.split('\\n')
    def writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    
    lines.eachWithIndex { line, index ->
        if (index == 0) {
            // Para o header, limpa caracteres especiais também
            def headers = line.split(',')
            def cleanHeaders = headers.collect { limparTexto(it) }
            writer.writeLine(cleanHeaders.join(','))
        } else {
            def columns = line.split(',')
            def cleanColumns = columns.collect { limparTexto(it) }
            writer.writeLine(cleanColumns.join(','))
        }
    }
    writer.flush()
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
"""
        }
    
    # Sugestões para espaços extras
    if analise.get("espacos_extras") and len(analise["espacos_extras"]) > 0:
        sugestoes["espacos_extras"] = {
            "problema": "Espaços em branco extras",
            "sugestoes": [
                "Remover espaços no início e fim",
                "Substituir múltiplos espaços por um único",
                "Aplicar trim em todas as colunas de texto"
            ],
            "codigo_exemplo": """
// Groovy para Apache NiFi - ExecuteScript
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (!flowFile) return

def cleanSpaces(text) {
    return text?.trim()?.replaceAll(/\\\\s+/, ' ') ?: ''
}

flowFile = session.write(flowFile, { inputStream, outputStream ->
    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def lines = content.split('\\n')
    def writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    
    lines.each { line ->
        def columns = line.split(',')
        def cleanColumns = columns.collect { col -> cleanSpaces(col) }
        writer.writeLine(cleanColumns.join(','))
    }
    writer.flush()
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
"""
        }
    
    # Sugestões para formatos de data
    if analise.get("formatos_data") and len(analise["formatos_data"]) > 0:
        sugestoes["formatos_data"] = {
            "problema": "Formatos de data inconsistentes",
            "sugestoes": [
                "Padronizar para formato ISO (YYYY-MM-DD)",
                "Usar parser flexível para múltiplos formatos",
                "Criar função customizada para conversão"
            ],
            "codigo_exemplo": """
// Groovy para Apache NiFi - ExecuteScript
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (!flowFile) return

def parseMultiFormatDate(String dateStr) {
    if (!dateStr || dateStr.trim().isEmpty()) return null
    
    def formats = [
        'dd/MM/yyyy', 'MM/dd/yyyy', 'yyyy-MM-dd',
        'dd-MM-yyyy', 'yyyy/MM/dd', 'dd.MM.yyyy',
        'yyyyMMdd', 'ddMMyyyy', 'MMddyyyy'
    ]
    
    for (format in formats) {
        try {
            def formatter = DateTimeFormatter.ofPattern(format)
            def date = LocalDate.parse(dateStr.trim(), formatter)
            return date.format(DateTimeFormatter.ISO_DATE)
        } catch (DateTimeParseException e) {
            // Continue para próximo formato
        }
    }
    
    // Se nenhum formato funcionar, retorna null ou a string original
    return null
}

flowFile = session.write(flowFile, { inputStream, outputStream ->
    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def lines = content.split('\\n')
    def writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    
    lines.eachWithIndex { line, index ->
        if (index == 0) {
            // Mantém o header
            writer.writeLine(line)
        } else {
            def columns = line.split(',')
            // Assume que campos de data estão em posições específicas
            columns.eachWithIndex { col, colIndex ->
                def parsedDate = parseMultiFormatDate(col)
                if (parsedDate != null) {
                    columns[colIndex] = parsedDate
                }
            }
            writer.writeLine(columns.join(','))
        }
    }
    writer.flush()
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
"""
        }
    
    # Sugestões para nomes de colunas
    if analise.get("nomes_colunas_problematicos") and len(analise["nomes_colunas_problematicos"]) > 0:
        sugestoes["nomes_colunas"] = {
            "problema": "Nomes de colunas problemáticos",
            "sugestoes": [
                "Substituir espaços por underscore",
                "Remover caracteres especiais",
                "Padronizar para snake_case",
                "Evitar começar com números"
            ],
            "codigo_exemplo": """
// Groovy para Apache NiFi - ExecuteScript
import java.text.Normalizer
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (!flowFile) return

def cleanColumnName(String columnName) {
    if (!columnName) return "column"
    
    // Remove acentos
    columnName = Normalizer.normalize(columnName, Normalizer.Form.NFD)
    columnName = columnName.replaceAll("\\\\p{M}", "")
    
    // Substitui espaços e caracteres especiais por underscore
    columnName = columnName.replaceAll(/[^a-zA-Z0-9]/, '_')
    
    // Remove underscores múltiplos
    columnName = columnName.replaceAll(/_+/, '_')
    
    // Remove underscore no início/fim
    columnName = columnName.replaceAll(/^_|_$/, '')
    
    // Se começar com número, adiciona prefixo
    if (columnName.matches("^\\\\d.*")) {
        columnName = "col_" + columnName
    }
    
    // Se ficar vazio após limpeza
    if (columnName.isEmpty()) {
        columnName = "column"
    }
    
    return columnName.toLowerCase()
}

flowFile = session.write(flowFile, { inputStream, outputStream ->
    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def lines = content.split('\\n')
    def writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    
    // Mapeamento de colunas antigas para novas
    def columnMapping = [:]
    
    lines.eachWithIndex { line, index ->
        if (index == 0) {
            // Processa cabeçalho
            def headers = line.split(',')
            def newHeaders = headers.collect { header -> cleanColumnName(header) }
            
            // Cria mapeamento
            headers.eachWithIndex { oldHeader, i ->
                columnMapping[oldHeader] = newHeaders[i]
            }
            
            writer.writeLine(newHeaders.join(','))
        } else {
            // Mantém os dados como estão
            writer.writeLine(line)
        }
    }
    writer.flush()
} as StreamCallback)

// Adiciona mapeamento como atributos do FlowFile para uso posterior
columnMapping.each { oldName, newName ->
    flowFile = session.putAttribute(flowFile, "column.mapping." + oldName, newName)
}

session.transfer(flowFile, REL_SUCCESS)
"""
        }
    
    return sugestoes

def gerar_sugestoes_nifi(analise: Dict) -> Dict:
    """Gera sugestões específicas para Apache NiFi com processadores e configurações"""
    sugestoes = {}
    
    # Sugestões para valores nulos
    if analise.get("valores_nulos") and len(analise["valores_nulos"]) > 0:
        sugestoes["valores_nulos"] = {
            "problema": "Valores nulos ou vazios encontrados",
            "processadores": [
                {
                    "nome": "UpdateRecord",
                    "descricao": "Atualiza registros para preencher valores nulos",
                    "configuracao": {
                        "Record Reader": "CSVReader ou ExcelReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "Replacement Value Strategy": "Literal Value",
                        "/campo_nulo": "VALOR_PADRAO"
                    }
                },
                {
                    "nome": "QueryRecord",
                    "descricao": "Remove registros com valores nulos",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "filtered_data": "SELECT * FROM FLOWFILE WHERE campo IS NOT NULL"
                    }
                },
                {
                    "nome": "ValidateRecord",
                    "descricao": "Valida e separa registros com/sem nulos",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "Schema Access Strategy": "Use Schema Text",
                        "Schema Text": "Define campos obrigatórios"
                    }
                }
            ],
            "fluxo_exemplo": """
1. GetFile → Lê arquivo Excel/CSV
2. ConvertExcelToCSV → Se necessário
3. ValidateRecord → Separa registros válidos/inválidos
4. UpdateRecord → Preenche valores nulos
5. PutFile → Salva arquivo corrigido
"""
        }
    
    # Sugestões para tipos inconsistentes
    if analise.get("tipos_inconsistentes") and len(analise["tipos_inconsistentes"]) > 0:
        sugestoes["tipos_inconsistentes"] = {
            "problema": "Tipos de dados inconsistentes",
            "processadores": [
                {
                    "nome": "ConvertRecord",
                    "descricao": "Converte e padroniza tipos de dados",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "Schema Registry": "AvroSchemaRegistry",
                        "Schema Access Strategy": "Use Schema Name Property"
                    }
                },
                {
                    "nome": "UpdateRecord",
                    "descricao": "Força conversão de tipos",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "/campo_texto": "toString(/campo_original)",
                        "/campo_numerico": "toNumber(/campo_original, 0.0)",
                        "/campo_data": "toDate(/campo_original, 'yyyy-MM-dd')"
                    }
                },
                {
                    "nome": "ValidateRecord",
                    "descricao": "Valida tipos de dados contra schema",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "Schema Access Strategy": "Use Schema Text",
                        "Validation Strategy": "Strict Type Checking"
                    }
                }
            ],
            "fluxo_exemplo": """
1. GetFile → Lê arquivo fonte
2. InferAvroSchema → Infere schema inicial
3. ConvertRecord → Converte para schema padronizado
4. RouteOnAttribute → Roteia baseado em validação
5. UpdateRecord → Corrige tipos inválidos
6. MergeContent → Junta registros corrigidos
7. PutFile → Salva arquivo
"""
        }
    
    # Sugestões para duplicatas
    if analise.get("duplicatas") and analise["duplicatas"]["registros_duplicados"] > 0:
        sugestoes["duplicatas"] = {
            "problema": "Registros duplicados encontrados",
            "processadores": [
                {
                    "nome": "DetectDuplicate",
                    "descricao": "Detecta e marca registros duplicados",
                    "configuracao": {
                        "Cache Entry Identifier": "${campo_chave}",
                        "Age Off Duration": "24 hours",
                        "Distributed Cache Service": "DistributedMapCacheClientService",
                        "FlowFile Description": "Duplicate detected"
                    }
                },
                {
                    "nome": "DeduplicateRecord",
                    "descricao": "Remove duplicatas automaticamente",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "Deduplication Strategy": "First Occurrence",
                        "Record Hashing Algorithm": "SHA-256"
                    }
                },
                {
                    "nome": "QueryRecord",
                    "descricao": "Remove duplicatas via SQL",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "unique_records": "SELECT DISTINCT * FROM FLOWFILE",
                        "grouped_records": "SELECT campo_chave, MAX(data) as ultima_data FROM FLOWFILE GROUP BY campo_chave"
                    }
                }
            ],
            "fluxo_exemplo": """
1. GetFile → Lê arquivo com duplicatas
2. SplitRecord → Divide em registros individuais
3. DetectDuplicate → Identifica duplicatas
4. RouteOnAttribute → Separa duplicatas/únicos
5. UpdateAttribute → Marca registros
6. MergeRecord → Junta registros únicos
7. PutFile → Salva resultado
"""
        }
    
    # Sugestões para caracteres especiais
    if analise.get("caracteres_especiais") and len(analise["caracteres_especiais"]) > 0:
        sugestoes["caracteres_especiais"] = {
            "problema": "Caracteres especiais encontrados",
            "processadores": [
                {
                    "nome": "ReplaceText",
                    "descricao": "Remove caracteres especiais",
                    "configuracao": {
                        "Search Value": "[^\\w\\s.,@-]",
                        "Replacement Value": "",
                        "Character Set": "UTF-8",
                        "Evaluation Mode": "Entire text",
                        "Regular Expression": "true"
                    }
                },
                {
                    "nome": "UpdateRecord",
                    "descricao": "Limpa caracteres em campos específicos",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "/campo_limpo": "replaceRegex(/campo_original, '[^\\w\\s]', '')",
                        "/campo_normalizado": "normalize(/campo_original)"
                    }
                },
                {
                    "nome": "TransformCharacterSet",
                    "descricao": "Converte encoding de caracteres",
                    "configuracao": {
                        "Input Character Set": "Windows-1252",
                        "Output Character Set": "UTF-8"
                    }
                }
            ],
            "fluxo_exemplo": """
1. GetFile → Lê arquivo com caracteres especiais
2. TransformCharacterSet → Converte para UTF-8
3. SplitRecord → Processa registro por registro
4. ReplaceText → Remove caracteres indesejados
5. ValidateRecord → Valida limpeza
6. MergeRecord → Junta registros limpos
7. PutFile → Salva arquivo limpo
"""
        }
    
    # Sugestões para espaços extras
    if analise.get("espacos_extras") and len(analise["espacos_extras"]) > 0:
        sugestoes["espacos_extras"] = {
            "problema": "Espaços em branco extras",
            "processadores": [
                {
                    "nome": "UpdateRecord",
                    "descricao": "Remove espaços extras de campos",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "/campo_trimmed": "trim(/campo_original)",
                        "/campo_single_space": "replaceRegex(/campo_original, '\\s+', ' ')"
                    }
                },
                {
                    "nome": "ReplaceText",
                    "descricao": "Limpa espaços em todo o conteúdo",
                    "configuracao": {
                        "Search Value": "\\s+",
                        "Replacement Value": " ",
                        "Regular Expression": "true",
                        "Evaluation Mode": "Entire text"
                    }
                },
                {
                    "nome": "ExecuteScript",
                    "descricao": "Script Groovy para limpeza avançada",
                    "configuracao": {
                        "Script Engine": "Groovy",
                        "Script Body": """
flowFile = session.get()
if (!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream ->
    def reader = new BufferedReader(new InputStreamReader(inputStream))
    def writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    
    reader.eachLine { line ->
        def cleaned = line.trim().replaceAll(/\\s+/, ' ')
        writer.writeLine(cleaned)
    }
    writer.flush()
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
"""
                    }
                }
            ],
            "fluxo_exemplo": """
1. GetFile → Lê arquivo com espaços extras
2. EvaluateJsonPath → Extrai campos (se JSON)
3. UpdateRecord → Aplica trim() em cada campo
4. ReplaceText → Normaliza espaços internos
5. ValidateRecord → Valida limpeza
6. PutFile → Salva arquivo limpo
"""
        }
    
    # Sugestões para formatos de data
    if analise.get("formatos_data") and len(analise["formatos_data"]) > 0:
        sugestoes["formatos_data"] = {
            "problema": "Formatos de data inconsistentes",
            "processadores": [
                {
                    "nome": "UpdateRecord",
                    "descricao": "Padroniza formatos de data",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "/data_padrao": "format(toDate(/data_original, 'dd/MM/yyyy'), 'yyyy-MM-dd')",
                        "/data_iso": "toDate(/data_original, ['dd/MM/yyyy', 'MM/dd/yyyy', 'yyyy-MM-dd'])"
                    }
                },
                {
                    "nome": "ConvertRecord",
                    "descricao": "Converte datas usando schema",
                    "configuracao": {
                        "Record Reader": "CSVReader",
                        "Record Writer": "CSVRecordSetWriter",
                        "Date Format": "yyyy-MM-dd",
                        "Time Format": "HH:mm:ss",
                        "Timestamp Format": "yyyy-MM-dd HH:mm:ss"
                    }
                },
                {
                    "nome": "ExecuteScript",
                    "descricao": "Conversão flexível com Groovy",
                    "configuracao": {
                        "Script Engine": "Groovy",
                        "Script Body": """
import java.time.LocalDate
import java.time.format.DateTimeFormatter

def parseDate(dateStr) {
    def formats = [
        'dd/MM/yyyy', 'MM/dd/yyyy', 'yyyy-MM-dd',
        'dd-MM-yyyy', 'yyyy/MM/dd'
    ]
    
    for (format in formats) {
        try {
            return LocalDate.parse(dateStr, DateTimeFormatter.ofPattern(format))
                          .format(DateTimeFormatter.ISO_DATE)
        } catch (Exception e) {
            continue
        }
    }
    return null
}

// Processar FlowFile
flowFile = session.get()
if (!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream ->
    // Implementar lógica de conversão
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
"""
                    }
                }
            ],
            "fluxo_exemplo": """
1. GetFile → Lê arquivo com datas diversas
2. EvaluateJsonPath → Extrai campos de data
3. RouteOnAttribute → Separa por formato detectado
4. UpdateRecord → Padroniza cada formato
5. MergeContent → Junta registros padronizados
6. ValidateRecord → Valida datas convertidas
7. PutFile → Salva arquivo padronizado
"""
        }
    
    # Sugestões para nomes de colunas
    if analise.get("nomes_colunas_problematicos") and len(analise["nomes_colunas_problematicos"]) > 0:
        sugestoes["nomes_colunas"] = {
            "problema": "Nomes de colunas problemáticos",
            "processadores": [
                {
                    "nome": "UpdateAttribute",
                    "descricao": "Renomeia atributos/colunas",
                    "configuracao": {
                        "Delete Attributes Expression": "column\\..*",
                        "column.nome_antigo": "nome_novo",
                        "schema.field.nome_antigo": "nome_novo"
                    }
                },
                {
                    "nome": "JoltTransformJSON",
                    "descricao": "Renomeia campos JSON",
                    "configuracao": {
                        "Jolt Specification": """[
  {
    "operation": "shift",
    "spec": {
      "Nome Antigo": "nome_novo",
      "Coluna Com Espaço": "coluna_com_espaco",
      "*": "&"
    }
  }
]""",
                        "Transform UI": "Chain"
                    }
                },
                {
                    "nome": "ExecuteScript",
                    "descricao": "Renomeia colunas dinamicamente",
                    "configuracao": {
                        "Script Engine": "Groovy",
                        "Script Body": """
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

def cleanColumnName(String name) {
    return name.toLowerCase()
              .replaceAll(/[^a-z0-9]/, '_')
              .replaceAll(/_+/, '_')
              .replaceAll(/^_|_$/, '')
}

flowFile = session.get()
if (!flowFile) return

// Processar cabeçalhos
flowFile = session.write(flowFile, { inputStream, outputStream ->
    def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    def lines = content.split('\\n')
    
    if (lines.length > 0) {
        // Limpar cabeçalhos
        def headers = lines[0].split(',')
        def cleanHeaders = headers.collect { cleanColumnName(it) }
        
        // Reescrever arquivo
        outputStream.write(cleanHeaders.join(',').getBytes())
        outputStream.write('\\n'.getBytes())
        
        for (int i = 1; i < lines.length; i++) {
            outputStream.write(lines[i].getBytes())
            outputStream.write('\\n'.getBytes())
        }
    }
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
"""
                    }
                }
            ],
            "fluxo_exemplo": """
1. GetFile → Lê arquivo com nomes problemáticos
2. ExtractText → Extrai primeira linha (headers)
3. ExecuteScript → Limpa nomes das colunas
4. ReplaceText → Substitui headers no arquivo
5. UpdateAttribute → Atualiza metadados
6. PutFile → Salva arquivo com colunas renomeadas
"""
        }
    
    return sugestoes

# Upload do arquivo
st.markdown("### 📁 Upload do Arquivo")
uploaded_file = st.file_uploader("Selecione um arquivo Excel para análise", type=['xlsx', 'xls'])

if uploaded_file is not None:
    try:
        # Ler arquivo Excel
        try:
            df = pd.read_excel(uploaded_file, engine='openpyxl')
        except:
            # Fallback para outro engine caso dê erro
            df = pd.read_excel(uploaded_file)
        
        # Informações básicas com layout customizado
        st.markdown("### 📊 Informações Básicas do Arquivo")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📋 Total de Registros", len(df))
        with col2:
            st.metric("📊 Total de Colunas", len(df.columns))
        with col3:
            st.metric("💾 Tamanho em Memória", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
        
        # Preview dos dados
        st.markdown("### 👀 Preview dos Dados")
        st.dataframe(df.head(10), use_container_width=True)
        
        # Tabs para diferentes análises
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Pandas Profiling", 
            "🚨 Problemas ETL", 
            "🔧 Apache NiFi", 
            "💡 Código Groovy", 
            "📋 Relatório"
        ])
        
        with tab1:
            st.markdown("### 📊 Análise com Pandas Profiling")
            
            profile_minimal = st.checkbox("🚀 Modo Minimal (mais rápido)", value=True, key="profiling_minimal")
            profile_config = {
                'minimal': profile_minimal,
                'explorative': not profile_minimal
            }
            
            if st.button("🔍 Gerar Relatório Profiling", key="generate_profiling"):
                with st.spinner("Gerando relatório de profiling..."):
                    profile = ProfileReport(df, 
                                          title="Análise ETL - Pandas Profiling",
                                          minimal=profile_config['minimal'],
                                          explorative=profile_config['explorative'])
                    
                    # Salvar como HTML
                    profile_html = profile.to_html()
                    
                    # Criar download button
                    b64 = base64.b64encode(profile_html.encode()).decode()
                    href = f'<a href="data:text/html;base64,{b64}" download="profiling_report.html">📥 Download Relatório Profiling</a>'
                    st.markdown(href, unsafe_allow_html=True)
                    
                    # Mostrar algumas estatísticas em cards customizados
                    st.markdown("### 📈 Resumo do Profiling")
                    
                    # Container personalizado para estatísticas
                    st.markdown('<div class="status-container">', unsafe_allow_html=True)
                    st.write(f"**Total de variáveis:** {len(df.columns)}")
                    st.write(f"**Observações:** {len(df)}")
                    
                    # Tipos de variáveis
                    st.write("**Tipos de variáveis:**")
                    tipos = df.dtypes.value_counts()
                    for tipo, count in tipos.items():
                        st.write(f"- {tipo}: {count}")
                    
                    # Valores missing
                    missing = df.isnull().sum()
                    if missing.sum() > 0:
                        st.write("**Valores missing por coluna:**")
                        for col, count in missing[missing > 0].items():
                            st.write(f"- {col}: {count} ({count/len(df)*100:.1f}%)")
                    st.markdown('</div>', unsafe_allow_html=True)
        
        with tab2:
            st.markdown("### 🚨 Problemas Identificados para ETL")
            
            with st.spinner("Analisando problemas ETL..."):
                # Executar todas as análises
                analise = {
                    "valores_nulos": verificar_valores_nulos(df),
                    "tipos_inconsistentes": verificar_tipos_inconsistentes(df),
                    "duplicatas": verificar_duplicatas(df),
                    "caracteres_especiais": verificar_caracteres_especiais(df),
                    "espacos_extras": verificar_espacos_extras(df),
                    "formatos_data": verificar_formatos_data(df),
                    "nomes_colunas_problematicos": analisar_nomes_colunas(df)
                }
            
            # Valores Nulos
            if analise["valores_nulos"]:
                st.markdown('<div class="problem-card">', unsafe_allow_html=True)
                st.error("**🔴 Valores Nulos/Vazios Encontrados**")
                for coluna, info in analise["valores_nulos"].items():
                    st.warning(f"Coluna '{coluna}': {info['total']} problemas ({info['percentual']}%)")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Tipos Inconsistentes
            if analise["tipos_inconsistentes"]:
                st.markdown('<div class="problem-card">', unsafe_allow_html=True)
                st.error("**🔴 Tipos de Dados Inconsistentes**")
                for coluna, tipos in analise["tipos_inconsistentes"].items():
                    st.warning(f"Coluna '{coluna}': tipos encontrados - {', '.join(tipos)}")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Duplicatas
            if analise["duplicatas"]["registros_duplicados"] > 0:
                st.markdown('<div class="problem-card">', unsafe_allow_html=True)
                st.error(f"**🔴 {analise['duplicatas']['registros_duplicados']} Registros Duplicados ({analise['duplicatas']['percentual']}%)**")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Caracteres Especiais
            if analise["caracteres_especiais"]:
                st.markdown('<div class="problem-card">', unsafe_allow_html=True)
                st.error("**🔴 Caracteres Especiais Problemáticos**")
                for coluna, info in analise["caracteres_especiais"].items():
                    st.warning(f"Coluna '{coluna}': {info['count']} valores com caracteres especiais")
                    st.text(f"Exemplos: {', '.join(info['exemplos'])}")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Espaços Extras
            if analise["espacos_extras"]:
                st.markdown('<div class="problem-card">', unsafe_allow_html=True)
                st.error("**🔴 Espaços em Branco Extras**")
                for coluna, info in analise["espacos_extras"].items():
                    st.warning(f"Coluna '{coluna}': {info['espacos_inicio_fim']} com espaços extras, {info['espacos_multiplos']} com múltiplos espaços")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Formatos de Data
            if analise["formatos_data"]:
                st.markdown('<div class="problem-card">', unsafe_allow_html=True)
                st.error("**🔴 Formatos de Data Inconsistentes**")
                for coluna, formatos in analise["formatos_data"].items():
                    st.warning(f"Coluna '{coluna}': múltiplos formatos encontrados")
                    for formato, count in formatos.items():
                        st.text(f"  - {formato}: {count} ocorrências")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Nomes de Colunas
            if analise["nomes_colunas_problematicos"]:
                st.markdown('<div class="problem-card">', unsafe_allow_html=True)
                st.error("**🔴 Nomes de Colunas Problemáticos**")
                for problema in analise["nomes_colunas_problematicos"]:
                    st.warning(f"Coluna '{problema['coluna']}': {', '.join(problema['problemas'])}")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Se não há problemas
            if not any([
                analise["valores_nulos"],
                analise["tipos_inconsistentes"],
                analise["duplicatas"]["registros_duplicados"] > 0,
                analise["caracteres_especiais"],
                analise["espacos_extras"],
                analise["formatos_data"],
                analise["nomes_colunas_problematicos"]
            ]):
                st.success("🎉 **Excelente!** Nenhum problema significativo foi encontrado no arquivo.")
        
        with tab3:
            st.markdown("### 🔧 Soluções com Apache NiFi")
            
            # Gerar sugestões
            sugestoes_nifi = gerar_sugestoes_nifi(analise)
            
            if len(sugestoes_nifi) == 0:
                st.success("✅ Nenhum problema foi identificado que necessite de correção no Apache NiFi. O arquivo parece estar em bom estado!")
            else:
                st.markdown("""
                <div class="solution-card">
                    <h4>🎯 Como implementar no Apache NiFi</h4>
                    <p>Esta seção mostra os processadores específicos do Apache NiFi e suas configurações
                    para resolver os problemas identificados no seu arquivo.</p>
                </div>
                """, unsafe_allow_html=True)
            
            for key, sugestao in sugestoes_nifi.items():
                st.markdown(f"### 📝 {sugestao['problema']}")
                
                st.markdown("**🔧 Processadores Recomendados:**")
                
                for i, processador in enumerate(sugestao['processadores'], 1):
                    with st.expander(f"{i}. {processador['nome']} - {processador['descricao']}"):
                        st.markdown("**Configuração:**")
                        for config_key, config_value in processador['configuracao'].items():
                            if isinstance(config_value, str) and len(config_value) > 50:
                                st.code(config_value, language="groovy" if "Script" in config_key else "json")
                            else:
                                st.markdown(f"- **{config_key}**: `{config_value}`")
                
                st.markdown("**📋 Fluxo Exemplo:**")
                st.code(sugestao['fluxo_exemplo'], language="text")
                st.divider()
        
        with tab4:
            st.markdown("### 💡 Código Groovy para ETL")
            
            # Gerar sugestões
            sugestoes_groovy = gerar_sugestoes_groovy(analise)
            
            if len(sugestoes_groovy) == 0:
                st.success("✅ Nenhum problema foi identificado que necessite de correção com código Groovy. O arquivo parece estar em bom estado!")
            else:
                st.markdown("""
                <div class="solution-card">
                    <h4>⚡ Exemplos de código Groovy para NiFi</h4>
                    <p>Estes códigos Groovy podem ser usados no processador ExecuteScript do Apache NiFi
                    para corrigir os problemas identificados.</p>
                </div>
                """, unsafe_allow_html=True)
            
            for key, sugestao in sugestoes_groovy.items():
                st.markdown(f"### 🔍 {sugestao['problema']}")
                st.markdown("**💡 Sugestões:**")
                for s in sugestao["sugestoes"]:
                    st.markdown(f"- {s}")
                
                st.markdown("**📜 Código Groovy de Exemplo:**")
                st.code(sugestao["codigo_exemplo"], language="groovy")
                st.divider()
        
        with tab5:
            st.markdown("### 📋 Relatório Completo de Análise ETL")
            
            # Resumo executivo
            st.markdown("#### 📊 Resumo Executivo")
            total_problemas = sum([
                len(analise["valores_nulos"]),
                len(analise["tipos_inconsistentes"]),
                1 if analise["duplicatas"]["registros_duplicados"] > 0 else 0,
                len(analise["caracteres_especiais"]),
                len(analise["espacos_extras"]),
                len(analise["formatos_data"]),
                len(analise["nomes_colunas_problematicos"])
            ])
            
            # Métricas do resumo
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("🚨 Total de Problemas", total_problemas)
            with col2:
                colunas_afetadas = len(set([col for d in analise.values() if isinstance(d, dict) for col in d.keys() if col not in ['registros_duplicados', 'percentual', 'por_coluna']]))
                st.metric("📊 Colunas Afetadas", colunas_afetadas)
            with col3:
                prioridade = 'Alta' if total_problemas > 10 else 'Média' if total_problemas > 5 else 'Baixa'
                st.metric("⚡ Prioridade", prioridade)
            
            # Detalhamento dos problemas
            st.markdown("#### 🔍 Detalhamento dos Problemas")
            
            # Criar dataframe com todos os problemas
            problemas_df = []
            
            # Adicionar cada tipo de problema
            for coluna, info in analise["valores_nulos"].items():
                problemas_df.append({
                    "Coluna": coluna,
                    "Tipo de Problema": "Valores Nulos/Vazios",
                    "Detalhes": f"{info['total']} valores ({info['percentual']}%)",
                    "Impacto ETL": "Alto",
                    "Sugestão": "Preencher ou remover valores nulos"
                })
            
            for coluna, tipos in analise["tipos_inconsistentes"].items():
                problemas_df.append({
                    "Coluna": coluna,
                    "Tipo de Problema": "Tipos Inconsistentes",
                    "Detalhes": f"Tipos encontrados: {', '.join(tipos)}",
                    "Impacto ETL": "Alto",
                    "Sugestão": "Padronizar tipos de dados"
                })
            
            if analise["duplicatas"]["registros_duplicados"] > 0:
                problemas_df.append({
                    "Coluna": "Todas",
                    "Tipo de Problema": "Duplicatas",
                    "Detalhes": f"{analise['duplicatas']['registros_duplicados']} registros duplicados",
                    "Impacto ETL": "Médio",
                    "Sugestão": "Remover ou agregar duplicatas"
                })
            
            if problemas_df:
                df_problemas = pd.DataFrame(problemas_df)
                st.dataframe(df_problemas, use_container_width=True)
                
                # Download do relatório
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_problemas.to_excel(writer, sheet_name='Problemas', index=False)
                    
                    # Adicionar sheet com sugestões NiFi
                    sugestoes_nifi_df = []
                    for key, sugestao in gerar_sugestoes_nifi(analise).items():
                        for processador in sugestao["processadores"]:
                            sugestoes_nifi_df.append({
                                "Problema": sugestao["problema"],
                                "Processador": processador["nome"],
                                "Descrição": processador["descricao"]
                            })
                    
                    if sugestoes_nifi_df:
                        pd.DataFrame(sugestoes_nifi_df).to_excel(writer, sheet_name='Sugestões NiFi', index=False)
                    
                    # Adicionar sheet com sugestões Groovy
                    sugestoes_groovy_df = []
                    for key, sugestao in gerar_sugestoes_groovy(analise).items():
                        for s in sugestao["sugestoes"]:
                            sugestoes_groovy_df.append({
                                "Problema": sugestao["problema"],
                                "Sugestão": s
                            })
                    
                    if sugestoes_groovy_df:
                        pd.DataFrame(sugestoes_groovy_df).to_excel(writer, sheet_name='Sugestões Groovy', index=False)
                
                buffer.seek(0)
                st.download_button(
                    label="📥 Download Relatório Completo",
                    data=buffer,
                    file_name="relatorio_etl_analise.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.success("🎉 Nenhum problema significativo encontrado! O arquivo está pronto para ETL.")
    
    except Exception as e:
        st.error(f"❌ Erro ao processar o arquivo: {str(e)}")
        st.info("💡 Verifique se o arquivo é um Excel válido e tente novamente.")

# Instruções de uso
with st.expander("📖 Como usar este aplicativo"):
    st.markdown("""
    <div class="solution-card">
        <h4>🎯 Guia de Uso</h4>
        <ol>
            <li><strong>📁 Faça upload de um arquivo Excel</strong> (.xlsx ou .xls)</li>
            <li><strong>🔍 Explore as análises em diferentes abas</strong>:
                <ul>
                    <li><strong>📊 Pandas Profiling</strong>: Análise estatística completa</li>
                    <li><strong>🚨 Problemas ETL</strong>: Identificação de problemas específicos</li>
                    <li><strong>🔧 Apache NiFi</strong>: Processadores e configurações do NiFi</li>
                    <li><strong>💡 Código Groovy</strong>: Scripts Groovy para ExecuteScript do NiFi</li>
                    <li><strong>📋 Relatório</strong>: Relatório completo exportável</li>
                </ul>
            </li>
            <li><strong>🎛️ Para Apache NiFi</strong>:
                <ul>
                    <li>Veja os processadores recomendados</li>
                    <li>Copie as configurações sugeridas</li>
                    <li>Siga o fluxo exemplo para implementar</li>
                </ul>
            </li>
            <li><strong>⚡ Código Groovy pronto para NiFi</strong>:
                <ul>
                    <li>Use os códigos diretamente no ExecuteScript</li>
                    <li>Adapte os exemplos conforme necessário</li>
                </ul>
            </li>
        </ol>
        
        <h4>🌟 Benefícios</h4>
        <ul>
            <li>✅ Análise estatística avançada com pandas profiling</li>
            <li>✅ Sugestões específicas para Apache NiFi</li>
            <li>✅ Processadores e configurações prontas</li>
            <li>✅ Código Groovy específico para ExecuteScript do NiFi</li>
            <li>✅ Fluxos de exemplo detalhados</li>
            <li>✅ Relatórios exportáveis em múltiplos formatos</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

# Rodapé personalizado
st.markdown("---")
st.markdown("""
<div style="text-align: center; padding: 2rem; background: linear-gradient(135deg, #23476f 0%, #2a5282 100%); border-radius: 10px; margin-top: 2rem;">
    <p style="color: white; font-weight: 600; font-size: 1.1rem; margin: 0;">
        🚀 Desenvolvido para otimizar processos de ETL com foco em Apache NiFi
    </p>
    <p style="color: #e2e8f0; margin: 0.5rem 0 0 0;">
        Powered by Streamlit • Pandas • Apache NiFi
    </p>
</div>
""", unsafe_allow_html=True)
