# Analisador Avançado de Excel para ETL com Apache NiFi

Uma ferramenta poderosa para analisar arquivos Excel e identificar problemas potenciais em processos de ETL, com foco especial em soluções para Apache NiFi.

## 🚀 Funcionalidades

- **Análise Estatística Avançada**: Utiliza pandas profiling para análise detalhada
- **Identificação de Problemas ETL**: Detecta automaticamente:
  - Valores nulos/vazios
  - Tipos de dados inconsistentes
  - Registros duplicados
  - Caracteres especiais problemáticos
  - Espaços em branco extras
  - Formatos de data inconsistentes
  - Nomes de colunas problemáticos

- **Soluções Específicas para Apache NiFi**:
  - Processadores recomendados com configurações
  - Fluxos de exemplo detalhados
  - Melhores práticas para cada tipo de problema

- **Código Groovy**: Scripts prontos para uso em várias ferramentas ETL
- **Relatórios Exportáveis**: Excel, JSON e CSV

## 📋 Pré-requisitos

- Python 3.8+
- Streamlit
- Pandas
- YData Profiling

## 🔧 Instalação

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/PoupaTempoETL.git
cd PoupaTempoETL
```

2. Instale as dependências:
```bash
pip install -r requirements.txt
```

3. Execute o aplicativo:
```bash
streamlit run PoupaTempoETL.py
```

## 🌐 Deploy no Streamlit Cloud

1. Faça fork deste repositório
2. Acesse [share.streamlit.io](https://share.streamlit.io)
3. Conecte sua conta GitHub
4. Selecione o repositório e branch
5. Clique em "Deploy"

## 📝 Como Usar

1. **Upload do Arquivo**: Faça upload de um arquivo Excel (.xlsx ou .xls)
2. **Análise Automática**: O sistema irá analisar automaticamente o arquivo
3. **Explore as Abas**:
   - **Pandas Profiling**: Análise estatística completa
   - **Problemas ETL**: Lista de problemas identificados
   - **Apache NiFi**: Processadores e configurações específicas
   - **Código Groovy**: Scripts para correção
   - **Relatório**: Relatório completo exportável
   - **Configurações**: Personalize a análise

## 🔍 Exemplos de Uso

### Apache NiFi

Para problemas de valores nulos:
1. Use o processador `UpdateRecord`
2. Configure: Record Reader = CSVReader, Record Writer = CSVRecordSetWriter
3. Defina o valor padrão: `/campo_nulo` = "VALOR_PADRAO"

### Código Groovy

Para limpar caracteres especiais:
```groovy
def cleanText(String text) {
    return text?.replaceAll(/[^\w\s]/, '')?.trim() ?: ''
}
```

## 🤝 Contribuindo

1. Faça fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/NovaFeature`)
3. Commit suas mudanças (`git commit -m 'Adiciona NovaFeature'`)
4. Push para a branch (`git push origin feature/NovaFeature`)
5. Abra um Pull Request

## 🐛 Reportando Bugs

Encontrou um bug? Por favor, abra uma issue com:
- Descrição detalhada do problema
- Passos para reproduzir
- Screenshots (se aplicável)
- Versão do navegador e sistema operacional

## 📄 Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para detalhes.

## 👥 Autores

- Lucas Guedes

## 🙏 Agradecimentos

- Time DSU