# Lab 8 - Web Data App Para Financial Analytics em Tempo Real no Snowflake

# Imports
import os
import json
import pandas as pd
import streamlit as st
import snowflake.connector
import matplotlib.pyplot as plt
import yfinance as yf

# Configuração da página do Streamlit
st.set_page_config(page_title="Data Science Academy", page_icon=":100:", layout="wide")
st.title("Web Data App Para Financial Analytics em Tempo Real no Snowflake")

# Aplicar CSS para deixar a tabela de dados mais larga
st.markdown("""
    <style>
    .stDataFrame {width: 100% !important;}
    </style>
""", unsafe_allow_html=True)

# Função para carregar credenciais do arquivo JSON
def dsa_carrega_credenciais():
    try:
        with open("credenciais.json", "r") as file:
            return json.load(file)
    except FileNotFoundError:
        st.error("DSA-Log - Arquivo 'credenciais.json' não encontrado.")
        return None
    except json.JSONDecodeError:
        st.error("DSA-Log - Erro ao carregar as credenciais. Verifique o arquivo JSON.")
        return None

# Carregar credenciais
creds = dsa_carrega_credenciais()
if creds is None:
    st.stop()  # Para a execução do script se as credenciais não forem carregadas

# Parâmetros do Snowflake (Carregados do JSON)
SNOWFLAKE_ACCOUNT   = creds["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = creds["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = creds["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_WAREHOUSE = creds["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = creds["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA    = creds["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_TABLE     = creds["SNOWFLAKE_TABLE"]

# Função para conectar ao Snowflake
def dsa_conecta_snowflake():
    try:
        return snowflake.connector.connect(user = SNOWFLAKE_USER,
                                           password = SNOWFLAKE_PASSWORD,
                                           account = SNOWFLAKE_ACCOUNT,
                                           warehouse = SNOWFLAKE_WAREHOUSE,
                                           database = SNOWFLAKE_DATABASE,
                                           schema = SNOWFLAKE_SCHEMA)
    except Exception as e:
        st.error(f"Erro ao conectar ao Snowflake: {e}")
        return None

# Barra lateral para entrada de dados
st.sidebar.header("Configurações")
ticker = st.sidebar.text_input("📌 Insira o Ticker da Ação:", value="SNOW")
period = st.sidebar.selectbox("⏳ Período de Análise:", ["3mo", "6mo", "1y"], index=0)
fetch_button = st.sidebar.button("🔄 Buscar e Armazenar Dados")

# Adiciona um botão de suporte
if st.sidebar.button("Suporte"):
    st.sidebar.write("No caso de dúvidas envie e-mail para: suporte@datascienceacademy.com.br")

# Buscar e armazenar dados no Snowflake
if fetch_button:
    try:
        st.sidebar.info(f"Buscando dados de {ticker} para o período de {period}...")

        # Buscar dados no Yahoo Finance
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period, interval="1d")

        if hist.empty:
            st.sidebar.error("DSA-Log - Nenhum dado encontrado para o ticker informado.")
        else:
            # Resetar índice e selecionar colunas
            hist.reset_index(inplace=True)
            hist = hist[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            
            # Conectar ao Snowflake
            cnx = dsa_conecta_snowflake()
            if cnx:
                cur = cnx.cursor()
                
                # Inserir dados no Snowflake
                for _, row in hist.iterrows():
                    cur.execute(f"""
                        INSERT INTO {SNOWFLAKE_TABLE} (date, stock_price, volume, ticker)
                        VALUES ('{row['Date'].strftime('%Y-%m-%d')}', {row['Close']}, {row['Volume']}, '{ticker}')
                    """)
                
                cnx.commit()
                cur.close()
                cnx.close()
                st.sidebar.success("DSA-Log - Dados armazenados com sucesso no Snowflake!")
    except Exception as e:
        st.sidebar.error(f"DSA-Log - Erro ao buscar ou armazenar dados: {e}")

# Conectar ao Snowflake para análise
cnx = dsa_conecta_snowflake()
if cnx:
    cur = cnx.cursor()
    
    # Consultar dados do preço médio por mês
    query_stock_price = f"""
        SELECT DATE_TRUNC('month', date) AS month, AVG(stock_price) AS avg_stock_price
        FROM {SNOWFLAKE_TABLE} WHERE ticker = '{ticker}'
        GROUP BY month ORDER BY month ASC
    """
    
    # Consultar dados do volume total por mês
    query_stock_volume = f"""
        SELECT DATE_TRUNC('month', date) AS month, SUM(volume) AS total_volume
        FROM {SNOWFLAKE_TABLE} WHERE ticker = '{ticker}'
        GROUP BY month ORDER BY month ASC
    """
    
    # Consultar os dados brutos
    query_raw_data = f"SELECT * FROM {SNOWFLAKE_TABLE} WHERE ticker = '{ticker}' ORDER BY date DESC LIMIT 100"
    
    try:
        # Preço médio da ação por mês
        cur.execute(query_stock_price)
        results_stock_price = cur.fetchall()
        df_stock_price = pd.DataFrame(results_stock_price, columns=['Month', 'Avg Stock Price'])

        # Cálculo da variação percentual do preço médio
        df_stock_price['Price Change (%)'] = df_stock_price['Avg Stock Price'].pct_change() * 100
        
        # Volume total por mês
        cur.execute(query_stock_volume)
        results_stock_volume = cur.fetchall()
        df_stock_volume = pd.DataFrame(results_stock_volume, columns=['Month', 'Total Volume'])

        # Dados brutos
        cur.execute(query_raw_data)
        results_raw_data = cur.fetchall()
        df_raw_data = pd.DataFrame(results_raw_data, columns=['Date', 'Stock Price', 'Volume', 'Ticker'])
        
        cur.close()
        cnx.close()
        
        # Layout dos gráficos e tabela (2 colunas x 2 linhas)
        col1, col2 = st.columns(2)
        col3, col4 = st.columns(2)

        # Gráfico de Preço Médio Mensal
        with col1:
            st.subheader("📊 Preço Médio da Ação Por Mês")
            fig, ax = plt.subplots()
            ax.plot(df_stock_price['Month'], df_stock_price['Avg Stock Price'], marker='o', linestyle='-')
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)

        # Gráfico de Volume Total Negociado Por Mês
        with col2:
            st.subheader("📈 Volume Total Negociado Por Mês")
            fig, ax = plt.subplots()
            ax.bar(df_stock_volume['Month'], df_stock_volume['Total Volume'], color='green')
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)

        # Gráfico de Variação Percentual do Preço Médio
        with col3:
            st.subheader("📉 Variação Percentual Mensal do Preço")
            fig, ax = plt.subplots()
            ax.bar(df_stock_price['Month'], df_stock_price['Price Change (%)'], color='red')
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)

        # Exibir tabela de dados brutos mais larga
        with col4:
            st.subheader("📋 Dados Brutos")
            with st.expander("🔍 Clique para expandir"):
                st.dataframe(df_raw_data)
    
    except Exception as e:
        st.error(f"Erro ao executar consultas no Snowflake: {e}")

# Fim


