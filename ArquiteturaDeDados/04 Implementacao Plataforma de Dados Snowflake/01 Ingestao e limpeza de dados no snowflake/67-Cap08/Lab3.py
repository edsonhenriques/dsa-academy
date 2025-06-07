# Lab 3 - Criando Data App Para Análise de Dados de Data Warehouse na Plataforma Snowflake

# Imports
import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# Título
st.title("Lab 3 - App de Dados - Análise de Vendas")

# Obtém a sessão Snowflake
session = get_active_session()

# Queries para extrair os dados
query_dim_cliente = "SELECT * FROM DSA_DB.schema3.dim_cliente"
query_dim_data = "SELECT * FROM DSA_DB.schema3.dim_data"
query_dim_loja = "SELECT * FROM DSA_DB.schema3.dim_loja"
query_dim_produto = "SELECT * FROM DSA_DB.schema3.dim_produto"
query_dim_vendedor = "SELECT * FROM DSA_DB.schema3.dim_vendedor"
query_fato_venda = "SELECT * FROM DSA_DB.schema3.fato_venda"

# Carregar os dados em dataframes do pandas para facilitar o uso na app de dados
df_cliente = session.sql(query_dim_cliente).to_pandas()
df_data = session.sql(query_dim_data).to_pandas()
df_loja = session.sql(query_dim_loja).to_pandas()
df_produto = session.sql(query_dim_produto).to_pandas()
df_vendedor = session.sql(query_dim_vendedor).to_pandas()
df_fato_venda = session.sql(query_fato_venda).to_pandas()

# Função para realizar joins e preparar os dados para análise
def preparar_dados_venda():
    df_vendas = df_fato_venda.merge(df_cliente, left_on='CLIENTE', right_on='ID_CLIENTE', how='left')
    df_vendas = df_vendas.merge(df_loja, left_on='LOJA', right_on='CODIGO', how='left', suffixes=('_VENDA', '_LOJA'))
    df_vendas = df_vendas.merge(df_produto, left_on='PRODUTO', right_on='CODIGO_SKU', how='left', suffixes=('_LOJA', '_PRODUTO'))
    df_vendas = df_vendas.merge(df_vendedor, left_on='VENDEDOR', right_on='MATRICULA', how='left', suffixes=('_PRODUTO', '_VENDEDOR'))
    df_vendas = df_vendas.merge(df_data, left_on='DATA', right_on='DATA_COMPLETA', how='left')
    return df_vendas

# Prepara os dados em um único dataframe
df_vendas = preparar_dados_venda()

# Filtros
st.sidebar.title("Filtros")
cidade_filtro = st.sidebar.multiselect("Selecione a Cidade:", options=df_vendas["CIDADE_LOJA"].unique())
produto_filtro = st.sidebar.multiselect("Selecione o Produto:", options=df_vendas["NOME_PRODUTO"].unique())
vendedor_filtro = st.sidebar.multiselect("Selecione o Vendedor:", options=df_vendas["NOME_VENDEDOR"].unique())

# Aplicar filtros
if cidade_filtro:
    df_vendas = df_vendas[df_vendas["CIDADE_LOJA"].isin(cidade_filtro)]
if produto_filtro:
    df_vendas = df_vendas[df_vendas["NOME_PRODUTO"].isin(produto_filtro)]
if vendedor_filtro:
    df_vendas = df_vendas[df_vendas["NOME_VENDEDOR"].isin(vendedor_filtro)]

# Mostrar dados filtrados
st.dataframe(df_vendas)

# Gráficos interativos
st.subheader("Total de Vendas por Produto")
grafico_vendas_produto = (
    alt.Chart(df_vendas)
    .mark_bar()
    .encode(
        x="NOME_PRODUTO",  
        y="sum(TOTAL_VENDA)", 
        color="NOME_PRODUTO", 
        tooltip=["NOME_PRODUTO", "sum(TOTAL_VENDA)"]
    )
    .properties(width=700)
)
st.altair_chart(grafico_vendas_produto)

st.subheader("Total de Vendas por Cidade")
grafico_vendas_cidade = (
    alt.Chart(df_vendas)
    .mark_bar()
    .encode(
        x="CIDADE_LOJA",  
        y="sum(TOTAL_VENDA)",  
        color="CIDADE_LOJA", 
        tooltip=["CIDADE_LOJA", "sum(TOTAL_VENDA)"]
    )
    .properties(width=700)
)
st.altair_chart(grafico_vendas_cidade)

# Filtros de cidade na barra lateral para o gráfico de vendas ao longo do tempo
cidade_tempo_filtro = st.sidebar.multiselect(
    "Filtrar por Cidade (Vendas ao Longo do Tempo):",
    options=df_vendas["CIDADE_LOJA"].unique(),
    default=df_vendas["CIDADE_LOJA"].unique()
)

# Aplicar o filtro de cidade ao dataframe para o gráfico de vendas ao longo do tempo
df_vendas_filtrado = df_vendas[df_vendas["CIDADE_LOJA"].isin(cidade_tempo_filtro)]

# Gráfico de Vendas ao longo do Tempo filtrado por cidade
st.subheader("Vendas ao Longo do Tempo")
grafico_vendas_tempo = (
    alt.Chart(df_vendas_filtrado)
    .mark_line(point=True)
    .encode(
        x="DATA_COMPLETA:T",  
        y="sum(TOTAL_VENDA)",  
        tooltip=["DATA_COMPLETA", "sum(TOTAL_VENDA)", "CIDADE_LOJA"],
        color="CIDADE_LOJA"  
    )
    .properties(width=700)
)
st.altair_chart(grafico_vendas_tempo)

# Resumo
st.subheader("Resumo dos Filtros Aplicados")
st.write(f"Total de Transações: {len(df_vendas)}")
st.write(f"Total de Vendas: R$ {df_vendas['TOTAL_VENDA'].sum():,.2f}")
