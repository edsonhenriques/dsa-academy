# Projeto 8 - Construção de Uma Modern Data Stack com 7 Camadas

# Execute o script na sua máquina com o comando:

# python cria_tabela_custo.py

# Imports
import pandas as pd
import matplotlib.pyplot as plt

# Dados da estimativa de custo
cost_data = {
    "Item": [
        "Planejamento e Análise",
        "Configuração de Infraestrutura",
        "Ferramentas de Ingestão de Dados",
        "Data Lake e Data Warehouse",
        "Processamento de Dados (nuvem e local)",
        "Orquestração e Automação",
        "Desenvolvimento de Modelos e Transformações",
        "Ferramentas de Visualização",
        "Governança e Segurança",
        "Treinamento e Suporte"
    ],
    "Descrição": [
        "Custos associados ao levantamento de requisitos e escolha de tecnologias.",
        "Provisionamento de ambientes e infraestrutura inicial.",
        "Licenciamento e/ou custos de operação de ferramentas como Kafka e NiFi.",
        "Custos com armazenamento (S3, Redshift, Snowflake ou Hadoop).",
        "Custos de execução de pipelines em Databricks, Spark e afins.",
        "Configuração e operação de Airflow, Step Functions ou Composer.",
        "Desenvolvimento de modelos e aplicação de transformações de dados.",
        "Licenciamento de Tableau, Power BI, Looker ou similares.",
        "Ferramentas e políticas para garantir segurança e conformidade.",
        "Treinamento interno e suporte inicial pós-implementação."
    ],
    "Custo Estimado (USD)": [
        "5,000", "10,000", "8,000", "15,000", "12,000", 
        "7,000", "6,000", "10,000", "9,000", "4,000"
    ],
    "Período": [
        "1 mês", "1 mês", "6 meses", "12 meses", "12 meses",
        "6 meses", "6 meses", "12 meses", "12 meses", "1 mês"
    ]
}

# Criando o DataFrame
cost_df = pd.DataFrame(cost_data)

# Configurando o estilo da tabela
fig, ax = plt.subplots(figsize=(12, 6))
ax.axis('tight')
ax.axis('off')
table = ax.table(cellText=cost_df.values, colLabels=cost_df.columns, cellLoc='center', loc='center')

# Ajustando o layout da tabela
table.auto_set_font_size(False)
table.set_fontsize(10)
table.auto_set_column_width(col=list(range(len(cost_df.columns))))

# Salvando a imagem
cost_file_path = "Estimativa_Custos_Projeto8.png"
plt.savefig(cost_file_path, bbox_inches='tight', dpi=300)
