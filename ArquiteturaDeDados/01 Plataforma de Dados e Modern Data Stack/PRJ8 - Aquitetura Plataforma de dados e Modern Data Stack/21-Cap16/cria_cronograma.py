# Projeto 8 - Construção de Uma Modern Data Stack com 7 Camadas

# Execute o script na sua máquina com o comando:

# python cria_cronograma.py

# Imports
import pandas as pd
import matplotlib.pyplot as plt

# Dados da tabela
data = {
    "Etapa": [
        "Planejamento e Análise",
        "Configuração de Infraestrutura",
        "Implementação - Camada 1",
        "Implementação - Camada 2",
        "Implementação - Camada 3",
        "Implementação - Camada 4",
        "Implementação - Camada 5",
        "Implementação - Camada 6",
        "Implementação - Camada 7",
        "Testes e Validação",
        "Treinamento e Documentação",
        "Go-Live e Suporte Inicial"
    ],
    "Descrição": [
        "Levantamento de requisitos, análise de fontes de dados, escolha de ferramentas e tecnologias.",
        "Configuração dos ambientes (nuvem e local) e provisionamento de recursos necessários.",
        "Desenvolvimento e teste da ingestão de dados, integração com fontes de dados (batch e streaming).",
        "Configuração do armazenamento centralizado (Data Lake e Data Warehouse).",
        "Desenvolvimento de pipelines de processamento e transformação inicial de dados.",
        "Configuração de workflows e automação com Apache Airflow e ferramentas específicas.",
        "Desenvolvimento e aplicação de modelos de dados, regras de negócios e testes no dbt.",
        "Criação de dashboards e relatórios interativos com ferramentas de visualização.",
        "Configuração de governança e segurança, com auditorias e políticas de conformidade.",
        "Realização de testes integrados em todas as camadas e validação final com stakeholders.",
        "Capacitação das equipes internas e entrega de documentação completa do sistema.",
        "Lançamento oficial da solução e suporte contínuo para ajustes pós-implementação."
    ],
    "Responsáveis": [
        "Arquitetos de Dados",
        "Equipe de Infraestrutura",
        "Engenheiros de Dados",
        "Engenheiros de Dados",
        "Engenheiros de Dados",
        "Engenheiros de Dados",
        "Engenheiros de Dados",
        "Cientistas de Dados",
        "Especialistas em Governança",
        "Arquitetos de Dados",
        "Arquitetos de Dados",
        "Equipe de Suporte"
    ],
    "Duração Estimada": [
        "3 semanas", "2 semanas", "4 semanas", "3 semanas", "4 semanas", 
        "3 semanas", "3 semanas", "4 semanas", "3 semanas", "3 semanas", 
        "2 semanas", "2 semanas"
    ],
    "Prazo": [
        "Semana 1 a 3", "Semana 4 a 5", "Semana 6 a 9", "Semana 10 a 12", "Semana 13 a 16",
        "Semana 17 a 19", "Semana 20 a 22", "Semana 23 a 26", "Semana 27 a 29", 
        "Semana 30 a 32", "Semana 33 a 34", "Semana 35 a 36"
    ]
}

# Criando o DataFrame
df_dsa = pd.DataFrame(data)

# Configurando o estilo da tabela
fig, ax = plt.subplots(figsize=(12, 6))
ax.axis('tight')
ax.axis('off')
table = ax.table(cellText=df_dsa.values, colLabels=df_dsa.columns, cellLoc='center', loc='center')

# Ajustando o layout da tabela
table.auto_set_font_size(False)
table.set_fontsize(10)
table.auto_set_column_width(col=list(range(len(df_dsa.columns))))

# Salvando a imagem
file_path = "Cronograma_Projeto8.png"
plt.savefig(file_path, bbox_inches='tight', dpi=300)
file_path
