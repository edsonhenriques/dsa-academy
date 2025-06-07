# Projeto 6 - Implementação do Plano de Governança, Observabilidade, Qualidade e Segurança de Dados

# Imports
import pandas as pd
import numpy as np

# Criar um dicionário provocando problemas comuns nos dados
data = {
    'id': [1, 2, 3, 4, 5, 'seis', 7],
    'nome': ['Mariana', 'Gabriel', 'Carlos', None, 'Ana', 'Francisco', 'Helena'],
    'idade': [26, None, 35, 28, -6, 40, 'unknown'],
    'salario': [50000, 60000, None, 70000, 80000, 90000, 100000]
}

# Converte o dicionário em dataframe
df_dsa = pd.DataFrame(data)

# Salvar o DataFrame como CSV
df_dsa.to_csv('arquivos/dados_brutos.csv', index=False)

print("\nDSA Log - Dados brutos gerados e salvos com sucesso.\n")
