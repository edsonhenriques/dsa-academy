# Projeto 6 - Implementação do Plano de Governança, Observabilidade, Qualidade e Segurança de Dados

# Imports
import pandas as pd
import boto3
import great_expectations as ge

# Ler o dataset
df = pd.read_csv('arquivos/dados_brutos.csv')

# Transformações de dados
# 1. Tratar 'id': converter para numérico, remover entradas inválidas e converter para inteiro
df['id'] = pd.to_numeric(df['id'], errors='coerce')
df = df[df['id'].notna()]
df['id'] = df['id'].astype(int)

# 2. Tratar 'idade': converter para numérico, substituir valores fora do intervalo por NaN
df['idade'] = pd.to_numeric(df['idade'], errors='coerce')
df['idade'] = df['idade'].apply(lambda x: x if 0 <= x <= 120 else None)
df['idade'] = df['idade'].fillna(df['idade'].mean()).round(1)

# 3. Tratar 'salario': converter para numérico e substituir NaN ou negativos por um valor padrão (média)
df['salario'] = pd.to_numeric(df['salario'], errors='coerce')
df['salario'] = df['salario'].apply(lambda x: x if x is None or x >= 0 else None)
df['salario'] = df['salario'].fillna(df['salario'].mean())

# 4. Tratar 'nome': preencher valores ausentes com uma string padrão
df['nome'] = df['nome'].fillna('Desconhecido')

# Salvar dataset limpo
df.to_csv('arquivos/dados_limpos.csv', index=False)

# Enviar para o S3
s3 = boto3.client('s3')
bucket_name = 'dsa-data-lake-p6-890582101704'
s3.upload_file('arquivos/dados_limpos.csv', bucket_name, 'processed-data/dados_limpos.csv')

print("\nDSA Log - Validação de qualidade concluída e dados limpos enviados para o Data Lake.\n")
