# Projeto 6 - Implementação do Plano de Governança, Observabilidade, Qualidade e Segurança de Dados

# Imports
import pandas as pd
import boto3

# Ler o dataset enriquecido
df = pd.read_csv('arquivos/dados_enriquecidos.csv')

# Mascarar dados sensíveis (por exemplo, nome)
df['nome_mascarado'] = df['nome'].apply(lambda x: x[0] + '*' * (len(x) - 1) if isinstance(x, str) else '')

# Remover a coluna original
df = df.drop('nome', axis=1)

# Salvar dataset governado
df.to_csv('arquivos/dados_finais.csv', index=False)

# Enviar para o S3
s3 = boto3.client('s3')
bucket_name = 'dsa-data-lake-p6-890582101704'
s3.upload_file('arquivos/dados_finais.csv', bucket_name, 'governed-data/dados_finais.csv')

print("\nDSA Log - Mascaramento de dados concluído e arquivo enviado para o Data Lake.\n")
