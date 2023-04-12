import pandas as pd

#Abrindo um arquivo para criar um DataFrame
dataset = pd.read_csv('C:\PythonFundamentos\Exercicios Feitos\Data Science Academy\Cap8\Pandas\Minerando Dados\Dominando Pandas\kc_house_data.csv', sep=',')

# Mostra o tipo da variaval
print(type(dataset))

# Mostra as 5 primeiras linhas do DataFrame
print(dataset.head(5))

# Mostra todas as colunas no DataFrame
print(dataset.columns)

# Mostra quantas linhas tem a base de dados
print(dataset.count())

# Mostra algumas informações estatisticas da base de dados
print(dataset.describe())

# Agrupa as informações e faz a soma por linhas do dataset
# Quantidade de moveis pelo tamanho de quarto
print(pd.value_counts(dataset['bedrooms']))

# Somente imoveis com 3 quartos
print(dataset.loc[dataset['bedrooms']==3])

# Somente imoveis com 3 quartos e com mais de 2 banheiros
print(dataset.loc[(dataset['bedrooms'] == 3) & (dataset['bathrooms'] > 2)])

# Ordenar em ordem decrescente de acordo com a coluna price
print(dataset.sort_values(by='price', ascending=False))

# Contar a quantidade de imoveis que tem 4 quartos
print(dataset.loc[dataset['bedrooms']==4].count())

# Criar coluna
dataset['size'] = (dataset['bedrooms']* 20)
print(dataset.columns)
print(dataset['size'])

# Categorizar as colunas pelo tamanahos
def categoriza(s):
    if s >= 80:
       return 'Big'
    elif s >= 60:
       return 'Medium'
    elif s >= 40:
       return 'Small'
# Utilizando o metodo apply() e criando uma nova coluna
dataset['cat_size'] = dataset['size'].apply(categoriza)
print(dataset['cat_size'])
# Vendo a distribuição da nova coluna
print(pd.value_counts(dataset['cat_size']))

# Remover dados (a coluna recem criada)
dataset.drop(['cat_size'], axis=1, inplace=True)

# Apangando imoveis sem quartos e imoveis com mais de 30 quartos
dataset.drop(dataset[dataset.bedrooms==0].index ,inplace=True)
dataset.drop(dataset[dataset.bedrooms > 30].index, inplace=True)

# Busca e mostra os valores faltantes e nulos de cada linha e coluna
print(dataset.isnull().sum())
# Remove todas a linhas com colunas com valores faltantes ou nulos
dataset.dropna(inplace=True)
print(dataset.isnull().sum())
#Remove a linhas onde todas a colunas possui valores faltantes ou nulos
dataset.dropna(how='all', inplace=True)











