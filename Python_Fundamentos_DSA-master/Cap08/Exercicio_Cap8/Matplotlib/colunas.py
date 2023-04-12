import pandas as pd
import matplotlib.pyplot as plt

# ABRIR E LER O ARQUIVO CONSIDERANDO VIRGULAS COMO SEPARADOR DE DADOS
dataset = pd.read_csv("C:\PythonFundamentos\Exercicios Feitos\Data Science Academy\Cap8\Matplotlib\dados.csv", sep = ";", decimal = ",", encoding='ISO-8859-1' )

# CRIANDO UM DATAFRAME A PARTIR DO ARQUIVO ABERTO
df_raw = pd.DataFrame(dataset[0].iloc[:,:].values)
# DEFININDO OS NOMES DAS COLUNAS
df_raw.columns = ["Finalidade","Custos (kR$)"]

df = df_raw["Custos"].tolist()

fig = plt.Figure()
coluna = fig.subplots(11)

# EDITANDO AS BARRAS DO GRAFICO
coluna.bar(df_raw["Finalidade"].values , df , align ='center', color = 'r')

# EDITANDO OS DADOS NO GRAFICO
coluna.set_title("Pagamentos Realizados em 2018",fontsize = 24)
coluna.set_xlabel("Finalidade",fontsize = 12)
coluna.set_ylabel("Custos", fontsize = 12)

#MOSTRAR O GRAFICO
plt.show()