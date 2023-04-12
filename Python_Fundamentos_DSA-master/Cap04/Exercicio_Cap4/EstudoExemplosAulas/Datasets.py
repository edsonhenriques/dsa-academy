print("Abrindo Datasets")


f = open('arquivos/salarios.csv','r') #abrindo o arquivo
data = f.read() #lendo o arquivo
rows = data.split('\n') #espaçando os dados
print(rows) #printando dataset em uma unica linha

full_data = [] #lista vazia para dividir o dataset em colunas
for row in rows: #loop
    split_row = row.split(",")
    full_data.append(split_row)
    first_row = full_data[0] #para ser usada cna contagem da coluna
print(full_data) #printa dataset na forma de listas concatenadas

#contanto linhas
countL = 0
for row in full_data:
    countL += 1
print(countL)

#contanto colunas
countC = 0
for column in first_row:
    countC = countC + 1
print(countC)