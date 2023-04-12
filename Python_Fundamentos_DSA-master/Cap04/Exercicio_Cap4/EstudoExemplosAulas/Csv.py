import csv

with open('arquivos/numeros.csv','w') as arquivo:
    writer = csv.writer(arquivo)
    writer.writerow(('primeira', 'segunda', 'terceira')) #função escreve na linha
    writer.writerow((55, 93, 76))
    writer.writerow((62, 14, 86))


with open('arquivos/numeros.csv','r', encoding='utf8', newline='\r\n') as arquivo:
    reader = csv.reader(arquivo)
    for x in reader:
        print(x)

with open('arquivos/numeros.csv', 'r', encoding='utf8', newline='\r\n') as arquivo:
    leitor = csv.reader(arquivo)
    dados = list(leitor)

print(dados)

for linha in dados[1:]:
    print(linha)