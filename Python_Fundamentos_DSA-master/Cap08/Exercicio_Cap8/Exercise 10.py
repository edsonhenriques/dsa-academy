"""# Crie um banco de dados no SQLite, crie uma tabela, insira registros,
# consulte a tabela e retorne os dados em dataframe do Pandas"""

import sqlite3
import pandas as pd

# Criação do banco de dados
query = """
CREATE TABLE TESTE
(Cidade VARCHAR(20), 
 Estado VARCHAR(20),
 taxa REAL,        
 Impostos INTEGER
);"""
con = sqlite3.connect('dsa.db')
con.execute(query)
con.commit()

# Criando Tabela
data = [('Natal', 'Rio Grande do Norte', 1.25, 6),
        ('Recife', 'Pernambuco', 2.6, 3),
        ('Londrina', 'Paraná', 1.7, 5)]
stmt = "INSERT INTO TESTE VALUES(?, ?, ?, ?)"

# Inserindo Registros
con.executemany(stmt, data)
con.commit()

# Consultando a tabela
cursor = con.execute('select * from teste')
rows = cursor.fetchall()

# Retornando dados em um DataFrame Pandas
cursor.description
df = pd.DataFrame(rows, columns=[x[0] for x in cursor.description])
print(df)