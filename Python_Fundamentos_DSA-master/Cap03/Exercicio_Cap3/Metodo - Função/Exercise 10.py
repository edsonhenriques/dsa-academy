print("Exercise 10")

import pandas as pd

def retornaArq(file_name):
    df = pd.read_csv(file_name)
    return df.describe() #metodo

file_name = "binary.csv"
df2 = retornaArq(file_name)
print(df2)
