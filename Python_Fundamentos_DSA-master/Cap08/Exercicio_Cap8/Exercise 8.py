import pandas as pd

print("Exercise 8")

obj = pd.Series(['c', 'a', 'd', 'a', 'a', 'b', 'b', 'c', 'c', 'a', 'b'])
uniques = obj.unique()
print(uniques)