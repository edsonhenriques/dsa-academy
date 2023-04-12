import time
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.datasets import load_iris

fontsize = 14
ticklabelsize = 14

iris = load_iris()
df = pd.DataFrame(iris.data, columns=iris.feature_names)
print(len(df))

#print(df.head())

#print(df.describe())
#print(list(df))

#print(iris.target_names)
#print(iris.target)

df['species'] = pd.Categorical.from_codes(iris.target, iris.target_names)
df['target'] = iris.target
#print(df.head())