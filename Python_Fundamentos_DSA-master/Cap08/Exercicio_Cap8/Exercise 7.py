import numpy as np

print("Exercise 7")

A = np.arange(10)
print(A)
np.save('array_a',A)

B = np.load('array_a.npy')
print(B)
