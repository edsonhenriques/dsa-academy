import numpy as np

print("Exercise 5")

arr = np.arange(15).reshape((3, 5))
print(arr)
print(arr.shape)
arr_t = arr.transpose()
print(arr_t)
print(arr_t.shape)