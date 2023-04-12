import numpy as np
import timeit

print("Exercise 1")

my_array = np.random.rand(100000)
my_list = list(range(1,100000))

start_array = timeit.default_timer()
new_array = my_array * 2
finish_array = timeit.default_timer()

start_list = timeit.default_timer()
new_list = [i * 2 for i in my_list]
finish_list = timeit.default_timer()

time_array = finish_array - start_array
time_list = finish_list - start_list

print(f"TIME ARRAY: {time_array}")
print(f"TIME LIST: {time_list}")

if (time_array < time_list):
    print("ARRAY IT´S FASTER")
else:
    print("LIST IT´S FASTER")