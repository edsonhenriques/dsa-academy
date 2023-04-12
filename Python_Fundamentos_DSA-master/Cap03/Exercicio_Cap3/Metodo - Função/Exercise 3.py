print("Exercise 3")


lista1 = [1,2,3,4]

def newList():
    for i in range(0,2):
        lista1.append(int(input("Type a number: ")))

newList()
print(lista1)