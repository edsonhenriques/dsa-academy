print("Exercise 1")

def numberPair():
    x = int(input("Type a number: "))
    y = int(input("Type a number: "))

    if (x > y):
        for i in range (y,x,2):
            print(i)
    elif (y > x):
        for i in range(x,y,2):
            print(i)

numberPair()