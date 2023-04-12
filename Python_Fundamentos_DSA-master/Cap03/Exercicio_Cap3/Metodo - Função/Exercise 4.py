print("Exercise 4")


def function(arg1,*lista):
    print(arg1)
    for i in lista:
        print(i)
    return;

function(100)

function('A','B','C','D')
