print("-"*70)
print("                            CALCULATOR")

def soma(x,y):
    return x + y

def sub(x,y):
    return x - y

def mult(x,y):
    return x * y

def divisao(x,y):
    if (y == 0):
        return "Imposivel dividir por 0"
    else:
        return x / y

def calculate():
    number1 = float(input("Type a first number: "))
    number2 = float(input("Type a second number: "))

    if (decision == 1):
        result = soma(number1,number2)
    elif (decision == 2):
        result = sub(number1,number2)
    elif (decision == 3):
        result = mult(number1, number2)
    else:
        result = divisao(number1, number2)

    print("-"*70)
    print("Result: ", result)

while True:

    print("-" * 70)
    print("Enter the number corresponding to the operation you want to do")
    print("1 - Sum")
    print("2 - Subtraction")
    print("3 - Multiplication")
    print("4 - Division")
    print("0 - Go Out")

    decision = int(input("===> "))

    if (decision > 0) and (decision <= 4):
        calculate()
    elif (decision == 0):
        break
    else:
        print("Invalid Operation")