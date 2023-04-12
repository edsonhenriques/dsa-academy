print("Exercise 10")


frase = "É melhor, muito melhor, contentar-se com a realidade; " \
        "\nse ela não é tão brilhante como os sonhos," \
        "\n tem pelo menos a vantagem de existir."

count = 0

for caracter in frase:
    if caracter == 'r':
        count += 1

print("O caracter r aparece %s vezes na frase." %(count))