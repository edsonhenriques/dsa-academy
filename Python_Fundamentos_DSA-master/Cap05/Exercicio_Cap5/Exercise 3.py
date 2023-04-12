print("Exercise 3")

class Smartphone(object):
    def __init__(self, tamanho, interface):
        self.tamanho = tamanho
        self.interface = interface

class MP3Player(Smartphone): #o atributo da classe mp3Player é a classe Smartphone
    def __init__(self, capacidade, tamanho='Pequeno', interface='Led'):
        self.capacidade = capacidade
        Smartphone.__init__(self, tamanho, interface)

    def print_mp3player(self):
        print("Valores para o objeto criado: %s %s %s" % (self.tamanho, self.interface, self.capacidade))


device1 = MP3Player('64 GB')
device1.print_mp3player()