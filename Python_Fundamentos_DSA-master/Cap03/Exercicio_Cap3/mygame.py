""" Video Aula do Game
https://www.youtube.com/watch?v=c_bwA7U4PKQ """

import turtle #biblioteca para grafico

class Box(turtle.Turtle): #classe janela
    def __init__(self ,x ,y ,w ,h): #definir função
        turtle.Turtle.__init__(self)
        self.shape("square") #formato
        self.color("white") #cor
        self.penup()
        self.goto(x ,y) #localizaçao
        self.shapesize(w ,h)  #dimensao

    def up(self): #mecher o objeto ára cima
        self.sety(self.ycor() + (self.ycor() < 220) * 20) #na posição y

    def dw(self): #mecher o objeto para baixo
        self.sety(self.ycor() - (self.ycor() > -220) * 20) #na posição y

    def draw(self): #placar
        self.write("{}              {}".format(self.pts, self.ptsPc), font = ("Arial", 40, "normal"), align = "center")


class Score(Box): #classe placar
    def __init__(self, x, y, w, h): #definindo funçoes do placar
        Box. __init__(self, x, y, w, h) #atribuindo variaveis para a função do palcar
        self.goto(x, y) #variavel para guardar valor
        self.pts = 0 #pontos
        self.ptsPc = 0 #pontos pc
        self.hideturtle()
        self.draw() #desenha textos


class Ball(Box): #classe bola
    def __init__(self, x, y, w, h): #definindo funçoes da bola
        Box.__init__(self, x, y, w, h) #atribuindo variaveis para a função da bola
        self.pcy = 0
        self.shape("circle") #mudei o formato da bolinha por conta
        self.bx = 0 #posição da bolinha no começo do jogo
        self.by = 0
        self.vx = 1 #posição movimento da bolinha
        self.vy = 1

    def update(self): #funçao que atualiza
        #definindo a velocidade da bola
        self.bx += self.vx #ela vai esta recebendo a velocidade x
        self.by += self.vy #ela vai esta recebendo a velocidade de y
        ball.goto(self.bx ,self.by)
        pc.goto(pc.xcor(), self.by)
        if (self.bx >= pc.xcor() - 30): #condição para a bolinha voltar quando bater nas barras
            self.vx *= -1
        if (self.bx <= hm.xcor() + 30) and (self.by < hm.ycor() + 100) and (self.by > hm.ycor() - 100):
            self.vx *= -1
        if (self.bx < hm.xcor()): #condição para a bolinha não voltar quando ultrapassa a barra
            self.vx *= -1
            self.bx = 0 #para a bolinha voltar pro centro
            self.by = 0
            score.ptsPc += 1 #adiciona um ponto no placar
            score.clear() #limpa o placar com os pontos "velhos"
            score.draw() #mostra o ponto adicionado no placar
        if (self.by >= 280) or (self.by <= -280): #condição para a bolinha voltar quando bater nas bordas
            self.vy *= -1
        if (self.bx > 20): #para a barra do pc mecher sozinha
            #ele vai esta seguindo a bolinha
            if (pc.ycor() < self.by) and (self.pcy < 220):
                self.pcy += 1
            if (pc.ycor() > self.by) and (self.pcy > -220):
                    self.pcy -= 1


#obejtos de acordo com os paramentro da classe box
hm = Box(-350, 0, 8, 1)
pc = Box(350, 1, 8, 1)
ball = Ball(0, 0, 1, 1)
score = Score(0, 230, 10, 10) #posição do placar

win = turtle.Screen()
win.title("My Ping Pong") #titulo da janela
win.bgcolor("black") #cor de fundo da janela
win.setup(width=800, height=600) #tamanho da janela
win.tracer(0)

win.listen()
win.onkeypress(hm.up, "Up") #para saber qual tecla esta sendo prensionada
win.onkeypress(hm.dw, "Down")

while True: #para atualizar sempre
    win.update()
    ball.update()