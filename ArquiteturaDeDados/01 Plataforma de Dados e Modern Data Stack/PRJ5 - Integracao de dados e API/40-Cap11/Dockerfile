# Define a imagem base
ARG PYTHON_VERSION=3.11.5
FROM python:${PYTHON_VERSION}-slim as base

# Instala pacote de utilitários
RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

# Impede que Python grave arquivos pyc
ENV PYTHONDONTWRITEBYTECODE=1

# Impede que Python armazene em buffer stdout e stderr para evitar situações em que
# o aplicativo trava sem emitir nenhum log devido ao buffer
ENV PYTHONUNBUFFERED=1

# Define a pasta de trabalho
WORKDIR /app

# Baixa e instala as dependências como uma etapa separada para aproveitar o cache do Docker
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python -m pip install -r requirements.txt

# Copia o código-fonte para o container
COPY . .

# Define a porta em que o aplicativo recebe conexão
EXPOSE 3310

# Executa a API
CMD ["python", "dsa_app.py"]
