# Projeto 4 - Augmented Analytics com Inteligência Artificial e LLMs
# App

# Imports
import os
import openai
import langchain
import chromadb
from flask import Flask, request, render_template
from langchain_community.document_loaders import PyPDFDirectoryLoader
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from langchain_community.llms import OpenAI
from langchain.chains.question_answering import load_qa_chain
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

# Inicializa o Flask
app = Flask(__name__)

# Carrega as variáveis de ambiente
load_dotenv()

# Função para ler os arquivos em pdf
def dsa_read_pdf(directory_path):
    file_loader = PyPDFDirectoryLoader(directory_path)
    documents = file_loader.load()
    return documents

# Executa a função para carregar os documentos
dsa_doc = dsa_read_pdf('arquivos/')

# Cria o gerador de embeddings
gerador_embeddings = OpenAIEmbeddings(api_key = os.environ['OPENAI_API_KEY'])

# Aplica o gerador de embeddings aos documentos e armazena no banco de dados vetorial
index_name = 'dsa-index'
index = Chroma.from_documents(dsa_doc, gerador_embeddings, collection_name = index_name)

# Define a função de busca por similaridade
def dsa_busca_similaridade(query, k = 2):
    matching_results = index.similarity_search(query, k)
    return matching_results

# Cria instância do LLM
llm_dsa = OpenAI(openai_api_key = os.environ['OPENAI_API_KEY'], temperature = 0.3)

# Cria a chain para sistema de QA (Perguntas e Respostas)
chain = load_qa_chain(llm_dsa, chain_type = 'stuff')

# Define a função para obter resposta
def dsa_obtem_resposta(query):
    doc_search = dsa_busca_similaridade(query)
    response = chain.run(input_documents = doc_search, question = query)
    return response

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/dsallm', methods=['POST'])
def dsallm():
    query = request.form['query']
    response = dsa_obtem_resposta(query)
    return render_template('index.html', query = query, response = response)

if __name__ == '__main__':
    app.run(debug = False)





