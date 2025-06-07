# Projeto 6 - Implementação do Plano de Governança, Observabilidade, Qualidade e Segurança de Dados

# Imports
import pandas as pd
import great_expectations as ge

# Ler o dataset
df = pd.read_csv('arquivos/dados_limpos.csv')

# Converter DataFrame do Pandas para um Great Expectations Dataset
df['idade'] = pd.to_numeric(df['idade'], errors='coerce')  # Garantir que 'idade' é numérica antes da validação
df['salario'] = pd.to_numeric(df['salario'], errors='coerce')  # Garantir que 'salario' é numérico antes da validação
gdf = ge.from_pandas(df)

# Adicionar validações de qualidade
# 1. Verificar se a coluna 'id' é numérica e não possui valores nulos
gdf.expect_column_to_exist("id")
gdf.expect_column_values_to_be_of_type("id", "int")
gdf.expect_column_values_to_not_be_null("id")

# 2. Verificar se a coluna 'idade' está no intervalo 0 a 120
gdf.expect_column_to_exist("idade")
gdf.expect_column_values_to_be_between("idade", 0, 120)

# 3. Verificar se a coluna 'salario' possui valores não negativos
gdf.expect_column_to_exist("salario")
gdf.expect_column_values_to_be_between("salario", 0, None)

# 4. Verificar se a coluna 'nome' não possui valores nulos
gdf.expect_column_to_exist("nome")
gdf.expect_column_values_to_not_be_null("nome")

# Validar o DataFrame
results = gdf.validate(result_format="SUMMARY")

# Verificar se houve falhas na validação
if not results["success"]:
    print("\nProblemas encontrados na validação de qualidade. Gerando relatório...\n")
else:
    print("\nNenhum problema de qualidade detectado.\n")

# Geração do relatório HTML manualmente
import json

# Função para gerar HTML simples a partir dos resultados
def gerar_relatorio_html(resultados, caminho_arquivo="arquivos/relatorio_validacao_dados_limpos.html"):
    html = "<html><head><title>Relatório de Validação</title></head><body>"
    html += "<h1>Relatório de Validação de Qualidade DSA</h1>"
    
    if resultados["success"]:
        html += "<p style='color:green;'>Todas as validações foram concluídas com sucesso!</p>"
    else:
        html += "<p style='color:red;'>Foram encontrados problemas na validação dos dados.</p>"
    
    html += "<h2>Detalhes das Validações:</h2>"
    for expectation in resultados["results"]:
        expectation_config = expectation["expectation_config"]
        result = expectation["result"]
        
        html += f"<h3>{expectation_config['expectation_type']}</h3>"
        html += "<ul>"
        html += f"<li>Coluna: {expectation_config['kwargs'].get('column', 'N/A')}</li>"
        html += f"<li>Sucesso: {'Sim' if expectation['success'] else 'Não'}</li>"
        html += f"<li>Observado: {result.get('observed_value', 'N/A')}</li>"
        html += "</ul>"
    
    html += "</body></html>"
    
    # Salvar o HTML no arquivo especificado
    with open(caminho_arquivo, "w") as f:
        f.write(html)
    
    print(f"Relatório HTML gerado com sucesso: {caminho_arquivo}\n")

# Gera o relatório
gerar_relatorio_html(results)



