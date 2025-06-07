# Lab 2 - Pipeline de Ingestão e Limpeza de Dados no Snowflake com Python Worksheet

# Imports
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, round

# Handler (função) principal
def main(session: snowpark.Session): 

    # Nome da tabela original
    tableName = 'dados_sensores'

    # Importa a tabela em um dataframe
    dataframe = session.table(tableName)
    print(type(dataframe))

    # 1. Selecionar somente dados com temperatura superior a 80.
    df_filtrado = dataframe.filter(col('temperatura') > 80)
    print(type(df_filtrado))
    
    # 2. Criar uma nova coluna multiplicando 'vibracao' e 'pressao' para formar um índice.
    # Isso é uma tarefa de Engenharia de Atributos para enriquecimento dos dados.
    df_com_indice = df_filtrado.with_column('indice_vibracao_pressao', col('vibracao') * col('pressao'))
    print(type(df_com_indice))
    
    # 3. Converter todos os valores para o formato de apenas duas casas decimais.
    df_formatado = df_com_indice.select(
        round(col('vibracao'), 2).alias('vibracao'),
        round(col('temperatura'), 2).alias('temperatura'),
        round(col('pressao'), 2).alias('pressao'),
        round(col('umidade'), 2).alias('umidade'),
        col('horas_trabalho'),
        col('manutencao_necessaria'),
        round(col('indice_vibracao_pressao'), 2).alias('indice_vibracao_pressao')
    )

    print(type(df_formatado))
    
    # 4. Criar uma nova tabela no banco de dados DSA_DB no schema2 com o resultado do processamento.
    df_formatado.write.save_as_table('DSA_DB.schema2.nova_tabela_sensores', mode='overwrite')
    
    # 5. Imprimir uma amostra dos dados formatados.
    df_formatado.show()

    # Converte para dataframe do pandas
    df_pandas = df_formatado.toPandas()
    print(type(df_pandas))

    # Retornar o dataframe formatado.
    return df_formatado



