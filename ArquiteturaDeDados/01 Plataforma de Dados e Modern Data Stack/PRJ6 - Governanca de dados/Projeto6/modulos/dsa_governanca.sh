#!/bin/bash

log_file="pipeline_execution.log"
echo "$(date): Iniciando pipeline de dados" >> $log_file

# Executa os scripts sequenciais com logging
echo "Executando 01_extrai_dados_dsa.py" | tee -a $log_file
if python 01_extrai_dados_dsa.py; then
    echo "$(date): Sucesso ao executar 01_extrai_dados_dsa.py" >> $log_file
    echo "Sucesso ao executar 01_extrai_dados_dsa.py"
else
    echo "$(date): Erro ao executar 01_extrai_dados_dsa.py. Encerrando pipeline." >> $log_file
    echo "Erro ao executar 01_extrai_dados_dsa.py. Encerrando pipeline."
    exit 1
fi

echo "Executando 02_upload_s3_dsa.py" | tee -a $log_file
if python 02_upload_s3_dsa.py; then
    echo "$(date): Sucesso ao executar 02_upload_s3_dsa.py" >> $log_file
    echo "Sucesso ao executar 02_upload_s3_dsa.py"
else
    echo "$(date): Erro ao executar 02_upload_s3_dsa.py. Encerrando pipeline." >> $log_file
    echo "Erro ao executar 02_upload_s3_dsa.py. Encerrando pipeline."
    exit 1
fi

echo "Executando 03_observabilidade_dsa.py" | tee -a $log_file
if python 03_observabilidade_dsa.py; then
    echo "$(date): Sucesso ao executar 03_observabilidade_dsa.py" >> $log_file
    echo "Sucesso ao executar 03_observabilidade_dsa.py"
else
    echo "$(date): Erro ao executar 03_observabilidade_dsa.py. Encerrando pipeline." >> $log_file
    echo "Erro ao executar 03_observabilidade_dsa.py. Encerrando pipeline."
    exit 1
fi

echo "Executando 04_valida_qualidade_dados_brutos_dsa.py" | tee -a $log_file
if python 04_valida_qualidade_dados_brutos_dsa.py; then
    echo "$(date): Sucesso ao executar 04_valida_qualidade_dados_brutos_dsa.py" >> $log_file
    echo "Sucesso ao executar 04_valida_qualidade_dados_brutos_dsa.py"
else
    echo "$(date): Erro ao executar 04_valida_qualidade_dados_brutos_dsa.py. Encerrando pipeline." >> $log_file
    echo "Erro ao executar 04_valida_qualidade_dados_brutos_dsa.py. Encerrando pipeline."
    exit 1
fi

echo "Executando 05_aplica_qualidade_dsa.py" | tee -a $log_file
if python 05_aplica_qualidade_dsa.py; then
    echo "$(date): Sucesso ao executar 05_aplica_qualidade_dsa.py" >> $log_file
    echo "Sucesso ao executar 05_aplica_qualidade_dsa.py"
else
    echo "$(date): Erro ao executar 05_aplica_qualidade_dsa.py. Encerrando pipeline." >> $log_file
    echo "Erro ao executar 05_aplica_qualidade_dsa.py. Encerrando pipeline."
    exit 1
fi

echo "Executando 06_valida_qualidade_dados_limpos_dsa.py" | tee -a $log_file
if python 06_valida_qualidade_dados_limpos_dsa.py; then
    echo "$(date): Sucesso ao executar 06_valida_qualidade_dados_limpos_dsa.py" >> $log_file
    echo "Sucesso ao executar 06_valida_qualidade_dados_limpos_dsa.py"
else
    echo "$(date): Erro ao executar 06_valida_qualidade_dados_limpos_dsa.py. Encerrando pipeline." >> $log_file
    echo "Erro ao executar 06_valida_qualidade_dados_limpos_dsa.py. Encerrando pipeline."
    exit 1
fi

echo "Executando 07_enriquecimento_dsa.py" | tee -a $log_file
if python 07_enriquecimento_dsa.py; then
    echo "$(date): Sucesso ao executar 07_enriquecimento_dsa.py" >> $log_file
    echo "Sucesso ao executar 07_enriquecimento_dsa.py"
else
    echo "$(date): Erro ao executar 07_enriquecimento_dsa.py. Encerrando pipeline." >> $log_file
    echo "Erro ao executar 07_enriquecimento_dsa.py. Encerrando pipeline."
    exit 1
fi

echo "Executando 08_seguranca_dsa.py" | tee -a $log_file
if python 08_seguranca_dsa.py; then
    echo "$(date): Sucesso ao executar 08_seguranca_dsa.py" >> $log_file
    echo "Sucesso ao executar 08_seguranca_dsa.py"
else
    echo "$(date): Erro ao executar 08_seguranca_dsa.py. Encerrando pipeline." >> $log_file
    echo "Erro ao executar 08_seguranca_dsa.py. Encerrando pipeline."
    exit 1
fi

echo "$(date): Pipeline de dados concluído com sucesso." >> $log_file
echo "Pipeline de dados concluído com sucesso."
