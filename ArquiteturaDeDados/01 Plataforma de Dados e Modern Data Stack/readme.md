# PLATAFORMA DE DADOS E MODERN DATA STACK

### 🚀 ***Pós Graduação de Arquitetura de Dados (DataScicence Academy)***

#### Tabela de Conteúdo:
- Fundamentos
- Arquitetura de soluções BI e Data Science
- Arquitetura de soluções de ML, IA Generativa e LLM
- Integração de dados e API
- Governança de dados, observalidade, qualidade de dados e segurança de dados
- Arquitetura de plataforma de dados
- Arquitetura de plataofmra de dados e Modern Data Stack


## Plataforma de Dados 
### Definição:
Uma Plataforma de Dados é um sistema integrado e centralizado projetado para gerenciar todo o ciclo de vida dos dados de uma organização. Isso inclui a ingestão, armazenamento, processamento, transformação, análise, governança e disponibilização de dados de diversas fontes para diferentes usuários e aplicações.

O objetivo principal de uma plataforma de dados é fornecer acesso confiável, escalável e seguro a dados de alta qualidade para suportar decisões de negócio, análises, machine learning, relatórios e outras iniciativas baseadas em dados.

### Principais características:
- **Centralização:** Consolida dados de múltiplas fontes de dados em um único local ou ambiente acessível.
- **Ciclo de Vida do Dado**: Abrange todas as etapas, desde a origem até o consumo final.
- **Acesso e Compartilhamento:** Facilita o acesso seguro e controlado aos dados para diferentes equipes e ferramentas.
- **Escalabilidade:** Deve ser capaz de lidar com volumes crescentes de dados e usuários.
- **Governança e Segurança:** Implementa políticas de segurança, conformidade, qualidade e linhagem de dados.
- **Suporte a Diversos Casos de Uso:** Deve atender necessidades de BI, Analytics, Data Science, aplicações, etc.


## Modern Data Stack (MDS)

### Definição:
O Modern Data Stack (MDS) é uma arquitetura e um conjunto de ferramentas e tecnologias, geralmente baseadas em nuvem (SaaS - Software as a Service), que representam a abordagem contemporânea para construir uma Plataforma de Dados. Em vez de uma única solução monolítica, o MDS utiliza ferramentas "best-of-breed" (as melhores em sua categoria) e especializadas para cada etapa do pipeline de dados.

Ele surgiu como resposta às limitações das arquiteturas tradicionais (como data warehouses on-premise rígidos) e à necessidade de maior flexibilidade, escalabilidade, velocidade e capacidade de lidar com diversos tipos e volumes de dados.

### Principais Componentes (Camadas Típicas de um MDS):

- **Camada de Ingestão e transporte:*** Ferramentas que automatizam a extração de dados de diversas fontes (bancos de dados, APIs, arquivos, SaaS) e os carregam diretamente para o destino central (geralmente um Data Warehouse ou Data Lakehouse na nuvem).  
🔹***Ex:*** Apache Kafka, Apache Nifi, Fivetrans, Airbyte, Stitch.
- **Camada de Armazenamento, Data Warehouse e Data Lakehouse:** O repositório principal onde os dados são armazenados e organizados. As soluções modernas combinam a flexibilidade de um Data Lake com a estrutura e performance de um Data Warehouse.  
🔹***Ex:*** AmazonS3, Azure Storage, Google Cloud Storage, Snowflake, Google BigQuery, Amazon Redshift, Databricks Lakehouse Platform, Dremio.
- **Camada de Transformação (ELT/ETL):** Ferramentas que permitem modelar, limpar e transformar os dados brutos carregados na camada de armazenamento, tornando-os prontos para análise. A abordagem ELT (em vez de ETL) é comum, onde a transformação acontece após o carregamento, aproveitando o poder computacional do Data Warehouse/Lakehouse.  
🔹***Ex:*** dbt, (data build tool), Talend, Fivetran, Stitch.
- **Business Intelligence (BI) e Análise:** Ferramentas para visualizar dados, criar dashboards, relatórios e realizar análises exploratórias.  
🔹***Ex:*** Tableau, Looker, Power BI, Metabase.
- **Camada de Orquestração:** Ferramentas para agendar, monitorar e gerenciar os fluxos de trabalho (pipelines) de dados.  
🔹***Ex:*** Apache Airflow, Prefect, Dagster.
- **Camada de Governança e Observabilidade:** Ferramentas para gerenciar metadados, linhagem de dados, qualidade de dados, segurança, custos e monitorar a saúde do pipeline.  
🔹***Ex:*** Open Metadata, Atlan, Alation, Monte Carlo, Great Expectations.
- **Camada de Consumo/Ativação:** Ferramentas que levam os dados transformados de volta para as aplicações de negócio (Reverse ETL) ou servem dados para modelos de ML ou aplicações.  
🔹***Ex:*** Census, Hightouch.

### Relação entre Plataforma de Dados e MDS:

O Modern Data Stack é a implementação ou a abordagem arquitetural mais comum e eficaz para construir uma Plataforma de Dados nos dias de hoje. Ele fornece os componentes e a flexibilidade necessários para realizar as funções de uma Plataforma de Dados de maneira ágil, escalável e econômica, aproveitando as capacidades da computação em nuvem e ferramentas especializadas.

Em resumo, uma Plataforma de Dados é o conceito ou o objetivo (ter um sistema centralizado para gerenciar dados), enquanto o Modern Data Stack é a forma (usando ferramentas cloud-native, best-of-breed) de atingir esse objetivo na era atual.
