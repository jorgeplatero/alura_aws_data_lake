# AWS Data Lake

## Descrição do Projeto

Este repositório contém os códigos e arquivos desenvolvidos durante a Formação AWS Data Lake, curso de engenharia de dados da plataforma Alura com foco em serviços AWS, Apache Spark e Python.

O objetivo principal do projeto foi construir um data lake completo na AWS. O fluxo abrangeu desde a ingestão de dados até a construção de um dashboard para análise das informações, seguindo a arquitetura medalhão para garantir a qualidade e a governança dos dados.

<div align="center"><img src='https://github.com/jorgeplatero/alura_aws_data_lake/blob/dfb588fdd92f9e76bb7d914ac2d631b7ca5cca01/img/arquitetura.png' width=500></div>

- **Bronze:** os dados são coletados via script python e transferidos sem transformações para a camada bronze do bucket S3
- **Silver:** os dados são validados, limpos e padronizados via AWS Glue
- **Gold:** os dados são refinados, agregados e preparados para consumo de negócio na camada gold, fonte do dashboard AWS Quicksight

<div align="center"><img src='https://github.com/jorgeplatero/alura_aws_data_lake/blob/dfb588fdd92f9e76bb7d914ac2d631b7ca5cca01/img/dashboard.png' width=700></div>

## Fonte de Dados

Os dados utilizados no projeto são da base de dados do site Data Boston (https://data.boston.gov/). Trata-se de uma coleção de solicitações de melhorias urbanas feitas pelos moradores da cidade de Boston, EUA. Esse conjunto de dados registra uma variedade de problemas reportados pela população, funcionando como um registro histórico das preocupações e necessidades da comunidade em relação à infraestrutura e aos serviços urbanos.

Clique <a style="text-decoration:none;" href="https://data.boston.gov/dataset/311-service-requests" target="_blank">aqui</a> para acessar os arquivos utilizados.

## Tecnologias Utilizadas

O projeto utiliza a seguinte combinação de ferramentas e serviços para criar uma arquitetura de dados robusta e eficiente:

- **AWS S3:** serviço utilizado para armazenar os arquivos parquet do projeto
- **AWS Glue:** serviço utilizado para criar e gerenciar jobs de ETL para a camada silver
- **AWS EMR:** serviço utilizado para o processamento distribuído de jobs de ETL para a camada gold
- **AWS Quicksight:** serviço utilizado utilizado para a construção do dashboard
- **Apache Spark:** framework utilizado para o processamento distribuído dos dados no serviço AWS EMR
- **Python:** a principal linguagem utilizada nos scripts, incluindo bibliotecas de web scraping como requests e o SDK boto3 para integração com a AWS
