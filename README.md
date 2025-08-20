# Formação AWS Data Lake

## Descrição

Este repositório contém os códigos e arquivos desenvolvidos durante a Formação AWS Data Lake, curso de engenharia de dados da plataforma Alura com foco em serviços AWS, Apache Spark e Python.

O objetivo principal do projeto foi construir um data lake completo na AWS. O fluxo abrangeu desde a ingestão de dados até a construção de um dashboard para análise das informações, seguindo a arquitetura medalhão para garantir a qualidade e a governança dos dados.

<img src=''>

- Bronze: os dados são coletados via script python e transferidos sem transformações para a camada bronze do bucket S3
- Silver: os dados são validados, limpos e padronizados via AWS Glue
- Gold: os dados são refinados, agregados e preparados para consumo de negócio na camada gold, fonte do dashboard AWS Quicksight

<img src=''>

## Technologias

O projeto utiliza a seguinte combinação de ferramentas e serviços para criar uma arquitetura de dados robusta e eficiente:

- AWS (Amazon Web Services): a plataforma de nuvem que hospeda todos os serviços.
- Apache Spark: utilizado para o processamento distribuído dos dados
- Python: a principal linguagem utilizada nos scripts, incluindo bibliotecas de web scraping como requests e o SDK boto3 para integração com a AWS
- AWS Glue: utilizado para criar e gerenciar jobs de ETL
- AWS EMR: para processamento distribuído com Apache Spark
- AWS Quicksight: ferramenta AWS utilizada para a construção de dashboards