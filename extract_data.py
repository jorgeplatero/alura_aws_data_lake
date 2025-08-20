import pandas as pd
import requests
import os
import boto3
from io import BytesIO
import warnings
warnings.filterwarnings('ignore')


def extract_data(url, local_file):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() 
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        with open(local_file, 'wb') as file:
            file.write(response.content)
        print(f'Data saved to {local_file}')
    except requests.exceptions.HTTPError as e:
        print(f'HTTP Error: {e}')
    except Exception as e:
        print(f'Error: {e}')


if __name__ == '__main__':

    urls = [
        ('https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/ea2e4696-4a2d-429c-9807-d02eb92e0222/download/tmpcje3ep_w.csv', 'data/dados_2019.csv'),
        ('https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/6ff6a6fd-3141-4440-a880-6f60a37fe789/download/tmpcv_10m2s.csv', 'data/dados_2020.csv'),
        ('https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/f53ebccd-bc61-49f9-83db-625f209c95f5/download/tmp88p9g82n.csv', 'data/dados_2021.csv'),
        ('https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/81a7b022-f8fc-4da5-80e4-b160058ca207/download/tmpfm8veglw.csv', 'data/dados_2022.csv'),
        ('https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/e6013a93-1321-4f2a-bf91-8d8a02f1e62f/download/tmpwbgyud93.csv', 'data/dados_2023.csv'),
    ]

    for url, file in urls:
        extract_data(url, file)

    arquivos = [
        'data/raw/dados_2019.csv',
        'data/raw/dados_2020.csv',
        'data/raw/dados_2021.csv',
        'data/raw/dados_2022.csv',
        'data/raw/dados_2023.csv'
    ]

    dfs = {}

    for arquivo in arquivos:
        ano = arquivo.split('_')[-1].split('.')[0]
        dfs[ano] = pd.read_csv(arquivo)

    #define acesso
    aws_access_key_id = 'aws_access_key_id'
    aws_secret_access_key = 'aws_secret_access_key'
    region_name = 'region_name'
    #cria uma sessão padrão
    boto3.setup_default_session(
        aws_access_key_id = aws_access_key_id, 
        aws_secret_access_key= aws_secret_access_key, 
        region_name = region_name
    )
    #cria cliente s3
    s3 = boto3.client('s3')

    for ano, df in dfs.items():
        parquet_buffer = BytesIO() #cria arquivo virtual
        df.to_parquet(parquet_buffer) #converte dataframe para o formato parquet
        #instrui o envio do arquivo para o bucket no S3
        s3.put_object(
            Bucket = 'aws-datalake-datalake', #define nome do bucket do S3
            Key = f'bronze/dados_{ano}.parquet', #define nome e caminho do arquivo
            Body = parquet_buffer.getvalue() #fornece conteúdo do arquivo a ser enviado
        )

    #lista objetos no bucket S3
    response = s3.list_objects(Bucket='aws-datalake-datalake')
    keys = [obj['Key'] for obj in response ['Contents']]
    print(keys)