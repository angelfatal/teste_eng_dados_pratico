import requests
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from botocore.exceptions import NoCredentialsError

# credenciais aws 
# APENAS PELA FACILIDADE NA EXECUÇÃO DO TESTE, NÃO DEVE SER IMPLEMENTADO DESSA FORMA EM PRODUÇÃO
aws_access_key = 'aws_access_key'
aws_secret_key = 'aws_secret_key'
region_name = 'sa-east-1'
bucket_name = 'weather-forecast-teste-itau'

API_KEY = "e52fee1c69b7ea4ff2d4fd1e01da417c"
CITY = "São Paulo"  

# Conectar à API e obter dados da cidade de São Paulo
def get_weather_data(city):
    try:
        # Construindo a URL para requisição
        url = f"http://api.openweathermap.org/data/2.5/forecast?q={CITY}&appid={API_KEY}&units=metric"
        response = requests.get(url)

        # Se a resposta for válida devolve os dados
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao obter os dados: {response.status_code}")
            return None
    except Exception as e:
        print(f"Erro ao conectar à API: {e}")
        return None

def add_columns_date(json_data):
    weather_data = json_data['list']
    
    df = pd.DataFrame(weather_data)
    
    df['dt_txt'] = pd.to_datetime(df['dt_txt'])
    
    # Adicionar colunas de partição (ano, mês, dia)
    df['year'] = df['dt_txt'].dt.year
    df['month'] = df['dt_txt'].dt.month
    df['day'] = df['dt_txt'].dt.day
    
    return df

def save_s3(df):
    
    try:
        # Conectando ao s3
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )    
        
        # Particionando os dados por ano, mês e dia
        for (ano, mes, dia), grupo in df.groupby(['year', 'month', 'day']):
            # Definindo o caminho para as partições
            s3_file_name = f"forecast/year={ano}/month={mes}/day={dia}/forecast_weather_sp.parquet"
            
            # Convertendo o dataframe para parquet 
            tabela_parquet = pa.Table.from_pandas(grupo)
            
            # Salvando a tabela para arquivo Parquet em memória
            buffer = BytesIO()
            pq.write_table(tabela_parquet, buffer)
            buffer.seek(0)
            
            # Salvar arquivo Parquet no S3
            s3.put_object(Bucket=bucket_name, Key=s3_file_name, Body=buffer.getvalue())
            print(f"Arquivo salvo com sucesso em: s3://{bucket_name}/{s3_file_name}")
    
    except NoCredentialsError:
        print("Credenciais AWS não encontradas.")
    except Exception as e:
        print(f"Erro ao salvar no S3: {e}")


def main():
    # Obter os dados da API
    weather_data = get_weather_data(CITY)
    
    if weather_data:
        # Criando as colunas de ano, mês e dia pra salvar particionado no s3
        df = add_columns_date(weather_data)

        print(df)
        # Salvando no s3
        #save_s3(df)

        #Ver arquivo forecast_weather.sql que cria a tabela no Athena com base nos dados de s3
    

if __name__ == "__main__":
    main()
