import psycopg2
import requests
import pandas as pd

postgres_host = '34.173.103.16'
postgres_port = 5432
postgres_db = 'postgres'
postgres_user = 'junior'
postgres_password = '|?7LXmg+FWL&,2('

def extract_data_from_postgres():
    conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password
    )
    cursor = conn.cursor()

    query = 'SELECT * FROM public.venda;'
    
    cursor.execute(query)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    
    return rows

def read_parquet_file(file_url):
    try:
        df = pd.read_parquet(file_url)
        return df
    except Exception as e:
        print(f'Erro ao ler o arquivo Parquet: {e}')
        return None


def extract_data_from_api(employee_id):
    url = f'https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={employee_id}'
    
    try:
        response = requests.get(url)
        response.raise_for_status()  
        
  
        print(response.json())
        
        employee_name = response.json().get('name')
        return employee_name
    except requests.exceptions.HTTPError as err:
        print(f'Erro ao chamar a API. Código de resposta: {response.status_code}')
        return None
    except (requests.exceptions.JSONDecodeError, KeyError) as err:
        print('Erro ao decodificar a resposta JSON ou recuperar o nome do funcionário')
        return None



postgres_data = extract_data_from_postgres()
api_data = extract_data_from_api(9)
parquet_data = read_parquet_file('https://storage.googleapis.com/challenge_junior/categoria.parquet')

print('Dados do PostgreSQL:')
for row in postgres_data:
    print(row)

print('\nDados da API:')
print(api_data)

print('\nDados do arquivo Parquet:')
print(parquet_data.head())