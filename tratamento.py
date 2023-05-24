import psycopg2
import requests
import pandas as pd
import pyarrow.parquet as pq

# Conexão com o banco PostgreSQL
postgres_host = "34.173.103.16"
postgres_user = "junior"
postgres_password = "|?7LXmg+FWL&,2("
postgres_port = 5432
postgres_database = "postgres"

conn = psycopg2.connect(
    host=postgres_host,
    user=postgres_user,
    password=postgres_password,
    port=postgres_port,
    database=postgres_database
)

# Extração dos dados da tabela "venda" do banco PostgreSQL
query = "SELECT * FROM public.venda;"

try:
    df_vendas = pd.read_sql_query(query, conn)
    print("Dados de Vendas:")
    print(df_vendas.head())
except Exception as e:
    print(f"Erro ao extrair dados do PostgreSQL: {str(e)}")
    df_vendas = pd.DataFrame()

# Chamada à API para obter dados de funcionários
api_endpoint = "https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior"
funcionario_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9]

dados_funcionarios = []
for funcionario_id in funcionario_ids:
    params = {"id": funcionario_id}
    try:
        response = requests.get(api_endpoint, params=params)
        response.raise_for_status()
        try:
            data = response.json()
            if data and "nome" in data:
                dados_funcionario = {"id": funcionario_id, "nome": data["nome"]}
                dados_funcionarios.append(dados_funcionario)
            else:
                print(f"Dados inválidos para o funcionário ID: {funcionario_id}")
        except ValueError as ve:
            print(f"Erro ao converter resposta da API para JSON: {str(ve)}")
    except requests.exceptions.RequestException as re:
        print(f"Erro na chamada da API: {str(re)}")

df_funcionarios = pd.DataFrame(dados_funcionarios)

# Download do arquivo Parquet de categorias
parquet_url = "https://storage.googleapis.com/challenge_junior/categoria.parquet"
parquet_file = "categoria.parquet"

try:
    response = requests.get(parquet_url)
    response.raise_for_status()
    with open(parquet_file, "wb") as file:
        file.write(response.content)
    print(f"Arquivo Parquet baixado: {parquet_file}")
except requests.exceptions.RequestException as re:
    print(f"Erro no download do arquivo Parquet: {str(re)}")

# Leitura do arquivo Parquet de categorias
try:
    table = pq.read_table(parquet_file)
    df_categorias = table.to_pandas()
    print("\nDados de Categorias:")
    print(df_categorias.head())
except Exception as e:
    print(f"Erro ao ler o arquivo Parquet: {str(e)}")
    df_categorias = pd.DataFrame()

# Tratamento dos dados de vendas
# Converter a coluna 'data_venda' para o tipo datetime
df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])

# Tratamento dos dados de funcionários
# Verificar se há valores nulos
if df_funcionarios.isnull().values.any():
    df_funcionarios = df_funcionarios.dropna()  # Remover linhas com valores nulos

# Tratamento dos dados de categorias
# Verificar se há valores duplicados
if df_categorias.duplicated().any():
    df_categorias = df_categorias.drop_duplicates()  # Remover linhas duplicadas

# Realizar qualquer manipulação adicional nos dados, como limpeza, filtragem, agregação, etc.
# Por exemplo, podemos filtrar as vendas realizadas apenas em 2022
df_vendas = df_vendas[df_vendas['data_venda'].dt.year == 2022]

# Exemplo de agregação: Calcular a soma das vendas por categoria
df_vendas_agregado = df_vendas.groupby('id_categoria')['venda'].sum().reset_index()

# Exibir os dados tratados e manipulados
print("\nDados de Vendas (Tratados):")
print(df_vendas.head())

print("\nDados de Funcionários (Tratados):")
print(df_funcionarios.head())

print("\nDados de Categorias (Tratados):")
print(df_categorias.head())

print("\nDados de Vendas Agregados por Categoria:")
print(df_vendas_agregado)

# conexão com o banco
conn = psycopg2.connect(
    host=postgres_host,
    user=postgres_user,
    password=postgres_password,
    port=postgres_port,
    database=postgres_database
)

# Criar as tabelas necessárias

create_table_vendas = """
CREATE TABLE IF NOT EXISTS vendas (
    id_venda SERIAL PRIMARY KEY,
    id_funcionario INTEGER,
    id_categoria INTEGER,
    data_venda DATE,
    venda INTEGER
);
"""

create_table_funcionarios = """
CREATE TABLE IF NOT EXISTS funcionarios (
    id_funcionario SERIAL PRIMARY KEY,
    nome VARCHAR(100)
);
"""

create_table_categorias = """
CREATE TABLE IF NOT EXISTS categorias (
    id_categoria SERIAL PRIMARY KEY,
    nome_categoria VARCHAR(100)
);
"""

with conn.cursor() as cursor:
    cursor.execute(create_table_vendas)
    cursor.execute(create_table_funcionarios)
    cursor.execute(create_table_categorias)

# Inserir os dados tratados nas tabelas correspondentes

with conn.cursor() as cursor:
    for row in df_vendas.itertuples(index=False):
        cursor.execute("INSERT INTO vendas (id_funcionario, id_categoria, data_venda, venda) VALUES (%s, %s, %s, %s)",
                       (row.id_funcionario, row.id_categoria, row.data_venda, row.venda))
    conn.commit()

with conn.cursor() as cursor:
    for row in df_funcionarios.itertuples(index=False):
        cursor.execute("INSERT INTO funcionarios (nome) VALUES (%s)", (row.nome,))
    conn.commit()

with conn.cursor() as cursor:
    # Adicionar coluna "nome_categoria" à tabela "categorias" se ela não existir
    cursor.execute("ALTER TABLE categorias ADD COLUMN IF NOT EXISTS nome_categoria VARCHAR(255);")

    for row in df_categorias.itertuples(index=False):
        
        cursor.execute("INSERT INTO categorias (nome_categoria) VALUES (%s)", (row.nome_categoria,))
        # Verificar se as tabelas foram criadas

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = cursor.fetchall()

    if tables:
        print("Tabelas criadas:")
        for table in tables:
            print(table[0])
    else:
        print("Nenhuma tabela encontrada.")

    conn.commit()
#


conn.close()

