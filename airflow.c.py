from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import *
import requests
import pandas as pd
from sqlalchemy import create_engine

engine_solution = create_engine('postgresql://beatriz:|?7LXmg+FWL&,2(,'
                                '2(@bix-solution.c3ksuxpujoxa.sa-east-1.rds.amazonaws.com/postgres')
engine = create_engine('postgresql://junior:|?7LXmg+FWL&,2(@34.173.103.16/postgres')
df = pd.read_sql(""" SELECT * FROM public.venda """, con=engine)


def funcionarios():
    engine_solution.execute(""" DELETE FROM auxiliar.funcionarios """)
    for i in sorted(df['id_funcionario'].unique()):
        try:
            resp = requests.get("https://us-central1-bix-tecnologia-prd.cloudfunctions.net/"
                                "/api_challenge_junior?id={}".
                                format(i))
            engine_solution.execute("""INSERT INTO auxiliar.funcionarios (id, nome_funcionario) VALUES ('%s', 
            '%s') """ % (i, resp.text))

        except IndexError as e:
            engine_solution.execute("""INSERT INTO auxiliar.funcionarios (id, nome_funcionario) VALUES ('%s', 
                        '%s') """ % (i, i))


def categorias():
    df_cat = pd.read_parquet('https://storage.googleapis.com/challenge_junior/categoria.parquet', engine='pyarrow')
    df_cat.to_sql("categorias", schema="auxiliar", index=False, if_exists='replace', con=engine_solution)


def transform_load():
    funcionarios = pd.read_sql(""" SELECT * FROM auxiliar.funcionarios """, con=engine_solution)
    df_categoria = pd.read_sql(""" SELECT * FROM auxiliar.categorias """, con=engine_solution)

    for i, v in df.iterrows():
        df.loc[i, 'id_funcionario'] = funcionarios['nome_funcionario'][funcionarios['id'] == v['id_funcionario']][
            funcionarios['nome_funcionario'][
                funcionarios['id'] == v['id_funcionario']].index[0]]

        df.loc[i, 'id_categoria'] = df_categoria['nome_categoria'][df_categoria['id'] == v['id_categoria']][
            df_categoria['nome_categoria'][
                df_categoria['id'] == v['id_categoria']].index[0]]

    df.rename(columns={'id_funcionario': 'funcionario', 'id_categoria': 'categoria'}, inplace=True)
    df.to_sql("vendas_solution", con=engine_solution, if_exists='replace', index=False)


# Criando um DataWarehouse simplificado para o dashboard

def line_chart():
    df_line_chart = pd.read_sql(""" SELECT data_venda, venda FROM public.vendas_solution ORDER BY data_venda""",
                                con=engine_solution)
    # Transformando a coluna em Ano
    for k, v in df_line_chart['data_venda'].items():
        df_line_chart.loc[k, 'data_venda'] = v.year

    # Puxando os anos da tabela e criando dicionário base para o dataframe
    anos = sorted(df_line_chart['data_venda'].unique())
    total = []
    for v in anos:
        total.append(df_line_chart['venda'][df_line_chart['data_venda'] == v].sum())
    d = {'ano': anos, 'total': total}

    df_line_chart = pd.DataFrame(data=d)
    df_line_chart.to_sql("line_chart", schema="dashboard", if_exists='replace', index=False, con=engine_solution)


def pie_chart():
    df_pie_chart = pd.read_sql(""" SELECT categoria, venda FROM public.vendas_solution ORDER BY data_venda""",
                               con=engine_solution)

    total_categoria = []
    for v in sorted(df_pie_chart['categoria'].unique()):
        print('-=-' * 50)
        print(v)
        print(df_pie_chart['venda'][df_pie_chart['categoria'] == v].sum())
        total_categoria.append(df_pie_chart['venda'][df_pie_chart['categoria'] == v].sum())
        print('-=-' * 50)

    d_cat = {'categoria': sorted(df_pie_chart['categoria'].unique()), 'total': total_categoria}

    df_total_categoria = pd.DataFrame(data=d_cat)
    df_total_categoria.to_sql("pie_chart", schema="dashboard", if_exists='replace', index=False, con=engine_solution)


def table():
    df_table = pd.read_sql(""" SELECT funcionario, venda FROM public.vendas_solution ORDER BY data_venda""",
                           con=engine_solution)

    print(df_table['funcionario'].unique())

    total_categoria = []
    for v in sorted(df_table['funcionario'].unique()):
        print('-=-' * 50)
        print(v)
        print(df_table['venda'][df_table['funcionario'] == v].sum())
        total_categoria.append(df_table['venda'][df_table['funcionario'] == v].sum())
        print('-=-' * 50)

    d_cat = {'funcionario': sorted(df_table['funcionario'].unique()), 'total': total_categoria}

    df_total_funcionario = pd.DataFrame(data=d_cat)
    df_total_funcionario.to_sql("resultado_individual_funcionarios", schema="dashboard", if_exists='replace',
                                index=False, con=engine_solution)


with DAG("bix_challenge_additional",
         start_date=datetime(2022, 10, 8),
         schedule='*/5 * * * *',
         catchup=False) as dag:
    refresh_funcionarios = PythonOperator(
        task_id="funcionarios",
        python_callable=funcionarios
    )

    refresh_categorias = PythonOperator(
        task_id="categorias",
        python_callable=categorias
    )

    transf_l = PythonOperator(
        task_id="transform_load",
        python_callable=transform_load
    )
    atualiza_line_chart = PythonOperator(
        task_id="line_chart",
        python_callable=line_chart
    )
    atualiza_pie_chart = PythonOperator(
        task_id="pie_chart",
        python_callable=pie_chart
    )
    atualiza_table = PythonOperator(
        task_id="tabela_funcionarios",
        python_callable=table
    )

[refresh_funcionarios, refresh_categorias] >> transf_l >> [atualiza_line_chart, atualiza_pie_chart, atualiza_table]
