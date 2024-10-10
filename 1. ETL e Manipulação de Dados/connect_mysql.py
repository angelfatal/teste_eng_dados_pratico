import pymysql
from sqlalchemy import create_engine, text
import pandas as pd

def connect():
    try:
        # Configurações de conexão
        user = 'root'
        password = 'abc123'
        host = 'localhost'
        database = 'db_sales'
        port = '3306'
        
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
        connection = engine.connect()
        print("Conexão com MySQL estabelecida com sucesso.")
        return connection, engine
    except Exception as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None, None
    
def create_table(conn):
    try:
        # Criar tabela se não existir
        create_table_query = text("""
        CREATE TABLE IF NOT EXISTS db_sales.sales (
            transaction_id INT PRIMARY KEY,
            date DATE,
            product_id INT,
            seller_id INT,
            sale_value FLOAT,
            currency VARCHAR(255)
        );
        """)

        conn.execute(create_table_query)
        print("Tabela 'sales' criada com sucesso!")    
    except Exception as e:
        print(f"Ocorreu um erro ao criar a tabela: {e}")

def load_data_from_df(engine, df, table_name):
    try:
        # Inserindo os dados do DataFrame na tabela
        result = df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"{result} linhas inseridas na tabela '{table_name}' com sucesso.")
        return result
    except Exception as e:
        print(f"Erro ao inserir dados no MySQL: {e}")
        return 0

def save_db(df, table_name):
    # Conectando ao banco MySQL
    connection, engine = connect()

    # Se conexão bem sucedida, segue
    if connection and engine:
        
        # Criando tabela
        create_table(connection)
                
        # Inserir os dados no MySQL
        num_rows = load_data_from_df(engine, df, table_name)
        
        # Fechando a conexão
        connection.close()
        print("Conexão fechada.")

        return num_rows
