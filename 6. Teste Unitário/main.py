import pandas as pd
from connect_mysql import save_db


########Validações de Qualidade de Dados
def check_unique_id(df):
    if df['transaction_id'].is_unique:
        print("Todos os IDs são únicos.")
    else:
        dups = df[df.duplicated('transaction_id', keep=False)]
        ids = dups['transaction_id'].unique()
        print(f"Há transaction_id duplicados: {ids}")


def check_sales_values(df):
    negativos = df[df['sale_value'] < 0]
    if negativos.empty:
        print("Não há valores de venda negativos.")
    else:        
        ids = negativos['transaction_id'].unique()
        print(f"Há valores de vendas negativos encontrados para os transactions_ids: {ids}")

def check_timestamps(df):
    try:
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='raise')
        print("Todas as datas são válidas.")
    except Exception as e:
        datas_invalidas = df[pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce').isna()]
        ids = datas_invalidas['transaction_id'].unique()
        print(f"Datas inválidas encontrados para os transaction_id: {ids}")

def data_quality(df):
    check_unique_id(df)
    check_sales_values(df)
    check_timestamps(df)
    
########################################

def check_null(df):
    try:
        # Verificando dados nulos
        df_nulls = df[df.isnull().any(axis=1)] 

        if not df_nulls.empty: #se tiver algum dado nulo
            print(f"Foram encontradas {len(df_nulls)} registros com algum valor nulo. Os ids da coluna `transaction_id` abaixo serão removidos:")
            print(df_nulls['transaction_id'])  

            # Apagando linhas com valores nulos
            df_clean = df.dropna(how='any')
            print(f"Total de linhas removidas: {len(df) - len(df_clean)}")
            df = df_clean
        else:
            print("Nenhum valor nulo encontrado.")
        
        return df

    except Exception as e:
        print(f'Erro: {e}')


def check_dups(df):
    try:
        # Verificando dados duplicados
        duplicates = df.duplicated(subset='transaction_id').sum()
        if duplicates > 0:
            print("Removendo registros duplicados")
            df_clean = df.drop_duplicates(subset='transaction_id', keep='first')
            print(f"Número de linhas removidas: {duplicates}")
            df = df_clean
        else:
            print("Não há registros duplicados.")

        return df

    except Exception as e:
        print(f'Erro: {e}')

# Limpeza dos dados
def clean_data(df):
    df_clean_null = check_null(df)
    df_clean_dup  = check_dups(df_clean_null)
    return df_clean_dup

# Ajustando os valores FICT -> USD
def transform_value(df):
    try:
        print("Convertendo valores de FICT pra USD")
        df['sale_value'] = pd.to_numeric(df['sale_value'], errors='raise')
        conversion_rate = 0.75
        df["sale_value"] = df["sale_value"] * conversion_rate
        df["currency"] = "USD"
        return df
    except Exception as e:
        print(f'Erro: {e}')


def main():
    try:
        # Lendo o arquivo CSV
        csv_file = 'sales_data.csv'
        sales_data = pd.read_csv(csv_file)

        # Validações de Data Quality
        data_quality(sales_data)

        df_sales_cleaned = clean_data(sales_data)
        df_sales_final = transform_value(df_sales_cleaned)

        num_row_df = len(df_sales_final)
        num_rows_saved = save_db(df_sales_final, 'sales')
        
        if(num_row_df == num_rows_saved):
            print("Todos os registros foram inseridos com sucesso!")
        else:
            print(f"{num_rows_saved}/{num_row_df} foram inseridos.")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{csv_file}' não foi encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    main()