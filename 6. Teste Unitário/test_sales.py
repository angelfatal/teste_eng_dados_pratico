import pandas as pd
import pytest
from main import check_null, check_dups


# Teste para os casos padrões (happy path) => sem valores nulos e sem duplicidade
def test_check_null_happy_path():
    df = pd.DataFrame({
        'transaction_id': [1001, 1002, 1003],
        'sale_value': [150, 232, 340]
    })
    df_clean = check_null(df)
    # Comparando o df antes e depois, se positivo não deve haver diferenças, nenhuma linha deve ter sido removida
    assert df_clean.equals(df)  

def test_check_dups_happy_path():
    df = pd.DataFrame({
        'transaction_id': [1001, 1002, 1003],
        'sale_value': [150, 232, 340]
    })
    df_clean = check_dups(df)
    # Comparando o df antes e depois, se positivo não deve haver diferenças, nenhuma linha deve ter sido removida
    assert df_clean.equals(df)  


# Teste para os casos de borda => dataframe com valores nulos ou df vazio
def test_check_null_with_nulls():
    df = pd.DataFrame({
        'transaction_id': [1001, 1002, 1003],
        'sale_value': [150, None, 340]
    })
    df_clean = check_null(df)
    # Verificação: uma linha deve ser removida, restando duas linhas
    assert len(df_clean) == 2  
    # Verificação: se excluiu a linha certa
    assert df_clean['transaction_id'].tolist() == [1001, 1003]  

def test_check_null_empty_dataframe():
    df = pd.DataFrame(columns=['transaction_id', 'sale_value'])
    df_clean = check_null(df)
    # dataframe deve continuar vazio (a função deveria apenas remover valores nulos)
    assert df_clean.empty  

# Teste para os casos de borda => dataframe com valores duplicados ou df vazio
def test_check_dups_with_duplicates():
    df = pd.DataFrame({
        'transaction_id': [1001, 1002, 1002, 1003],
        'sale_value': [150, 232, 232, 340]
    })
    df_clean = check_dups(df)
    # Verificação: uma linha deve ser removida, restando três linhas
    assert len(df_clean) == 3  
    # Verificação: se excluiu a linha certa
    assert df_clean['transaction_id'].tolist() == [1001, 1002, 1003] 

def test_check_dups_empty_dataframe():
    df = pd.DataFrame(columns=['transaction_id', 'sale_value'])
    df_clean = check_dups(df)
    # dataframe deve continuar vazio (a função deveria apenas remover valores duplicados)
    assert df_clean.empty  


# Teste para os casos de erro => campos inexistentes
def test_check_null_missing_column():
    df = pd.DataFrame({
        'sale_value': [150, 232, 340]
    })
    # Retirado coluna de transaction_id, portanto deve dar erro
    with pytest.raises(KeyError):
        check_null(df)

def test_check_dups_missing_column():
    df = pd.DataFrame({
        'sale_value': [150, 232, 340]
    })
    # Retirado coluna de transaction_id (utilizada na função), portanto deve dar erro
    with pytest.raises(KeyError):
        check_dups(df)
