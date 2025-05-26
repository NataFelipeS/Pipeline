import pandas as pd
import os
import duckdb
import datetime

# criar função de padronizar colunas
def padronizar_colunas(df):
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

# somente dados_funcionarios
dados_funcionarios = pd.read_json(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\dados_funcionarios.json")
dados_funcionarios['data_admissao'] = pd.to_datetime(dados_funcionarios['data_admissao'])
dados_funcionarios = padronizar_colunas(dados_funcionarios)
dados_funcionarios = dados_funcionarios.dropna()

con = duckdb.connect()

query1 = """
select * 
from dados_funcionarios
where salario > 10000
"""
con.register("dados_funcionarios", dados_funcionarios)
resultado_funcionarios = con.execute(query1).fetchdf()
print(resultado_funcionarios)

resultado_funcionarios.to_excel(r"C:\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Resultados\\resultado_funcionarios.xlsx", index=False)
# -----------------------------------------------------------------------------------------------

# somente dados_clientes
dados_clientes = pd.read_excel(r"C:\\Users\\T-Gamer\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\dados_clientes.xlsx")
dados_clientes = padronizar_colunas(dados_clientes)
dados_clientes['data_cadastro'] = pd.to_datetime(dados_clientes['data_cadastro'])
dados_clientes = dados_clientes.dropna()

query2 = """
select *
from dados_clientes
where data_cadastro between '2023-07-01' and current_date
"""
con.register("dados_clientes", dados_clientes)
resultado_clientes = con.execute(query2).fetchdf()
print(resultado_clientes)

resultado_clientes.to_csv(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Resultados\\resultado_clientes.csv", index=False)
# -----------------------------------------------------------------------------------------------

# somente dados_vendas
dados_vendas = pd.read_csv(r"C:\\Users\\T-Gamer\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\dados_vendas.csv")
dados_vendas['data_venda'] = pd.to_datetime(dados_vendas['data_venda'])
dados_vendas = dados_vendas.dropna()

query = """
select 
    id_venda,
    (preco_unitario * quantidade) as valor
from dados_vendas;
"""
con.register("dados_vendas", dados_vendas)
resultado_vendas = con.execute(query).fetchdf()
print(resultado_vendas)

resultado_vendas.to_excel(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Resultados\\resultado_vendas.xlsx", index=False)
# -----------------------------------------------------------------------------------------------

# somente dados_sujos
dados_sujos = pd.read_csv(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\dados_sujos.csv")
dados_sujos = dados_sujos.rename(columns={
    'Nome ': 'nome',
    ' Idade': 'idade',
    'Salário': 'salario',
    ' Data de Admissão ': 'data_admissao'
})
dados_sujos['data_admissao'] = pd.to_datetime(dados_sujos['data_admissao'])
dados_sujos['nome'] = dados_sujos['nome'].fillna('Teste') 
dados_sujos['idade'] = dados_sujos['idade'].fillna(0)
dados_sujos['salario'] = dados_sujos['salario'].fillna(0)
dados_sujos['data_admissao'] = dados_sujos['data_admissao'].fillna(pd.Timestamp("2000-01-01"))

dados_sujos.to_excel(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Resultados\\resultado_limpo.xlsx", index=False)
# -----------------------------------------------------------------------------------------------





