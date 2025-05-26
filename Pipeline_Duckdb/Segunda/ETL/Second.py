import pandas as pd
import os
import datetime
import duckdb


dados_produtos = pd.read_csv(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Segunda\\dados_produtos.csv")
dados_produtos['data_cadastro'] = pd.to_datetime(dados_produtos['data_cadastro'])
dados_produtos['preco_unitario'] = dados_produtos['preco_unitario'].fillna('1.0')
dados_produtos['data_cadastro'] = dados_produtos['data_cadastro'].fillna(pd.Timestamp('2024-01-20'))
dados_produtos['estoque'] = dados_produtos['estoque'].fillna(10.0)


con = duckdb.connect()

query = """
select 
    categoria,
    count(categoria),
    sum(estoque) as estoque
from dados_produtos
group by
    categoria
order by estoque desc;
"""

con.register("dados_produtos", dados_produtos)

resultado_produtos = con.execute(query).fetchdf()
print(resultado_produtos)

resultado_produtos.to_excel(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Segunda\\Resultado\\resultado_produtos.xlsx")

