import pandas as pd
import duckdb
import os

dados_clientes = pd.read_csv(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Terceira\\ETL\\clientes.csv")
dados_compras = pd.read_csv(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Terceira\\ETL\\compras.csv")

con = duckdb.connect()

con.register("dados_clientes", dados_clientes)
con.register("dados_compras", dados_compras)

query = """
select
    cl.id_cliente,
    cl.cidade,
    co.id_compra,
    round(co.quantidade * co.preco_unitario, 2) as valor
from dados_clientes cl
inner join dados_compras co on cl.id_cliente = co.id_cliente;
"""


resultado_join = con.execute(query).fetchdf()
print(resultado_join)

resultado_join.to_csv(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Terceira\\Resultado\\resultado_join.csv", index=False)