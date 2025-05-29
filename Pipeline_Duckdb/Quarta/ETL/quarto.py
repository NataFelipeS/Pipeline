import pandas as pd
import glob
import duckdb
import os

caminho_pasta = r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Quarta\\dados_vendas"

arquivos_csv = glob.glob(os.path.join(caminho_pasta, "*.csv"))

dataframes = []
print(f"Arquivos CSV encontrados: {arquivos_csv}")
print(f"Total: {len(arquivos_csv)}")


for arquivo in arquivos_csv:
    df = pd.read_csv(arquivo)
    print(f"Lendo {arquivo} com {len(df)} linhas")
    df['arquivo_origem'] = os.path.basename(arquivo)
    dataframes.append(df)

df_vendas_completo = pd.concat(dataframes, ignore_index=True)

print(df_vendas_completo.head())
print(f"Tamanho total do DataFrame: {len(df_vendas_completo)}")
print(df_vendas_completo.tail(5))

con = duckdb.connect()
con.register('dados_vendas', df_vendas_completo)
query = """ 
select
    produto,
    sum(quantidade) as quantidade_vendido,
    sum(preco_unitario * quantidade) as valor_vendido
from dados_vendas
group by
    produto
order by
    valor_vendido desc
"""

resultado = con.execute(query).fetchdf()
print(resultado)

resultado.to_excel(r"C:\\Users\\T-Gamer\\Documents\\GitHub\\Pipeline\\Pipeline_Duckdb\\Quarta\\resultado\\valor_quantidade_vendido.xlsx", index=False)