import pandas as pd
import os
import glob 

# Uma função de extract que lê e consolida os jsons

def extrair_dados_e_consolidar(path: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(path, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

# Uma função que transforma

def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

# uma função que da load em csv ou parquet

def carregar_dados(df: pd.DataFrame, formato_saida: list):
    if "csv" in formato_saida:
        df.to_csv("dados.csv", index=False)
    if "parquet" in formato_saida:
        df.to_parquet("dados.parquet", index=False)



def pipeline_calcular_kpi_de_vendas_consolidado(pasta: str, formato_de_saida: list):
    data_frame = extrair_dados_e_consolidar(pasta)
    data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
    carregar_dados(data_frame_calculado, formato_de_saida)
    


if __name__ == "__main__":
    pasta_argumento: str = 'data'
    data_frame = extrair_dados_e_consolidar(path=pasta_argumento)
    data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
    formato_de_saida: list = ["csv","parquet"]
    carregar_dados(data_frame_calculado, formato_de_saida)

