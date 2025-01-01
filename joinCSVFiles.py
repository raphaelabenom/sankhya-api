import pandas as pd
import glob

arquivos_csv = glob.glob("./*.csv")

lista_df = []

for arquivo in arquivos_csv:
    df = pd.read_csv(arquivo, delimiter=",", header=0)
    lista_df.append(df)

df_final = pd.concat(lista_df, ignore_index=True)

df_final.to_csv("./arquivo.csv", index=False, encoding="latin1", decimal=",")
