import pandas as pd
import os
from pathlib import Path
import kagglehub


def carregar_dados():
    path = kagglehub.dataset_download("mkechinov/ecommerce-behavior-data-from-multi-category-store")
    arquivo = Path(path) / "2019-Nov.csv"
    return arquivo

def limpar_chunk(chunk):
    chunk['event_time'] = pd.to_datetime(chunk['event_time'], utc=True).dt.tz_localize(None)
    chunk = chunk.drop_duplicates()
    chunk = chunk.reset_index(drop=True)
    return chunk


def criar_pastas(base,ano,mes,dia):
    caminho = Path(base) / str(ano) / str(mes) / str(dia)
    caminho.mkdir(exist_ok=True, parents=True)
    return caminho


def particionar_dados(arquivo_entrada, pasta_saida):
    print(f"Lendo arquivo: {arquivo_entrada}")
    total_chunks = 0
    for chunk in pd.read_csv(arquivo_entrada, chunksize=100_000):
        total_chunks += 1
        print(f"Processando chunk {total_chunks}...")
        chunk = limpar_chunk(chunk)
        chunk['ano'] = chunk['event_time'].dt.year
        chunk['mes'] = chunk['event_time'].dt.month
        chunk['dia'] = chunk['event_time'].dt.day
        for (ano, mes, dia, event_type), grupo in chunk.groupby(['ano', 'mes', 'dia', 'event_type']):
            caminho = criar_pastas(pasta_saida, ano, mes, dia)
            arquivo = caminho / f"{event_type}.csv"
            # Adiciona ao arquivo se ja existir, cria novo se nao existir
            header = not arquivo.exists()
            grupo.to_csv(arquivo, mode='a', header=header, index=False)
    print(f"Particionamento concluido! Total de chunks: {total_chunks}")


def main():
    print("Iniciando particionamento...")
    arquivo = carregar_dados()
    print(f"Arquivo encontrado: {arquivo}")
    pasta_saida = Path('dados_particionados')
    particionar_dados(arquivo, pasta_saida)


if __name__=='__main__':
    main()