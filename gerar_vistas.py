import pandas as pd
import os
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent))
from log_utils import registrar_log

def vista_eventos_por_tipo(pasta_entrada, pasta_saida):
    dfs = []
    for arquivo in Path(pasta_entrada).rglob('*.csv'):
        df_temp = pd.read_csv(arquivo, usecols=['event_type'])
        dfs.append(df_temp)
    df = pd.concat(dfs, ignore_index=True)
    resultado = df['event_type'].value_counts().reset_index()
    resultado.columns = ['event_type', 'total']
    resultado.to_csv(Path(pasta_saida) / 'eventos_por_tipo.csv', index=False)
    print("Vista eventos_por_tipo gerada!")

def vista_marcas(pasta_entrada, pasta_saida):
    dfs = []
    for arquivo in Path(pasta_entrada).rglob('*.csv'):
        if arquivo.name != 'view.csv':
            continue
        df_temp = pd.read_csv(arquivo, usecols=['brand'])
        dfs.append(df_temp)
    df = pd.concat(dfs, ignore_index=True)
    resultado = df['brand'].value_counts().reset_index()
    resultado.columns = ['brand', 'total']
    resultado.to_csv(Path(pasta_saida) / 'marcas.csv', index=False)
    print("Vista marcas gerada!")

def vista_taxa_de_conversao(pasta_entrada, pasta_saida):
    dfs = []
    for arquivo in Path(pasta_entrada).rglob('*.csv'):
        df_temp = pd.read_csv(arquivo, usecols=['event_type'])
        dfs.append(df_temp)
    df = pd.concat(dfs, ignore_index=True)
    contagem = df['event_type'].value_counts()
    taxa = (contagem['purchase'] / contagem['view']) * 100
    resultado = pd.DataFrame([{'taxa_conversao': round(taxa, 2)}])
    resultado.to_csv(Path(pasta_saida) / 'taxa_conversao.csv', index=False)
    print("Vista taxa_de_conversao gerada!")

def vista_eventos_por_hora(pasta_entrada, pasta_saida):
    resultados = []
    for arquivo in Path(pasta_entrada).rglob('*.csv'):
        df_temp = pd.read_csv(arquivo, usecols=['event_type', 'event_time'])
        df_temp['event_time'] = pd.to_datetime(df_temp['event_time'])
        df_temp['hour'] = df_temp['event_time'].dt.hour
        grupo = df_temp.groupby(['hour', 'event_type']).size().reset_index()
        resultados.append(grupo)
    df_final = pd.concat(resultados, ignore_index=True)
    df_final = df_final.groupby(['hour', 'event_type'])[0].sum().reset_index()
    df_final.columns = ['hour', 'event_type', 'total']
    df_final = df_final.sort_values(['hour', 'event_type'])
    df_final.to_csv(Path(pasta_saida) / 'eventos_por_hora.csv', index=False)
    print("Vista eventos_por_hora gerada!")

def main():
    print("Iniciando geração de vistas...")
    pasta_entrada = Path('dados_particionados')
    pasta_saida = Path('vistas_lote')
    print(f"Pasta entrada existe: {pasta_entrada.exists()}")
    print(f"Pasta saida existe: {pasta_saida.exists()}")
    vista_eventos_por_tipo(pasta_entrada, pasta_saida)
    vista_marcas(pasta_entrada, pasta_saida)
    vista_taxa_de_conversao(pasta_entrada, pasta_saida)
    vista_eventos_por_hora(pasta_entrada, pasta_saida)
    pasta_log = Path(__file__).parent.parent / 'gerenciamento'
    registrar_log(pasta_log, 'gerar_vistas.py', 0, 'sucesso')
    print("Log registrado!")

if __name__ == '__main__':
    main()