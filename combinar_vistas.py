import pandas as pd
from pathlib import Path

def combinar_eventos_por_tipo(pasta_lote, pasta_tempo_real):
    lote = pd.read_csv(f'{pasta_lote}/eventos_por_tipo.csv')
    tempo_real = pd.read_csv(f'{pasta_tempo_real}/eventos_por_tipo.csv')
    
    combinado = pd.merge(lote, tempo_real, on='event_type', suffixes=('_lote', '_tempo_real'))
    combinado['total'] = combinado['total_lote'] + combinado['total_tempo_real']
    combinado = combinado[['event_type', 'total']]
    
    print("Eventos por tipo — novembro completo:")
    print(combinado)
    return combinado

def combinar_eventos_por_hora(pasta_lote, pasta_tempo_real):
    lote = pd.read_csv(f'{pasta_lote}/eventos_por_hora.csv')
    tempo_real = pd.read_csv(f'{pasta_tempo_real}/eventos_por_hora.csv')
    
    # Agrupa o lote por hora somando todos os event_types
    lote_agrupado = lote.groupby('hour')['total'].sum().reset_index()
    
    # Converte hora do tempo real para inteiro para bater com o lote
    tempo_real['hour'] = tempo_real['hour'].astype(int)
    
    combinado = pd.merge(lote_agrupado, tempo_real, on='hour', suffixes=('_lote', '_tempo_real'))
    combinado['total'] = combinado['total_lote'] + combinado['total_tempo_real']
    combinado = combinado[['hour', 'total']].sort_values('hour')
    
    print("\nEventos por hora — novembro completo:")
    print(combinado)
    return combinado

def combinar_marcas(pasta_lote, pasta_tempo_real):
    lote = pd.read_csv(f'{pasta_lote}/marcas.csv')
    tempo_real = pd.read_csv(f'{pasta_tempo_real}/marcas.csv')
    
    combinado = pd.merge(lote, tempo_real, on='brand', suffixes=('_lote', '_tempo_real'))
    combinado['total'] = combinado['total_lote'] + combinado['total_tempo_real']
    combinado = combinado[['brand', 'total']]
    combinado = combinado.sort_values('total', ascending=False)
    
    print("\nTop marcas — novembro completo:")
    print(combinado.head(10))
    return combinado

def main():
    pasta_lote = 'vistas_lote'
    pasta_tempo_real = 'vistas_tempo_real'
    
    combinar_eventos_por_tipo(pasta_lote, pasta_tempo_real)
    combinar_eventos_por_hora(pasta_lote, pasta_tempo_real)
    combinar_marcas(pasta_lote, pasta_tempo_real)

if __name__ == '__main__':
    main()