import pandas as pd
import os
from pathlib import Path

def preparar_fluxo():
    # 1. Lê os dias 29 e 30 dos dados particionados
    dfs = []
    for dia in ['29', '30']:
        caminho = Path('dados_particionados/2019/11') / dia
        for arquivo in caminho.glob('*.csv'):
            dfs.append(pd.read_csv(arquivo))
    
    # 2. Junta e ordena por tempo
    df = pd.concat(dfs, ignore_index=True)
    df = df.sort_values('event_time')
    
    # 3. Salva no fluxo.log
    df.to_csv('dados_novos/fluxo.log', index=False)
    print(f"Fluxo preparado com {len(df)} eventos!")

preparar_fluxo()