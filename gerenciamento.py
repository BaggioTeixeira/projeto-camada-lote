import pandas as pd
import os
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent))
from log_utils import registrar_log

def ler_dados_particionados(pasta, ano=None, mes=None, dia=None, event_type=None):
    dfs = []
    for root, dirs, files in os.walk(pasta):
        for file in files:
            if file.endswith('.csv'):
                if event_type and file != f"{event_type}.csv":
                    continue
                caminho = Path(root) / file
                df = pd.read_csv(caminho)
                dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def verificar_quarentena(df, pasta_saida, nome_arquivo):
    eventos_validos = ['view', 'cart', 'purchase']
    quarentenas = []

    preco_invalido = df[df['price'] <= 0].copy()
    preco_invalido['motivo'] = 'preco_invalido'
    quarentenas.append(preco_invalido)

    usuario_nulo = df[df['user_id'].isnull()].copy()
    usuario_nulo['motivo'] = 'usuario_nulo'
    quarentenas.append(usuario_nulo)

    evento_invalido = df[~df['event_type'].isin(eventos_validos)].copy()
    evento_invalido['motivo'] = 'evento_desconhecido'
    quarentenas.append(evento_invalido)

    quarentena = pd.concat(quarentenas).drop_duplicates()

    # Salva linhas brutas
    if len(quarentena) > 0:
        arquivo_q = Path(pasta_saida) / 'quarentena_linhas_brutas.csv'
        quarentena.to_csv(arquivo_q, index=False)
        print(f"Quarentena: {len(quarentena)} linhas encontradas!")
        print(quarentena['motivo'].value_counts())

    # Salva resumo por arquivo
    resumo = pd.DataFrame([{
        'arquivo': nome_arquivo,
        'total_quarentena': len(quarentena)
    }])
    
    arquivo_resumo = Path(pasta_saida) / 'quarentena_linhas_por_arquivo.csv'
    header = not arquivo_resumo.exists()
    resumo.to_csv(arquivo_resumo, mode='a', header=header, index=False)

    return df[~df.index.isin(quarentena.index)]
def main():
    pasta_entrada = Path('dados_particionados')
    pasta_saida = Path('gerenciamento')
    
    print("Iniciando gerenciamento...")
    
    # Ordena os arquivos numericamente pelo dia
    arquivos = sorted(pasta_entrada.rglob('*.csv'), 
                     key=lambda x: int(x.parent.name))
    
    total_validos = 0
    for arquivo in arquivos:
        df = pd.read_csv(arquivo)
        df = verificar_quarentena(df, pasta_saida, str(arquivo))
        total_validos += len(df)

    pasta_log = Path(__file__).parent.parent / 'gerenciamento'
    registrar_log(pasta_log, 'gerenciamento.py', total_validos, 'sucesso')
    print("Gerenciamento concluido!")

if __name__ == '__main__':
    main()