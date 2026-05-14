import threading
import time
import pandas as pd
from pathlib import Path

finaliza_laco = False

def inicializar_contadores():
    return {
        'tipos': {},
        'horas': {},
        'marcas': {},
        'total': 0
    }

def processar_linha(linha, contadores):
    campos = linha.split(',')
    event_time = campos[0]
    event_type = campos[1]
    brand = campos[5]
    hora = event_time[11:13]

    contadores['tipos'][event_type] = contadores['tipos'].get(event_type, 0) + 1
    contadores['horas'][hora] = contadores['horas'].get(hora, 0) + 1
    contadores['marcas'][brand] = contadores['marcas'].get(brand, 0) + 1
    contadores['total'] += 1
    return contadores

def salvar_vistas(contadores, pasta_saida):
    pd.DataFrame(
        contadores['tipos'].items(),
        columns=['event_type', 'total']
    ).to_csv(f'{pasta_saida}/eventos_por_tipo.csv', index=False)

    pd.DataFrame(
        contadores['horas'].items(),
        columns=['hour', 'total']
    ).to_csv(f'{pasta_saida}/eventos_por_hora.csv', index=False)

    pd.DataFrame(
        contadores['marcas'].items(),
        columns=['brand', 'total']
    ).to_csv(f'{pasta_saida}/marcas.csv', index=False)

    print(f"Vistas atualizadas! Total processado: {contadores['total']}")

def stream_dados(arquivo):
    with open(arquivo, 'r') as arq:
        next(arq)
        while not finaliza_laco:
            linha = arq.readline().strip()
            if not linha:
                time.sleep(0.001)
                continue
            yield linha

def monitora_linhas(arquivo, pasta_saida):
    print('Inicializando monitoramento...')
    contadores = inicializar_contadores()

    for nova_linha in stream_dados(arquivo):
        contadores = processar_linha(nova_linha, contadores)
        if contadores['total'] % 100_000 == 0:
            salvar_vistas(contadores, pasta_saida)

    salvar_vistas(contadores, pasta_saida)
    print('Monitoramento finalizado!')

def main():
    global finaliza_laco
    finaliza_laco = False

    arquivo = 'dados_novos/fluxo.log'
    pasta_saida = 'vistas_tempo_real'

    t = threading.Thread(target=monitora_linhas, args=(arquivo, pasta_saida))
    t.start()
    t.join()

if __name__ == '__main__':
    main()