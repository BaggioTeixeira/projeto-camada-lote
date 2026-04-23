import pandas as pd
from pathlib import Path
from datetime import datetime

def registrar_log(pasta_saida, script, total_registros, status):
    log = {
        'timestamp': datetime.now(),
        'script': script,
        'total_registros': total_registros,
        'status': status
    }
    
    arquivo_log = Path(pasta_saida) / 'log_processamento.csv'
    df_log = pd.DataFrame([log])
    
    if arquivo_log.exists():
        df_log.to_csv(arquivo_log, mode='a', header=False, index=False)
    else:
        df_log.to_csv(arquivo_log, index=False)
    
    print(f"Log registrado: {script} — {status}")