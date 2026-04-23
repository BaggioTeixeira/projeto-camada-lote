# Projeto Camada de Lote — E-commerce User Behavior

Trabalho prático da disciplina Arquitetura de Grandes Volumes de Dados — PUC-Campinas.

## Dataset

E-commerce Behavior Data from Multi Category Store (Kaggle)
Arquivo utilizado: 2019-Nov.csv (8,39 GB — novembro de 2019 completo)

## Estrutura do Projeto
projeto/
|-- dados_particionados/  # Dados particionados por ano/mês/dia/event_type
|-- vistas_lote/          # Análises pré-computadas em CSV
|-- gerenciamento/        # Log de processamento e quarentena
|-- scripts/
|   |-- particionamento.py   # Particiona os dados brutos em chunks
|   |-- gerar_vistas.py      # Gera as vistas de lote
|   |-- gerenciamento.py     # Verifica qualidade e registra log
|   |-- log_utils.py         # Utilitário de log compartilhado

## Como executar

```bash
python scripts/particionamento.py
python scripts/gerar_vistas.py
python scripts/gerenciamento.py
```

## Autor

Bruno Baggio Teixeira 
