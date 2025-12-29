import json
import time
import random
from pytrends.request import TrendReq
import pandas as pd

# Configuração de destinos
DESTINATIONS = [
    {"id": "monte_verde_mg", "kw": "Monte Verde MG"},
    {"id": "serra_negra_sp", "kw": "Serra Negra SP"},
    {"id": "gramado_canela_rs", "kw": "Gramado Canela"},
    {"id": "campos_do_jordao_sp", "kw": "Campos do Jordao"},
    {"id": "pocos_de_caldas_mg", "kw": "Pocos de Caldas"},
    {"id": "sao_lourenco_mg", "kw": "Sao Lourenco MG"}
]

def get_data_with_retry(pytrends, kw, retries=3):
    """Tenta buscar dados com repetição em caso de falha"""
    for i in range(retries):
        try:
            pytrends.build_payload([kw], timeframe='today 3-m', geo='BR')
            df = pytrends.interest_over_time()
            if not df.empty:
                return df
            time.sleep(random.uniform(2, 5)) # Pausa entre tentativas
        except Exception as e:
            print(f"Tentativa {i+1} falhou para {kw}: {e}")
            time.sleep(random.uniform(5, 10))
    return pd.DataFrame()

def main():
    pytrends = TrendReq(hl='pt-BR', tz=180)
    results = {}
    
    print("Iniciando coleta de dados robusta...")
    
    for dest in DESTINATIONS:
        print(f"Coletando: {dest['id']}...")
        df = get_data_with_retry(pytrends, dest['kw'])
        
        if not df.empty:
            series = df[dest['kw']]
            avg_full = series.mean()
            avg_recent = series.tail(7).mean()
            
            # Cálculo de persistência (dias acima da média nos últimos 7 dias)
            days_above = (series.tail(7) > avg_full).sum()
            persistence = float(days_above / 7)
            
            recent_change = float((avg_recent - avg_full) / avg_full) if avg_full > 0 else 0
            
            results[dest['id']] = {
                "recentChange": round(recent_change, 4),
                "persistence": round(persistence, 2),
                "note": "Dados reais atualizados via Google Trends (v2)."
            }
            time.sleep(random.uniform(3, 6)) # Pausa ética entre cidades
        else:
            print(f"Aviso: Não foi possível obter dados para {dest['id']}")

    # SEGURANÇA: Só salva se tivermos dados de pelo menos um destino
    if results:
        with open('pulse-data.json', 'w') as f:
            json.dump(results, f, indent=2)
        print("Sucesso: pulse-data.json atualizado com segurança.")
    else:
        print("ERRO CRÍTICO: Nenhum dado coletado. O arquivo original NÃO foi alterado.")

if __name__ == "__main__":
    main()
