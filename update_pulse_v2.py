import json
import time
import random
import requests
from pytrends.request import TrendReq
import pandas as pd

# Configuração de destinos com coordenadas para o clima
DESTINATIONS = [
    {"id": "monte_verde_mg", "kw": "Monte Verde MG", "lat": -22.86, "lon": -46.03},
    {"id": "serra_negra_sp", "kw": "Serra Negra SP", "lat": -22.61, "lon": -46.70},
    {"id": "gramado_canela_rs", "kw": "Gramado Canela", "lat": -29.37, "lon": -50.87},
    {"id": "campos_do_jordao_sp", "kw": "Campos do Jordao", "lat": -22.73, "lon": -45.59},
    {"id": "pocos_de_caldas_mg", "kw": "Pocos de Caldas", "lat": -21.78, "lon": -46.56},
    {"id": "sao_lourenco_mg", "kw": "Sao Lourenco MG", "lat": -22.11, "lon": -45.05}
]

def get_weather(lat, lon):
    """Busca clima atual via Open-Meteo (Grátis e sem chave)"""
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true&timezone=America%2FSao_Paulo"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            current = data.get("current_weather", {})
            temp = current.get("temperature")
            code = current.get("weathercode")
            
            # Mapeamento simples de códigos de clima
            weather_map = {
                0: "Céu Limpo", 1: "Limpo", 2: "Parcial. Nublado", 3: "Nublado",
                45: "Nevoeiro", 48: "Nevoeiro", 51: "Garoa", 61: "Chuva Leve",
                63: "Chuva", 80: "Pancadas Chuva", 95: "Trovoada"
            }
            condition = weather_map.get(code, "Estável")
            return {"temp": temp, "condition": condition}
    except Exception as e:
        print(f"Erro ao buscar clima: {e}")
    return None

def get_data_with_retry(pytrends, kw, retries=3):
    """Tenta buscar dados com repetição em caso de falha"""
    for i in range(retries):
        try:
            pytrends.build_payload([kw], timeframe='today 3-m', geo='BR')
            df = pytrends.interest_over_time()
            if not df.empty:
                return df
            time.sleep(random.uniform(2, 5))
        except Exception as e:
            print(f"Tentativa {i+1} falhou para {kw}: {e}")
            time.sleep(random.uniform(5, 10))
    return pd.DataFrame()

def main():
    pytrends = TrendReq(hl='pt-BR', tz=180)
    results = {}
    
    print("Iniciando coleta de dados robusta (Trends + Clima)...")
    
    for dest in DESTINATIONS:
        print(f"Processando: {dest['id']}...")
        
        # 1. Coleta Google Trends
        df = get_data_with_retry(pytrends, dest['kw'])
        
        # 2. Coleta Clima (Independente)
        weather = get_weather(dest['lat'], dest['lon'])
        
        if not df.empty:
            series = df[dest['kw']]
            avg_full = series.mean()
            avg_recent = series.tail(7).mean()
            
            days_above = (series.tail(7) > avg_full).sum()
            persistence = float(days_above / 7)
            recent_change = float((avg_recent - avg_full) / avg_full) if avg_full > 0 else 0
            
            results[dest['id']] = {
                "recentChange": round(recent_change, 4),
                "persistence": round(persistence, 2),
                "weather": weather, # Adiciona o clima aqui
                "note": "Dados reais atualizados via Google Trends + Clima (v2.1)."
            }
            time.sleep(random.uniform(3, 6))
        else:
            print(f"Aviso: Não foi possível obter dados de busca para {dest['id']}")

    if results:
        with open('pulse-data.json', 'w') as f:
            json.dump(results, f, indent=2)
        print("Sucesso: pulse-data.json atualizado com dados de busca e clima.")
    else:
        print("ERRO CRÍTICO: Nenhum dado coletado. O arquivo original NÃO foi alterado.")

if __name__ == "__main__":
    main()
