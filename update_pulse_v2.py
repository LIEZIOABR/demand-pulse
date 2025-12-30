import json
import time
import random
import requests
from pytrends.request import TrendReq
import pandas as pd

# Configuração de destinos expandida (8 destinos) com coordenadas para clima
DESTINATIONS = [
    {"id": "monte_verde_mg", "kw": "Monte Verde MG", "lat": -22.86, "lon": -46.03},
    {"id": "campos_do_jordao_sp", "kw": "Campos do Jordao", "lat": -22.73, "lon": -45.59},
    {"id": "sao_bento_sapucai_sp", "kw": "Sao Bento do Sapucai", "lat": -22.68, "lon": -45.73},
    {"id": "passa_quatro_mg", "kw": "Passa Quatro MG", "lat": -22.38, "lon": -44.96},
    {"id": "serra_negra_sp", "kw": "Serra Negra SP", "lat": -22.61, "lon": -46.70},
    {"id": "gramado_canela_rs", "kw": "Gramado Canela", "lat": -29.37, "lon": -50.87},
    {"id": "pocos_de_caldas_mg", "kw": "Pocos de Caldas", "lat": -21.78, "lon": -46.56},
    {"id": "sao_lourenco_mg", "kw": "Sao Lourenco MG", "lat": -22.11, "lon": -45.05}
]

def get_weather_forecast(lat, lon):
    """Busca previsão de 7 dias via Open-Meteo (Grátis e sem chave)"""
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=weathercode,temperature_2m_max,temperature_2m_min&current_weather=true&timezone=America%2FSao_Paulo"
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            current = data.get("current_weather", {})
            daily = data.get("daily", {})
            
            # Mapeamento simples de códigos de clima
            weather_map = {
                0: "Limpo", 1: "Limpo", 2: "Parcial", 3: "Nublado",
                45: "Nevoeiro", 48: "Nevoeiro", 51: "Garoa", 61: "Chuva",
                63: "Chuva", 80: "Pancadas", 95: "Trovoada"
            }
            
            forecast = []
            for i in range(min(7, len(daily.get("time", [])))):
                forecast.append({
                    "date": daily["time"][i],
                    "max": daily["temperature_2m_max"][i],
                    "min": daily["temperature_2m_min"][i],
                    "cond": weather_map.get(daily["weathercode"][i], "Estável")
                })
            
            return {
                "current": {
                    "temp": current.get("temperature"),
                    "cond": weather_map.get(current.get("weathercode"), "Estável")
                },
                "daily": forecast
            }
    except Exception as e:
        print(f"Erro ao buscar clima: {e}")
    return None

def generate_insight(rc, ps, weather):
    """Lógica de Inteligência Consultiva ABR"""
    if rc > 0.15 and ps > 0.7:
        if weather and any(d["cond"] in ["Limpo", "Parcial"] for d in weather["daily"][:3]):
            return "Oportunidade: Demanda alta + Clima bom. Sugestão: Revisar tarifas e restrições."
        return "Alerta: Busca aquecida. Ótimo momento para campanhas de conversão."
    if rc < -0.10:
        return "Atenção: Queda na busca. Sugestão: Reforçar marketing e pacotes de valor agregado."
    return "Estabilidade: Manter estratégia de vendas padrão e focar em fidelização."

def get_data_with_retry(pytrends, kw, retries=3):
    """Tenta buscar dados com repetição em caso de falha (Sua lógica original)"""
    for i in range(retries):
        try:
            pytrends.build_payload([kw], timeframe='today 3-m', geo='BR')
            df = pytrends.interest_over_time()
            if not df.empty:
                return df
            time.sleep(random.uniform(3, 7))
        except Exception as e:
            print(f"Tentativa {i+1} falhou para {kw}: {e}")
            time.sleep(random.uniform(10, 20))
    return pd.DataFrame()

def main():
    pytrends = TrendReq(hl='pt-BR', tz=180)
    results = {}
    
    print(f"Iniciando coleta de dados para {len(DESTINATIONS)} destinos...")
    
    for dest in DESTINATIONS:
        print(f"Processando: {dest['id']}...")
        
        # 1. Coleta Google Trends (Sua lógica original)
        df = get_data_with_retry(pytrends, dest['kw'])
        
        # 2. Coleta Clima (Nova função de 7 dias)
        weather = get_weather_forecast(dest['lat'], dest['lon'])
        
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
                "weather": weather,
                "insight": generate_insight(recent_change, persistence, weather),
                "note": "Dados reais atualizados via ABR_ALL-IN-ONE."
            }
            # Espera entre destinos para evitar bloqueio
            time.sleep(random.uniform(5, 10))
        else:
            print(f"Aviso: Não foi possível obter dados para {dest['id']}")

    if results:
        with open('pulse-data.json', 'w') as f:
            json.dump(results, f, indent=2)
        print("Sucesso: pulse-data.json atualizado com sucesso.")
    else:
        print("ERRO CRÍTICO: Nenhum dado coletado.")

if __name__ == "__main__":
    main()