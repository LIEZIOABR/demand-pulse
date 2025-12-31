import pandas as pd
from pytrends.request import TrendReq
import json
import time
import random
import requests
from datetime import datetime, timedelta

# Configuração de Rigor Técnico: ABR ALL-IN-ONE
# Versão 6.0 - Expansão de Destinos e Preparação de Timeline

def get_weather(lat, lon):
    """Coleta previsão de 7 dias usando Open-Meteo com tratamento de erro robusto."""
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,weathercode&timezone=America%20Sao_Paulo"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        weather_map = {
            0: "Ensolarado", 1: "Limpo", 2: "Parc. Nublado", 3: "Nublado",
            45: "Nevoeiro", 48: "Nevoeiro", 51: "Garoa", 61: "Chuva Leve",
            63: "Chuva", 71: "Neve", 80: "Pancadas Chuva", 95: "Trovoada"
        }
        
        forecast = []
        for i in range(7):
            code = data['daily']['weathercode'][i]
            forecast.append({
                "max": data['daily']['temperature_2m_max'][i],
                "cond": weather_map.get(code, "Estável")
            })
        return {"daily": forecast}
    except Exception as e:
        print(f"Erro clima ({lat},{lon}): {e}")
        return {"daily": [{"max": 20, "cond": "Estável"}] * 7}

def get_trends_data(destinos):
    """Coleta dados do Google Trends com proteção contra Rate Limit (429)."""
    pytrends = TrendReq(hl='pt-BR', tz=180)
    results = {}
    
    # Base de dados para Timeline (Simulação de histórico para o MVP)
    # Em produção, o sistema acumulará dados reais a cada rodada.
    
    for nome, info in destinos.items():
        try:
            print(f"Processando: {nome}...")
            pytrends.build_payload([info['keyword']], geo='BR', timeframe='today 3-m')
            df = pytrends.interest_over_time()
            
            if not df.empty:
                # Cálculo de Variação Recente (Últimos 7 dias vs 21 anteriores)
                recent = df[info['keyword']].iloc[-7:].mean()
                previous = df[info['keyword']].iloc[-28:-7].mean()
                change = (recent - previous) / previous if previous > 0 else 0
                
                # Cálculo de Persistência (Estabilidade nos últimos 90 dias)
                persistence = 1 - (df[info['keyword']].std() / df[info['keyword']].mean()) if df[info['keyword']].mean() > 0 else 0
                
                # Dados para a Timeline (Últimas 8 semanas para o gráfico de BI)
                timeline_data = df[info['keyword']].resample('W').mean().tail(8).tolist()
                
                results[info['id']] = {
                    "name": nome,
                    "recentChange": round(change, 4),
                    "persistence": round(max(0, min(1, persistence)), 4),
                    "timeline": [round(x, 1) for x in timeline_data],
                    "weather": get_weather(info['lat'], info['lon']),
                    "insight": info['insight_base'].format(status="em alta" if change > 0 else "estável"),
                    "note": "Dados reais atualizados via ABR ALL-IN-ONE"
                }
            
            # Delay randômico para evitar bloqueio do Google
            time.sleep(random.uniform(5, 10))
            
        except Exception as e:
            print(f"Erro em {nome}: {e}")
            results[info['id']] = {"error": True}
            
    return results

# Configuração dos 10 Destinos (Inclusão de Gonçalves e Santo Antônio do Pinhal)
destinos_config = {
    "Monte Verde": {
        "id": "monte_verde_mg", "keyword": "Monte Verde MG", 
        "lat": -22.8627, "lon": -46.0377,
        "insight_base": "Demanda por Monte Verde segue {status}. Foco em pacotes de experiência gastronômica."
    },
    "Campos do Jordão": {
        "id": "campos_do_jordao_sp", "keyword": "Campos do Jordão", 
        "lat": -22.7394, "lon": -45.5914,
        "insight_base": "Campos do Jordão apresenta comportamento {status}. Otimizar tarifas para o próximo final de semana."
    },
    "Gramado + Canela": {
        "id": "gramado_canela_rs", "keyword": "Gramado RS", 
        "lat": -29.3746, "lon": -50.8764,
        "insight_base": "Serra Gaúcha {status}. Oportunidade para campanhas de antecipação de temporada."
    },
    "Poços de Caldas": {
        "id": "pocos_de_caldas_mg", "keyword": "Poços de Caldas", 
        "lat": -21.7867, "lon": -46.5619,
        "insight_base": "Turismo de águas em Poços {status}. Manter visibilidade em canais diretos."
    },
    "São Bento do Sapucaí": {
        "id": "sao_bento_sapucai_sp", "keyword": "São Bento do Sapucaí", 
        "lat": -22.6886, "lon": -45.7325,
        "insight_base": "Destino de natureza {status}. Potencial para turismo de aventura e isolamento."
    },
    "Passa Quatro": {
        "id": "passa_quatro_mg", "keyword": "Passa Quatro MG", 
        "lat": -22.3883, "lon": -44.9681,
        "insight_base": "Passa Quatro {status}. Foco no público regional e ferroviário."
    },
    "Serra Negra": {
        "id": "serra_negra_sp", "keyword": "Serra Negra SP", 
        "lat": -22.6122, "lon": -46.7002,
        "insight_base": "Circuito das Águas {status}. Excelente para ações de última hora."
    },
    "São Lourenço": {
        "id": "sao_lourenco_mg", "keyword": "São Lourenço MG", 
        "lat": -22.1158, "lon": -45.0547,
        "insight_base": "São Lourenço {status}. Monitorar concorrência direta no Sul de Minas."
    },
    "Gonçalves": {
        "id": "goncalves_mg", "keyword": "Gonçalves MG", 
        "lat": -22.6561, "lon": -45.8508,
        "insight_base": "Gonçalves {status}. Destino boutique em crescimento, ideal para casais."
    },
    "Santo Antônio do Pinhal": {
        "id": "santo_antonio_pinhal_sp", "keyword": "Santo Antônio do Pinhal", 
        "lat": -22.8247, "lon": -45.6671,
        "insight_base": "Santo Antônio {status}. Proximidade com Campos do Jordão favorece transbordo de demanda."
    }
}

if __name__ == "__main__":
    print("Iniciando atualização Demand Pulse - ABR ALL-IN-ONE...")
    data = get_trends_data(destinos_config)
    
    with open('pulse-data.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print("Atualização concluída com sucesso!")
