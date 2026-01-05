import pandas as pd
from pytrends.request import TrendReq
import json
import time
import random
import requests
from datetime import datetime

# =================================================================
# ABR ALL-IN-ONE - MOTOR DE INTELIGÊNCIA V3.1 (COMPATIBILIDADE)
# FOCO: CORREÇÃO DE CONFLITO DE BIBLIOTECA E DESTRAVAR DADOS
# =================================================================

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
        if 'daily' in data:
            for i in range(min(7, len(data['daily']['temperature_2m_max']))):
                code = data['daily']['weathercode'][i]
                forecast.append({
                    "max": data['daily']['temperature_2m_max'][i],
                    "cond": weather_map.get(code, "Estável")
                })
        return {"daily": forecast if forecast else [{"max": 20, "cond": "Estável"}] * 7}
    except Exception as e:
        print(f"Erro clima ({lat},{lon}): {e}")
        return {"daily": [{"max": 20, "cond": "Estável"}] * 7}

def get_trends_data_v3_1(destinos_dict):
    """
    COLETA COMPARATIVA V3.1: Versão com compatibilidade máxima para GitHub Actions.
    """
    # Removido parâmetros de retries complexos para evitar conflito de versão da biblioteca
    pytrends = TrendReq(hl='pt-BR', tz=180)
    
    principais_nomes = ["Monte Verde", "Campos do Jordão", "Gramado + Canela", "São Lourenço", "Poços de Caldas"]
    keywords = [destinos_dict[n]['keyword'] for n in principais_nomes if n in destinos_dict]
    
    results = {}
    
    try:
        print(f"Iniciando Consulta Comparativa para: {principais_nomes}")
        # Adicionado pequeno delay antes da consulta principal
        time.sleep(2)
        pytrends.build_payload(keywords, geo='BR', timeframe='today 3-m')
        df = pytrends.interest_over_time()
        
        if not df.empty:
            for nome in principais_nomes:
                if nome not in destinos_dict: continue
                info = destinos_dict[nome]
                kw = info['keyword']
                
                if kw in df.columns:
                    recent = df[kw].iloc[-7:].mean()
                    previous = df[kw].iloc[-28:-7].mean()
                    change = (recent - previous) / previous if previous > 0 else 0
                    timeline_data = df[kw].resample('W').mean().tail(8).tolist()
                    timeline_final = [round(max(0.1, x), 1) for x in timeline_data]
                    
                    results[info['id']] = {
                        "name": nome,
                        "recentChange": round(change, 4),
                        "timeline": timeline_final,
                        "weather": get_weather(info['lat'], info['lon']),
                        "insight": info['insight_base'].format(status="em alta" if change > 0 else "estável"),
                        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
            print("Sucesso: Dados comparativos normalizados.")
        else:
            print("Aviso: Google retornou DataFrame vazio.")
            
    except Exception as e:
        print(f"Erro na coleta comparativa: {e}")
        
    # Coleta individual para os destinos secundários
    secundarios = [n for n in destinos_dict.keys() if n not in principais_nomes]
    for nome in secundarios:
        try:
            print(f"Coletando destino secundário: {nome}...")
            info = destinos_dict[nome]
            time.sleep(random.uniform(5, 10)) # Delay maior para evitar bloqueio
            pytrends.build_payload([info['keyword']], geo='BR', timeframe='today 3-m')
            df_sec = pytrends.interest_over_time()
            
            if not df_sec.empty:
                kw = info['keyword']
                recent = df_sec[kw].iloc[-7:].mean()
                previous = df_sec[kw].iloc[-28:-7].mean()
                change = (recent - previous) / previous if previous > 0 else 0
                timeline_data = df_sec[kw].resample('W').mean().tail(8).tolist()
                
                results[info['id']] = {
                    "name": nome,
                    "recentChange": round(change, 4),
                    "timeline": [round(max(0.1, x), 1) for x in timeline_data],
                    "weather": get_weather(info['lat'], info['lon']),
                    "insight": info['insight_base'].format(status="em alta" if change > 0 else "estável"),
                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
        except Exception as e:
            print(f"Erro no destino secundário {nome}: {e}")

    return results

# Configuração dos Destinos
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
    "São Lourenço": {
        "id": "sao_lourenco_mg", "keyword": "São Lourenço MG", 
        "lat": -22.1158, "lon": -45.0547,
        "insight_base": "São Lourenço {status}. Monitorar concorrência direta no Sul de Minas."
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
    print("--- INICIANDO MOTOR ABR V3.1 (FIX) ---")
    data = get_trends_data_v3_1(destinos_config)
    
    if data:
        with open('pulse-data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"--- SUCESSO: {len(data)} DESTINOS ATUALIZADOS ---")
    else:
        print("--- ERRO: NENHUM DADO COLETADO. VERIFIQUE OS LOGS ---")
