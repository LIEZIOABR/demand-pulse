import pandas as pd
from pytrends.request import TrendReq
import json
import time
import random
import requests
import os
from datetime import datetime

# =================================================================
# ABR ALL-IN-ONE - MOTOR DE INTELIGÊNCIA V3.2 (INTEGRADOR)
# FOCO: UPLOAD DIRETO PARA SUPABASE E DESTRAVAR RADAR
# =================================================================

def get_weather(lat, lon):
    """Coleta previsão de 7 dias usando Open-Meteo."""
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,weathercode&timezone=America%20Sao_Paulo"
        response = requests.get(url, timeout=10)
        data = response.json()
        weather_map = {0: "Ensolarado", 1: "Limpo", 2: "Parc. Nublado", 3: "Nublado", 45: "Nevoeiro", 48: "Nevoeiro", 51: "Garoa", 61: "Chuva Leve", 63: "Chuva", 71: "Neve", 80: "Pancadas Chuva", 95: "Trovoada"}
        forecast = []
        if 'daily' in data:
            for i in range(min(7, len(data['daily']['temperature_2m_max']))):
                code = data['daily']['weathercode'][i]
                forecast.append({"max": data['daily']['temperature_2m_max'][i], "cond": weather_map.get(code, "Estável")})
        return {"daily": forecast if forecast else [{"max": 20, "cond": "Estável"}] * 7}
    except:
        return {"daily": [{"max": 20, "cond": "Estável"}] * 7}

def upload_to_supabase(payload):
    """Envia os dados coletados para o histórico do Supabase."""
    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_KEY')
    
    if not url or not key:
        print("Erro: Chaves do Supabase não encontradas no ambiente.")
        return False
    
    try:
        headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        
        # Estrutura de Snapshot para o histórico do BI
        data_to_send = {
            "captured_at": datetime.now().isoformat(),
            "payload": {"destinations": payload}
        }
        
        endpoint = f"{url}/rest/v1/demand_snapshots"
        response = requests.post(endpoint, headers=headers, json=data_to_send, timeout=15)
        
        if response.status_code in [200, 201]:
            print("Sucesso: Dados enviados para o histórico do Supabase!")
            return True
        else:
            print(f"Erro no upload Supabase: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Erro crítico no upload: {e}")
        return False

def get_trends_data_v3_2(destinos_dict):
    """Coleta comparativa normalizada."""
    pytrends = TrendReq(hl='pt-BR', tz=180)
    principais_nomes = ["Monte Verde", "Campos do Jordão", "Gramado + Canela", "São Lourenço", "Poços de Caldas"]
    keywords = [destinos_dict[n]['keyword'] for n in principais_nomes if n in destinos_dict]
    
    results_list = []
    
    try:
        print(f"Iniciando Coleta Comparativa...")
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
                    
                    results_list.append({
                        "id": info['id'],
                        "name": nome,
                        "recentChange": round(change, 4),
                        "timeline": [round(max(0.1, x), 1) for x in timeline_data],
                        "weather": get_weather(info['lat'], info['lon']),
                        "insight": info['insight_base'].format(status="em alta" if change > 0 else "estável")
                    })
            print("Sucesso: Dados comparativos coletados.")
    except Exception as e:
        print(f"Erro na coleta: {e}")

    return results_list

destinos_config = {
    "Monte Verde": {"id": "monte_verde_mg", "keyword": "Monte Verde MG", "lat": -22.8627, "lon": -46.0377, "insight_base": "Demanda por Monte Verde segue {status}."},
    "Campos do Jordão": {"id": "campos_do_jordao_sp", "keyword": "Campos do Jordão", "lat": -22.7394, "lon": -45.5914, "insight_base": "Campos do Jordão apresenta comportamento {status}."},
    "Gramado + Canela": {"id": "gramado_canela_rs", "keyword": "Gramado RS", "lat": -29.3746, "lon": -50.8764, "insight_base": "Serra Gaúcha {status}."},
    "São Lourenço": {"id": "sao_lourenco_mg", "keyword": "São Lourenço MG", "lat": -22.1158, "lon": -45.0547, "insight_base": "São Lourenço {status}."},
    "Poços de Caldas": {"id": "pocos_de_caldas_mg", "keyword": "Poços de Caldas", "lat": -21.7867, "lon": -46.5619, "insight_base": "Poços {status}."}
}

if __name__ == "__main__":
    print("--- INICIANDO MOTOR ABR V3.2 (INTEGRADOR) ---")
    data_list = get_trends_data_v3_2(destinos_config)
    
    if data_list:
        # Salva localmente para backup
        with open('pulse-data.json', 'w', encoding='utf-8') as f:
            json.dump({d['id']: d for d in data_list}, f, ensure_ascii=False, indent=4)
        
        # Realiza o upload para o Supabase para atualizar o Radar
        upload_to_supabase(data_list)
        print(f"--- PROCESSO CONCLUÍDO: {len(data_list)} DESTINOS ---")
    else:
        print("--- ERRO: NENHUM DADO COLETADO ---")
