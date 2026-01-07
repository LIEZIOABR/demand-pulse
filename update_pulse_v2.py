import pandas as pd
from pytrends.request import TrendReq
import json
import time
import random
import requests
import os
from datetime import datetime

# =================================================================
# ABR ALL-IN-ONE - MOTOR DE INTELIGÊNCIA V3.4 (COM ORIGEM DOMINANTE)
# FOCO: 10 DESTINOS, RANKING, ORIGEM DOMINANTE E UPLOAD SUPABASE
# =================================================================

def get_weather(lat, lon):
    """Coleta previsão de 7 dias usando Open-Meteo."""
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,weathercode&timezone=America%20Sao_Paulo"
        response = requests.get(url, timeout=10 )
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

def calculate_origem_dominante(results_list):
    """Calcula ranking de destinos por demanda (Google Trends)."""
    # Ordena por recentChange (mudança recente) em ordem decrescente
    ranked = sorted(results_list, key=lambda x: x.get('recentChange', 0), reverse=True)
    
    # Retorna top 3 com posição
    top_3 = []
    for idx, item in enumerate(ranked[:3], 1):
        top_3.append({
            "posicao": idx,
            "destino": item['name'],
            "demanda": round(item.get('recentChange', 0), 4)
        })
    
    return top_3
    
def calculate_perfil_publico(data_atual):
    """
    Calcula o perfil de público baseado na sazonalidade para destinos de serra.
    
    Retorna:
    - "Família Verão" (dez-fev): férias de verão
    - "Família Inverno" (jun-ago): férias de inverno  
    - "Casal Feriados" (fins de semana): casais em feriados
    - "Turista Geral" (resto): turistas em geral
    """
    mes = data_atual.month
    dia_semana = data_atual.weekday()
    
    # Férias de verão (dezembro, janeiro, fevereiro)
    if mes in [12, 1, 2]:
        return "Família Verão"
    
    # Férias de inverno (junho, julho, agosto)
    elif mes in [6, 7, 8]:
        return "Família Inverno"
    
    # Fins de semana (sexta, sábado, domingo)
    elif dia_semana >= 4:  # 4=sexta, 5=sábado, 6=domingo
        return "Casal Feriados"
    
    # Resto do ano
    else:
        return "Turista Geral"



def upload_to_supabase(payload, top_3_ranking):
    """Envia os dados coletados para a tabela correta no Supabase."""
    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_KEY')
    
    if not url or not key:
        print("Erro: Chaves do Supabase não encontradas.")
        return False
    
    try:
        headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        
        # Adiciona ranking ao payload de cada destino
        for item in payload:
            # Encontra a posição deste destino no ranking
            ranking_pos = next((idx+1 for idx, r in enumerate(top_3_ranking) if r['destino'] == item['name']), None)
            item['origem_dominante'] = ranking_pos if ranking_pos else None
        item['perfil_publico'] = perfil_publico        
        data_to_send = {
            "captured_at": datetime.now().isoformat(),
            "payload": {"destinations": payload},
            "top_3_ranking": top_3_ranking
        }
        
        # NOME DA TABELA CONFORME SUPABASE
        endpoint = f"{url}/rest/v1/demand_pulse_snapshots"
        response = requests.post(endpoint, headers=headers, json=data_to_send, timeout=15)
        
        if response.status_code in [200, 201]:
            print("Sucesso: Dados com Origem Dominante entregues ao Supabase!")
            print(f"Top 3 Ranking: {top_3_ranking}")
            return True
        else:
            print(f"Erro no upload: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Erro crítico no upload: {e}")
        return False

def get_trends_data_v3_4(destinos_dict):
    """Coleta 10 destinos com normalização comparativa e ranking."""
    pytrends = TrendReq(hl='pt-BR', tz=180)
    
    # 1. NORMALIZAÇÃO (Os 5 principais na mesma régua)
    principais = ["Monte Verde", "Campos do Jordão", "Gramado + Canela", "São Lourenço", "Poços de Caldas"]
    keywords_main = [destinos_dict[n]['keyword'] for n in principais]
    
    results_map = {}
    
    try:
        print(f"Fase 1: Normalizando Share para {principais}...")
        pytrends.build_payload(keywords_main, geo='BR', timeframe='today 3-m')
        df_main = pytrends.interest_over_time()
        
        if not df_main.empty:
            for nome in principais:
                info = destinos_dict[nome]
                kw = info['keyword']
                if kw in df_main.columns:
                    recent = df_main[kw].iloc[-7:].mean()
                    previous = df_main[kw].iloc[-28:-7].mean()
                    change = (recent - previous) / previous if previous > 0 else 0
                    timeline = [round(max(0.1, x), 1) for x in df_main[kw].resample('W').mean().tail(8).tolist()]
                    
                    results_map[info['id']] = {
                        "id": info['id'], "name": nome, "recentChange": round(change, 4),
                        "timeline": timeline, "weather": get_weather(info['lat'], info['lon']),
                        "insight": info['insight_base'].format(status="em alta" if change > 0 else "estável")
                    }
    except Exception as e:
        print(f"Erro na Fase 1: {e}")

    # 2. COMPLEMENTO (Os outros 5 destinos)
    secundarios = [n for n in destinos_dict.keys() if n not in principais]
    for nome in secundarios:
        try:
            print(f"Fase 2: Coletando {nome}...")
            info = destinos_dict[nome]
            time.sleep(random.uniform(5, 8))
            pytrends.build_payload([info['keyword']], geo='BR', timeframe='today 3-m')
            df_sec = pytrends.interest_over_time()
            
            if not df_sec.empty:
                kw = info['keyword']
                recent = df_sec[kw].iloc[-7:].mean()
                previous = df_sec[kw].iloc[-28:-7].mean()
                change = (recent - previous) / previous if previous > 0 else 0
                timeline = [round(max(0.1, x), 1) for x in df_sec[kw].resample('W').mean().tail(8).tolist()]
                
                results_map[info['id']] = {
                    "id": info['id'], "name": nome, "recentChange": round(change, 4),
                    "timeline": timeline, "weather": get_weather(info['lat'], info['lon']),
                    "insight": info['insight_base'].format(status="em alta" if change > 0 else "estável")
                }
        except Exception as e:
            print(f"Erro em {nome}: {e}")

    return list(results_map.values())

destinos_config = {
    "Monte Verde": {"id": "monte_verde_mg", "keyword": "Monte Verde MG", "lat": -22.8627, "lon": -46.0377, "insight_base": "Demanda por Monte Verde segue {status}."},
    "Campos do Jordão": {"id": "campos_do_jordao_sp", "keyword": "Campos do Jordão", "lat": -22.7394, "lon": -45.5914, "insight_base": "Campos do Jordão apresenta comportamento {status}."},
    "Gramado + Canela": {"id": "gramado_canela_rs", "keyword": "Gramado RS", "lat": -29.3746, "lon": -50.8764, "insight_base": "Serra Gaúcha {status}."},
    "São Lourenço": {"id": "sao_lourenco_mg", "keyword": "São Lourenço MG", "lat": -22.1158, "lon": -45.0547, "insight_base": "São Lourenço {status}."},
    "Poços de Caldas": {"id": "pocos_de_caldas_mg", "keyword": "Poços de Caldas", "lat": -21.7867, "lon": -46.5619, "insight_base": "Poços {status}."},
    "São Bento do Sapucaí": {"id": "sao_bento_sapucai_sp", "keyword": "São Bento do Sapucaí", "lat": -22.6886, "lon": -45.7325, "insight_base": "São Bento {status}."},
    "Passa Quatro": {"id": "passa_quatro_mg", "keyword": "Passa Quatro MG", "lat": -22.3883, "lon": -44.9681, "insight_base": "Passa Quatro {status}."},
    "Serra Negra": {"id": "serra_negra_sp", "keyword": "Serra Negra SP", "lat": -22.6122, "lon": -46.7002, "insight_base": "Serra Negra {status}."},
    "Gonçalves": {"id": "goncalves_mg", "keyword": "Gonçalves MG", "lat": -22.6561, "lon": -45.8508, "insight_base": "Gonçalves {status}."},
    "Santo Antônio do Pinhal": {"id": "santo_antonio_pinhal_sp", "keyword": "Santo Antônio do Pinhal", "lat": -22.8247, "lon": -45.6671, "insight_base": "Santo Antônio {status}."}
}

if __name__ == "__main__":
    print("--- INICIANDO MOTOR ABR V3.4 (COM ORIGEM DOMINANTE) ---")
    final_data = get_trends_data_v3_4(destinos_config)
    
    if final_data:
    # Calcula perfil de público
    perfil_publico = calculate_perfil_publico(datetime.now())

        # Calcula ranking (top 3)
        top_3_ranking = calculate_origem_dominante(final_data)
        
        # Salva localmente para backup no GitHub
        with open('pulse-data.json', 'w', encoding='utf-8') as f:
            json.dump({d['id']: d for d in final_data}, f, ensure_ascii=False, indent=4)
        
        # Upload final para o Supabase (com ranking)
        upload_to_supabase(final_data, top_3_ranking)
        print(f"--- PROCESSO CONCLUÍDO: {len(final_data)} DESTINOS ATUALIZADOS ---")
        print(f"--- TOP 3 RANKING: {top_3_ranking} ---")
    else:
        print("--- ERRO: NENHUM DADO COLETADO ---")





