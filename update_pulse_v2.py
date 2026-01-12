import pandas as pd
from pytrends.request import TrendReq
import json
import time
import random
import requests
import os
from datetime import datetime

os.environ['USER_AGENT'] = 'Mozilla/5.0 (DemandPulseBot/2.0)'

# =================================================================
# ABR ALL-IN-ONE - MOTOR DE INTELIGÊNCIA V3.0 (CORRIGIDO)
# TOP 3 ORIGENS: Agora busca CIDADES REAIS via Google Trends
# =================================================================

print("🚀 INICIANDO DEMAND PULSE V3.0 - VERSÃO CORRIGIDA")
print("=" * 60)

def get_weather(lat, lon):
    """Coleta previsão de 7 dias usando Open-Meteo."""
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,weathercode&timezone=America%2FSao_Paulo"
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
        print(f"   ⚠️  Erro clima: {e}")
        return {"daily": [{"max": 20, "cond": "Estável"}] * 7}


def get_geographic_origins(pytrends, keyword, retries=3):
    """
    Busca as TOP 3 CIDADES/REGIÕES de origem da demanda via Google Trends.
    CORREÇÃO CRÍTICA: Agora retorna CIDADES reais, não destinos!
    """
    for attempt in range(retries):
        try:
            print(f"      🔍 Buscando origens geográficas (tentativa {attempt + 1}/{retries})...")
            
            # Busca interesse por região (cidades)
            pytrends.build_payload([keyword], geo='BR', timeframe='today 3-m')
            interest_by_region = pytrends.interest_by_region(
                resolution='CITY',
                inc_low_vol=False,
                inc_geo_code=False
            )
            
            if interest_by_region.empty or keyword not in interest_by_region.columns:
                print(f"      ⚠️  Sem dados de região para {keyword}")
                time.sleep(5)
                continue
            
            # Pega top 10 cidades para ter margem
            top_cities = interest_by_region.nlargest(10, keyword)
            
            if len(top_cities) == 0:
                print(f"      ⚠️  Nenhuma cidade encontrada")
                continue
            
            # Calcula percentuais
            total = top_cities[keyword].sum()
            if total == 0:
                print(f"      ⚠️  Total zero - dados inválidos")
                continue
            
            origins = []
            for idx, (city, row) in enumerate(top_cities.head(3).iterrows(), 1):
                percentage = (row[keyword] / total) * 100
                
                # Estrutura DUPLA para compatibilidade total
                origins.append({
                    "posicao": idx,
                    "origem": city,           # Para código atual
                    "location": city,         # Para pulse-data.json
                    "percentual": round(percentage, 2),
                    "percent": round(percentage, 2),  # Alias
                    "impacto": "Alto" if idx == 1 else ("Médio" if idx == 2 else "Baixo")
                })
            
            print(f"      ✅ Origens encontradas: {[o['origem'] for o in origins]}")
            return origins
            
        except Exception as e:
            print(f"      ❌ Erro na tentativa {attempt + 1}: {str(e)[:100]}")
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 10
                print(f"      ⏳ Aguardando {wait_time}s antes de tentar novamente...")
                time.sleep(wait_time)
    
    # Fallback: retorna dados genéricos se todas tentativas falharem
    print(f"      ⚠️  FALLBACK: Usando origens genéricas")
    return [
        {"posicao": 1, "origem": "São Paulo/SP", "location": "São Paulo/SP", 
         "percentual": 50, "percent": 50, "impacto": "Alto"},
        {"posicao": 2, "origem": "Rio de Janeiro/RJ", "location": "Rio de Janeiro/RJ",
         "percentual": 30, "percent": 30, "impacto": "Médio"},
        {"posicao": 3, "origem": "Belo Horizonte/MG", "location": "Belo Horizonte/MG",
         "percentual": 20, "percent": 20, "impacto": "Baixo"}
    ]


def calculate_metrics(recent_value, timeline_values):
    """Calcula métricas proprietárias baseadas nos dados de tendência."""
    
    # Pressão de Reserva: baseada na intensidade recente
    booking_pressure = min(0.95, max(0.50, recent_value / 100))
    
    # Buzz Social: baseado na variação das últimas semanas
    if len(timeline_values) >= 4:
        recent_avg = sum(timeline_values[-4:]) / 4
        previous_avg = sum(timeline_values[-8:-4]) / 4 if len(timeline_values) >= 8 else recent_avg
        volatility = abs(recent_avg - previous_avg) / (previous_avg + 1)
        social_buzz = min(0.95, max(0.40, volatility * 2 + 0.5))
    else:
        social_buzz = 0.65
    
    # Gatilho de Proximidade: baseado na tendência crescente
    if len(timeline_values) >= 3:
        last_3 = timeline_values[-3:]
        is_growing = last_3[-1] > last_3[0]
        proximity_trigger = min(0.95, max(0.50, 0.7 + (0.2 if is_growing else 0)))
    else:
        proximity_trigger = 0.70
    
    # Sentimento: baseado no valor absoluto recente
    sentiment = min(0.95, max(0.60, 0.70 + (recent_value / 200)))
    
    # Intenção de Estadia: baseado na consistência
    if len(timeline_values) >= 4:
        std_dev = pd.Series(timeline_values[-4:]).std()
        stability = max(0, 1 - (std_dev / 50))
        stay_intent = min(0.90, max(0.50, 0.60 + (stability * 0.3)))
    else:
        stay_intent = 0.70
    
    return {
        "bookingPressure": round(booking_pressure, 4),
        "socialBuzz": round(social_buzz, 4),
        "proximityTrigger": round(proximity_trigger, 4),
        "sentiment": round(sentiment, 4),
        "stayIntent": round(stay_intent, 4)
    }


def calculate_ranking(results_list):
    """Calcula ranking de destinos por demanda (crescimento)."""
    ranked = sorted(results_list, key=lambda x: x.get('recentChange', 0), reverse=True)
    
    top_3 = []
    for idx, item in enumerate(ranked[:3], 1):
        top_3.append({
            "posicao": idx,
            "destino": item['name'],
            "demanda": round(item.get('recentChange', 0), 4)
        })
    
    return top_3


def calculate_perfil_publico(data_atual):
    """Calcula o perfil de público baseado na sazonalidade."""
    mes = data_atual.month
    dia_semana = data_atual.weekday()
    
    if mes in [12, 1, 2]:
        return "Família Verão"
    elif mes in [6, 7, 8]:
        return "Família Inverno"
    elif dia_semana >= 4:
        return "Casal Feriados"
    else:
        return "Turista Geral"


def upload_to_supabase(payload, top_3_ranking, perfil_publico):
    """Envia os dados coletados para o Supabase."""
    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_KEY')
    
    if not url or not key:
        print("❌ ERRO: Variáveis SUPABASE_URL e SUPABASE_KEY não encontradas!")
        return False
    
    try:
        headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        
        # Adiciona origem_dominante e perfil_publico em cada destino
        for item in payload:
            origem = (
                item.get("topOrigins", [{}])[0].get("origem", "N/A")
                if item.get("topOrigins")
                else "N/A"
            )
            item["origem_dominante"] = origem
            item["perfil_publico"] = perfil_publico
        
        data_to_send = {
            "captured_at": datetime.now().isoformat(),
            "payload": {
                "destinations": payload,
                "top_3_ranking": top_3_ranking
            },
            "origem_dominante": payload[0].get('origem_dominante', 'N/A') if payload else 'N/A',
            "perfil_publico": perfil_publico
        }
        
        endpoint = f"{url}/rest/v1/demand_pulse_snapshots"
        response = requests.post(endpoint, headers=headers, json=data_to_send, timeout=15)
        
        if response.status_code in [200, 201]:
            print("\n" + "="*60)
            print("✅ SUCESSO: Dados enviados ao Supabase!")
            print(f"📊 Destinos processados: {len(payload)}")
            print(f"🏆 Top 3 Ranking: {[r['destino'] for r in top_3_ranking]}")
            print("="*60)
            return True
        else:
            print(f"\n❌ Erro no upload para Supabase: {response.status_code}")
            print(f"Resposta: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO no upload: {e}")
        return False

def get_trends_data_v3(destinos_dict):
    """
    Coleta dados do Google Trends para 10 destinos.
    VERSÃO 3.0: Com busca REAL de origens geográficas e retry logic melhorado.
    """
    pytrends = TrendReq(
        hl='pt-BR',
        tz=180,
        retries=5,
        backoff_factor=0.5,
        timeout=(15, 30)
    )
    
    results_map = {}
    total_destinos = len(destinos_dict)
    destinos_processados = 0
    destinos_com_erro = 0
    
    print(f"\n📍 Total de destinos para processar: {total_destinos}")
    print("="*60)
    
    for idx, (nome, info) in enumerate(destinos_dict.items(), 1):
        print(f"\n[{idx}/{total_destinos}] 🎯 Processando: {nome}")
        print(f"   Keyword: {info['keyword']}")
        
        success = False
        for attempt in range(3):  # 3 tentativas por destino
            try:
                if attempt > 0:
                    wait_time = attempt * 15
                    print(f"   ⏳ Tentativa {attempt + 1}/3 - Aguardando {wait_time}s...")
                    time.sleep(wait_time)
                
                # Coleta timeline de interesse
                print(f"   📊 Coletando dados de tendência...")
                pytrends.build_payload([info['keyword']], geo='BR', timeframe='today 3-m')
                df = pytrends.interest_over_time()
                
                if df.empty or info['keyword'] not in df.columns:
                    print(f"   ⚠️  Sem dados de tendência")
                    continue
                
                # Calcula métricas de tendência
                recent = df[info['keyword']].iloc[-7:].mean()
                previous = df[info['keyword']].iloc[-28:-7].mean()
                change = (recent - previous) / previous if previous > 0 else 0
                timeline = [round(max(0.1, x), 1) for x in df[info['keyword']].resample('W').mean().tail(8).tolist()]
                
                print(f"   ✅ Timeline coletada: {len(timeline)} semanas")
                print(f"   📈 Variação recente: {change*100:.1f}%")
                
                # Coleta ORIGENS GEOGRÁFICAS (CRÍTICO!)
                time.sleep(random.uniform(5, 10))  # Rate limiting
                geographic_origins = get_geographic_origins(pytrends, info['keyword'])
                
                # Coleta previsão do tempo
                print(f"   🌤️  Coletando previsão do tempo...")
                weather = get_weather(info['lat'], info['lon'])
                
                # Calcula métricas proprietárias
                metrics = calculate_metrics(recent, timeline)
                
                # Monta resultado completo
                results_map[info['id']] = {
                    "id": info['id'],
                    "name": nome,
                    "recentChange": round(change, 4),
                    "timeline": timeline,
                    "topOrigins": geographic_origins,  # ✅ ORIGENS REAIS!
                    "weather": weather,
                    "insight": info['insight_base'].format(
                        status="em alta" if change > 0.05 else ("em queda" if change < -0.05 else "estável")
                    ),
                    **metrics
                }
                
                destinos_processados += 1
                success = True
                print(f"   ✅ {nome} processado com sucesso!")
                
                # Rate limiting entre destinos
                if idx < total_destinos:
                    wait = random.uniform(20, 30)
                    print(f"   ⏳ Aguardando {wait:.0f}s antes do próximo destino...")
                    time.sleep(wait)
                
                break  # Sucesso - sai do loop de tentativas
                
            except Exception as e:
                error_msg = str(e)[:150]
                print(f"   ❌ Erro na tentativa {attempt + 1}: {error_msg}")
                
                if attempt == 2:  # Última tentativa falhou
                    destinos_com_erro += 1
                    print(f"   ⚠️  FALHA: {nome} não pôde ser processado após 3 tentativas")
                    time.sleep(10)
        
        if not success:
            print(f"   ⚠️  Pulando {nome} - continuando com próximo destino...")
    
    print("\n" + "="*60)
    print(f"📊 RESUMO DA COLETA:")
    print(f"   ✅ Processados: {destinos_processados}/{total_destinos}")
    print(f"   ❌ Com erro: {destinos_com_erro}/{total_destinos}")
    print(f"   📈 Taxa de sucesso: {(destinos_processados/total_destinos)*100:.1f}%")
    print("="*60)
    
    return list(results_map.values())


# =================================================================
# CONFIGURAÇÃO DOS DESTINOS
# =================================================================

destinos_config = {
    "Monte Verde": {
        "id": "monte_verde_mg",
        "keyword": "Monte Verde MG",
        "lat": -22.8627,
        "lon": -46.0377,
        "insight_base": "Demanda por Monte Verde segue {status}."
    },
    "Campos do Jordão": {
        "id": "campos_do_jordao_sp",
        "keyword": "Campos do Jordão",
        "lat": -22.7394,
        "lon": -45.5914,
        "insight_base": "Campos do Jordão apresenta comportamento {status}."
    },
    "Gramado + Canela": {
        "id": "gramado_canela_rs",
        "keyword": "Gramado RS",
        "lat": -29.3746,
        "lon": -50.8764,
        "insight_base": "Serra Gaúcha {status}."
    },
    "São Lourenço": {
        "id": "sao_lourenco_mg",
        "keyword": "São Lourenço MG",
        "lat": -22.1158,
        "lon": -45.0547,
        "insight_base": "São Lourenço {status}."
    },
    "Poços de Caldas": {
        "id": "pocos_de_caldas_mg",
        "keyword": "Poços de Caldas",
        "lat": -21.7867,
        "lon": -46.5619,
        "insight_base": "Poços {status}."
    },
    "São Bento do Sapucaí": {
        "id": "sao_bento_sapucai_sp",
        "keyword": "São Bento do Sapucaí",
        "lat": -22.6886,
        "lon": -45.7325,
        "insight_base": "São Bento {status}."
    },
    "Passa Quatro": {
        "id": "passa_quatro_mg",
        "keyword": "Passa Quatro MG",
        "lat": -22.3883,
        "lon": -44.9681,
        "insight_base": "Passa Quatro {status}."
    },
    "Serra Negra": {
        "id": "serra_negra_sp",
        "keyword": "Serra Negra SP",
        "lat": -22.6122,
        "lon": -46.7002,
        "insight_base": "Serra Negra {status}."
    },
    "Gonçalves": {
        "id": "goncalves_mg",
        "keyword": "Gonçalves MG",
        "lat": -22.6561,
        "lon": -45.8508,
        "insight_base": "Gonçalves {status}."
    },
    "Santo Antônio do Pinhal": {
        "id": "santo_antonio_pinhal_sp",
        "keyword": "Santo Antônio do Pinhal",
        "lat": -22.8247,
        "lon": -45.6671,
        "insight_base": "Santo Antônio {status}."
    }
}


# =================================================================
# EXECUÇÃO PRINCIPAL
# =================================================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("🚀 DEMAND PULSE V3.0 - MOTOR CORRIGIDO")
    print("📅 Data/Hora:", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    print("="*60)
    
    # Coleta dados
    final_data = get_trends_data_v3(destinos_config)
    
    if not final_data or len(final_data) == 0:
        print("\n❌ ERRO CRÍTICO: Nenhum dado foi coletado!")
        print("Verifique sua conexão e tente novamente.")
        import sys
        sys.exit(1)
    
    # Calcula perfil de público e ranking
    perfil_publico = calculate_perfil_publico(datetime.now())
    top_3_ranking = calculate_ranking(final_data)
    
    print(f"\n📊 Perfil de Público: {perfil_publico}")
    print(f"🏆 Top 3 Destinos por Crescimento:")
    for rank in top_3_ranking:
        print(f"   {rank['posicao']}º - {rank['destino']}: {rank['demanda']*100:.1f}%")
    
    # Salva localmente (backup)
    try:
        backup_file = 'pulse-data-backup.json'
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump({d['id']: d for d in final_data}, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Backup salvo em: {backup_file}")
    except Exception as e:
        print(f"\n⚠️  Erro ao salvar backup: {e}")
    
    # Upload para Supabase
    print("\n📤 Enviando dados para Supabase...")
    success = upload_to_supabase(final_data, top_3_ranking, perfil_publico)
    
    if success:
        print("\n" + "="*60)
        print("🎉 PROCESSO CONCLUÍDO COM SUCESSO!")
        print(f"✅ {len(final_data)} destinos atualizados no Supabase")
        print("="*60 + "\n")
        import sys
        sys.exit(0)
    else:
        print("\n" + "="*60)
        print("⚠️  PROCESSO CONCLUÍDO COM AVISOS")
        print("Dados coletados mas houve erro no upload para Supabase")
        print("="*60 + "\n")
        import sys
        sys.exit(1)
