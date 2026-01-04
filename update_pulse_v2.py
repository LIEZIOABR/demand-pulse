import os
import json
import requests
import pandas as pd
from datetime import datetime
from supabase import create_client, Client

# Configurações do Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_trends_data():
    # Simulação de coleta de dados (substitua pela sua lógica real de coleta se necessário)
    # Esta função deve retornar o payload formatado para o Demand Pulse
    
    destinations = [
        {"id": "monte_verde_mg", "name": "Monte Verde (MG)"},
        {"id": "campos_do_jordao_sp", "name": "Campos do Jordão (SP)"},
        {"id": "gramado_canela_rs", "name": "Gramado + Canela (RS)"},
        {"id": "pocos_de_caldas_mg", "name": "Poços de Caldas (MG)"},
        {"id": "sao_bento_sapucai_sp", "name": "São Bento do Sapucaí (SP)"},
        {"id": "passa_quatro_mg", "name": "Passa Quatro (MG)"},
        {"id": "serra_negra_sp", "name": "Serra Negra (SP)"},
        {"id": "sao_lourenco_mg", "name": "São Lourenço (MG)"},
        {"id": "goncalves_mg", "name": "Gonçalves (MG)"},
        {"id": "santo_antonio_pinhal_sp", "name": "Santo Antônio do Pinhal (SP)"}
    ]
    
    payload_destinations = []
    
    for dest in destinations:
        # Aqui entra a lógica de IA/Trends para cada destino
        # Valores de exemplo baseados na tendência atual
        data = {
            "id": dest["id"],
            "name": dest["name"],
            "marketShare": 0.25,
            "bookingPressure": 0.85,
            "sentiment": 0.88,
            "proximityTrigger": 0.75,
            "socialBuzz": 0.80,
            "audienceProfile": 0.65,
            "stayIntent": 0.82,
            "confidence": 0.90,
            "elasticityIndex": 0.45,
            "recentChange": 0.12,
            "persistence": 0.78,
            "climateProfile": "cold",
            "topOrigins": [
                {"location": "SP", "percent": 65},
                {"location": "RJ", "percent": 20},
                {"location": "MG", "percent": 10}
            ],
            "timeline": [55, 58, 62, 60, 65, 68, 70, 72],
            "insight": "Forte aceleração de demanda devido à queda de temperatura. Gatilho de reserva ativo.",
            "weather": {
                "daily": [
                    {"max": 14, "cond": "Limpo"},
                    {"max": 12, "cond": "Nublado"},
                    {"max": 15, "cond": "Limpo"},
                    {"max": 13, "cond": "Chuva"},
                    {"max": 14, "cond": "Limpo"}
                ]
            }
        }
        payload_destinations.append(data)
        
    return {
        "lastUpdate": datetime.now().isoformat(),
        "destinations": payload_destinations
    }

def main():
    print(f"🚀 Iniciando atualização de dados: {datetime.now()}")
    
    try:
        # 1. Coleta os dados
        payload = get_trends_data()
        
        # 2. Salva no arquivo local (backup)
        with open('pulse-data.json', 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=4)
        print("✅ Arquivo pulse-data.json atualizado localmente.")
        
        # 3. Envia para o Supabase criando um NOVO registro (Histórico)
        # Mudamos de .upsert() para .insert() para permitir o seletor de datas no futuro
        data, count = supabase.table('demand_pulse_snapshots').insert({
            "payload": payload,
            "source": "demand_pulse"
        }).execute()
        
        print("✅ Dados enviados para o histórico do Supabase com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro durante a atualização: {e}")
        exit(1)

if __name__ == "__main__":
    main()
