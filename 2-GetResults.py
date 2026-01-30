import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÕES E CARREGAMENTO DE VARIÁREIS
# ==============================================================================
load_dotenv()

BOT_ID = os.getenv("BOT_ID")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
OUTPUT_DIR = 'output'
RESULTS_JSON_PATH = os.path.join(OUTPUT_DIR, 'results.json')
SCHEMA_JSON_PATH = os.path.join(OUTPUT_DIR, 'bot_schema.json')
OUTPUT_CSV_PATH = f'DB/extracted_results_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'

if not BOT_ID or not AUTH_TOKEN:
    print("Erro: BOT_ID e AUTH_TOKEN devem estar configurados no arquivo .env.")
    exit()

API_BASE_URL = os.getenv("API_BASE_URL", "https://typebot.io/api/v1")

# ==============================================================================

def get_variable_mapping():
    """Lê o schema ou results.json para mapear IDs de variáveis para nomes."""
    mapping = {}
    
    # Tenta carregar do bot_schema.json primeiro (mais limpo)
    if os.path.exists(SCHEMA_JSON_PATH):
        try:
            with open(SCHEMA_JSON_PATH, 'r', encoding='utf-8') as f:
                schema = json.load(f)
                # O schema consolidado pode não ter o mapping direto de ID, 
                # então vamos precisar do results.json de qualquer forma para os IDs
        except Exception as e:
            print(f"Aviso ao ler schema: {e}")

    # Carrega do results.json para pegar os mappings de ID -> Name
    if os.path.exists(RESULTS_JSON_PATH):
        try:
            with open(RESULTS_JSON_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
                bot = data.get('typebot', {})
                variables = bot.get('variables', [])
                for v in variables:
                    mapping[v['id']] = v['name']
            print(f"Mapeamento de {len(mapping)} variáveis carregado.")
        except Exception as e:
            print(f"Erro ao ler results.json: {e}")
            
    return mapping

def fetch_all_results(bot_id, token, mapping):
    """Busca resultados do Typebot usando paginação por offset (cursor numérico)."""
    base_url = f"{API_BASE_URL}/typebots/{bot_id}/results"
    headers = {"Authorization": f"Bearer {token}"}
    
    all_extracted_map = {}
    limit = 100 # Recomenda-se 100 para estabilidade
    
    # Diferentes views que podem existir no Typebot (embora a API muitas vezes retorne tudo)
    filter_sets = [
        {"timeFilter": "allTime"},
        {"isArchived": "true", "timeFilter": "allTime"}
    ]
    
    print(f"\nIniciando extração exaustiva para o bot: {bot_id}")
    
    for fs in filter_sets:
        offset = 0
        has_more = True
        page = 1
        
        filter_str = ", ".join([f"{k}={v}" for k, v in fs.items()])
        print(f"\n--- Verificando conjunto: {filter_str} ---")
        
        while has_more:
            params = fs.copy()
            params["limit"] = limit
            params["cursor"] = offset
            
            try:
                print(f"Buscando offset {offset} (Página {page})...", end="\r")
                response = requests.get(base_url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                results = data.get('results', [])
                if not results:
                    has_more = False
                    break
                
                # O Typebot usa padrão N+1: se pedir 100 e vier 101, há mais páginas.
                # Para evitar duplicatas na última página, processamos apenas os registros novos.
                num_returned = len(results)
                
                # Se vier 101 itens para limit 100, processamos os 100 primeiros e o 101 será o primeiro da próxima
                batch_to_process = results[:limit] if num_returned > limit else results
                
                for res in batch_to_process:
                    res_id = res.get("id")
                    if res_id not in all_extracted_map:
                        row = {
                            "ResultId": res_id,
                            "SubmittedAt": res.get("createdAt"),
                            "IsCompleted": res.get("isCompleted"),
                            "ChatSessionId": res.get("lastChatSessionId")
                        }
                        
                        # Processa variáveis
                        for var_entry in res.get("variables", []):
                            var_name = var_entry.get("name")
                            if not var_name:
                                var_id = var_entry.get("id")
                                var_name = mapping.get(var_id, var_id)
                            if var_name:
                                row[var_name] = var_entry.get("value")
                                
                        # Processa respostas (fallback se variável não mapeada ou se não tiver variável)
                        for ans in res.get("answers", []):
                            content = ans.get("content")
                            if content is None:
                                continue

                            var_id = ans.get("variableId")
                            block_id = ans.get("blockId")
                            
                            # Tenta encontrar o nome da variável
                            col_name = None
                            if var_id:
                                col_name = mapping.get(var_id)
                            
                            # Se não achou nome mapeado, usa o ID da variável
                            if not col_name and var_id:
                                col_name = f"Var_{var_id}"
                            
                            # Se não tem variável, usa o Block ID
                            if not col_name and block_id:
                                col_name = f"Block_{block_id}"
                                
                            # Último recurso: Answer ID (improvável, mas garante captura)
                            if not col_name:
                                col_name = f"Answer_{ans.get('blockId', 'unknown')}"

                            # Só salva se ainda não tiver valor para essa coluna (prioriza a lista de variables acima)
                            if col_name and col_name not in row:
                                row[col_name] = content
                        
                        all_extracted_map[res_id] = row
                
                if num_returned > limit:
                    offset += limit
                    page += 1
                else:
                    has_more = False
                    
            except requests.exceptions.RequestException as e:
                print(f"\nErro no offset {offset}: {e}")
                break
                
    final_list = list(all_extracted_map.values())
    print(f"\nTotal de registros únicos extraídos: {len(final_list)}")
    return final_list

def main():
    # 1. Mapeamento de variáveis
    mapping = get_variable_mapping()
    
    # 2. Busca resultados
    results = fetch_all_results(BOT_ID, AUTH_TOKEN, mapping)
    
    if not results:
        print("Fim do processo: Nenhum dado para salvar.")
        return

    # 3. Converte para DataFrame
    df = pd.DataFrame(results)
    
    # Reordena colunas: IDs, Status e Datas primeiro, depois as variáveis do bot
    fixed_cols = ["ResultId", "SubmittedAt", "IsCompleted", "ChatSessionId"]
    other_cols = [c for c in df.columns if c not in fixed_cols]
    
    # Se tivermos o schema, podemos tentar ordenar as colunas conforme o fluxo do bot
    if os.path.exists(SCHEMA_JSON_PATH):
        try:
            with open(SCHEMA_JSON_PATH, 'r', encoding='utf-8') as f:
                schema = json.load(f)
                ordered_vars = schema.get("variables", [])
                # Mantém apenas as colunas que existem no DF
                valid_ordered = [v for v in ordered_vars if v in other_cols]
                remaining = [c for c in other_cols if c not in valid_ordered]
                other_cols = valid_ordered + remaining
        except:
            pass
            
    df = df[fixed_cols + other_cols]

    # 4. Salva em CSV
    if not os.path.exists('DB'):
        os.makedirs('DB')
        
    df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8-sig')
    print(f"\nSucesso! Dados salvos em: {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    main()
