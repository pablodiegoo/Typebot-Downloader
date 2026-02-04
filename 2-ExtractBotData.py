import requests
import json
import os
import re
import argparse
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÕES E CARREGAMENTO DE VARIÁREIS
# ==============================================================================
load_dotenv()

AUTH_TOKEN = os.getenv("AUTH_TOKEN")
API_BASE_URL = os.getenv("API_BASE_URL", "https://typebot.io/api/v1")
OUTPUT_DIR = 'output'
GLOBAL_INFO_PATH = os.path.join(OUTPUT_DIR, 'global_info.json')

if not AUTH_TOKEN:
    print("Erro: AUTH_TOKEN deve estar configurado no arquivo .env.")
    exit()

# ==============================================================================
# LOGIC EXTRACTION HELPERS
# ==============================================================================

def fetch_bot_structure(bot_id, token):
    """Busca a estrutura do bot na API do Typebot."""
    url = f"{API_BASE_URL}/typebots/{bot_id}"
    headers = {"Authorization": f"Bearer {token}"}
    print(f"Buscando estrutura do bot ID: {bot_id}...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar dados da API: {e}")
        return None

def save_json_file(data, file_path, indent=2):
    """Salva dados em arquivo JSON."""
    output_dir = os.path.dirname(file_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)

def get_ordered_variables(bot_data):
    """Determina a ordem cronológica das variáveis rastreando o fluxo."""
    bot = bot_data.get('typebot', {})
    if not bot: return []

    groups = {g['id']: g for g in bot.get('groups', [])}
    edges = bot.get('edges', [])
    events = bot.get('events', [])
    variables = {v['id']: v['name'] for v in bot.get('variables', [])}

    def get_targets(block_id=None, group_id=None, item_id=None, event_id=None):
        targets = []
        for edge in edges:
            f = edge.get('from', {})
            if event_id and f.get('eventId') == event_id: targets.append(edge.get('to', {}))
            elif item_id and f.get('itemId') == item_id: targets.append(edge.get('to', {}))
            elif block_id and f.get('blockId') == block_id and not f.get('itemId'): targets.append(edge.get('to', {}))
            elif group_id and f.get('groupId') == group_id: targets.append(edge.get('to', {}))
        return targets

    start_targets = []
    for event in events:
        if event.get('type') == 'start':
            start_targets.extend(get_targets(event_id=event.get('id')))
    
    if not start_targets and bot.get('startGroupId'):
        start_targets.append({'groupId': bot.get('startGroupId')})
    
    ordered_variables = []
    visited_groups = set()

    def trace_flow(group_id, start_block_id=None):
        if group_id in visited_groups: return
        visited_groups.add(group_id)
        group = groups.get(group_id)
        if not group: return
        
        blocks = group.get('blocks', [])
        start_idx = 0
        if start_block_id:
            for i, b in enumerate(blocks):
                if b['id'] == start_block_id:
                    start_idx = i
                    break
        
        for i in range(start_idx, len(blocks)):
            block = blocks[i]
            var_id = block.get('options', {}).get('variableId')
            if var_id and var_id in variables:
                var_name = variables[var_id]
                if var_name not in ordered_variables:
                    ordered_variables.append(var_name)
            
            for item in block.get('items', []):
                for target in get_targets(item_id=item.get('id')):
                    tg, tb = target.get('groupId'), target.get('blockId')
                    if tg: trace_flow(tg, tb)

            for target in get_targets(block_id=block.get('id')):
                tg, tb = target.get('groupId'), target.get('blockId')
                if tg: trace_flow(tg, tb)
            
            if block.get('type') == 'Jump':
                target_g = block.get('options', {}).get('groupId')
                target_b = block.get('options', {}).get('blockId')
                if target_g: trace_flow(target_g, target_b)

    for target in start_targets:
        tg, tb = target.get('groupId'), target.get('blockId')
        if tg: trace_flow(tg, tb)
    
    for var_name in variables.values():
        if var_name not in ordered_variables:
            ordered_variables.append(var_name)
            
    return ordered_variables

def generate_constraints(bot_data):
    """Gera regras de lógica baseadas em condições."""
    bot = bot_data.get('typebot', {})
    if not bot: return []

    variables = {v['id']: v['name'] for v in bot.get('variables', [])}
    constraints = []
    all_blocks = {}
    block_to_group = {}
    
    for group in bot.get('groups', []):
        for block in group.get('blocks', []):
            all_blocks[block['id']] = block
            block_to_group[block['id']] = group['id']
            
    group_block_orders = {g['id']: [b['id'] for b in g.get('blocks', [])] for g in bot.get('groups', [])}
    edges = bot.get('edges', [])

    for block_id, block in all_blocks.items():
        if block.get('type') == 'Condition':
            for item in block.get('items', []):
                content = item.get('content', {})
                comparisons = content.get('comparisons', [])
                if not comparisons: continue
                
                comparison = comparisons[0]
                cond_var_name = variables.get(comparison.get('variableId'))
                operator = comparison.get('comparisonOperator', '')
                cond_value = comparison.get('value', '')
                
                if not cond_var_name: continue
                
                outgoing_edge_id = item.get('outgoingEdgeId')
                target_edge = next((e for e in edges if e.get('id') == outgoing_edge_id), None)
                if not target_edge: continue
                
                to_info = target_edge.get('to', {})
                to_block_id = to_info.get('blockId')
                to_group_id = to_info.get('groupId')
                
                skipped_var_ids = []
                from_group_id = block_to_group.get(block_id)
                
                if from_group_id:
                    block_order = group_block_orders.get(from_group_id, [])
                    if to_group_id and to_group_id != from_group_id:
                        try:
                            start_index = block_order.index(block_id)
                            skipped_block_ids = block_order[start_index + 1:]
                            for sb_id in skipped_block_ids:
                                sb = all_blocks.get(sb_id)
                                if sb and 'input' in sb.get('type', '').lower():
                                    if vid := sb.get('options', {}).get('variableId'): skipped_var_ids.append(vid)
                        except ValueError: pass
                    elif to_block_id and to_block_id in block_order:
                        try:
                            start_index = block_order.index(block_id)
                            end_index = block_order.index(to_block_id)
                            skipped_block_ids = block_order[start_index + 1:end_index]
                            for sb_id in skipped_block_ids:
                                sb = all_blocks.get(sb_id)
                                if sb and 'input' in sb.get('type', '').lower():
                                    if vid := sb.get('options', {}).get('variableId'): skipped_var_ids.append(vid)
                        except ValueError: pass
                
                affected_columns = [variables[vid] for vid in skipped_var_ids if vid in variables]
                if affected_columns:
                    constraints.append({
                        "condition_column": cond_var_name,
                        "condition_value": f"{operator} {cond_value}",
                        "affected_columns": affected_columns
                    })
                    
    return constraints

def generate_metadata(bot_data, ordered_vars=None):
    """Gera metadados (sdtype) dos campos."""
    bot = bot_data.get('typebot', {})
    if not bot: return None

    variables_map = {v['id']: v['name'] for v in bot.get('variables', [])}
    all_blocks = []
    for group in bot.get('groups', []):
        all_blocks.extend(group.get('blocks', []))

    sdtype_map = {
        'text input': 'text', 'choice input': 'categorical', 'rating input': 'numerical',
        'number input': 'numerical', 'email input': 'email', 'phone number input': 'phone_number',
        'date input': 'datetime', 'url input': 'url'
    }

    columns_metadata = {}
    target_vars = ordered_vars if ordered_vars else variables_map.values()
    
    for var_name in target_vars:
        found = False
        for block in all_blocks:
            v_id = block.get('options', {}).get('variableId')
            if v_id and variables_map.get(v_id) == var_name:
                b_type = block.get('type')
                if b_type in sdtype_map:
                    columns_metadata[var_name] = {"sdtype": sdtype_map[b_type]}
                    found = True
                    break
        if not found:
            columns_metadata[var_name] = {"sdtype": "text"}

    return {"METADATA_SPEC_VERSION": "SINGLE_TABLE_V1", "columns": columns_metadata}

def extract_questions(bot_data):
    """Extrai os textos das perguntas associadas às variáveis."""
    questions_map = {}
    bot = bot_data.get('typebot', {})
    if not bot: return {}

    variables_map = {v['id']: v['name'] for v in bot.get('variables', [])}
    
    for group in bot.get('groups', []):
        blocks = group.get('blocks', [])
        last_text_content = ""
        
        for block in blocks:
            block_type = block.get('type')
            if block_type == 'text':
                rich_text = block.get('content', {}).get('richText', [])
                lines = []
                for element in rich_text:
                    children = element.get('children', [])
                    text = "".join([c.get('text', '') for c in children])
                    if text.strip(): lines.append(text.strip())
                if lines: last_text_content = "\n".join(lines)
            elif 'input' in block_type:
                var_id = block.get('options', {}).get('variableId')
                if var_id:
                    var_name = variables_map.get(var_id)
                    if var_name:
                        if last_text_content:
                            questions_map[var_name] = last_text_content
                            last_text_content = ""
                            
    return questions_map

def generate_documentation(bot_schema, output_file):
    """Gera documentação Markdown da estrutura."""
    variables_list = bot_schema.get("variables", [])
    columns_info = bot_schema.get("metadata", {}).get("columns", {})
    constraints = bot_schema.get("constraints", [])
    questions_map = bot_schema.get("questions", {})
    
    sorted_variables = variables_list
    md_content = [
        "# Documentação da Estrutura do Bot",
        f"**Gerado em:** {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        "",
        "## 1. Lista de Perguntas (Variáveis)",
        "| Nome da Variável | Pergunta (Contexto) | Tipo de Dado |",
        "| :--- | :--- | :--- |"
    ]
    
    type_trans = {"text": "Texto", "numerical": "Numérico", "categorical": "Opções", "email": "E-mail", "phone_number": "Telefone", "datetime": "Data"}

    for var in sorted_variables:
        raw_type = columns_info.get(var, {}).get("sdtype", "text")
        display_type = type_trans.get(raw_type, raw_type)
        q_text = questions_map.get(var, "").replace("\n", "<br>")
        md_content.append(f"| {var} | {q_text} | {display_type} |")
    
    md_content.append("")
    md_content.append("## 2. Regras Lógicas")
    if constraints:
        for i, rule in enumerate(constraints, 1):
            md_content.append(f"### Regra #{i}")
            md_content.append(f"- **Se** `{rule.get('condition_column')}` {rule.get('condition_value')}")
            md_content.append(f"- **Pula:** {', '.join([f'`{c}`' for c in rule.get('affected_columns', [])])}")
            md_content.append("")
    else:
        md_content.append("_Nenhuma regra de pulo encontrada._")

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(md_content))

# ==============================================================================
# DATA EXTRACTION HELPERS
# ==============================================================================

def fetch_all_results(bot_id, token, mapping, bot_metadata):
    """Busca resultados do Typebot usando paginação e injeta metadados do bot."""
    base_url = f"{API_BASE_URL}/typebots/{bot_id}/results"
    headers = {"Authorization": f"Bearer {token}"}
    
    all_extracted_map = {}
    limit = 100
    filter_sets = [{"timeFilter": "allTime"}, {"isArchived": "true", "timeFilter": "allTime"}]
    
    print(f"Extraindo dados de {bot_metadata.get('name', bot_id)}...")
    
    for fs in filter_sets:
        offset = 0
        has_more = True
        while has_more:
            params = fs.copy()
            params.update({"limit": limit, "cursor": offset})
            try:
                response = requests.get(base_url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    has_more = False
                    break
                
                num_returned = len(results)
                batch = results[:limit] if num_returned > limit else results
                
                for res in batch:
                    res_id = res.get("id")
                    if res_id not in all_extracted_map:
                        # Injeta colunas de metadados do bot no INÍCIO
                        row = {}
                        for key, value in bot_metadata.items():
                            if not key.startswith("_"): # Evita metadados internos do script_1
                                row[f"Bot_{key.capitalize()}"] = value
                        
                        row.update({
                            "ResultId": res_id, "SubmittedAt": res.get("createdAt"),
                            "IsCompleted": res.get("isCompleted"), "ChatSessionId": res.get("lastChatSessionId")
                        })
                        
                        # Variavéis
                        for var_entry in res.get("variables", []):
                            var_name = var_entry.get("name") or mapping.get(var_entry.get("id"), var_entry.get("id"))
                            if var_name: row[var_name] = var_entry.get("value")
                        
                        # Respostas Fallback
                        for ans in res.get("answers", []):
                            content = ans.get("content")
                            if content is None: continue
                            col = mapping.get(ans.get("variableId")) or f"Ans_{ans.get('blockId')}"
                            if col not in row: row[col] = content
                        
                        all_extracted_map[res_id] = row
                
                if num_returned > limit:
                    offset += limit
                else:
                    has_more = False
            except Exception as e:
                print(f"Erro na extração: {e}")
                break
    
    return list(all_extracted_map.values())

# ==============================================================================
# BATCH / INTERACTIVE HELPERS
# ==============================================================================

def load_global_info():
    """Carrega informações globais dos bots."""
    if not os.path.exists(GLOBAL_INFO_PATH):
        print(f"Erro: Arquivo {GLOBAL_INFO_PATH} não encontrado. Execute o script 1 primeiro.")
        return []
    with open(GLOBAL_INFO_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

def process_bot(bot_entry, token):
    """Executa o fluxo completo para um bot."""
    bot_id = bot_entry.get("id")
    bot_name = bot_entry.get("name", bot_id)
    safe_name = re.sub(r'[^\w\-_\. ]', '_', bot_name).replace(' ', '_')
    
    bot_output_dir = os.path.join(OUTPUT_DIR, safe_name)
    os.makedirs(bot_output_dir, exist_ok=True)
    
    # 1. Estrutura
    bot_structure = fetch_bot_structure(bot_id, token)
    if not bot_structure: 
        print(f"Falha ao obter estrutura de {bot_name}.")
        return

    save_json_file(bot_structure, os.path.join(bot_output_dir, 'structure.json'))
    
    ordered_vars = get_ordered_variables(bot_structure)
    schema = {
        "variables": ordered_vars,
        "constraints": generate_constraints(bot_structure),
        "metadata": generate_metadata(bot_structure, ordered_vars),
        "questions": extract_questions(bot_structure)
    }
    save_json_file(schema, os.path.join(bot_output_dir, 'schema.json'))
    generate_documentation(schema, os.path.join(bot_output_dir, 'docs.md'))
    
    # 2. Dados
    mapping = {v['id']: v['name'] for v in bot_structure.get('typebot', {}).get('variables', [])}
    results = fetch_all_results(bot_id, token, mapping, bot_entry)
    
    if results:
        df = pd.DataFrame(results)
        # Ordenação de colunas: Bot Meta -> Fixed -> Ordered Vars -> Others
        meta_cols = [c for c in df.columns if c.startswith("Bot_")]
        fixed_cols = ["ResultId", "SubmittedAt", "IsCompleted", "ChatSessionId"]
        ordered_cols = [v for v in ordered_vars if v in df.columns]
        other_cols = [c for c in df.columns if c not in meta_cols + fixed_cols + ordered_cols]
        
        df = df[meta_cols + fixed_cols + ordered_cols + other_cols]
        
        if not os.path.exists('DB'): os.makedirs('DB')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        csv_path = f'DB/{safe_name}_results_{timestamp}.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"CSV salvo: {csv_path}")
    else:
        print(f"Nenhum resultado para {bot_name}.")

def main():
    parser = argparse.ArgumentParser(description="Extração de dados do Typebot")
    parser.add_argument("-a", "--all", action="store_true", help="Baixa todos os bots")
    parser.add_argument("-m", "--multi", action="store_true", help="Seleção múltipla interativa")
    parser.add_argument("-l", "--list", action="store_true", help="Lista e seleciona um bot")
    args = parser.parse_args()

    bots = load_global_info()
    if not bots: return

    selected_bots = []

    if args.all:
        selected_bots = bots
        print(f"Modo ALL: Processando {len(bots)} bots...")
    elif args.multi or args.list:
        print("\n--- LISTA DE BOTS DISPONÍVEIS ---")
        for i, bot in enumerate(bots, 1):
            folder = bot.get("_folderName", "Raiz")
            print(f"[{i}] {bot.get('name')} (Pasta: {folder})")
        
        selection = input("\nDigite os números dos bots (ex: 1,3,5) ou um único número: ")
        try:
            indices = [int(i.strip()) - 1 for i in selection.split(',')]
            selected_bots = [bots[i] for i in indices if 0 <= i < len(bots)]
        except:
            print("Seleção inválida.")
            return
    else:
        # Default: Procura BOT_ID no .env (como fallback compatível)
        env_bot_id = os.getenv("BOT_ID")
        if env_bot_id:
            bot_entry = next((b for b in bots if b['id'] == env_bot_id), None)
            if not bot_entry:
                bot_entry = {"id": env_bot_id, "name": "Bot_no_Env"}
            selected_bots = [bot_entry]
        else:
            print("Nenhum BOT_ID no .env e nenhuma flag especificada. Use -l ou -a.")
            return

    if not selected_bots:
        print("Nenhum bot selecionado.")
        return

    for bot in selected_bots:
        try:
            process_bot(bot, AUTH_TOKEN)
        except Exception as e:
            print(f"Erro ao processar {bot.get('name')}: {e}")

if __name__ == "__main__":
    main()
