import requests
import json
import os
import re
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÕES E CARREGAMENTO DE VARIÁREIS
# ==============================================================================
load_dotenv()

BOT_ID = os.getenv("BOT_ID")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
API_BASE_URL = os.getenv("API_BASE_URL", "https://typebot.io/api/v1")
OUTPUT_DIR = 'output'

# Caminhos de Saída
RESULTS_JSON_PATH = os.path.join(OUTPUT_DIR, 'results.json')
SCHEMA_JSON_PATH = os.path.join(OUTPUT_DIR, 'bot_schema.json')
DOCUMENTATION_PATH = os.path.join(OUTPUT_DIR, 'documentacao_do_bot.md')
OUTPUT_CSV_PATH = f'DB/extracted_results_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'

if not BOT_ID or not AUTH_TOKEN:
    print("Erro: BOT_ID e AUTH_TOKEN devem estar configurados no arquivo .env.")
    exit()

# ==============================================================================
# PART 1: LOGIC EXTRACTION HELPERS
# ==============================================================================

def fetch_bot_structure(bot_id, token):
    """Busca a estrutura do bot na API do Typebot."""
    url = f"{API_BASE_URL}/typebots/{bot_id}"
    headers = {"Authorization": f"Bearer {token}"}
    print(f"Buscando estrutura do bot ID: {bot_id}...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        print("Estrutura do bot obtida com sucesso.")
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
    print(f"Dados salvos em {file_path}")

def get_ordered_variables(bot_data):
    """
    Rastreia o fluxo do bot para determinar a ordem cronológica das variáveis.
    """
    print("\nRastreando fluxo do bot para determinar sequência de variáveis...")
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
    
    if not start_targets and groups:
        start_targets.append({'groupId': list(groups.keys())[0]})

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
    
    # Adicionar variáveis não encontradas no fluxo
    for var_name in variables.values():
        if var_name not in ordered_variables:
            ordered_variables.append(var_name)
            
    print(f"Sequência determinada para {len(ordered_variables)} variáveis.")
    return ordered_variables

def generate_constraints(bot_data):
    """Gera regras de lógica (loops e condicionais)."""
    print("\nAnalisando lógica do bot para encontrar caminhos condicionais...")
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
                try:
                    cond_value = comparison.get('value', '')
                except:
                    cond_value = ''
                
                if not cond_var_name: continue
                
                # Encontrar aresta de saída
                outgoing_edge_id = item.get('outgoingEdgeId')
                target_edge = next((e for e in edges if e.get('id') == outgoing_edge_id), None)
                if not target_edge: continue
                
                # Identificar pulos
                to_info = target_edge.get('to', {})
                to_block_id = to_info.get('blockId')
                to_group_id = to_info.get('groupId')
                
                skipped_var_ids = []
                from_group_id = block_to_group.get(block_id)
                
                if from_group_id:
                    block_order = group_block_orders.get(from_group_id, [])
                    if to_group_id and to_group_id != from_group_id:
                        # Pula restante do grupo
                        try:
                            start_index = block_order.index(block_id)
                            skipped_block_ids = block_order[start_index + 1:]
                            for sb_id in skipped_block_ids:
                                sb = all_blocks.get(sb_id)
                                if sb and 'input' in sb.get('type', '').lower():
                                    if vid := sb.get('options', {}).get('variableId'): skipped_var_ids.append(vid)
                        except ValueError: pass
                    elif to_block_id and to_block_id in block_order:
                        # Pula dentro do grupo
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
                    cond_desc = f"{operator} {cond_value}"
                    constraints.append({
                        "condition_column": cond_var_name,
                        "condition_value": cond_desc,
                        "affected_columns": affected_columns
                    })
                    
    return constraints

def generate_metadata(bot_data, ordered_vars=None):
    """Gera metadados (tipos de dados) dos campos."""
    print("\nGerando metadados dos campos...")
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
    print("\nExtraindo textos das perguntas...")
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
                            
    print(f"Textos extraídos para {len(questions_map)} perguntas.")
    return questions_map

def generate_documentation(bot_schema, output_file):
    """Gera documentação Markdown."""
    variables_list = bot_schema.get("variables", [])
    columns_info = bot_schema.get("metadata", {}).get("columns", {})
    constraints = bot_schema.get("constraints", [])
    questions_map = bot_schema.get("questions", {})
    
    # Sort natural
    def natural_sort_key(s):
        return [int(t) if t.isdigit() else t.lower() for t in re.split('([0-9]+)', s)]
    
    sorted_variables = variables_list if bot_schema.get("_ordered") else sorted(variables_list, key=natural_sort_key)

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
    print(f"✅ Documentação gerada em: {output_file}")

# ==============================================================================
# PART 2: DATA EXTRACTION HELPERS
# ==============================================================================

def fetch_all_results(bot_id, token, mapping):
    """Busca resultados do Typebot usando paginação."""
    base_url = f"{API_BASE_URL}/typebots/{bot_id}/results"
    headers = {"Authorization": f"Bearer {token}"}
    
    all_extracted_map = {}
    limit = 100
    filter_sets = [{"timeFilter": "allTime"}, {"isArchived": "true", "timeFilter": "allTime"}]
    
    print(f"\nIniciando extração de dados do bot: {bot_id}")
    
    for fs in filter_sets:
        offset = 0
        has_more = True
        page = 1
        print(f"--- Filtro: {fs} ---")
        
        while has_more:
            params = fs.copy()
            params.update({"limit": limit, "cursor": offset})
            
            try:
                print(f"Buscando página {page} (Offset {offset})...", end="\r")
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
                        row = {
                            "ResultId": res_id, "SubmittedAt": res.get("createdAt"),
                            "IsCompleted": res.get("isCompleted"), "ChatSessionId": res.get("lastChatSessionId")
                        }
                        
                        # Processa variáveis
                        for var_entry in res.get("variables", []):
                            var_name = var_entry.get("name")
                            if not var_name:
                                var_name = mapping.get(var_entry.get("id"), var_entry.get("id"))
                            if var_name: row[var_name] = var_entry.get("value")
                        
                        # Processa respostas (fallback)
                        for ans in res.get("answers", []):
                            content = ans.get("content")
                            if content is None: continue
                            
                            var_id, block_id = ans.get("variableId"), ans.get("blockId")
                            col_name = mapping.get(var_id)
                            if not col_name and var_id: col_name = f"Var_{var_id}"
                            if not col_name and block_id: col_name = f"Block_{block_id}"
                            if not col_name: col_name = f"Answer_{block_id}"
                            
                            if col_name and col_name not in row: row[col_name] = content
                        
                        all_extracted_map[res_id] = row
                
                if num_returned > limit:
                    offset += limit
                    page += 1
                else:
                    has_more = False
                    
            except Exception as e:
                print(f"\nErro no offset {offset}: {e}")
                break
    
    final_list = list(all_extracted_map.values())
    print(f"\nTotal de registros únicos extraídos: {len(final_list)}")
    return final_list

# ==============================================================================
# MAIN COMBINADO
# ==============================================================================

def main():
    print(">>> FASE 1: Extração de Lógica e Estrutura <<<")
    bot_structure = fetch_bot_structure(BOT_ID, AUTH_TOKEN)
    if not bot_structure:
        return

    # Salva estrutura bruta
    save_json_file(bot_structure, RESULTS_JSON_PATH, indent=4)

    # Processa lógica
    ordered_vars = get_ordered_variables(bot_structure)
    constraints = generate_constraints(bot_structure)
    metadata = generate_metadata(bot_structure, ordered_vars)
    questions = extract_questions(bot_structure)

    # Cria schema consolidado
    schema = {
        "variables": ordered_vars,
        "_ordered": True,
        "constraints": constraints,
        "metadata": metadata,
        "questions": questions
    }
    save_json_file(schema, SCHEMA_JSON_PATH, indent=4)
    generate_documentation(schema, DOCUMENTATION_PATH)

    print("\n>>> FASE 2: Extração de Respostas e Dados <<<")
    
    # Cria mapa de variáveis (ID -> Nome) baseado na estrutura recém baixada
    variables_map = {v['id']: v['name'] for v in bot_structure.get('typebot', {}).get('variables', [])}
    
    results = fetch_all_results(BOT_ID, AUTH_TOKEN, variables_map)
    
    if results:
        # Cria DataFrame
        df = pd.DataFrame(results)
        
        # Ordenação de Colunas
        fixed_cols = ["ResultId", "SubmittedAt", "IsCompleted", "ChatSessionId"]
        dynamic_cols = [c for c in df.columns if c not in fixed_cols]
        
        # Tenta ordenar dinâmicas pela ordem lógica do bot
        sorted_dynamic = [v for v in ordered_vars if v in dynamic_cols]
        remaining = [c for c in dynamic_cols if c not in sorted_dynamic]
        
        final_cols = fixed_cols + sorted_dynamic + remaining
        df = df[final_cols]
        
        if not os.path.exists('DB'): os.makedirs('DB')
        df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8-sig')
        print(f"\nExtracão Concluída! CSV salvo em: {OUTPUT_CSV_PATH}")
    else:
        print("Nenhum resultado encontrado para salvar.")

if __name__ == "__main__":
    main()
