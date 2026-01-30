import requests
import json
import os
import re
import pandas as pd
from dotenv import load_dotenv

# ==============================================================================
# LOAD ENVIRONMENT VARIABLES
# ==============================================================================
load_dotenv()

BOT_ID = os.getenv("BOT_ID")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
OUTPUT_DIR = 'output'

API_BASE_URL = os.getenv("API_BASE_URL", "https://typebot.io/api/v1")

if not BOT_ID or not AUTH_TOKEN:
    print("Error: BOT_ID and AUTH_TOKEN must be set in the .env file.")
    exit()
# ==============================================================================

def fetch_bot_structure(bot_id, token):
    """Fetches the bot structure from the Typebot API."""
    url = f"{API_BASE_URL}/typebots/{bot_id}"
    headers = {"Authorization": f"Bearer {token}"}
    print(f"Fetching bot structure for bot ID: {bot_id}...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        print("Successfully fetched bot structure.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Typebot API: {e}")
        return None

def save_json_file(data, file_path, indent=2):
    """Saves data to a JSON file."""
    output_dir = os.path.dirname(file_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
    print(f"Data saved to {file_path}")

def find_downstream_variables(start_block_id, end_block_id, blocks, block_order):
    """Finds all variables between a start and end block in the linear flow."""
    downstream_vars = []
    try:
        start_index = block_order.index(start_block_id)
        end_index = block_order.index(end_block_id)
    except ValueError:
        try:
            start_index = block_order.index(start_block_id)
            end_index = len(block_order)
        except ValueError:
            return []

    skipped_block_ids = block_order[start_index + 1 : end_index]
    
    for block_id in skipped_block_ids:
        block = blocks.get(block_id)
        if block and block.get('options', {}).get('variableId'):
            downstream_vars.append(block['options']['variableId'])
            
    return downstream_vars

def get_ordered_variables(bot_data):
    """
    Traces the bot flow from the starting event to identify the chronological 
    order in which variables are collected.
    """
    print("\nTracing bot flow to determine variable sequence...")
    bot = bot_data.get('typebot', {})
    if not bot:
        return []

    groups = {g['id']: g for g in bot.get('groups', [])}
    edges = bot.get('edges', [])
    events = bot.get('events', [])
    variables = {v['id']: v['name'] for v in bot.get('variables', [])}

    def get_targets(block_id=None, group_id=None, item_id=None, event_id=None):
        targets = []
        for edge in edges:
            f = edge.get('from', {})
            if event_id and f.get('eventId') == event_id:
                targets.append(edge.get('to', {}))
            elif item_id and f.get('itemId') == item_id:
                targets.append(edge.get('to', {}))
            elif block_id and f.get('blockId') == block_id and not f.get('itemId'):
                targets.append(edge.get('to', {}))
            elif group_id and f.get('groupId') == group_id:
                targets.append(edge.get('to', {}))
        return targets

    # Find start entry point
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
        if group_id in visited_groups:
            return
        # We don't mark visited_groups here yet because we might jump back to it 
        # at a specific block, but for simplicity in a survey bot, we'll mark it.
        visited_groups.add(group_id)
        
        group = groups.get(group_id)
        if not group: return
        
        blocks = group.get('blocks', [])
        
        # Determine starting index
        start_idx = 0
        if start_block_id:
            for i, b in enumerate(blocks):
                if b['id'] == start_block_id:
                    start_idx = i
                    break
        
        for i in range(start_idx, len(blocks)):
            block = blocks[i]
            
            # Capture variable if present
            var_id = block.get('options', {}).get('variableId')
            if var_id and var_id in variables:
                var_name = variables[var_id]
                if var_name not in ordered_variables:
                    ordered_variables.append(var_name)
            
            # Follow edges from block items (inclusive discovery)
            for item in block.get('items', []):
                targets = get_targets(item_id=item.get('id'))
                for target in targets:
                    tg = target.get('groupId')
                    tb = target.get('blockId')
                    if tg: trace_flow(tg, tb)

            # Follow outgoing edges from the block itself
            for target in get_targets(block_id=block.get('id')):
                tg = target.get('groupId')
                tb = target.get('blockId')
                if tg: trace_flow(tg, tb)
            
            # Special case for Jump blocks
            if block.get('type') == 'Jump':
                target_g = block.get('options', {}).get('groupId')
                target_b = block.get('options', {}).get('blockId')
                if target_g: trace_flow(target_g, target_b)

    for target in start_targets:
        tg = target.get('groupId')
        tb = target.get('blockId')
        if tg: trace_flow(tg, tb)
    
    # Safety: add variables that weren't hit by trace (optional but helps robustness)
    for var_name in variables.values():
        if var_name not in ordered_variables:
            ordered_variables.append(var_name)
            
    print(f"Determined sequence for {len(ordered_variables)} variables.")
    if ordered_variables:
        print(f"First 3 variables: {ordered_variables[:3]}")
        
    return ordered_variables

def generate_constraints(bot_data):
    """Generates constraints from the bot structure data."""
    print("\nAnalyzing bot logic to find conditional paths...")
    bot = bot_data.get('typebot', {})
    if not bot:
        print("Error: 'typebot' key not found in the JSON structure.")
        return []

    variables = {v['id']: v['name'] for v in bot.get('variables', [])}
    groups = {g['id']: g for g in bot.get('groups', [])}
    
    # Build a comprehensive blocks dictionary across all groups
    all_blocks = {}
    block_to_group = {}
    for group in bot.get('groups', []):
        for block in group.get('blocks', []):
            all_blocks[block['id']] = block
            block_to_group[block['id']] = group['id']
    
    # Build block order within each group
    group_block_orders = {}
    for group in bot.get('groups', []):
        group_block_orders[group['id']] = [b['id'] for b in group.get('blocks', [])]
    
    edges = bot.get('edges', [])
    constraints = []

    # Process Condition blocks
    for block_id, block in all_blocks.items():
        if block.get('type') == 'Condition':
            items = block.get('items', [])
            
            for item in items:
                content = item.get('content', {})
                comparisons = content.get('comparisons', [])
                
                if not comparisons:
                    continue
                
                # Get the first comparison (main condition)
                comparison = comparisons[0]
                condition_variable_id = comparison.get('variableId')
                condition_variable_name = variables.get(condition_variable_id)
                operator = comparison.get('comparisonOperator', '')
                condition_value = comparison.get('value', '')
                
                if not condition_variable_name:
                    continue
                
                # Find the edge for this item
                outgoing_edge_id = item.get('outgoingEdgeId')
                target_edge = None
                for edge in edges:
                    if edge.get('id') == outgoing_edge_id:
                        target_edge = edge
                        break
                
                if not target_edge:
                    continue
                
                # Determine what blocks are skipped
                to_info = target_edge.get('to', {})
                to_block_id = to_info.get('blockId')
                to_group_id = to_info.get('groupId')
                
                skipped_var_ids = []
                from_group_id = block_to_group.get(block_id)
                
                if from_group_id:
                    block_order = group_block_orders.get(from_group_id, [])
                    
                    # If jumping to a different group, all remaining blocks in current group are skipped
                    if to_group_id and to_group_id != from_group_id:
                        try:
                            start_index = block_order.index(block_id)
                            skipped_block_ids = block_order[start_index + 1:]
                            for skipped_block_id in skipped_block_ids:
                                skipped_block = all_blocks.get(skipped_block_id)
                                if skipped_block:
                                    # Check for input blocks with variables
                                    if 'input' in skipped_block.get('type', '').lower():
                                        var_id = skipped_block.get('options', {}).get('variableId')
                                        if var_id:
                                            skipped_var_ids.append(var_id)
                        except ValueError:
                            pass
                    
                    # If jumping within the same group
                    elif to_block_id and to_block_id in block_order:
                        try:
                            start_index = block_order.index(block_id)
                            end_index = block_order.index(to_block_id)
                            skipped_block_ids = block_order[start_index + 1:end_index]
                            for skipped_block_id in skipped_block_ids:
                                skipped_block = all_blocks.get(skipped_block_id)
                                if skipped_block:
                                    # Check for input blocks with variables
                                    if 'input' in skipped_block.get('type', '').lower():
                                        var_id = skipped_block.get('options', {}).get('variableId')
                                        if var_id:
                                            skipped_var_ids.append(var_id)
                        except ValueError:
                            pass
                
                affected_columns = [variables[var_id] for var_id in skipped_var_ids if var_id in variables]

                if affected_columns:
                    # Format the condition description
                    condition_desc = f"{operator} {condition_value}"
                    if operator == "Equal to":
                        condition_desc = condition_value
                    elif operator == "Contains":
                        condition_desc = f"contém {condition_value}"
                    elif operator == "Does not contain":
                        condition_desc = f"não contém {condition_value}"
                    elif operator == "Not equal":
                        condition_desc = f"diferente de {condition_value}"
                    
                    constraint = {
                        "condition_column": condition_variable_name,
                        "condition_value": condition_desc,
                        "affected_columns": affected_columns
                    }
                    constraints.append(constraint)
                    print(f"Found rule: IF '{condition_variable_name}' {condition_desc}, THEN {affected_columns} are skipped.")
    
    return constraints

def generate_metadata(bot_data, ordered_vars=None):
    """Generates SDV metadata from the bot structure."""
    print("\nGenerating SDV metadata...")
    bot = bot_data.get('typebot', {})
    if not bot:
        print("Error: 'typebot' key not found in the JSON structure.")
        return None

    variables_map = {v['id']: v['name'] for v in bot.get('variables', [])}
    
    # Iterate through ALL groups, not just the first one
    all_blocks = []
    for group in bot.get('groups', []):
        all_blocks.extend(group.get('blocks', []))

    sdtype_map = {
        'text input': 'text',
        'choice input': 'categorical',
        'rating input': 'numerical',
        'number input': 'numerical',
        'email input': 'email',
        'phone number input': 'phone_number',
        'date input': 'datetime',
        'url input': 'url'
    }

    columns_metadata = {}
    
    # If we have an ordered list, fill it in that order first
    if ordered_vars:
        for var_name in ordered_vars:
            # Find the block type for this variable
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
                # Default sdtype if not found in input blocks
                columns_metadata[var_name] = {"sdtype": "text"}
    else:
        # Fallback to original logic if no order is provided
        for block in all_blocks:
            variable_id = block.get('options', {}).get('variableId')
            if variable_id:
                variable_name = variables_map.get(variable_id)
                block_type = block.get('type')
                if variable_name and block_type in sdtype_map:
                    columns_metadata[variable_name] = {"sdtype": sdtype_map[block_type]}

    metadata = {
        "METADATA_SPEC_VERSION": "SINGLE_TABLE_V1",
        "columns": columns_metadata
    }
    
    print(f"Generated metadata for {len(columns_metadata)} columns.")
    return metadata

def generate_documentation(bot_schema, output_file):
    """
    Generates a Markdown documentation file from the bot schema.
    Integrated from 4-SchemaDocumentation.py.
    """
    
    variables_list = bot_schema.get("variables", [])
    metadata = bot_schema.get("metadata", {})
    columns_info = metadata.get("columns", {})
    constraints = bot_schema.get("constraints", [])
    questions_map = bot_schema.get("questions", {})

    # Sort variables only if they are not already in a specific order via schema
    if "variables" in bot_schema and bot_schema.get("_ordered"):
        sorted_variables = bot_schema["variables"]
    else:
        # Natural sort for variables
        def natural_sort_key(s):
            return [int(text) if text.isdigit() else text.lower()
                    for text in re.split('([0-9]+)', s)]

        try:
            sorted_variables = sorted(variables_list, key=natural_sort_key)
        except Exception:
            sorted_variables = sorted(variables_list)

    # Build Markdown content
    md_content = []
    
    md_content.append("# Documentação da Estrutura do Bot")
    md_content.append(f"**Gerado automaticamente em:** {os.path.basename(output_file)}")
    md_content.append("")
    md_content.append("Este documento lista todas as variáveis (perguntas) identificadas na estrutura do Typebot, juntamente com seus tipos de dados esperados e regras de lógica.")
    md_content.append("")

    # Variables table
    md_content.append("## 1. Lista de Perguntas (Variáveis)")
    md_content.append("")
    md_content.append("A tabela a seguir apresenta os campos que serão preenchidos durante a execução do fluxo.")
    md_content.append("")
    md_content.append("| Nome da Variável | Pergunta (Contexto) | Tipo de Dado (Estimado) |")
    md_content.append("| :--- | :--- | :--- |")

    type_translation = {
        "text": "Texto Livre",
        "numerical": "Numérico",
        "categorical": "Categórico (Opções)",
        "boolean": "Sim/Não",
        "datetime": "Data/Hora",
        "phone_number": "Telefone",
        "email": "E-mail",
        "id": "Identificador"
    }

    for var in sorted_variables:
        raw_type = columns_info.get(var, {}).get("sdtype", "Texto / Genérico")
        display_type = type_translation.get(raw_type, raw_type)
        question_text = questions_map.get(var, "").replace("\n", "<br>")
        md_content.append(f"| {var} | {question_text} | {display_type} |")

    md_content.append("")

    # Conditional logic section
    if constraints:
        md_content.append("## 2. Regras de Lógica (Condicionais)")
        md_content.append("")
        md_content.append("As regras abaixo definem o fluxo lógico: quando uma certa resposta faz com que perguntas subsequentes sejam puladas.")
        md_content.append("")
        
        for i, rule in enumerate(constraints, 1):
            condition_col = rule.get("condition_column", "(Desconhecido)")
            condition_val = rule.get("condition_value", "(Desconhecido)")
            affected_cols = rule.get("affected_columns", [])
            affected_str = ", ".join(f"`{col}`" for col in affected_cols)
            
            md_content.append(f"### Regra #{i}")
            md_content.append(f"- **Gatilho:** Se a variável `{condition_col}` for igual a `{condition_val}`")
            md_content.append(f"- **Efeito:** As seguintes perguntas **NÃO** serão feitas (valor será vazio):")
            md_content.append(f"  - {affected_str}")
            md_content.append("")
    else:
        md_content.append("## 2. Regras de Lógica")
        md_content.append("")
        md_content.append("Nenhuma regra de lógica condicional (pulos de pergunta) foi encontrada neste schema.")
        md_content.append("")

    # Save file
    print(f"Gerando documentação em: {output_file}...")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(md_content))
        print("✅ Documentação gerada com sucesso!")
    except IOError as e:
        print(f"❌ Erro ao salvar o arquivo de documentação: {e}")

def extract_questions(bot_data):
    """
    Extracts the text (question) immediately preceding an input block
    and maps it to the variable name.
    """
    print("\nExtracting questions from bot flow...")
    questions_map = {}
    bot = bot_data.get('typebot', {})
    if not bot:
        return {}

    variables_map = {v['id']: v['name'] for v in bot.get('variables', [])}
    
    # Iterate through all groups
    for group in bot.get('groups', []):
        blocks = group.get('blocks', [])
        last_text_content = ""
        
        for block in blocks:
            block_type = block.get('type')
            
            # If it's a text block, extract the content
            if block_type == 'text':
                rich_text = block.get('content', {}).get('richText', [])
                extracted_lines = []
                for element in rich_text:
                    children = element.get('children', [])
                    line_text = "".join([c.get('text', '') for c in children])
                    if line_text.strip():
                        extracted_lines.append(line_text.strip())
                
                if extracted_lines:
                    # Join lines with space or newline. 
                    # Using newline helps preserve structure if it's a long question.
                    last_text_content = "\n".join(extracted_lines)
            
            # If it's an input block, map the last text to the variable
            elif block_type in ['text input', 'number input', 'email input', 'url input', 
                                'date input', 'phone number input', 'choice input', 
                                'rating input', 'file input', 'payment input']:
                variable_id = block.get('options', {}).get('variableId')
                if variable_id:
                    var_name = variables_map.get(variable_id)
                    if var_name:
                        # Only assign if we have text. 
                        # If a variable is set without a preceding text block (e.g. calculation), 
                        # it won't have a question, which is correct.
                        if last_text_content:
                            questions_map[var_name] = last_text_content
                            # Reset last_text_content to avoid assigning the same question 
                            # to a subsequent unrelated block (though unlikely in standard flow)
                            last_text_content = ""
                            
    print(f"Extracted {len(questions_map)} questions.")
    return questions_map

def main():
    """
    Main function to fetch bot structure, generate a consolidated schema, and save them.
    """
    results_file = os.path.join(OUTPUT_DIR, 'results.json')
    schema_file = os.path.join(OUTPUT_DIR, 'bot_schema.json')
    documentation_file = os.path.join(OUTPUT_DIR, 'documentacao_do_bot.md')

    bot_structure_data = fetch_bot_structure(BOT_ID, AUTH_TOKEN)
    
    if bot_structure_data:
        # Save raw structure
        save_json_file(bot_structure_data, results_file, indent=4)

        # Get existing headers from CSV if available
        csv_path = os.getenv("INPUT_CSV_PATH")
        csv_headers = set()
        if csv_path and os.path.exists(csv_path):
            try:
                df_temp = pd.read_csv(csv_path, nrows=0)
                csv_headers = set(df_temp.columns)
                print(f"Loaded {len(csv_headers)} headers from {csv_path} for synchronization.")
            except Exception as e:
                print(f"Warning: Could not read CSV headers from {csv_path}: {e}")

        # Determine chronological order of variables
        ordered_vars = get_ordered_variables(bot_structure_data)
        
        # Synchronize with CSV: Prioritize variables that exist in both flow and CSV
        synced_vars = [v for v in ordered_vars if v in csv_headers or v in ['Phone', 'Name']]
        # Add remaining flow variables at the end
        for v in ordered_vars:
            if v not in synced_vars:
                synced_vars.append(v)
        
        ordered_vars = synced_vars

        # Generate and save constraints
        constraints = generate_constraints(bot_structure_data)
        print(f"\nSuccessfully extracted {len(constraints)} constraint(s).")
        
        # Generate and save metadata
        metadata = generate_metadata(bot_structure_data, ordered_vars)
        
        # Extract questions text
        questions = extract_questions(bot_structure_data)

        # Consolidate all schema artifacts into a single file
        if metadata:
            bot_schema = {
                "variables": ordered_vars,
                "_ordered": True,
                "constraints": constraints,
                "metadata": metadata,
                "questions": questions
            }
            save_json_file(bot_schema, schema_file, indent=4)
            
            # Generate documentation automatically (integrated from script 4)
            print("\n" + "="*60)
            generate_documentation(bot_schema, documentation_file)
            print("="*60)

if __name__ == "__main__":
    main()
