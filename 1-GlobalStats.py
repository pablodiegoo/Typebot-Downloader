import os
import requests
import json
import time
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

BOT_ID = os.getenv("BOT_ID")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
API_BASE_URL = os.getenv("API_BASE_URL", "https://typebot.io/api/v1")
OUTPUT_DIR = 'output'

if not BOT_ID or not AUTH_TOKEN:
    print("Erro: BOT_ID e AUTH_TOKEN devem estar configurados no arquivo .env.")
    exit()

# ==============================================================================

def fetch_current_bot_details(bot_id, token):
    """Busca detalhes do bot atual para descobrir o workspaceId."""
    url = f"{API_BASE_URL}/typebots/{bot_id}"
    headers = {"Authorization": f"Bearer {token}"}
    
    print(f"Buscando detalhes do bot ID: {bot_id} para encontrar Workspace ID...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Erro ao buscar bot: {e}")
        if response.status_code == 404:
            print("Verifique se o BOT_ID está correto.")
        elif response.status_code == 401:
            print("Erro de autenticação. Verifique seu AUTH_TOKEN.")
        return None
    except Exception as e:
        print(f"Erro inesperado: {e}")
        return None

def fetch_workspace_typebots(workspace_id, token):
    """Lista todos os typebots do workspace."""
    url = f"{API_BASE_URL}/typebots"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"workspaceId": workspace_id}

    print(f"Listando todos os bots do Workspace ID: {workspace_id}...")
    try:
        response = requests.get(url, headers=headers, params=params)
        
        # Se der erro 500, tenta sem o workspaceId como fallback
        if response.status_code >= 500:
            print(f"Erro {response.status_code} com workspaceId. Tentando sem parâmetros...")
            response = requests.get(url, headers=headers)
            
        if response.status_code != 200:
            print(f"Erro na requisição: {response.status_code}")
            print(f"Detalhes: {response.text}")
            response.raise_for_status()

        data = response.json()
        # A resposta pode ser uma lista direta ou um objeto com chave 'typebots'
        if isinstance(data, list):
            return data
        return data.get("typebots", [])
    except Exception as e:
        print(f"Erro ao listar bots do workspace: {e}")
        return []

def save_json(data, filename):
    filepath = os.path.join(OUTPUT_DIR, filename)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Dados salvos em {filepath}")

def generate_markdown_report(typebots, workspace_id):
    filepath = os.path.join(OUTPUT_DIR, "global_info_report.md")
    
    # Agrupar por Nome da Pasta (usando o metadado injetado _folderName)
    folders = {}
    
    for bot in typebots:
        # Usa o nome da pasta injetado ou "Indefinido"
        folder_name = bot.get("_folderName", "Indefinido")
        
        # Se for Raiz, tratamos separado ou como uma pasta "Raiz"
        if folder_name not in folders:
            folders[folder_name] = []
        folders[folder_name].append(bot)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"# Relatório Global do Workspace\n\n")
        f.write(f"**Workspace ID:** `{workspace_id}`\n")
        f.write(f"**Total de Bots encontrados:** {len(typebots)}\n\n")
        
        # Primeiro, exibe bots da Raiz se houver
        if "Raiz" in folders and folders["Raiz"]:
            f.write("## Bots na Raiz\n")
            for bot in folders["Raiz"]:
                name = bot.get("name", "Sem Nome")
                public_id = bot.get("publicId", "N/A")
                f.write(f"- **{name}** (Public ID: `{public_id}`)\n")
            del folders["Raiz"]
        else:
            f.write("## Bots na Raiz\n_Nenhum bot encontrado na raiz ou falha ao listar raiz._\n")
        
        f.write("\n## Bots por Pasta\n")
        if not folders:
            f.write("_Nenhuma outra pasta com bots detectada._\n")
        
        for folder_name, bots in folders.items():
            f.write(f"\n### Pasta: {folder_name}\n")
            for bot in bots:
                name = bot.get("name", "Sem Nome")
                bot_id = bot.get("id")
                public_id = bot.get("publicId", "N/A")
                f.write(f"- **{name}** (ID: `{bot_id}`, Public ID: `{public_id}`)\n")
    
    print(f"Relatório gerado em {filepath}")

def fetch_workspaces(token):
    """Lista os workspaces do usuário."""
    url = f"{API_BASE_URL}/workspaces"
    headers = {"Authorization": f"Bearer {token}"}
    
    print("Buscando lista de workspaces...")
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 404:
            print("Endpoint /workspaces não encontrado.")
            return []
        
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        return data.get("workspaces", [])
    except Exception as e:
        print(f"Erro ao buscar workspaces: {e}")
        return []

def fetch_workspace_folders(workspace_id, token):
    """Lista as pastas do workspace."""
    url = f"{API_BASE_URL}/folders"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"workspaceId": workspace_id} # Alguns endpoints pedem workspaceId na query

    print(f"Buscando pastas do Workspace ID: {workspace_id}...")
    try:
        response = requests.get(url, headers=headers, params=params)
        
        # Se 404, pode ser que o endpoint seja diferente ou não existam pastas
        if response.status_code == 404:
             # Tenta endpoint alternativo se houver (ex: /workspaces/{id}/folders)
             alt_url = f"{API_BASE_URL}/workspaces/{workspace_id}/folders"
             response = requests.get(alt_url, headers=headers)

        if response.status_code != 200:
             print(f"Erro ao buscar pastas: {response.status_code} - {response.text}")
             return []

        data = response.json()
        if isinstance(data, list):
            return data
        return data.get("folders", [])
    except Exception as e:
        print(f"Erro ao buscar pastas: {e}")
        return []

def fetch_bots_by_folder(workspace_id, folder_id, token):
    """Lista bots dentro de uma pasta específica ou na raiz (folder_id=None)."""
    url = f"{API_BASE_URL}/typebots"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"workspaceId": workspace_id}
    
    if folder_id:
        params["folderId"] = folder_id
    
    print(f"Buscando bots na pasta {folder_id if folder_id else 'RAIZ'}...")

    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
             # Não falha hard, apenas loga e retorna vazio para continuar outras pastas
             print(f"Erro ao buscar bots da pasta {folder_id}: {response.status_code}")
             return []
             
        data = response.json()
        if isinstance(data, list):
            return data
        return data.get("typebots", [])
    except Exception as e:
        print(f"Exceção ao buscar bots da pasta {folder_id}: {e}")
        return []

def main():
    # 0. Verifica se já temos o Workspace ID no .env
    env_workspace_id = os.getenv("WORKSPACE_ID")
    workspaces = []

    if env_workspace_id:
        print(f"Usando WORKSPACE_ID do .env: {env_workspace_id}")
        workspaces = [{"id": env_workspace_id, "name": "Workspace Configurado (.env)"}]
    else:
        # Tenta listar workspaces
        workspaces = fetch_workspaces(AUTH_TOKEN)
    
    all_bots = []
    if not workspaces:
        print("Nenhum workspace encontrado/configurado.")
        return

    for ws in workspaces:
        ws_id = ws.get("id")
        ws_name = ws.get("name", "Desconhecido")
        print(f"\n---> Processando Workspace: {ws_name} ({ws_id})")
        
        # 1. Busca pastas
        folders = fetch_workspace_folders(ws_id, AUTH_TOKEN)
        print(f"Encontradas {len(folders)} pastas.")

        # 2. Busca bots na RAIZ (sem folderId)
        root_bots = fetch_bots_by_folder(ws_id, None, AUTH_TOKEN)
        for b in root_bots:
            b["_folderName"] = "Raiz" # Marcador para o relatório
        all_bots.extend(root_bots)

        # 3. Busca bots em cada pasta
        for folder in folders:
            f_id = folder.get("id")
            f_name = folder.get("name", "Sem Nome")
            folder_bots = fetch_bots_by_folder(ws_id, f_id, AUTH_TOKEN)
            
            # Adiciona metadados para relatório
            for b in folder_bots:
                b["_folderName"] = f_name
            
            all_bots.extend(folder_bots)

    # 4. Salvar resultados
    save_json(all_bots, "global_info.json")
    
    # 5. Gerar relatório legível
    primary_ws_id = workspaces[0].get("id") if workspaces else "N/A"
    generate_markdown_report(all_bots, primary_ws_id)


if __name__ == "__main__":
    main()
