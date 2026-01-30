---
name: typebot-api
description: Skill focada na interação com a API do Typebot (v1/v2/v3), especialmente em instâncias auto-hospedadas. Inclui extração de lógica, coleta de resultados e simulação de conversas.
---

# Typebot API Skill

Este guia fornece instruções detalhadas para interagir com a API do Typebot em instâncias auto-hospedadas.

## 1. Configuração de Ambiente

Sempre utilize um arquivo `.env` para gerenciar as credenciais.

```env
BOT_ID="seu_bot_id"
AUTH_TOKEN="seu_token_bearer"
BASE_URL="https://seu-dominio.com.br/api/v1"
WORKSPACE_ID="seu_workspace_id"
```

## 2. Endpoints Principais (Auto-hospedado)

### Estrutura e Lógica
- `GET /typebots/{botId}`: Retorna a "planta" do bot (blocos, variáveis, grupos).
- `GET /typebots/{botId}/results`: Lista resultados (precisa de paginação).
- `GET /typebots/{botId}/results/{resultId}`: Detalhes de um resultado específico.

### Fluxo de Conversa (Runtime)
- `POST /typebots/{botId}/startChat`: Inicia uma sessão.
- `POST /sessions/{sessionId}/continueChat`: Envia resposta e avança no fluxo.

## 3. Extração de Resultados (Criação de Scripts)

Ao criar scripts para extrair dados, siga este padrão:

1. **Mapeamento de Variáveis**: Antes de baixar os resultados, baixe a estrutura (`/typebots/{botId}`) para converter IDs de variáveis em nomes amigáveis.
2. **Paginação**: Use o parâmetro `cursor` para percorrer todas as páginas.
3. **Filtro de Tempo**: A API costuma filtrar os últimos 7 dias por padrão. Use `timeFilter=allTime` para extração completa.
4. **Resilience**: Implemente tratamento de erros para códigos 401 (token expirado) e 404 (ID incorreto).

## 4. Workflows Comuns

### Extração de Lógica para ML
Ao extrair a lógica para alimentar modelos de Machine Learning (como o `multiplier_ml.py`), foque nos blocos do tipo `Condition` e nos `variableId` dos blocos de entrada.

### Upload Massivo (Simulação)
Para popular um bot com dados sintéticos:
1. Use `startChat` com `prefilledVariables`.
2. Percorra as colunas do CSV enviando cada valor via `continueChat`.
3. Capture o `sessionId` retornado no `startChat` para usar nas sequências.

## 5. Dicas para Instâncias Auto-hospedadas
- Verifique se a `BASE_URL` termina em `/api/v1` ou apenas o domínio, dependendo da versão do Typebot.
- Algumas instâncias exigem `workspaceId` em requisições de listagem (ex: `/typebots`). Obtenha o ID através de um `GET` no bot individual.
- Se o bot estiver em uma subpasta ou domínio específico, ajuste os headers de `Origin` e `Referer` se houver bloqueio de CORS.
