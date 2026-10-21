# Databricks notebook source
# MAGIC %md
# MAGIC # Catálogo completo de indicadores 4intelligence
# MAGIC Lista código e nome de TODOS os indicadores do Feature Store da 4i e procura os que ainda estão sem código.
# MAGIC
# MAGIC Usa o login da 4i do notebook `notebooks/token_4i`.

# COMMAND ----------
# MAGIC %run ../token_4i

# COMMAND ----------
# 1) Configuração
import time, unicodedata, requests, pandas as pd

BASE = "https://apis.4intelligence.ai/api-feature-store/api/v1/"   # mesma base usada pelo pacote oficial da 4i

HEADERS = get_headers()   # vem da célula do token; se faltar menos de 15 min pra expirar, ela renova
print("Token OK")

# COMMAND ----------
# 2) Baixa o catálogo inteiro, página por página
def baixar(endpoint, limit=500, pausa=1.0):
    global HEADERS
    linhas, skip, renovou = [], 0, False
    while True:
        r = requests.get(BASE + endpoint, headers=HEADERS, params={"limit": limit, "skip": skip}, timeout=60)
        if r.status_code == 401 and not renovou:   # token venceu no meio: renova uma vez e tenta de novo
            HEADERS = {"Authorization": refresh()}; renovou = True; continue
        if r.status_code == 429:            # limite de requisições: espera e tenta de novo
            time.sleep(30); continue
        if r.status_code != 200:
            print(f"{endpoint}: HTTP {r.status_code} -> {r.text[:300]}")
            return linhas
        corpo = r.json()
        pagina = corpo.get("data", corpo) if isinstance(corpo, dict) else corpo
        if isinstance(pagina, dict):        # alguns endpoints devolvem {"data": {"items": [...]}}
            pagina = pagina.get("items") or pagina.get("results") or []
        if not pagina:
            return linhas
        linhas += pagina
        print(f"{endpoint}: {len(linhas)} registros...")
        if len(pagina) < limit:
            return linhas
        skip += limit
        time.sleep(pausa)

brutos = baixar("indicators")
if not brutos:                              # se "indicators" não existir pra sua conta, tenta a lista de séries
    brutos = baixar("series")

# COMMAND ----------
# 3) Organiza em tabela (nome em pt-br quando existir)
df = pd.json_normalize(brutos)
print("Colunas que vieram da API:", list(df.columns))

def col(*opcoes):
    for c in opcoes:
        if c in df.columns:
            return df[c]
    return pd.Series([None] * len(df))

catalogo = pd.DataFrame({
    "indicator_code": col("indicator_code", "code"),
    "name_api":       col("name.pt-br", "name.en-us", "name"),
    "short_name_api": col("short_name.pt-br", "short_name.en-us", "short_name"),
    "source":         col("source.pt-br", "source.en-us", "source"),
}).drop_duplicates().sort_values("name_api").reset_index(drop=True)

print(f"{len(catalogo)} indicadores no catálogo")
display(catalogo)

# Opcional: salvar pra consultar depois (ajuste o caminho do Volume)
# catalogo.to_csv("/Volumes/lcb_im_stage/default/4i/catalogo_4i.csv", index=False)

# COMMAND ----------
# 4) Busca por palavra-chave (sem acento, sem maiúscula). Edite a lista à vontade.
def sem_acento(s):
    return unicodedata.normalize("NFD", str(s)).encode("ascii", "ignore").decode().lower()

texto = (catalogo["name_api"].fillna("") + " | " + catalogo["short_name_api"].fillna("") + " | " + catalogo["source"].fillna("")).map(sem_acento)

PROCURAR = {
    "Produção de Linha Branca":                    ["linha branca", "fogoes", "refrigeradores", "maquinas de lavar"],
    "Unidades Lançadas - Todos":                   ["unidades lancadas", "abrainc"],
    "Unidades Vendidas - Todos":                   ["unidades vendidas", "abrainc"],
    "Secovi SP - Vendas de Imóveis Novos":         ["vendas do mercado imobiliario", "secovi"],
    "Vendas de Materiais de Construção":           ["abramat", "materiais de construcao", "material de construcao"],
    "ANIP - Vendas de Pneus":                      ["anip", "pneus"],
    "Produção de Motocicletas":                    ["motos", "motocicletas", "abraciclo"],
}

for alvo, termos in PROCURAR.items():
    achou = catalogo[texto.str.contains("|".join(termos), regex=True)]
    print(f"\n### {alvo}: {len(achou)} resultado(s)")
    if len(achou):
        print(achou.to_string(index=False, max_colwidth=80))
