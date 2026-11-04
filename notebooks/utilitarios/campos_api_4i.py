# Databricks notebook source
# MAGIC %md
# MAGIC # Diagnóstico: a 4i tem data de divulgação ou calendário?
# MAGIC Só lê da API, não grava nada. Serve pra decidir se dá pra montar o **calendário de divulgações** do portal com dados da 4i.
# MAGIC
# MAGIC 1. Mostra **todos os campos** que a API devolve pro indicador, pras séries e pras observações (procure algo como
# MAGIC    `release`, `next_release`, `published_at`, `divulgacao`, `calendar`, `updated_at`).
# MAGIC 2. Testa alguns endereços que costumam existir em APIs desse tipo (`calendar`, `releases`...). HTTP 404 = não existe.
# MAGIC
# MAGIC Manda um print das duas saídas pra Lari.

# COMMAND ----------
# MAGIC %run ../token_4i

# COMMAND ----------
import time, json, requests

BASE = "https://apis.4intelligence.ai/api-feature-store/api/v1/"
TESTAR = ["BRPRC0046", "BRIND0001"]   # IPCA e produção industrial: divulgação com data marcada pelo IBGE
PALAVRAS = ("releas", "publish", "divulg", "calend", "updated", "next", "last", "date", "schedule", "agenda")

def get(path, **params):
    r = requests.get(BASE + path, headers=get_headers(), params=params, timeout=60)
    time.sleep(1)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text[:300]

def primeiro(corpo):
    d = corpo.get("data", corpo) if isinstance(corpo, dict) else corpo
    return d[0] if isinstance(d, list) and d else d

def campos_de_data(obj, prefixo=""):
    """Lista os campos (em qualquer nível) cujo nome lembra data ou divulgação."""
    achados = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            nome = f"{prefixo}{k}"
            if any(p in k.lower() for p in PALAVRAS):
                achados.append((nome, str(v)[:80]))
            achados += campos_de_data(v, nome + ".")
    elif isinstance(obj, list) and obj:
        achados += campos_de_data(obj[0], prefixo + "[0].")
    return achados

# COMMAND ----------
# 1) Campos que a API devolve
for ind in TESTAR:
    print("=" * 100, "\n", ind)
    st, cad = get(f"indicators/{ind}")
    print(f"\n-- cadastro do indicador (HTTP {st}) --")
    print(json.dumps(primeiro(cad), ensure_ascii=False, indent=1, default=str)[:3000])

    st, ser = get(f"indicators/{ind}/series", limit=5, skip=0)
    s0 = primeiro(ser) if st == 200 else None
    print(f"\n-- 1a série (HTTP {st}) --")
    print(json.dumps(s0, ensure_ascii=False, indent=1, default=str)[:2000])

    if isinstance(s0, dict) and s0.get("code"):
        st, obs = get(f"indicators/{ind}/series/{s0['code']}/observations", limit=3, skip=0)
        print(f"\n-- observações (HTTP {st}), campos de uma linha --")
        print(json.dumps(primeiro(obs), ensure_ascii=False, indent=1, default=str)[:1000])
        print("   chaves da resposta:", list(obs.keys()) if isinstance(obs, dict) else type(obs).__name__)

    print("\n>>> campos que parecem data/divulgação:")
    for nome, v in campos_de_data({"cadastro": cad, "serie": s0}):
        print(f"   {nome:<45} {v}")

# COMMAND ----------
# 2) Endereços candidatos a calendário (404 = não existe; 200 = existe, ver conteúdo)
ind = TESTAR[0]
candidatos = [f"indicators/{ind}/calendar", f"indicators/{ind}/releases", f"indicators/{ind}/release-calendar",
              "calendar", "releases", "release-calendar", "economic-calendar"]
for c in candidatos:
    st, corpo = get(c)
    print(f"HTTP {st}  {c}  {'→ ' + json.dumps(corpo, ensure_ascii=False, default=str)[:200] if st == 200 else ''}")

# COMMAND ----------
# 3) "Última atualização histórica" (a data que o site da 4i mostra): qual campo da API bate com ela?
# Pra cada indicador do portal: updated_at do cadastro e os campos de data das séries.
# Compare com a data que o site da 4i mostra pro mesmo indicador (ex.: IPCA, PIM, Anfavea).
# Se alguma coluna bater, o Export passa a usar ela pra ordenar a home.

# COMMAND ----------
# MAGIC %run ../indicadores_portal

# COMMAND ----------
import pandas as pd
linhas = []
for item in INDICADORES:
    code = item["code"]
    st, cad = get(f"indicators/{code}")
    c = primeiro(cad) if st == 200 else {}
    st, ser = get(f"indicators/{code}/series", limit=100, skip=0)
    series = (ser.get("data") if isinstance(ser, dict) else ser) or []
    linha = {"code": code, "nome": item["nome"][:35], "cadastro.updated_at": (c or {}).get("updated_at")}
    # campos de data de todas as séries (fica a mais recente de cada campo)
    for s in series if isinstance(series, list) else []:
        for nome, v in campos_de_data(s):
            if v and v > str(linha.get("serie." + nome) or ""):
                linha["serie." + nome] = v
    linhas.append(linha)
tab = pd.DataFrame(linhas)
display(tab)

