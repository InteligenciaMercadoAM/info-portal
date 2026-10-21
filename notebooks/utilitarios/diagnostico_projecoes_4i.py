# Databricks notebook source
# MAGIC %md
# MAGIC # Diagnóstico: a 4i devolve projeção pra esses indicadores?
# MAGIC Só lê da API, não grava nada.

# COMMAND ----------
# MAGIC %run ../token_4i

# COMMAND ----------
import time, requests, json

BASE = "https://apis.4intelligence.ai/api-feature-store/api/v1/"
TESTAR = ["BRGDP0002", "BRPRC0033", "BRIND0055"]   # PIB, INCC-M e produção de veículos

def get(path, **params):
    r = requests.get(BASE + path, headers=get_headers(), params=params, timeout=60)
    time.sleep(1)                                   # respeita o limite de requisições
    try:
        corpo = r.json()
    except Exception:
        corpo = r.text[:300]
    return r.status_code, corpo

def lista(corpo):
    d = corpo.get("data", corpo) if isinstance(corpo, dict) else corpo
    if isinstance(d, dict):
        d = d.get("items") or d.get("results") or [d]
    return d if isinstance(d, list) else []

# COMMAND ----------
# 1) Quais séries existem dentro de cada indicador (original, dessazonalizada, var. %...)
series_por_ind = {}
for ind in TESTAR:
    st, corpo = get(f"indicators/{ind}/series", limit=100, skip=0)
    itens = lista(corpo) if st == 200 else []
    series_por_ind[ind] = [s.get("code") for s in itens if isinstance(s, dict) and s.get("code")]
    print(f"\n{ind}: HTTP {st}, {len(itens)} séries")
    for s in itens[:20]:
        if isinstance(s, dict):
            nome = s.get("name", {})
            nome = nome.get("pt-br") if isinstance(nome, dict) else nome
            print(f"   {s.get('code')}  {nome or ''}")
    if st != 200:
        print("   resposta:", str(corpo)[:300])

# COMMAND ----------
# 2) Para cada série: última observação e se o endpoint de projeções devolve algo
for ind, codigos in series_por_ind.items():
    for serie in codigos:
        st_o, obs = get(f"indicators/{ind}/series/{serie}/observations", limit=2000, skip=0)
        st_p, proj = get(f"indicators/{ind}/series/{serie}/projections", limit=2000, skip=0)
        o, p = lista(obs), lista(proj)
        ult_o = max((x.get("date") for x in o if isinstance(x, dict) and x.get("date")), default="-")
        ult_p = max((x.get("date") for x in p if isinstance(x, dict) and x.get("date")), default="-")
        print(f"{ind} | {serie:<22} | observ.: HTTP {st_o}, {len(o):>4} pts, última {ult_o} "
              f"| projeções: HTTP {st_p}, {len(p):>3} pts, até {ult_p}")
