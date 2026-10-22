# Databricks notebook source
# MAGIC %md
# MAGIC # Export Portal Data
# MAGIC
# MAGIC Gera os arquivos do Portal IM em `public/data/`:
# MAGIC `feed.json` (bot + projetos piloto), `feed_automotivo.json` (piloto automotivo), `manual.json` (notícias e alertas da equipe), `resumo.json`, `meta.json`, `distinct.json` e `indicadores.json` (este lê a tabela gerada pelo notebook `novoind_4_Intelligence`, que precisa rodar antes).
# MAGIC No fim monta a mensagem diária do Teams.

# COMMAND ----------

# MAGIC %run ./indicadores_portal

# COMMAND ----------

# Configuração
TABLE = "lcb_qas_sandbox2_comercial.isabela_pereira.tb_noticias_classificadas"
MANUAL_TABLE = "lcb_qas_sandbox2_comercial.isabela_pereira.tb_portal_conteudo_manual"
# Outras tabelas de notícias com o mesmo schema do bot (ex.: piloto automotivo).
# Formato: {"Nome que aparece no portal": {"table": "catalogo.schema.tabela", "setor": "Setor padrão"}}
EXTRA_NEWS_TABLES = {
    # "Piloto automotivo": {"table": "lcb_qas_sandbox2_comercial.isabela_pereira.TABELA_DO_PILOTO", "setor": "Automotivo"},
}
EXPORT_DAYS = 90  # quantos dias de notícias exportar

# Piloto automotivo (Automotive Business + AutoData): schema diferente do bot, por isso tem célula própria e vai em feed_automotivo.json
AUTOMOTIVO_TABLE = "hive_metastore.default.noticias_automotivebusiness_arcelormittal"
AUTOMOTIVO_FONTES = ["automotivebusiness", "autodata"]   # o volume de keywords do LinkedIn fica de fora
AUTOMOTIVO_RELEVANTES = ["Alta"]                          # o que vira "Relevante" no portal; inclua "Média" para mais volume
OUTPUT_DIR = "/Workspace/Users/isabela.pereira@arcelormittal.com.br/portal-im/info-portal/public/data"
CONFIG_PATH = OUTPUT_DIR.rsplit("/", 1)[0] + "/config.json"
PORTAL_URL = "https://SEU-SITE.netlify.app"

# Tabelas geradas pelo notebook novoind_4_Intelligence (TABELA_FINAL e TABELA_PROJ de lá). Se o nome mudar lá, mude aqui também.
INDICADORES_TABLE = "lcb_qas_sandbox2_comercial.isabela_pereira.indicadores_lt_20260113"
INDICADORES_PROJ_TABLE = "lcb_qas_sandbox2_comercial.isabela_pereira.indicadores_lt_20260113_projecoes"
# Regra do portal (planilha, coluna Unidade): "var" = variação contra o período anterior, unidade pela frequência;
# "#" = série em nível, unidade e casas decimais vindas do indicadores_portal.
UNIDADE_POR_FREQ = {"Mensal": "% m/m", "Trimestral": "% t/t", "Semestral": "% s/s", "Anual": "% a/a"}

# Como cada indicador da 4intelligence aparece no portal. A chave é o indicator_code.
# Vem da lista oficial do notebook indicadores_portal (%run acima): nome = coluna name da planilha, setor, fonte, polaridade,
# tipo de série ("#" ou "var"), unidade/casas das séries em nível e se mostra projeção. Mexa só lá.
# Campos aceitos (opcionais): id, nome, setor, regiao, unidade, casas (decimais), polaridade (1 = alta é bom, -1 = alta é ruim),
#                 limiar_estavel, destaque, fonte_original, proxima_divulgacao (AAAA-MM-DD), referencia_proxima.
# O "id" é o que o config.json do portal usa em "indicadores_fixos" e "indicadores_nomes" (aqui, o próprio código 4i).
INDICADORES_PORTAL = {i["code"]: {"id": i["code"], **{k: v for k, v in i.items() if k != "code"}} for i in INDICADORES}
SETOR_PADRAO_INDICADOR = "Macroeconomia"
# Quantos pontos mandar ao gráfico: (últimos realizados, primeiras projeções) por frequência. A série completa deixaria o gráfico ilegível.
JANELA_SERIE = {"Diária": (90, 30), "Semanal": (52, 12), "Mensal": (24, 12), "Trimestral": (12, 6), "Semestral": (8, 4), "Anual": (8, 3)}

# Publicação no GitHub: quem faz agora é o notebook 00_atualizar_portal (um commit só, no fim). Deixe False aqui.
PUSH_TO_GITHUB = False
GITHUB_REPO = "InteligenciaMercadoAM/info-portal"
GITHUB_BRANCH = "main"
GITHUB_DATA_PATH = "public/data"  # pasta dos JSONs dentro do repositório (desde 21/10 o repo não tem mais a subpasta info-portal/)
SECRET_SCOPE, SECRET_KEY = "portal-im", "github-token"  # token com permissão Contents: read/write

# Mensagem diária no Teams (opcional): URL do fluxo "Workflows" do canal, guardada em secret
POST_TO_TEAMS = False
TEAMS_SECRET_KEY = "teams-webhook"

# COMMAND ----------

import json, os
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F, Window

os.makedirs(OUTPUT_DIR, exist_ok=True)
cfg = json.load(open(CONFIG_PATH, encoding="utf-8"))

def write_json(name, obj):
    # Escreve em arquivo temporário e troca no fim (nunca deixa um JSON pela metade)
    tmp, final = f"{OUTPUT_DIR}/{name}.tmp", f"{OUTPUT_DIR}/{name}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, allow_nan=False, default=str)
    os.replace(tmp, final)

def news_df(table, projeto=None, setor=None):
    src = spark.table(table)
    # Colunas opcionais: entram no JSON assim que existirem na tabela.
    optional = [c for c in ["setor", "regiao", "mercado"] if c in src.columns]
    latest = Window.partitionBy("id").orderBy(F.desc_nulls_last("classificado_em"))
    df = (
        src.filter(F.col("data_publicacao") >= F.date_sub(F.current_date(), EXPORT_DAYS))
        .withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1")
        .select(
            F.col("id").cast("string").alias("id"), "text", "url",
            F.col("data_publicacao").cast("string").alias("data_publicacao"),
            "source_type", "relevancia",
            F.col("score_relevancia").cast("int").alias("score_relevancia"),
            "categoria_evento",
            F.col("classificado_em").cast("string").alias("classificado_em"),
            "empresa_principal", *optional,
        )
    )
    if projeto:
        # prefixa o id para não colidir com os ids do bot principal
        df = df.withColumn("id", F.concat(F.lit(projeto.lower().replace(" ", "_") + "-"), "id")).withColumn("projeto", F.lit(projeto))
        if setor and "setor" not in optional:
            df = df.withColumn("setor", F.lit(setor))
    return df

df = news_df(TABLE)
for nome, spec in EXTRA_NEWS_TABLES.items():
    df = df.unionByName(news_df(spec["table"], nome, spec.get("setor")), allowMissingColumns=True)
records = [r.asDict() for r in df.orderBy(F.desc("data_publicacao"), F.desc("id")).collect()]
write_json("feed.json", records)
print(f"✅ feed.json: {len(records)} notícias ({EXPORT_DAYS} dias)" + (f", incluindo {', '.join(EXTRA_NEWS_TABLES)}" if EXTRA_NEWS_TABLES else ""))

# COMMAND ----------

# feed_automotivo.json: piloto automotivo (tabela classificada pelo notebook Scraper_e_Classificacao_Noticias_Automotivas).
# Arquivo separado do feed.json: se esta célula falhar, o portal segue só com o feed principal.
import re, unicodedata

def _slug(txt):
    txt = unicodedata.normalize("NFD", str(txt or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "_", txt).strip("_")

try:
    fontes_sql = ", ".join(f"'{f}'" for f in AUTOMOTIVO_FONTES)
    auto_rows = spark.sql(f"""
        SELECT id, fonte_site, titulo, resumo, link, data, coletado_em,
               relevancia, score_relevancia, empresa_principal, categoria_principal
        FROM {AUTOMOTIVO_TABLE}
        WHERE fonte_site IN ({fontes_sql})
          AND to_date(to_timestamp(data)) >= date_sub(current_date(), {EXPORT_DAYS})
          AND titulo IS NOT NULL AND trim(titulo) <> ''
    """).collect()
    auto = {}
    for r in auto_rows:
        pub = re.match(r"(\d{4}-\d{2}-\d{2})", str(r["data"] or ""))
        if not pub:
            continue
        item = {
            "id": f"auto-{r['fonte_site']}-{r['id']}",
            "titulo": (r["titulo"] or "").strip(), "text": (r["resumo"] or "").strip(), "url": r["link"] or "",
            "data_publicacao": pub.group(1), "source_type": r["fonte_site"],
            "relevancia": "RELEVANTE" if (r["relevancia"] or "") in AUTOMOTIVO_RELEVANTES else "IRRELEVANTE",
            "relevancia_original": r["relevancia"],
            "score_relevancia": int(r["score_relevancia"] or 0),          # 9 / 5 / 2, mesma escala 0-10 do bot
            "categoria_evento": "auto_" + (_slug(r["categoria_principal"]) or "outros"),
            "empresa_principal": r["empresa_principal"] or "nenhuma",
            "classificado_em": r["coletado_em"], "setor": "Automotivo", "projeto": "Piloto automotivo",
        }
        chave = item["url"] or item["id"]          # mesmo link nas duas fontes: fica o de maior score
        if chave not in auto or item["score_relevancia"] > auto[chave]["score_relevancia"]:
            auto[chave] = item
    auto_records = sorted(auto.values(), key=lambda x: x["data_publicacao"], reverse=True)
    write_json("feed_automotivo.json", auto_records)
    print(f"✅ feed_automotivo.json: {len(auto_records)} notícias ({sum(a['relevancia'] == 'RELEVANTE' for a in auto_records)} relevantes)")
except Exception as e:
    print(f"⚠️ feed_automotivo.json não atualizado ({type(e).__name__}: {str(e)[:200]}). O portal segue com o arquivo anterior.")

# COMMAND ----------

# Conteúdo manual (notícias, alertas e resumo semanal) da tabela preenchida pelo notebook "Incluir Conteúdo Manual"
manual, resumo = [], None
if spark.catalog.tableExists(MANUAL_TABLE):
    rows = [r.asDict() for r in spark.table(MANUAL_TABLE).filter("ativo").orderBy(F.desc("data_publicacao"), F.desc("criado_em")).collect()]
    for r in rows:
        item = {
            "id": r["id"], "tipo": r["tipo"], "titulo": r["titulo"], "text": r["texto"] or "", "url": r["url"],
            "data_publicacao": str(r["data_publicacao"]) if r["data_publicacao"] else None,
            "fonte": r["fonte"], "categoria_evento": r["tema"], "setor": r["setor"], "regiao": r["regiao"],
            "empresa_principal": r["empresa_principal"] or "",
            "valido_ate": str(r["valido_ate"]) if r["valido_ate"] else None, "adicionado_por": r["adicionado_por"],
        }
        if r["tipo"] == "resumo":
            if resumo is None:  # só o mais recente
                resumo = {"titulo": r["titulo"], "texto": r["texto"] or "", "url": r["url"],
                          "data_publicacao": item["data_publicacao"], "adicionado_por": r["adicionado_por"]}
        else:
            manual.append({k: v for k, v in item.items() if v is not None})
else:
    print(f"⚠️ Tabela {MANUAL_TABLE} ainda não existe: rode o notebook 'Incluir Conteúdo Manual' uma vez.")

write_json("manual.json", manual)
if resumo:
    write_json("resumo.json", resumo)
elif os.path.exists(f"{OUTPUT_DIR}/resumo.json"):
    os.remove(f"{OUTPUT_DIR}/resumo.json")
print(f"✅ manual.json: {sum(m['tipo'] == 'noticia' for m in manual)} notícias, {sum(m['tipo'] == 'alerta' for m in manual)} alertas"
      + (f" · resumo.json: {resumo['titulo']}" if resumo else " · sem resumo semanal"))

meta = {"atualizado_em": datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S"), "noticias": len(records)}
write_json("meta.json", meta)
print(f"✅ meta.json: {meta['atualizado_em']}")

# COMMAND ----------

distinct = {}
for col_name in ["source_type", "relevancia", "categoria_evento", "empresa_principal"]:
    vals = (
        spark.table(TABLE)
        .filter(F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != ""))
        .groupBy(col_name).count()
        .orderBy(F.desc("count")).limit(500)
        .collect()
    )
    distinct[col_name] = [{"val": r[col_name], "n": int(r["count"])} for r in vals]
    print(f"{col_name}: {len(distinct[col_name])} valores distintos")
write_json("distinct.json", distinct)
print("✅ distinct.json exportado")

# COMMAND ----------

# indicadores.json: lê a tabela que o notebook novoind_4_Intelligence grava (INDICADORES_TABLE) e monta o formato que o portal (app.js) espera:
# {"atualizado_em": "...", "indicadores": [{id, nome, setor, regiao, unidade, casas, frequencia, polaridade, fonte, fonte_original,
#                                          ultima_divulgacao, proxima_divulgacao, serie: [{periodo, valor, tipo: "realizado"|"projecao"}]}]}
# Realizado vem de INDICADORES_TABLE e projeção de INDICADORES_PROJ_TABLE (antes só o realizado era lido, por isso não aparecia projeção).
# Em automação é melhor falhar alto do que publicar um indicadores.json velho ou vazio.
import re, statistics
from datetime import date

MESES = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]

def _frequencia(datas):
    if len(datas) < 2:
        return "Mensal"
    passo = statistics.median((b - a).days for a, b in zip(datas, datas[1:]))
    return ("Diária" if passo <= 1.5 else "Semanal" if passo <= 8 else "Mensal" if passo <= 40
            else "Trimestral" if passo <= 100 else "Semestral" if passo <= 200 else "Anual")

def _periodo(d, freq):
    yy = f"{d.year % 100:02d}"
    if freq == "Mensal":     return f"{MESES[d.month - 1]}/{yy}"
    if freq == "Trimestral": return f"{(d.month - 1) // 3 + 1}T{yy}"
    if freq == "Semestral":  return f"{1 if d.month <= 6 else 2}S{yy}"
    if freq == "Anual":      return str(d.year)
    return d.strftime("%d/%m/%y")

def _casas_auto(real):
    """Casas decimais pra série em nível sem 'casas' definido: números grandes sem decimal."""
    m = abs(real[-1][1])
    return 0 if m >= 1000 else 1 if m >= 10 else 2

def montar_indicador(code, meta, obs, hoje, mapa, proj=None):
    """meta: campos do catálogo; obs: realizado [(date, valor)] ordenado; proj: projeção 4i [(date, valor)] da mesma série;
    mapa: ajustes do INDICADORES_PORTAL para este código."""
    real = [(d, v) for d, v in obs if d is not None and v is not None]
    if not real:
        return None
    freq = mapa.get("frequencia") or _frequencia([d for d, _ in real])
    # Projeção vem da tabela de projeções (TABELA_PROJ do novoind). Só entra o que for depois do último realizado.
    ultimo = real[-1][0]
    proj = [(d, v) for d, v in (proj or []) if d is not None and v is not None and d > ultimo]
    if mapa.get("projecao") is False:
        proj = []                                  # planilha pediu pra tirar a projeção (ex.: Dólar)
    n_real, n_proj = JANELA_SERIE.get(freq, (24, 12))
    serie = [{"periodo": _periodo(d, freq), "valor": round(v, 4), "tipo": "realizado"} for d, v in real[-n_real:]]
    serie += [{"periodo": _periodo(d, freq), "valor": round(v, 4), "tipo": "projecao"} for d, v in proj[:n_proj]]

    pais = str(meta.get("country") or "").strip().upper()
    atualizado = str(meta.get("updated_at") or "")[:10]
    item = {
        "id": mapa.get("id") or code.lower(),
        "nome": mapa.get("nome") or (meta.get("short_name") or "").strip() or (meta.get("name") or "").strip() or code,
        "setor": mapa.get("setor") or SETOR_PADRAO_INDICADOR,
        "regiao": mapa.get("regiao") or ("Nacional" if pais in ("BR", "BRA", "BRASIL", "BRAZIL", "") else "Internacional"),
        "frequencia": freq,
        "polaridade": mapa.get("polaridade", 1),
        "fonte": "4intelligence",
        "fonte_original": mapa.get("fonte_original") or (meta.get("source") or "").strip() or None,
        # "updated_at" do catálogo é a data de atualização do cadastro na 4i, não a divulgação do dado: não vai pro portal
        "unidade": mapa.get("unidade") if mapa.get("serie") == "#" else (mapa.get("unidade") or UNIDADE_POR_FREQ.get(freq, "%")),
        "casas": mapa.get("casas", _casas_auto(real) if mapa.get("serie") == "#" else 1),
        "serie": serie,
    }
    for k in ["limiar_estavel", "destaque", "proxima_divulgacao", "referencia_proxima"]:
        if mapa.get(k) is not None:
            item[k] = mapa[k]
    return {k: v for k, v in item.items() if v is not None}

# --- leitura da tabela e escrita do arquivo ---
if not spark.catalog.tableExists(INDICADORES_TABLE):
    raise RuntimeError(f"Tabela {INDICADORES_TABLE} não existe: rode o notebook novoind_4_Intelligence antes deste.")

ind = spark.table(INDICADORES_TABLE)
META_COLS = [c for c in ["name", "short_name", "source", "country", "updated_at"] if c in ind.columns]
catalogo = {r["indicator_code"]: r.asDict() for r in ind.select("indicator_code", *META_COLS).dropDuplicates(["indicator_code"]).collect()}
obs_rows = (
    ind.select("indicator_code", F.to_date("date", "dd/MM/yyyy").alias("d"), F.col("value").cast("double").alias("v"))
    .filter(F.col("d").isNotNull() & F.col("v").isNotNull() & ~F.isnan("v"))   # NaN quebraria o json (allow_nan=False)
    .dropDuplicates(["indicator_code", "d"])
    .orderBy("indicator_code", "d")
    .collect()
)
series = {}
for r in obs_rows:
    series.setdefault(r["indicator_code"], []).append((r["d"], r["v"]))

projecoes = {}
if spark.catalog.tableExists(INDICADORES_PROJ_TABLE):
    for r in (spark.table(INDICADORES_PROJ_TABLE)
              .select("indicator_code", F.to_date("date", "dd/MM/yyyy").alias("d"), F.col("value").cast("double").alias("v"))
              .filter(F.col("d").isNotNull() & F.col("v").isNotNull() & ~F.isnan("v"))
              .dropDuplicates(["indicator_code", "d"]).orderBy("indicator_code", "d").collect()):
        projecoes.setdefault(r["indicator_code"], []).append((r["d"], r["v"]))
else:
    print(f"⚠️ {INDICADORES_PROJ_TABLE} não existe: indicadores vão sem projeção.")

hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
itens, sem_dados = [], []
fora_da_lista = sorted(set(catalogo) - set(INDICADORES_PORTAL))
if fora_da_lista:
    print("Na tabela mas fora da lista oficial (não vão pro portal):", fora_da_lista)
sem_dados += sorted(set(INDICADORES_PORTAL) - set(catalogo))
for code, meta_ind in catalogo.items():   # não usar o nome "meta": ele é o meta.json, usado na mensagem do Teams
    if code not in INDICADORES_PORTAL:
        continue
    it = montar_indicador(code, meta_ind, series.get(code, []), hoje, INDICADORES_PORTAL.get(code, {}), projecoes.get(code, []))
    (itens.append(it) if it else sem_dados.append(code))

if not itens:
    raise RuntimeError(f"{INDICADORES_TABLE} sem observações válidas: não vou sobrescrever o indicadores.json.")
if sem_dados:
    print("⚠️ Indicadores sem nenhuma observação válida (ficam fora do portal):", sorted(sem_dados))
ids = [i["id"] for i in itens]
if len(ids) != len(set(ids)):
    raise RuntimeError(f"ids de indicador repetidos: {sorted({i for i in ids if ids.count(i) > 1})}. Ajuste o 'id' em INDICADORES_PORTAL.")
_fixos = [f["id"] if isinstance(f, dict) else f for f in cfg.get("indicadores_fixos", [])]
fixos_ausentes = [f for f in _fixos if str(f).lower() not in {str(i).lower() for i in ids}]
if fixos_ausentes:
    print("⚠️ 'indicadores_fixos' do config.json sem indicador correspondente (o portal usa os marcados com destaque no lugar):", fixos_ausentes)

_nomes_cfg = {str(k).upper(): v for k, v in cfg.get("indicadores_nomes", {}).items()}
_diverge = [c for c, m in INDICADORES_PORTAL.items() if _nomes_cfg.get(c) != m.get("nome")]
if _diverge or set(_nomes_cfg) - set(INDICADORES_PORTAL):
    print("⚠️ indicadores_nomes do config.json diferente do indicadores_portal (o site usa o do config):",
          _diverge + sorted(set(_nomes_cfg) - set(INDICADORES_PORTAL)))

itens.sort(key=lambda i: i["nome"].lower())
write_json("indicadores.json", {"atualizado_em": datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S"), "indicadores": itens})
print(f"✅ indicadores.json: {len(itens)} indicadores, {sum(len(i['serie']) for i in itens)} pontos")
for i in itens:
    real = [p for p in i["serie"] if p["tipo"] == "realizado"]
    print(f"   {i['id']} {i['nome'][:45]:<45} {i.get('unidade') or '(sem unidade)':<9} · último {real[-1]['periodo'] if real else '-'} = {real[-1]['valor'] if real else '-'} · {len(i['serie']) - len(real)} projeções")


# COMMAND ----------

import base64, requests

DATA_FILES = ["feed.json", "feed_automotivo.json", "manual.json", "resumo.json", "meta.json", "distinct.json", "indicadores.json"]  # indicadores.json é gerado na célula acima a partir da tabela do notebook novoind_4_Intelligence

def push_to_github(fname):
    token = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_DATA_PATH}/{fname}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    with open(f"{OUTPUT_DIR}/{fname}", "rb") as f:
        content = base64.b64encode(f.read()).decode()
    cur = requests.get(url, headers=headers, params={"ref": GITHUB_BRANCH})
    payload = {"message": f"Atualiza {fname} (export Databricks)", "content": content, "branch": GITHUB_BRANCH}
    if cur.status_code == 200:
        payload["sha"] = cur.json()["sha"]
    r = requests.put(url, headers=headers, json=payload)
    r.raise_for_status()
    print(f"✅ {fname} enviado ao GitHub ({GITHUB_REPO}@{GITHUB_BRANCH})")

if PUSH_TO_GITHUB:
    for name in DATA_FILES:
        if os.path.exists(f"{OUTPUT_DIR}/{name}"):
            push_to_github(name)  # o Netlify faz o deploy sozinho após o commit
else:
    print("PUSH_TO_GITHUB = False: o commit é feito pelo notebook 00_atualizar_portal.")

# COMMAND ----------

# Mensagem diária do Teams: alertas IM + a principal notícia de cada tema do último dia de notícias.
# Sempre imprime o texto (para copiar e colar). Com POST_TO_TEAMS = True, publica no canal via Workflows.
import re, unicodedata
from datetime import date, timedelta

MAX_TEMAS, MAX_ALERTAS, JANELA_DIAS = 5, 3, 1

def _norm(s):
    return unicodedata.normalize("NFD", str(s or "")).encode("ascii", "ignore").decode().lower().strip()

def _titulo(item):
    if item.get("titulo"):
        return item["titulo"].strip()
    t = re.sub(r"^\s*\[Data de publica[cç][aã]o:[^\]]*\]\s*", "", str(item.get("text") or ""), flags=re.I)
    t = re.sub(r"\s+", " ", t.replace("\\n", " ")).strip()
    first = re.split(r"(?<=[.!?])\s+(?=[A-ZÁÉÍÓÚÂÊÔÃÕÇ\"'(])", t)[0]
    return first.rstrip(" .;:") if len(first) <= 120 else first[:120].rsplit(" ", 1)[0] + "…"

def _fonte(item):
    return item.get("fonte") or cfg.get("fontes", {}).get(_norm(item.get("source_type")), str(item.get("source_type") or "").title())

def montar_mensagem(records, manual, cfg, meta, portal_url):
    hoje = date.today()
    temas, fora = cfg.get("temas", {}), set(cfg.get("temas_fora_da_home", []))
    alertas = [m for m in manual if m.get("tipo") == "alerta"
               and (m.get("valido_ate") or str(date.fromisoformat(m["data_publicacao"]) + timedelta(days=cfg["portal"].get("dias_validade_alerta", 7)))) >= str(hoje)]
    noticias = [m for m in manual if m.get("tipo") == "noticia"] + [
        r for r in records if _norm(r.get("relevancia")) == "relevante"]
    ultimo = max((n["data_publicacao"] for n in noticias if n.get("data_publicacao")), default=None)
    destaques = []
    if ultimo:
        corte = str(date.fromisoformat(ultimo) - timedelta(days=JANELA_DIAS - 1))
        recentes = [n for n in noticias if (n.get("data_publicacao") or "") >= corte]
        por_tema = {}
        for n in recentes:
            tema = temas.get(_norm(n.get("categoria_evento")), "Outros")
            if tema not in fora:
                por_tema.setdefault(tema, []).append(n)
        ordem = sorted(por_tema, key=lambda t: -len(por_tema[t]))[:MAX_TEMAS]
        for t in ordem:
            top = sorted(por_tema[t], key=lambda n: (n.get("tipo") == "noticia", n.get("score_relevancia") or 0), reverse=True)[0]
            destaques.append((t, _titulo(top), _fonte(top)))

    upd = meta["atualizado_em"]
    cab = f"Portal IM atualizado em {upd[8:10]}/{upd[5:7]} às {upd[11:16]}"
    linhas = [f"**{cab}**", ""]
    if alertas:
        linhas += ["**Alertas IM**"] + [f"- {_titulo(a)}" for a in alertas[:MAX_ALERTAS]] + [""]
    if destaques:
        linhas += ["**Destaques por tema**"] + [f"- **{t}:** {tit} ({f})" for t, tit, f in destaques] + [""]
    else:
        linhas += ["Sem notícias novas relevantes no último dia.", ""]
    linhas.append(f"Acesse o portal: {portal_url}")
    texto = "\n".join(linhas)

    # Adaptive Card para o fluxo "Workflows" do Teams ("Postar em um canal quando uma solicitação de webhook for recebida")
    body = [{"type": "TextBlock", "text": cab, "weight": "Bolder", "size": "Medium", "wrap": True}]
    if alertas:
        body += [{"type": "TextBlock", "text": "Alertas IM", "weight": "Bolder", "color": "Attention", "spacing": "Medium"},
                 {"type": "TextBlock", "wrap": True, "text": "\n".join(f"- {_titulo(a)}" for a in alertas[:MAX_ALERTAS])}]
    if destaques:
        body += [{"type": "TextBlock", "text": "Destaques por tema", "weight": "Bolder", "spacing": "Medium"},
                 {"type": "TextBlock", "wrap": True, "text": "\n".join(f"- **{t}:** {tit} ({f})" for t, tit, f in destaques)}]
    card = {"type": "message", "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "type": "AdaptiveCard", "version": "1.4", "body": body,
        "actions": [{"type": "Action.OpenUrl", "title": "Abrir o Portal IM", "url": portal_url}]}}]}
    return texto, card

texto_teams, card_teams = montar_mensagem(records, manual, cfg, meta, PORTAL_URL)
print(texto_teams)

if POST_TO_TEAMS:
    import requests
    r = requests.post(dbutils.secrets.get(SECRET_SCOPE, TEAMS_SECRET_KEY), json=card_teams, timeout=30)
    r.raise_for_status()
    print("\n✅ Mensagem publicada no Teams")

# COMMAND ----------

for fname in DATA_FILES:
    p = f"{OUTPUT_DIR}/{fname}"
    if os.path.exists(p):
        print(f"  {fname}: {os.path.getsize(p) / (1024 * 1024):.2f} MB")
print(f"\n📁 Arquivos em: {OUTPUT_DIR}")