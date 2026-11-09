# Databricks notebook source
# MAGIC %md
# MAGIC # Resumo semanal com IA (Llama)
# MAGIC
# MAGIC Gera o resumo da semana que aparece na home do portal, usando o Llama 3.3 70B das Foundation Model APIs do Databricks
# MAGIC (o mesmo modelo que o job de classificação já usa, via `ai_query`). Não precisa de token nem de chave: roda com o acesso do cluster.
# MAGIC
# MAGIC Como funciona:
# MAGIC 1. Junta as notícias **relevantes** da semana: Infomet com score >= 5 (mesmo corte do portal), automotivo "Alta" e o conteúdo manual da equipe (notícias e alertas).
# MAGIC 2. Pede ao Llama **uma frase** por notícia (fica guardada numa tabela, então cada notícia só é resumida uma vez).
# MAGIC 3. Pede ao Llama de 3 a 5 **destaques da semana**: manchete informativa + 2 ou 3 frases com números e o que significa pro mercado de aços longos, cada um apontando quais notícias o sustentam.
# MAGIC 4. Confere a resposta (descarta id inventado, destaque sem notícia) e grava na tabela `RESUMO_IA_TABLE`.
# MAGIC
# MAGIC O `Export_Portal_Data` lê essa tabela e gera o `resumo.json`. No portal, cada destaque abre a lista de notícias e cada notícia abre o texto completo.
# MAGIC
# MAGIC **Quando roda:** é chamado pelo `00_atualizar_portal` todo dia, mas só gera um resumo novo quando a semana muda (com `MODO = "semana_fechada"`).
# MAGIC Pra gerar de novo a mesma semana (ex.: mudou o prompt), rode este notebook na mão com o widget `forcar = sim`.
# MAGIC
# MAGIC **Rode num cluster, não no serverless.**

# COMMAND ----------

# ===================== CONFIGURAÇÃO =====================
CATALOGO = "lcb_qas_sandbox2_comercial.isabela_pereira"
TABLE = f"{CATALOGO}.tb_noticias_classificadas"              # notícias do bot (Infomet), já classificadas
SILVER_TABLE = f"{CATALOGO}.tb_silver_news_chatbot_v1"       # título e texto completo das notícias Infomet
MANUAL_TABLE = f"{CATALOGO}.tb_portal_conteudo_manual"       # notícias e alertas incluídos pela equipe
AUTOMOTIVO_TABLE = "hive_metastore.default.noticias_automotivebusiness_arcelormittal"
AUTOMOTIVO_FONTES = ["automotivebusiness", "autodata"]
AUTOMOTIVO_RELEVANTES = ["Alta"]
AUTOMOTIVO_RAW_TABLE = "hive_metastore.default.noticias_automotivebusiness"   # texto completo (coluna conteudo)

RESUMO_IA_TABLE = f"{CATALOGO}.tb_portal_resumo_semanal_ia"        # um registro por semana (o Export lê o mais recente)
RESUMO_NOTICIA_TABLE = f"{CATALOGO}.tb_portal_resumo_noticia_ia"   # cache da frase de cada notícia

LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"
SCORE_MIN = 5            # igual ao SCORE_MIN_RELEVANTE_PORTAL do Export
MAX_NOTICIAS = 60        # as de maior score entram no resumo (manual/alertas sempre entram)
MIN_NOTICIAS = 3         # com menos que isso não gera resumo
N_DESTAQUES = "3 a 5"

# "semana_fechada": segunda a domingo da semana anterior; gera uma vez por semana (na 1ª rodada de segunda).
# "ultimos_7_dias": janela móvel até hoje; gera de novo todo dia (custa mais e o texto muda diariamente).
MODO = "semana_fechada"

dbutils.widgets.text("forcar", "nao")
FORCAR = dbutils.widgets.get("forcar").strip().lower() in ("sim", "s", "true")

# COMMAND ----------

import json, re
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F, Window

hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
if MODO == "ultimos_7_dias":
    fim = hoje
else:
    fim = hoje - timedelta(days=hoje.weekday() + 1)   # domingo passado
ini = fim - timedelta(days=6)
print(f"Semana: {ini:%d/%m/%Y} a {fim:%d/%m/%Y} ({MODO})")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {RESUMO_IA_TABLE} (
    semana_inicio DATE, semana_fim DATE, conteudo STRING COMMENT 'JSON do resumo (o mesmo que vai pro resumo.json)',
    modelo STRING, n_noticias INT, gerado_em TIMESTAMP) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {RESUMO_NOTICIA_TABLE} (
    id STRING COMMENT 'mesmo id do portal', resumo STRING, modelo STRING, gerado_em TIMESTAMP) USING DELTA""")

ja = spark.table(RESUMO_IA_TABLE).filter((F.col("semana_inicio") == F.lit(ini)) & (F.col("semana_fim") == F.lit(fim))).count()
if ja and not FORCAR:
    dbutils.notebook.exit(f"resumo da semana {ini:%d/%m}–{fim:%d/%m} já existe (use forcar = sim pra refazer)")

# COMMAND ----------

# ===================== 1) NOTÍCIAS RELEVANTES DA SEMANA =====================
# Os ids seguem a mesma regra do Export, pra o portal achar a notícia e abrir o detalhe.
def limpa(t, n=None):
    t = re.sub(r"^\s*\[Data de publica[cç][aã]o:[^\]]*\]\s*", "", str(t or ""), flags=re.I)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:n] if n else t

noticias = []

# Infomet (bot): última classificação de cada id + título e texto completo da silver
latest = Window.partitionBy("id").orderBy(F.desc_nulls_last("classificado_em"))
bot = (spark.table(TABLE)
       .filter(F.col("data_publicacao").between(F.lit(ini), F.lit(fim)))
       .withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1")
       .filter(F.col("score_relevancia").cast("int") >= SCORE_MIN)
       .select(F.col("id").cast("string").alias("id"), "text", "url", "source_type",
               F.col("data_publicacao").cast("string").alias("data"), F.col("score_relevancia").cast("int").alias("score")))
if spark.catalog.tableExists(SILVER_TABLE):
    sil = spark.table(SILVER_TABLE)
    cols = [c for c in ["titulo", "texto", "fonte"] if c in sil.columns]
    sil = (sil.select(F.col("id").cast("string").alias("id"), "source_type", *cols)
              .dropDuplicates(["id", "source_type"]))
    bot = bot.join(sil, ["id", "source_type"], "left")
for r in bot.collect():
    d = r.asDict()
    texto = limpa(d.get("texto") or d.get("text"))
    titulo = limpa(d.get("titulo")) or texto[:120]
    noticias.append({"id": d["id"], "titulo": titulo, "texto": texto[:3000], "url": d["url"] or "",
                     "fonte": (d.get("fonte") if d.get("fonte") not in (None, "", "N/A") else "Infomet"),
                     "data_publicacao": d["data"], "score": d["score"] or 0, "manual": False})
print(f"Infomet relevantes: {len(noticias)}")

# Piloto automotivo (só "Alta"); mesmo link nas duas fontes fica uma vez só
try:
    fontes_sql = ", ".join(f"'{f}'" for f in AUTOMOTIVO_FONTES)
    rel_sql = ", ".join(f"'{f}'" for f in AUTOMOTIVO_RELEVANTES)
    auto = {}
    tem_conteudo = spark.catalog.tableExists(AUTOMOTIVO_RAW_TABLE) and "conteudo" in spark.table(AUTOMOTIVO_RAW_TABLE).columns
    for r in spark.sql(f"""
        SELECT c.id, c.fonte_site, c.titulo, c.resumo, c.link, c.data, c.score_relevancia,
               {"r.conteudo" if tem_conteudo else "CAST(NULL AS STRING)"} AS conteudo
        FROM {AUTOMOTIVO_TABLE} c
        {f"LEFT JOIN {AUTOMOTIVO_RAW_TABLE} r ON c.id = r.id AND c.fonte_site = r.fonte_site" if tem_conteudo else ""}
        WHERE c.fonte_site IN ({fontes_sql}) AND c.relevancia IN ({rel_sql})
          AND to_date(to_timestamp(c.data)) BETWEEN DATE'{ini}' AND DATE'{fim}'
          AND c.titulo IS NOT NULL AND trim(c.titulo) <> ''""").collect():
        item = {"id": f"auto-{r['fonte_site']}-{r['id']}", "titulo": limpa(r["titulo"]), "texto": limpa(r["conteudo"] or r["resumo"], 3000),
                "url": r["link"] or "", "fonte": {"automotivebusiness": "Automotive Business", "autodata": "AutoData"}.get(r["fonte_site"], r["fonte_site"]),
                "data_publicacao": str(r["data"])[:10], "score": int(r["score_relevancia"] or 0), "manual": False}
        k = item["url"] or item["id"]
        if k not in auto or item["score"] > auto[k]["score"]:
            auto[k] = item
    noticias += list(auto.values())
    print(f"Automotivo relevantes: {len(auto)}")
except Exception as e:
    print(f"⚠️ Automotivo fora do resumo ({type(e).__name__}: {str(e)[:200]})")

# Conteúdo manual (notícias e alertas da equipe): sempre entra
if spark.catalog.tableExists(MANUAL_TABLE):
    man = (spark.table(MANUAL_TABLE).filter("ativo").filter(F.col("tipo").isin("noticia", "alerta"))
           .filter(F.col("data_publicacao").between(F.lit(ini), F.lit(fim))).collect())
    for r in man:
        noticias.append({"id": r["id"], "titulo": limpa(r["titulo"]), "texto": limpa(r["texto"], 3000), "url": r["url"] or "",
                         "fonte": r["fonte"] or "Equipe IM", "data_publicacao": str(r["data_publicacao"]),
                         "score": 100, "manual": True})
    print(f"Conteúdo manual: {len(man)}")

# Mesma notícia por fontes diferentes: fica a de maior score (pelo título)
por_titulo = {}
for n in sorted(noticias, key=lambda x: -x["score"]):
    por_titulo.setdefault(re.sub(r"\W+", "", n["titulo"].lower())[:80], n)
noticias = sorted(por_titulo.values(), key=lambda x: (-x["score"], x["data_publicacao"]))[:MAX_NOTICIAS]
print(f"➡️ {len(noticias)} notícias entram no resumo")
if len(noticias) < MIN_NOTICIAS:
    dbutils.notebook.exit(f"só {len(noticias)} notícias relevantes na semana: resumo não gerado")

# COMMAND ----------

# ===================== 2) CHAMADA AO LLAMA (ai_query) =====================
_usa_params = True

def llm(prompts, max_tokens=300):
    """Manda uma lista de prompts ao Llama em lote (ai_query paraleliza). Devolve as respostas na mesma ordem."""
    global _usa_params
    spark.createDataFrame([(i, p) for i, p in enumerate(prompts)], "k INT, prompt STRING").createOrReplaceTempView("_prompts_ia")
    def rodar(com_params):
        extra = f", modelParameters => named_struct('max_tokens', {max_tokens}, 'temperature', 0.1)" if com_params else ""
        return spark.sql(f"SELECT k, ai_query('{LLM_MODEL}', prompt{extra}) AS r FROM _prompts_ia").collect()
    try:
        rows = rodar(_usa_params)
    except Exception as e:
        if not _usa_params:
            raise
        print(f"ℹ️ ai_query sem modelParameters neste runtime ({str(e).splitlines()[0][:120]}); seguindo com o padrão do modelo.")
        _usa_params = False
        rows = rodar(False)
    out = {r["k"]: (r["r"] or "").strip() for r in rows}
    return [out.get(i, "") for i in range(len(prompts))]

# Frase por notícia (com cache)
PROMPT_NOTICIA = (
    "Resuma a notícia abaixo em UMA frase curta, em português do Brasil, com no máximo 25 palavras. "
    "Diga só o fato principal e, se houver, o número mais importante (volume, preço, percentual, valor). "
    "Não invente nada que não esteja no texto. Responda apenas com a frase, sem aspas e sem introdução.\n\n"
)
cache = {r["id"]: r["resumo"] for r in spark.table(RESUMO_NOTICIA_TABLE).filter(F.col("id").isin([n["id"] for n in noticias])).collect()}
faltam = [n for n in noticias if not cache.get(n["id"])]
if faltam:
    print(f"Resumindo {len(faltam)} notícias com {LLM_MODEL}...")
    respostas = llm([f"{PROMPT_NOTICIA}Título: {n['titulo']}\nTexto: {n['texto']}" for n in faltam], max_tokens=120)
    novos = [(n["id"], limpa(r).strip('"“” '), LLM_MODEL) for n, r in zip(faltam, respostas) if r]
    if novos:
        (spark.createDataFrame(novos, "id STRING, resumo STRING, modelo STRING").withColumn("gerado_em", F.current_timestamp())
         .write.mode("append").saveAsTable(RESUMO_NOTICIA_TABLE))
    cache.update({i: r for i, r, _ in novos})
for n in noticias:
    n["resumo"] = cache.get(n["id"]) or n["texto"][:200]
print(f"✅ Frases prontas ({len(noticias) - len(faltam)} do cache)")

# COMMAND ----------

# ===================== 3) DESTAQUES DA SEMANA =====================
PROMPT_SEMANA = f"""Você é analista sênior de inteligência de mercado da ArcelorMittal Aços Longos no Brasil (vergalhão, fio-máquina, perfis, barras e trefilados; clientes em construção civil, indústria, agro e automotivo).
Abaixo estão as notícias relevantes da semana de {ini:%d/%m/%Y} a {fim:%d/%m/%Y}. Cada uma vem como [ID] (data · fonte) título, seguido de um resumo e de um trecho do texto.

Escreva o resumo semanal para a diretoria: {N_DESTAQUES} destaques, do mais importante para o menos importante para o negócio de aços longos.

Regras para cada destaque:
- "titulo": uma manchete informativa de 6 a 12 palavras que diga QUEM fez O QUÊ (ex.: "Gerdau tem lucro 40% maior no 3º trimestre com alta do vergalhão"). Nunca use só um tema solto como "Gerdau", "Infraestrutura" ou "Aço bruto".
- "resumo": 2 ou 3 frases completas (40 a 70 palavras no total). Diga o fato principal com os números das notícias (volumes, percentuais, valores, prazos), cite as empresas, órgãos ou regiões envolvidos e termine com o que isso significa para o mercado de aços longos (demanda, preço, concorrência, importação). Se as notícias não permitirem tirar uma implicação, não invente: só descreva os fatos.
- "ids": os IDs (exatamente como aparecem entre colchetes) das notícias que sustentam o destaque, de 1 a 6.
- Agrupe notícias sobre o mesmo assunto num destaque só, juntando os números de todas elas.
- Use só informação das notícias. Não invente números, empresas, datas nem fatos.
- Escreva em português do Brasil, em tom de relatório executivo, sem adjetivos vazios ("importante", "significativo") e sem repetir o título no resumo.

Exemplo de destaque RUIM: {{"titulo": "Infraestrutura", "resumo": "Investimentos em ferrovias", "ids": ["1"]}}
Exemplo de destaque BOM (só mostra o formato; os fatos são fictícios e NÃO podem aparecer na resposta): {{"titulo": "Governo leiloa dois trechos ferroviários em dezembro", "resumo": "O Ministério dos Transportes marcou para dezembro os leilões da Ferrogrão e do trecho Leste da FICO, com investimento previsto de R$ 28 bilhões em 35 anos. As obras demandam trilhos, perfis e vergalhão para pontes e terminais, o que reforça a demanda por aços longos a partir de 2027.", "ids": ["1", "2"]}}

Responda APENAS com JSON válido, sem texto antes ou depois, neste formato:
{{"destaques": [{{"titulo": "...", "resumo": "...", "ids": ["..."]}}]}}

Notícias:
"""
TRECHO_CHARS = 500   # quanto do texto de cada notícia vai junto da frase, pra o modelo ter números e contexto
linhas = "\n\n".join(
    f"[{n['id']}] ({n['data_publicacao'][8:10]}/{n['data_publicacao'][5:7]} · {n['fonte']}) {n['titulo']}\n"
    f"Resumo: {n['resumo']}\nTrecho: {n['texto'][:TRECHO_CHARS]}"
    for n in noticias)
por_id = {n["id"]: n for n in noticias}

def interpreta(resp):
    m = re.search(r"\{.*\}", resp or "", flags=re.S)
    if not m:
        return []
    try:
        dados = json.loads(m.group(0))
    except json.JSONDecodeError:
        return []
    destaques, usados = [], set()
    for d in (dados.get("destaques") or [])[:6]:
        ids = [str(i).strip().strip("[]") for i in (d.get("ids") or [])]
        ids = [i for i in dict.fromkeys(ids) if i in por_id and i not in usados][:6]   # descarta id inventado/repetido
        titulo, resumo = limpa(d.get("titulo")), limpa(d.get("resumo"))
        if not ids or not titulo:
            continue
        usados.update(ids)
        destaques.append({
            "titulo": titulo, "resumo": resumo,
            "noticias": [{k: por_id[i][k] for k in ("id", "titulo", "resumo", "fonte", "data_publicacao", "url")} for i in ids],
        })
    return destaques

destaques = []
for tentativa in range(3):
    novos = interpreta(llm([PROMPT_SEMANA + linhas], max_tokens=3000)[0])
    curtos = [d["titulo"] for d in novos if len(d["resumo"].split()) < 25 or len(d["titulo"].split()) < 4]
    if len(novos) >= 2 and not curtos:
        destaques = novos
        break
    if len(novos) >= 2 and not destaques:
        destaques = novos   # guarda a melhor até agora, caso as próximas também venham curtas
    print(f"⚠️ Tentativa {tentativa + 1}: " + (f"destaques curtos demais {curtos}" if novos else "resposta fora do formato esperado"))
if not destaques:
    raise RuntimeError("O Llama não devolveu um resumo válido em 2 tentativas. O portal segue com o resumo anterior.")

resumo = {
    "origem": "ia", "modelo": LLM_MODEL,
    "titulo": f"Semana de {ini:%d/%m} a {fim:%d/%m}",
    "semana_inicio": str(ini), "semana_fim": str(fim),
    "gerado_em": datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S"),
    "n_noticias": len(noticias), "destaques": destaques,
}
(spark.createDataFrame([(ini, fim, json.dumps(resumo, ensure_ascii=False), LLM_MODEL, len(noticias))],
                       "semana_inicio DATE, semana_fim DATE, conteudo STRING, modelo STRING, n_noticias INT")
 .withColumn("gerado_em", F.current_timestamp()).write.mode("append").saveAsTable(RESUMO_IA_TABLE))

print(f"✅ {resumo['titulo']}: {len(destaques)} destaques a partir de {len(noticias)} notícias\n")
for d in destaques:
    print(f"• {d['titulo']}: {d['resumo']}")
    for n in d["noticias"]:
        print(f"    - [{n['id']}] {n['titulo'][:90]}")
dbutils.notebook.exit(f"{len(destaques)} destaques, {len(noticias)} notícias")
