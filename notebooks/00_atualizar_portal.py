# Databricks notebook source
# MAGIC %md
# MAGIC # Atualizar Portal IM
# MAGIC Faz, em ordem:
# MAGIC 1. **Infomet** (desligado por padrão no `RODAR`): scraping → raw → silver → gold → classificação. Hoje isso já roda nos jobs
# MAGIC    "1- Pipeline Medallion" e "2- Classifica + Envia Teams", que também mandam as notícias pro Teams.
# MAGIC 2. **Automotivo:** scraping + classificação com o prompt automotivo (`noticias_automotivebusiness_arcelormittal`)
# MAGIC 3. **4intelligence:** indicadores, realizado + projeção (`novoind_4_Intelligence...`)
# MAGIC 4. **Resumo semanal com IA** (Llama, notebook `Resumo_Semanal_IA`): só gera quando a semana muda, nos outros dias sai em segundos
# MAGIC 5. **Export** dos JSONs do portal (notícias, textos completos, piloto automotivo, conteúdo manual, resumo, indicadores)
# MAGIC 6. **Um commit só** no GitHub com os arquivos que mudaram. O Netlify publica sozinho em ~1 minuto.
# MAGIC
# MAGIC Cada fonte é independente: se uma falhar (ex.: login da 4i vencido, site fora do ar), as outras seguem
# MAGIC e o portal é publicado com o que deu certo. No fim, o notebook falha de propósito pra avisar
# MAGIC (chega e-mail se estiver agendado). Notícias automotivas **não** vão pro Teams, só pro portal.
# MAGIC
# MAGIC **Rode num cluster, não no serverless.**
# MAGIC
# MAGIC **Pra agendar:** botão **Schedule** (canto superior direito) → diário, dias úteis, **depois** do horário em que o job "2- Classifica + Envia Teams" termina.
# MAGIC Precisa rodar como a Isabela, porque o token da 4i fica na pasta dela.
# MAGIC Se um dia ligar o Infomet aqui, desligue os jobs 1 e 2, senão tudo roda duas vezes.

# COMMAND ----------


# ===================== CONFIGURAÇÃO =====================
GITHUB_REPO = "InteligenciaMercadoAM/info-portal"
GITHUB_BRANCH = "main"
GITHUB_DATA_PATH = "public/data"
SECRET_SCOPE, SECRET_KEY = "portal-im", "github-token"

# Pasta do pipeline Infomet
PIPELINE_DIR = (
    "/Workspace/Users/isabela.pereira@arcelormittal.com.br/"
    "chatbot_v3 (final)/pipeline"
)

# Pasta dos notebooks do portal
PORTAL_NOTEBOOKS_DIR = (
    "/Workspace/Users/isabela.pereira@arcelormittal.com.br/"
    "info-portal/notebooks"
)

# Cada fonte: notebooks executados em sequência.
# (nome exibido, caminho do notebook, timeout em minutos)
FONTES = {
    "Infomet": [
        (
            "scraping",
            f"{PIPELINE_DIR}/01_stage_ingestion/01_infomet_scraping_v3",
            60,
        ),
        (
            "raw",
            f"{PIPELINE_DIR}/02_raw/01_infomet_tb_raw_ingestion",
            30,
        ),
        (
            "silver",
            f"{PIPELINE_DIR}/03_silver/01_tb_silver_creation",
            30,
        ),
        (
            "gold",
            f"{PIPELINE_DIR}/04_gold/01_chunking_data_v1",
            30,
        ),
        (
            "classificação (prompt Infomet)",
            f"{PIPELINE_DIR}/Job - Classificacao Relevancia Noticias",
            60,
        ),
    ],

    "Automotivo": [
        (
            "scraping + classificação econômica",
            f"{PORTAL_NOTEBOOKS_DIR}/Scraper_Automotivas_Economico",
            60,
        ),
    ],

    "4intelligence": [
        (
            "indicadores (realizado + projeção)",
            f"{PORTAL_NOTEBOOKS_DIR}/novoind_4_Intelligence_isabela_ajustado_v3",
            60,
        ),
    ],

    # Roda depois das notícias, porque resume o que elas trouxeram
    "Resumo semanal IA": [
        (
            "resumo da semana (Llama)",
            f"{PORTAL_NOTEBOOKS_DIR}/Resumo_Semanal_IA",
            30,
        ),
    ],
}

RODAR = {
    "Infomet": False,
    "Automotivo": True,
    "4intelligence": True,
    "Resumo semanal IA": True,
}

# Parâmetro usado pelo notebook "Incluir_Conteudo_Manual" (botão "Publicar no portal agora?"):
# com "sim", pula Infomet, Automotivo e 4i e roda só Export + commit, pra o conteúdo manual sair em minutos.
dbutils.widgets.text("so_conteudo_manual", "nao")
if dbutils.widgets.get("so_conteudo_manual").strip().lower() in ("sim", "s", "true"):
    RODAR = {fonte: False for fonte in RODAR}
    print("ℹ️ so_conteudo_manual = sim: só Export + commit nesta rodada.")

# Infomet fica False porque os jobs
# "1- Pipeline Medallion" e "2- Classifica + Envia Teams"
# já fazem scraping e classificação.
# Ative apenas se esses jobs forem desligados,
# para evitar processamento duplicado.

NB_EXPORT = f"{PORTAL_NOTEBOOKS_DIR}/Export_Portal_Data (3)"

# Diretório onde o Export grava os JSONs.
# Deve ser o mesmo OUTPUT_DIR definido no notebook Export.
OUTPUT_DIR = (
    "/Workspace/Users/isabela.pereira@arcelormittal.com.br/"
    "portal_dados"
)

ARQUIVOS = [
    "feed.json",
    "feed_automotivo.json",
    "manual.json",
    "resumo.json",
    "meta.json",
    "distinct.json",
    "indicadores.json",
]
# Texto completo das notícias: um arquivo por dia em textos/ (o Export cria e apaga os que saem da janela de 90 dias)
PASTA_TEXTOS = "textos"

FAZER_COMMIT = True  # False gera os arquivos sem publicar


# COMMAND ----------

# ===================== 1) FONTES: INFOMET, AUTOMOTIVO E 4i =====================
import time
from datetime import datetime
from zoneinfo import ZoneInfo

problemas = []

def etapa(nome, notebook, timeout_min):
    inicio = time.time()
    print(f"  ▶ {nome}...")
    try:
        saida = dbutils.notebook.run(notebook, timeout_min * 60)
        print(f"  ✅ {nome} ({(time.time() - inicio) / 60:.1f} min){'  → ' + saida if saida else ''}")
        return True
    except Exception as e:
        msg = str(e).split("\n")[0][:300]
        print(f"  ❌ {nome} falhou: {msg}")
        problemas.append(f"{nome}: {msg}")
        return False

for fonte, passos in FONTES.items():
    if not RODAR.get(fonte, True):
        print(f"⏭ {fonte}: pulado nesta rodada")
        continue
    print(f"\n■ {fonte}")
    for nome, caminho, timeout in passos:
        if not etapa(f"{fonte} · {nome}", caminho, timeout):
            print(f"  ⚠️ {fonte}: etapas seguintes puladas; o portal usa os últimos dados bons desta fonte.")
            if fonte == "4intelligence":
                print("  Se o erro for de login/timeout: rode o notebook token_4i na mão e logue com o usuário do Mateus.")
            break

# COMMAND ----------

# ===================== 2) EXPORT DOS JSONs =====================
print("\n■ Portal")
ok_export = etapa("Export dos dados do portal", NB_EXPORT, 30)
if not ok_export:
    raise RuntimeError("Export falhou: nada foi publicado. Abra o notebook do Export e rode na mão pra ver o erro. " + " | ".join(problemas))

# COMMAND ----------

# ===================== 3) COMMIT ÚNICO NO GITHUB =====================
# Usa a API do GitHub (blobs → tree → commit → ref) pra mandar todos os arquivos num commit só,
# e só os que mudaram de verdade.
import base64, hashlib, os, requests

def git_blob_sha(conteudo: bytes) -> str:
    # mesmo hash que o git usa: dá pra saber se o arquivo mudou sem baixar nada
    return hashlib.sha1(b"blob %d\0" % len(conteudo) + conteudo).hexdigest()

def gh(metodo, caminho, **kw):
    r = requests.request(metodo, f"https://api.github.com/repos/{GITHUB_REPO}{caminho}",
                         headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json"},
                         timeout=120, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub {metodo} {caminho}: HTTP {r.status_code} {r.text[:300]}")
    return r.json() if r.content else {}

if not FAZER_COMMIT:
    print("⏭ FAZER_COMMIT = False: arquivos gerados em", OUTPUT_DIR, "mas não publicados.")
else:
    TOKEN = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)

    # estado atual do branch e da pasta de dados no GitHub
    ref = gh("GET", f"/git/ref/heads/{GITHUB_BRANCH}")
    commit_atual = ref["object"]["sha"]
    tree_atual = gh("GET", f"/git/commits/{commit_atual}")["tree"]["sha"]
    try:
        no_github = {f["name"]: f["sha"] for f in gh("GET", f"/contents/{GITHUB_DATA_PATH}", params={"ref": GITHUB_BRANCH})}
    except RuntimeError:
        no_github = {}

    try:
        no_github_textos = {f["name"]: f["sha"] for f in gh("GET", f"/contents/{GITHUB_DATA_PATH}/{PASTA_TEXTOS}", params={"ref": GITHUB_BRANCH})}
    except RuntimeError:
        no_github_textos = {}
    pasta_textos_local = f"{OUTPUT_DIR}/{PASTA_TEXTOS}"
    textos_locais = sorted(f for f in os.listdir(pasta_textos_local) if f.endswith(".json")) if os.path.isdir(pasta_textos_local) else []

    entradas, mudaram = [], []
    n_textos = 0
    for nome in textos_locais:
        conteudo = open(f"{pasta_textos_local}/{nome}", "rb").read()
        if no_github_textos.get(nome) == git_blob_sha(conteudo):
            continue
        blob = gh("POST", "/git/blobs", json={"content": base64.b64encode(conteudo).decode(), "encoding": "base64"})
        entradas.append({"path": f"{GITHUB_DATA_PATH}/{PASTA_TEXTOS}/{nome}", "mode": "100644", "type": "blob", "sha": blob["sha"]})
        n_textos += 1
    # dias que saíram da janela (ou TEXTO_COMPLETO = False no Export): some do GitHub também
    # (só apaga se o Export criou a pasta nesta máquina, pra nunca limpar tudo por engano)
    if os.path.isdir(pasta_textos_local):
        for nome in set(no_github_textos) - set(textos_locais):
            entradas.append({"path": f"{GITHUB_DATA_PATH}/{PASTA_TEXTOS}/{nome}", "mode": "100644", "type": "blob", "sha": None})
            n_textos += 1
    if n_textos:
        mudaram.append(f"{PASTA_TEXTOS}/: {n_textos} arquivo(s) de texto completo")
    for nome in ARQUIVOS:
        caminho_local = f"{OUTPUT_DIR}/{nome}"
        if not os.path.exists(caminho_local):
            continue
        conteudo = open(caminho_local, "rb").read()
        if no_github.get(nome) == git_blob_sha(conteudo):
            continue                                  # igual ao que já está publicado
        blob = gh("POST", "/git/blobs", json={"content": base64.b64encode(conteudo).decode(), "encoding": "base64"})
        entradas.append({"path": f"{GITHUB_DATA_PATH}/{nome}", "mode": "100644", "type": "blob", "sha": blob["sha"]})
        mudaram.append(f"{nome} ({len(conteudo) / 1024:.0f} KB)")

    # resumo.json removido pelo Export (sem resumo semanal ativo) → remove do GitHub também
    if "resumo.json" in no_github and not os.path.exists(f"{OUTPUT_DIR}/resumo.json"):
        entradas.append({"path": f"{GITHUB_DATA_PATH}/resumo.json", "mode": "100644", "type": "blob", "sha": None})
        mudaram.append("resumo.json (removido)")

    if not entradas:
        print("✅ Nada mudou desde a última publicação: sem commit.")
    else:
        agora = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M")
        tree = gh("POST", "/git/trees", json={"base_tree": tree_atual, "tree": entradas})
        novo = gh("POST", "/git/commits", json={"message": f"Atualiza dados do portal ({agora})",
                                                  "tree": tree["sha"], "parents": [commit_atual]})
        gh("PATCH", f"/git/refs/heads/{GITHUB_BRANCH}", json={"sha": novo["sha"]})
        print(f"✅ Commit {novo['sha'][:7]} em {GITHUB_REPO}@{GITHUB_BRANCH}:")
        for m in mudaram:
            print("   •", m)
        print("   O Netlify publica em ~1 minuto.")

# COMMAND ----------

# ===================== RESUMO =====================
if problemas:
    # falha no fim (depois de publicar as notícias) pra o agendamento mandar e-mail
    raise RuntimeError("Portal publicado, mas com problemas: " + " | ".join(problemas))
print("🎉 Portal IM atualizado sem problemas.")
