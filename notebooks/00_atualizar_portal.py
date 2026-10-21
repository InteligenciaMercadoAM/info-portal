# Databricks notebook source
# MAGIC %md
# MAGIC # Atualizar Portal IM
# MAGIC **É só este notebook que roda (na mão ou agendado).** Ele faz tudo em ordem:
# MAGIC 1. Indicadores 4i: realizado + projeção (`novoind_4_Intelligence...`)
# MAGIC 2. Export dos JSONs do portal: notícias, piloto automotivo, conteúdo manual, indicadores (`Export_Portal_Data`)
# MAGIC 3. **Um commit só** no GitHub com os arquivos que mudaram. O Netlify publica sozinho em ~1 minuto.
# MAGIC
# MAGIC Se a etapa 1 falhar (ex.: login da 4i vencido), as etapas 2 e 3 rodam mesmo assim:
# MAGIC as notícias saem atualizadas e os indicadores ficam com o último dado bom. No fim, o notebook falha
# MAGIC de propósito pra avisar (chega e-mail se estiver agendado).
# MAGIC
# MAGIC **Pra agendar:** botão **Schedule** (canto superior direito) → diário, dias úteis, 6h30.
# MAGIC Precisa rodar como a Isabela, porque o token da 4i fica na pasta dela.

# COMMAND ----------

# ===================== CONFIGURAÇÃO =====================
GITHUB_REPO   = "DONO/NOME-DO-REPO"          # como aparece na URL do GitHub, ex.: "isabelapereira/portal-im"
GITHUB_BRANCH = "main"
GITHUB_DATA_PATH = "info-portal/public/data"  # pasta dos JSONs dentro do repositório
SECRET_SCOPE, SECRET_KEY = "portal-im", "github-token"   # token do GitHub com permissão Contents: read/write

# Notebooks que este aqui chama (mesma pasta)
NB_INDICADORES = "./novoind_4_Intelligence_isabela_ajustado_v3"
NB_EXPORT      = "./Export_Portal_Data (3)"

# Onde o Export grava os JSONs (tem que ser o mesmo OUTPUT_DIR do Export)
OUTPUT_DIR = "/Workspace/Users/isabela.pereira@arcelormittal.com.br/portal-im/info-portal/public/data"
ARQUIVOS = ["feed.json", "feed_automotivo.json", "manual.json", "resumo.json",
            "meta.json", "distinct.json", "indicadores.json"]

RODAR_INDICADORES = True    # False pula a 4i (útil pra atualizar só as notícias)
FAZER_COMMIT      = True    # False gera os arquivos sem publicar (pra testar)

# COMMAND ----------

# ===================== 1) INDICADORES 4i =====================
import time
from datetime import datetime
from zoneinfo import ZoneInfo

problemas = []

def etapa(nome, notebook, timeout_min):
    inicio = time.time()
    print(f"▶ {nome}...")
    try:
        dbutils.notebook.run(notebook, timeout_min * 60)
        print(f"✅ {nome} ({(time.time() - inicio) / 60:.1f} min)")
        return True
    except Exception as e:
        msg = str(e).split("\n")[0][:300]
        print(f"❌ {nome} falhou: {msg}")
        problemas.append(f"{nome}: {msg}")
        return False

if RODAR_INDICADORES:
    ok = etapa("Indicadores 4i", NB_INDICADORES, 60)
    if not ok:
        print("   Se o erro for de login/timeout: rode o notebook token_4i na mão e logue com o usuário do Mateus.")
        print("   Seguindo com o export: os indicadores ficam com o último dado gravado.")
else:
    print("⏭ Indicadores 4i pulados (RODAR_INDICADORES = False)")

# COMMAND ----------

# ===================== 2) EXPORT DOS JSONs =====================
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

    entradas, mudaram = [], []
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
