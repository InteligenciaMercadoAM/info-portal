# Databricks notebook source
# MAGIC %md
# MAGIC # Guardar o token do GitHub no Databricks (rodar uma vez)
# MAGIC Guarda o token no secret `portal-im` / `github-token`, que o `00_atualizar_portal` usa pra fazer o commit.
# MAGIC
# MAGIC 1. Rode a primeira célula: aparece um campo **token_github** no topo do notebook.
# MAGIC 2. Cole o token nesse campo (não escreva o token no código: este notebook fica no GitHub).
# MAGIC 3. Rode a segunda célula.
# MAGIC 4. Apague o conteúdo do campo depois.
# MAGIC
# MAGIC Quando o token vencer ou for trocado, é só repetir.

# COMMAND ----------

dbutils.widgets.text("token_github", "", "token_github")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

SCOPE, KEY = "portal-im", "github-token"
token = dbutils.widgets.get("token_github").strip()
if not token.startswith(("github_pat_", "ghp_")):
    raise ValueError("Cole o token do GitHub no campo token_github lá em cima (começa com github_pat_).")

w = WorkspaceClient()
if SCOPE not in [s.name for s in w.secrets.list_scopes()]:
    w.secrets.create_scope(scope=SCOPE)
    print(f"Scope '{SCOPE}' criado.")
w.secrets.put_secret(scope=SCOPE, key=KEY, string_value=token)
print(f"✅ Token guardado em {SCOPE}/{KEY}.")

# teste rápido: o token enxerga o repositório?
import requests
r = requests.get("https://api.github.com/repos/InteligenciaMercadoAM/info-portal",
                 headers={"Authorization": f"Bearer {dbutils.secrets.get(SCOPE, KEY)}"}, timeout=30)
print("Acesso ao repositório:", "OK" if r.ok else f"falhou (HTTP {r.status_code}): {r.text[:200]}")
if r.ok:
    # confere se a pasta de dados está onde o 00_atualizar_portal espera (GITHUB_DATA_PATH)
    for pasta in ["info-portal/public/data", "public/data"]:
        rr = requests.get(f"https://api.github.com/repos/InteligenciaMercadoAM/info-portal/contents/{pasta}",
                          headers={"Authorization": f"Bearer {dbutils.secrets.get(SCOPE, KEY)}"}, timeout=30)
        if rr.ok:
            print(f"Pasta de dados no GitHub: {pasta}  → use esse valor em GITHUB_DATA_PATH no 00_atualizar_portal")
            break
    else:
        print("Não achei public/data no repositório: confira o GITHUB_DATA_PATH.")
dbutils.widgets.remove("token_github")
