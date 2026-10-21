# Databricks notebook source
# MAGIC %md
# MAGIC # Token 4intelligence (usuário Mateus)
# MAGIC Usado pelos outros notebooks com `%run ./token_4i`. Expõe `get_headers()` e `refresh()`.
# MAGIC
# MAGIC **A cada ~30 dias** (quando o refresh_token vence) alguém precisa rodar este notebook manualmente,
# MAGIC abrir o link e logar com o usuário do Mateus. Sem isso o job falha na etapa de indicadores.
# MAGIC O token fica salvo na pasta pessoal de quem roda: o job tem que rodar como esse mesmo usuário.

# COMMAND ----------

import os, json, base64, time, requests

# ===================== TOKEN 4INTELLIGENCE (usuário Mateus) =====================
# access_token dura 10h e renova sozinho; refresh_token dura 30 dias.
# 1a vez (e depois a cada ~30 dias): rodar esta célula, abrir o link e logar com o usuário do MATEUS no 4CastHub.

AUTH0_DOMAIN = "4intelligence.auth0.com"
CLIENT_ID    = "BBpqcGVYyoFKsjwgf8GcEDhxIBQRQM1H"
AUDIENCE     = "4casthub"
SCOPE        = "offline_access openid profile email"
AUTH_HEADERS = {"content-type": "application/x-www-form-urlencoded"}
USUARIO_ESPERADO = "mateus"   # validação: só aceita token de e-mail que contenha "mateus"

_usuario_dbx = spark.sql("select current_user()").first()[0]
CONFIG_FILE  = f"/Workspace/Users/{_usuario_dbx}/4i_config.json"   # fica na pasta pessoal de quem roda o notebook

def _load():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def _save(tokens):
    data = _load(); data.update(tokens)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def _payload(tok):
    try:
        p = tok.split(".")[1]; p += "=" * (-len(p) % 4)
        return json.loads(base64.urlsafe_b64decode(p))
    except Exception:
        return {}

def login():
    r = requests.post(f"https://{AUTH0_DOMAIN}/oauth/device/code",
                      data={"client_id": CLIENT_ID, "scope": SCOPE, "audience": AUDIENCE},
                      headers=AUTH_HEADERS, timeout=10)
    r.raise_for_status(); dev = r.json()
    print("Abra o link e entre com o usuário do MATEUS no 4CastHub:")
    print(dev["verification_uri_complete"])
    print("Aguardando", end="", flush=True)
    deadline = time.time() + dev.get("expires_in", 300); interval = dev.get("interval", 5)
    while time.time() < deadline:
        time.sleep(interval); print(".", end="", flush=True)
        r = requests.post(f"https://{AUTH0_DOMAIN}/oauth/token",
                          data={"grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                                "device_code": dev["device_code"], "client_id": CLIENT_ID},
                          headers=AUTH_HEADERS, timeout=10)
        if r.ok:
            tokens = r.json()
            email = _payload(tokens["access_token"]).get("https://4intelligence.com.br/email", "")
            if USUARIO_ESPERADO not in email.lower():
                raise PermissionError(f"Login feito com '{email}'. Precisa ser o usuário do Mateus. Token NÃO salvo.")
            _save(tokens)
            print(f"\nLogin OK ({email}). Válido por 30 dias.")
            return tokens["access_token"]
        erro = r.json().get("error")
        if erro == "slow_down": interval += 5
        elif erro != "authorization_pending":
            raise RuntimeError(r.json().get("error_description", "Falha no login"))
    raise TimeoutError("Tempo esgotado. Rode a célula de novo.")

def refresh():
    rt = _load().get("refresh_token")
    if not rt:
        return login()
    r = requests.post(f"https://{AUTH0_DOMAIN}/oauth/token",
                      data={"grant_type": "refresh_token", "client_id": CLIENT_ID, "refresh_token": rt},
                      headers=AUTH_HEADERS, timeout=10)
    if not r.ok:   # refresh vencido (>30 dias) ou revogado
        return login()
    _save(r.json())
    return r.json()["access_token"]

def get_headers():
    """Devolve o headers no mesmo formato do notebook antigo, renovando o token se faltar <15 min."""
    at = _load().get("access_token")
    if not at or _payload(at).get("exp", 0) - time.time() < 15 * 60:
        at = refresh()
    return {"Authorization": at}
