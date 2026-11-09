# Databricks notebook source
# MAGIC %md
# MAGIC # Scraper e classificação econômica — Automotive Business + AutoData
# MAGIC
# MAGIC Notebook baseado no scraper corrigido, com foco em reduzir chamadas caras ao modelo:
# MAGIC
# MAGIC 1. Coleta e grava notícias novas de Automotive Business e AutoData.
# MAGIC 2. Aplica pré-filtro conservador por palavras-chave; só ruído claro recebe `Baixa` sem IA.
# MAGIC 3. Classifica um lote limitado por execução usando um modelo menor, título + resumo truncado.
# MAGIC 4. Tenta novamente com o modelo maior apenas registros cuja resposta não foi interpretada.
# MAGIC 5. Usa colunas explícitas e chave composta `id + fonte_site` para evitar desalinhamento e duplicatas.
# MAGIC
# MAGIC **Importante:** esta versão ainda não usa embeddings do Infomet: preciso do nome da tabela e dos nomes das colunas de texto e classificação para integrar essa etapa corretamente. Não inventei esses nomes. O notebook já reduz custo com filtro, texto curto, lote limitado e modelo menor; o modelo de 70B só é chamado para tentar corrigir respostas que o modelo menor não conseguiu interpretar.
# MAGIC
# MAGIC **Primeiro uso:** confira `DATA_INICIAL` e `RESETAR_TABELAS` na célula de configuração. O reset é opcional e destrutivo; deixe `False` se quiser preservar o histórico.
# MAGIC

# COMMAND ----------

# DBTITLE 1,1. Instalar dependências
# MAGIC %pip install requests beautifulsoup4 lxml

# COMMAND ----------

# DBTITLE 1,2. Importações e configuração
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import hashlib

# ID estável entre execuções (o hash() nativo do Python muda a cada sessão e gerava duplicatas no MERGE)
def stable_id(texto, digitos=15):
    return int(hashlib.md5(str(texto).encode("utf-8")).hexdigest(), 16) % (10 ** digitos)

BASE_URL = "https://www.automotivebusiness.com.br"
NOTICIAS_URL = f"{BASE_URL}/noticias"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": BASE_URL,
}

# Data inicial inclusiva para a nova base. Mantenha esta data para acumular a partir do recomeço.
DATA_INICIAL = "2026-10-27"

# Deixe False no uso normal. Para apagar as duas tabelas antigas, mude para True
# SOMENTE na primeira execução e depois volte para False antes de agendar o job.
RESETAR_TABELAS = False

TABELA_RAW = "hive_metastore.default.noticias_automotivebusiness"
TABELA_CLASS = "hive_metastore.default.noticias_automotivebusiness_arcelormittal"

print(f"Configuração OK | publicações a partir de {DATA_INICIAL} | reset={RESETAR_TABELAS}")
# Limite de chamadas caras por execução. Rode a célula de classificação novamente para o próximo lote.
TAMANHO_LOTE = 100
MAX_CARACTERES_RESUMO = 700
MODELO_RAPIDO = "databricks-meta-llama-3-1-8b-instruct"
MODELO_FALLBACK = "databricks-meta-llama-3-3-70b-instruct"
# Páginas da API do WordPress por fonte (100 posts cada). 2 basta no dia a dia.
# Pra preencher o texto completo das notícias que já estavam na base, rode UMA vez com 10 e depois volte pra 2.
PAGINAS_COLETA = 2



# COMMAND ----------

# DBTITLE 1,4. Scraper principal
from html import unescape
import re as _re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import socket

WP_API = "https://painel.automotivebusiness.com.br/wp-json/wp/v2"
FRONTEND = "www.automotivebusiness.com.br"
BACKEND  = "painel.automotivebusiness.com.br"

FIELDS = "id,date,title,link,excerpt,content,categories,tags,author,_links"   # content = texto completo

# --- Configuracao de Retry ---
MAX_RETRIES = 4          # tentativas totais (1 original + 3 retries)
BACKOFF_FACTOR = 2       # espera: 2s, 4s, 8s entre retries
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]


def verificar_dns(host="painel.automotivebusiness.com.br"):
    """Verifica se o dominio resolve antes de iniciar o scraping."""
    try:
        socket.gethostbyname(host)
        return True
    except socket.gaierror:
        return False


def criar_session_com_retry():
    """Cria session HTTP com retry e backoff exponencial."""
    session = requests.Session()
    session.headers.update(HEADERS)
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def limpar_html(texto):
    """Remove tags HTML e decodifica entidades."""
    return unescape(_re.sub(r"<[^>]+>", "", texto or "")).strip()


def html_para_texto(html):
    """Texto completo do post (campo content da API), mantendo a quebra entre parágrafos."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    for lixo in soup(["script", "style", "figure", "iframe", "noscript", "form"]):
        lixo.decompose()
    blocos = [b.get_text(" ", strip=True) for b in soup.find_all(["p", "h2", "h3", "h4", "li", "blockquote"])
              if not b.find_parent(["li", "blockquote"])]
    texto = "\n\n".join(b for b in blocos if b) or soup.get_text("\n", strip=True)
    texto = _re.sub(r"[ \t\xa0]+", " ", texto)
    return _re.sub(r" ([.,;:!?)])", r"\1", texto).strip()


def scrape_wp_api(n_paginas=5, por_pagina=100):
    """Coleta posts via WordPress REST API com paginacao e retry."""

    # Pre-check: verificar DNS antes de tentar requests
    print("Verificando conectividade DNS...", end=" ")
    if not verificar_dns():
        print("❌ FALHOU")
        print("⚠️  DNS nao resolve 'painel.automotivebusiness.com.br'.")
        print("   Possivel bloqueio de rede/firewall na VNet.")
        print("   Scraping abortado graciosamente — pipeline continua sem novos dados.")
        return []  # Retorna vazio em vez de crashar o job

    print("✅ OK")
    session = criar_session_com_retry()
    todos = []

    for pagina in range(1, n_paginas + 1):
        params = {
            "page"     : pagina,
            "per_page" : por_pagina,
            "_fields"  : FIELDS,
            "orderby"  : "date",
            "order"    : "desc",
        }
        try:
            resp = session.get(f"{WP_API}/posts", params=params, timeout=30)
        except requests.exceptions.ConnectionError as e:
            print(f"\n❌ Erro de conexao na pagina {pagina}: {e}")
            print("   Retornando dados coletados ate agora.")
            break
        except requests.exceptions.Timeout:
            print(f"\n⏰ Timeout na pagina {pagina} (30s). Retornando dados parciais.")
            break
        except requests.exceptions.RequestException as e:
            print(f"\n❌ Erro inesperado na pagina {pagina}: {e}")
            break

        print(f"Pagina {pagina} -> HTTP {resp.status_code}", end="")

        if resp.status_code == 400:
            print(" (sem mais paginas)")
            break
        if resp.status_code != 200:
            print(f" ERRO: {resp.text[:200]}")
            break

        posts = resp.json()
        print(f" | {len(posts)} posts")
        if not posts:
            break

        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))

        for p in posts:
            link_frontend = p.get("link", "").replace(BACKEND, FRONTEND)
            todos.append({
                "id"         : p.get("id"),
                "data"       : p.get("date"),
                "titulo"     : limpar_html(p.get("title",   {}).get("rendered", "")),
                "resumo"     : limpar_html(p.get("excerpt", {}).get("rendered", "")),
                "conteudo"   : html_para_texto(p.get("content", {}).get("rendered", "")),
                "link"       : link_frontend,
                "categorias" : str(p.get("categories", [])),
                "tags"       : str(p.get("tags", [])),
                "coletado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

        if pagina >= total_pages:
            print(f"Total de paginas: {total_pages} — coleta concluida.")
            break

        time.sleep(0.5)

    return todos


# --- Execucao ---
# Para coleta diaria incremental: 2 paginas (200 artigos mais recentes) e suficiente
# Para reprocessamento completo do historico, aumente para n_paginas=20 ou mais
noticias = scrape_wp_api(n_paginas=PAGINAS_COLETA, por_pagina=100)
df_noticias = pd.DataFrame(noticias)
print(f"\nArtigos coletados nesta execucao: {len(df_noticias)}")
if not df_noticias.empty:
    display(df_noticias.head(5))
else:
    print("Nenhum artigo coletado — verificar conectividade de rede.")

# COMMAND ----------

# DBTITLE 1,4b. Scraper Autodata (www.autodata.com.br)
from html import unescape
import re as _re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import socket

# ============================================================
# SCRAPER: www.autodata.com.br
# Estratégia: WordPress REST API (principal) + HTML fallback
# ============================================================

AUTODATA_DOMAIN = "www.autodata.com.br"
AUTODATA_WP_API = f"https://{AUTODATA_DOMAIN}/wp-json/wp/v2"
AUTODATA_URL = f"https://{AUTODATA_DOMAIN}"


def verificar_dns_autodata():
    """Verifica se o domínio resolve antes de iniciar o scraping."""
    try:
        socket.gethostbyname(AUTODATA_DOMAIN)
        return True
    except socket.gaierror:
        return False


def criar_session_autodata():
    """Cria session HTTP com retry e backoff exponencial."""
    session = requests.Session()
    session.headers.update(HEADERS)
    retry_strategy = Retry(
        total=4,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def scrape_autodata_wp_api(session, n_paginas=2, por_pagina=100):
    """Tenta coletar via WordPress REST API."""
    todos = []
    fields = "id,date,title,link,excerpt,content,categories,tags"   # content = texto completo

    for pagina in range(1, n_paginas + 1):
        params = {
            "page": pagina,
            "per_page": por_pagina,
            "_fields": fields,
            "orderby": "date",
            "order": "desc",
        }
        try:
            resp = session.get(f"{AUTODATA_WP_API}/posts", params=params, timeout=30)
        except Exception as e:
            print(f"  ❌ Erro na pagina {pagina}: {e}")
            break

        if resp.status_code == 404:
            print(f"  WP API não disponível (404). Usando fallback HTML.")
            return None  # Sinaliza para usar fallback
        if resp.status_code == 400:
            print(f"  Pagina {pagina}: sem mais resultados.")
            break
        if resp.status_code != 200:
            print(f"  Pagina {pagina}: HTTP {resp.status_code} - {resp.text[:200]}")
            break

        posts = resp.json()
        print(f"  Pagina {pagina} -> {len(posts)} posts")
        if not posts:
            break

        for p in posts:
            todos.append({
                "id": p.get("id"),
                "data": p.get("date"),
                "titulo": limpar_html(p.get("title", {}).get("rendered", "")),
                "resumo": limpar_html(p.get("excerpt", {}).get("rendered", "")),
                "conteudo": html_para_texto(p.get("content", {}).get("rendered", "")),
                "link": p.get("link", ""),
                "categorias": str(p.get("categories", [])),
                "tags": str(p.get("tags", [])),
                "coletado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

        time.sleep(0.5)

    return todos


def scrape_autodata_html(session, n_paginas=5):
    """Fallback: coleta via scraping HTML das páginas de notícias."""
    todos = []
    
    for pagina in range(1, n_paginas + 1):
        url = f"{AUTODATA_URL}/noticias/page/{pagina}/" if pagina > 1 else f"{AUTODATA_URL}/noticias/"
        try:
            resp = session.get(url, timeout=30)
        except Exception as e:
            print(f"  ❌ Erro HTML pagina {pagina}: {e}")
            break

        if resp.status_code != 200:
            print(f"  Pagina {pagina}: HTTP {resp.status_code}")
            break

        soup = BeautifulSoup(resp.text, "lxml")
        
        # Tentar encontrar artigos - padrões comuns WordPress
        artigos = soup.find_all("article") or soup.select(".post, .entry, .news-item, .td-module-thumb")
        
        if not artigos:
            # Fallback: buscar links com padrão de notícia
            artigos = soup.select("h2 a, h3 a, .entry-title a")
            if not artigos:
                print(f"  Pagina {pagina}: nenhum artigo encontrado no HTML.")
                break
            
            for a in artigos:
                link = a.get("href", "")
                if not link.startswith("http"):
                    link = AUTODATA_URL + link
                titulo = a.get_text(strip=True)
                if titulo and len(titulo) > 15:  # Filtrar links curtos/menus
                    todos.append({
                        "id": stable_id(link, 9),  # ID único baseado no link
                        "data": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                        "titulo": titulo,
                        "resumo": "",
                        "link": link,
                        "categorias": "[]",
                        "tags": "[]",
                        "coletado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })
        else:
            for art in artigos:
                # Extrair título
                titulo_tag = art.find(["h2", "h3", "h4"]) or art.find(class_=_re.compile(r"title"))
                titulo = titulo_tag.get_text(strip=True) if titulo_tag else ""
                
                # Extrair link
                link_tag = art.find("a", href=True) if not art.name == "a" else art
                link = link_tag["href"] if link_tag else ""
                if link and not link.startswith("http"):
                    link = AUTODATA_URL + link
                
                # Extrair resumo
                resumo_tag = art.find(class_=_re.compile(r"excerpt|summary|desc")) or art.find("p")
                resumo = resumo_tag.get_text(strip=True) if resumo_tag else ""
                
                # Extrair data
                data_tag = art.find("time") or art.find(class_=_re.compile(r"date|time"))
                data_str = data_tag.get("datetime", data_tag.get_text(strip=True)) if data_tag else datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                
                if titulo and len(titulo) > 15:
                    todos.append({
                        "id": stable_id(link, 9),
                        "data": data_str,
                        "titulo": limpar_html(titulo),
                        "resumo": limpar_html(resumo),
                        "link": link,
                        "categorias": "[]",
                        "tags": "[]",
                        "coletado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })

        print(f"  Pagina {pagina} -> {len(artigos)} artigos encontrados")
        time.sleep(1)  # Mais conservador para HTML scraping

    return todos


# --- Execução Autodata ---
print("="*60)
print("SCRAPER: www.autodata.com.br")
print("="*60)

print("\nVerificando DNS...", end=" ")
if not verificar_dns_autodata():
    print("❌ FALHOU")
    print("⚠️  DNS não resolve 'www.autodata.com.br'.")
    print("   Possível bloqueio de rede/firewall na VNet.")
    print("   Scraping Autodata abortado — pipeline continua sem esta fonte.")
    df_noticias_autodata = pd.DataFrame()
else:
    print("✅ OK")
    session_ad = criar_session_autodata()
    
    # Estratégia 1: WordPress REST API
    print("\n📡 Tentando WordPress REST API...")
    noticias_autodata = scrape_autodata_wp_api(session_ad, n_paginas=PAGINAS_COLETA, por_pagina=100)
    
    # Estratégia 2: Fallback HTML
    if noticias_autodata is None:
        print("\n🔄 Usando fallback HTML scraping...")
        noticias_autodata = scrape_autodata_html(session_ad, n_paginas=5)
    
    df_noticias_autodata = pd.DataFrame(noticias_autodata) if noticias_autodata else pd.DataFrame()
    print(f"\n✅ Autodata: {len(df_noticias_autodata)} artigos coletados")
    if not df_noticias_autodata.empty:
        display(df_noticias_autodata.head(5))

# COMMAND ----------

# DBTITLE 1,5. Salvar como tabela Delta
# Combina fontes, aplica a data inicial e grava a base bruta.
# O reset só acontece depois que a coleta retorna dados válidos, para evitar apagar tudo
# se o portal estiver indisponível ou a API falhar.

import pandas as pd
from pyspark.sql import functions as F

COLUNAS_RAW = [
    "id", "data", "titulo", "resumo", "link", "categorias", "tags", "coletado_em", "fonte_site", "conteudo"
]

_fontes = []
if "df_noticias" in globals() and not df_noticias.empty:
    _ab = df_noticias.copy()
    _ab["fonte_site"] = "automotivebusiness"
    _fontes.append(_ab[COLUNAS_RAW])
    print(f"Automotive Business coletadas: {len(_ab)}")

if "df_noticias_autodata" in globals() and not df_noticias_autodata.empty:
    _ad = df_noticias_autodata.copy()
    _ad["fonte_site"] = "autodata"
    for c in COLUNAS_RAW:
        if c not in _ad.columns:
            _ad[c] = None
    _fontes.append(_ad[COLUNAS_RAW])
    print(f"AutoData coletadas: {len(_ad)}")

if not _fontes:
    raise RuntimeError("Nenhuma fonte retornou notícias. Nenhuma tabela foi apagada ou alterada.")

df_todas_noticias = pd.concat(_fontes, ignore_index=True)
for c in COLUNAS_RAW:
    if c not in df_todas_noticias.columns:
        df_todas_noticias[c] = None
df_todas_noticias = df_todas_noticias[COLUNAS_RAW].copy()
# texto completo: vazio quando a fonte não trouxe (ex.: fallback HTML da AutoData); o MERGE não apaga o que já existe
df_todas_noticias["conteudo"] = df_todas_noticias["conteudo"].fillna("").astype(str)

# Normaliza tipos e filtra por data de publicação, sem usar a data de coleta.
df_todas_noticias["id"] = pd.to_numeric(df_todas_noticias["id"], errors="coerce")
df_todas_noticias["data_dt"] = pd.to_datetime(df_todas_noticias["data"], errors="coerce")
data_inicial_dt = pd.to_datetime(DATA_INICIAL).date()
df_todas_noticias = df_todas_noticias[
    df_todas_noticias["data_dt"].dt.date >= data_inicial_dt
].copy()
df_todas_noticias.drop(columns=["data_dt"], inplace=True)

# Descarta registros sem chave/link/título e remove duplicatas pela chave composta.
df_todas_noticias = df_todas_noticias[
    df_todas_noticias["id"].notna()
    & df_todas_noticias["link"].notna()
    & df_todas_noticias["titulo"].notna()
].copy()
df_todas_noticias["id"] = df_todas_noticias["id"].astype("int64")
df_todas_noticias = df_todas_noticias.drop_duplicates(["id", "fonte_site"])
if df_todas_noticias.empty:
    raise RuntimeError(
        f"Nenhuma notícia com data de publicação >= {DATA_INICIAL}. "
        "As tabelas foram preservadas. Confira a data e a coleta antes de continuar."
    )

print(f"Notícias válidas desde {DATA_INICIAL}: {len(df_todas_noticias)}")
display(df_todas_noticias[["fonte_site", "data", "titulo", "link"]].head(10))

# Se solicitado, apaga SOMENTE as duas tabelas deste pipeline.
if RESETAR_TABELAS:
    print("RESET habilitado: removendo as tabelas antigas do pipeline...")
    spark.sql(f"DROP TABLE IF EXISTS {TABELA_CLASS}")
    spark.sql(f"DROP TABLE IF EXISTS {TABELA_RAW}")
    print("Tabelas antigas removidas.")

# Cria a tabela bruta com schema explícito.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABELA_RAW} (
    id BIGINT,
    data STRING,
    titulo STRING,
    resumo STRING,
    link STRING,
    categorias STRING,
    tags STRING,
    coletado_em STRING,
    fonte_site STRING COMMENT 'Fonte: automotivebusiness ou autodata',
    conteudo STRING COMMENT 'Texto completo do post (campo content da API do WordPress)'
) USING DELTA
""")
# Tabela criada antes desta versão: ganha a coluna do texto completo
if "conteudo" not in spark.table(TABELA_RAW).columns:
    spark.sql(f"ALTER TABLE {TABELA_RAW} ADD COLUMN conteudo STRING COMMENT 'Texto completo do post (campo content da API do WordPress)'")
    print("Coluna conteudo criada na base bruta.")

# Converte para Spark e garante ordem/tipos de colunas.
sdf = spark.createDataFrame(df_todas_noticias[COLUNAS_RAW])
sdf = (sdf
    .withColumn("id", F.col("id").cast("long"))
    .withColumn("data", F.col("data").cast("string"))
    .withColumn("titulo", F.col("titulo").cast("string"))
    .withColumn("resumo", F.col("resumo").cast("string"))
    .withColumn("link", F.col("link").cast("string"))
    .withColumn("categorias", F.col("categorias").cast("string"))
    .withColumn("tags", F.col("tags").cast("string"))
    .withColumn("coletado_em", F.col("coletado_em").cast("string"))
    .withColumn("fonte_site", F.col("fonte_site").cast("string"))
    .withColumn("conteudo", F.when(F.trim(F.col("conteudo")) != "", F.col("conteudo")).cast("string"))
    .select(*COLUNAS_RAW)
)
sdf.createOrReplaceTempView("_noticias_novas")

# MERGE explícito por id + fonte_site. Sem INSERT * ou UPDATE *.
spark.sql(f"""
MERGE INTO {TABELA_RAW} AS t
USING _noticias_novas AS s
ON t.id = s.id AND t.fonte_site = s.fonte_site
WHEN MATCHED THEN UPDATE SET
    t.data = s.data,
    t.titulo = s.titulo,
    t.resumo = s.resumo,
    t.link = s.link,
    t.categorias = s.categorias,
    t.tags = s.tags,
    t.coletado_em = s.coletado_em,
    t.conteudo = coalesce(s.conteudo, t.conteudo)
WHEN NOT MATCHED THEN INSERT
    (id, data, titulo, resumo, link, categorias, tags, coletado_em, fonte_site, conteudo)
VALUES
    (s.id, s.data, s.titulo, s.resumo, s.link, s.categorias, s.tags, s.coletado_em, s.fonte_site, s.conteudo)
""")

total_raw = spark.table(TABELA_RAW).count()
print(f"Base bruta atualizada. Total acumulado: {total_raw}")
print(f"Com texto completo nesta coleta: {int((df_todas_noticias['conteudo'].str.len() > 0).sum())} de {len(df_todas_noticias)}")
display(spark.table(TABELA_RAW).filter(F.to_date("data") >= F.lit(DATA_INICIAL))
        .groupBy("fonte_site").count().orderBy("fonte_site"))


# COMMAND ----------

# DBTITLE 1,5b. Migração de schema (score + empresa + categoria)
# Schema da tabela classificada
# A tabela classificada será criada com o schema completo na célula de pré-filtro ou na célula SQL
# de classificação. Não fazemos ALTER/UPDATE antes da criação para evitar dependência de ordem.
print("Schema classificado será criado pela etapa de classificação, com colunas explícitas.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Pré-filtro por palavras-chave (antes da IA)
# MAGIC Notícias claramente **Baixa** (premiação, patrocínio, supercarros, automobilismo, trânsito, preço de tabela...) entram direto como Baixa e **não gastam chamada ao modelo**.
# MAGIC
# MAGIC 1. Rode com `APLICAR = False` e confira as amostras. 2. Se estiver bom, troque para `True` e rode de novo. 3. Rode a célula da IA abaixo: ela pula o que já foi gravado.
# MAGIC
# MAGIC Só exclui se o **título** bater numa regra e **nenhuma** palavra de proteção (aço, produção, fábrica, Anfavea, tarifa...) aparecer no título+resumo.

# COMMAND ----------

# ==================== PRÉ-FILTRO POR PALAVRAS-CHAVE (antes da IA) ====================
# Notícias claramente "Baixa" para a ArcelorMittal (premiação, patrocínio, supercarros, automobilismo,
# trânsito, preço de tabela...) não precisam gastar uma chamada ao modelo: entram direto como Baixa.
#
# Como funciona (e por que é conservador):
#   1) a notícia só é excluída se o TÍTULO bater com uma regra de exclusão;
#   2) e NÃO pode aparecer nenhuma palavra de PROTEGER no título+resumo (aço, produção, fábrica,
#      Anfavea, tarifa...). Na dúvida, vai pra IA, igual ao prompt, que manda errar pra cima.
#   3) as excluídas são gravadas na mesma tabela como Baixa, com o motivo em `motivo_exclusao`.
#      Assim a célula de classificação (IA) pula essas notícias sozinha.
#
# O filtro economiza chamadas da IA; mantenha as regras conservadoras. APLICAR=True grava apenas ruído claro como Baixa.

import re
from pyspark.sql import functions as F

APLICAR = True   # False = simulação (não grava nada) | True = grava as excluídas como Baixa

TABELA_RAW   = "hive_metastore.default.noticias_automotivebusiness"
TABELA_CLASS = "hive_metastore.default.noticias_automotivebusiness_arcelormittal"

# ---- regras de exclusão (olham só o TÍTULO). Edite à vontade. A ordem define a prioridade do motivo. ----
REGRAS = {
    "luxo_nicho": ["Ferrari", "Lamborghini", "Porsche", "McLaren", "Aston Martin", "Bentley", "Rolls-Royce",
                   "Maserati", "Bugatti", "Pagani", "Koenigsegg", "Lotus", "superesportivo", "supercarro", "hipercarro"],
    "premiacao_evento": ["Carro do Ano", "Prêmio", "Premiação", "Troféu", "Melhores do Ano", "Festival"],
    "automobilismo": ["Fórmula 1", "F1", "Stock Car", "MotoGP", "Rali", "Rally", "Copa Truck", "Fórmula Indy",
                      "Le Mans", "Grande Prêmio", "pole position"],
    "marketing_patrocinio": ["patrocínio", "patrocina", "patrocinador", "campanha publicitária",
                             "garoto-propaganda", "embaixador da marca"],
    "transito_varejo": ["trânsito", "Detran", "IPVA", "CNH", "habilitação", "rodízio", "radar de velocidade",
                        "tabela de preços", "reajuste de preços", "reajuste de preço", "novos preços"],
    "curiosidade_historica": ["antigomobilismo", "carro antigo", "carros antigos", "clássicos", "relíquia", "aniversário"],
}

# ---- palavras que PROTEGEM a notícia (olham título + resumo): se aparecer qualquer uma, vai pra IA ----
PROTEGER = [
    "aço", "aco", "siderurg", "ArcelorMittal", "Gerdau", "Usiminas", "CSN", "Aperam", "laminado", "chapa", "bobina",
    "AHSS", "alumínio", "aluminio", "lightweight", "aço verde",
    "produção", "producao", "fábrica", "fabrica", "planta", "linha de montagem", "capacidade produtiva",
    "investimento", "investimentos", "Anfavea", "emplacamento", "emplacamentos",
    "Mover", "Rota 2030", "tarifa", "tarifas", "imposto de importação", "antidumping",
    "descarbonização", "bateria", "baterias", "autopeças", "fornecedor", "fornecedores", "estampagem", "sistemista",
]

def rx(termos):
    # (?iu) = ignora maiúsculas/minúsculas COM acento; lookarounds = palavra inteira (\b falha com acentos)
    return r"(?iu)(?<![\p{L}\p{N}])(?:" + "|".join(re.escape(t) for t in termos) + r")(?![\p{L}\p{N}])"

# ---- notícias ainda não classificadas (mesma lógica da célula da IA: anti-join por id) ----
raw = spark.table(TABELA_RAW).select("id", "data", "titulo", "resumo", "link", "categorias", "tags", "coletado_em", "fonte_site")
if spark.catalog.tableExists(TABELA_CLASS):
    pend = raw.join(spark.table(TABELA_CLASS).select("id", "fonte_site").distinct(), ["id", "fonte_site"], "left_anti")
else:
    pend = raw

titulo = F.coalesce(F.col("titulo"), F.lit(""))
texto  = F.concat_ws(". ", F.col("titulo"), F.col("resumo"))

motivo = F.lit(None).cast("string")
for nome, termos in reversed(list(REGRAS.items())):        # reversed: a 1ª regra da lista fica com prioridade
    motivo = F.when(titulo.rlike(rx(termos)), F.lit(nome)).otherwise(motivo)

pend = (pend.withColumn("motivo_exclusao", motivo)
            .withColumn("protegida", texto.rlike(rx(PROTEGER))))
pend = pend.withColumn("excluir", F.col("motivo_exclusao").isNotNull() & ~F.col("protegida")).cache()

n_pend = pend.count()
n_excl = pend.filter("excluir").count()
n_prot = pend.filter(F.col("motivo_exclusao").isNotNull() & F.col("protegida")).count()
print(f"Pendentes de classificação: {n_pend}")
print(f"Seriam excluídas (Baixa sem IA): {n_excl}  ({(n_excl / n_pend * 100) if n_pend else 0:.1f}% das chamadas economizadas)")
print(f"Bateram numa regra mas foram protegidas (vão pra IA): {n_prot}")

print("\nPor regra (excluídas):")
display(pend.filter("excluir").groupBy("motivo_exclusao").count().orderBy(F.desc("count")))
print("Amostra do que seria excluído (confira se alguma não deveria):")
display(pend.filter("excluir").select("motivo_exclusao", "fonte_site", "titulo").orderBy(F.rand(7)).limit(40))
print("Amostra do que bateu numa regra mas foi protegido:")
display(pend.filter(F.col("motivo_exclusao").isNotNull() & F.col("protegida")).select("motivo_exclusao", "titulo").limit(15))

# ---- grava as excluídas como Baixa (só com APLICAR = True) ----
if not APLICAR:
    print("\nSimulação: nada foi gravado. Se estiver bom, troque APLICAR para True e rode de novo.")
else:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABELA_CLASS} (
          id BIGINT, data STRING, titulo STRING, resumo STRING, link STRING, categorias STRING, tags STRING,
          coletado_em STRING, fonte_site STRING COMMENT 'Fonte: automotivebusiness ou autodata',
          relevancia STRING, temas STRING, erro_relevancia STRING, score_relevancia INT,
          empresa_principal STRING, categoria_principal STRING
        ) USING DELTA""")
    try:
        spark.sql(f"ALTER TABLE {TABELA_CLASS} ADD COLUMN motivo_exclusao STRING COMMENT 'Preenchido quando excluída por palavra-chave antes da IA'")
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise
    saida = (pend.filter("excluir").dropDuplicates(["id", "fonte_site"]).select(
        "id", "data", "titulo", "resumo", "link", "categorias", "tags", "coletado_em", "fonte_site",
        F.lit("Baixa").alias("relevancia"),
        F.lit("[]").alias("temas"),
        F.lit(None).cast("string").alias("erro_relevancia"),
        F.lit(2).alias("score_relevancia"),
        F.lit(None).cast("string").alias("empresa_principal"),
        F.lit(None).cast("string").alias("categoria_principal"),
        "motivo_exclusao"))
    saida.writeTo(TABELA_CLASS).append()      # append por NOME de coluna
    print(f"\n✅ {n_excl} notícias gravadas como Baixa (motivo em motivo_exclusao). A célula da IA vai pular essas.")
    print("Pra devolver uma regra pra IA depois: DELETE FROM", TABELA_CLASS, "WHERE motivo_exclusao = '<regra>'")
pend.unpersist()


# COMMAND ----------

# DBTITLE 1,6. Classificação de relevância ArcelorMittal (ai_classify)
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.default.noticias_automotivebusiness_arcelormittal (
# MAGIC   id BIGINT, data STRING, titulo STRING, resumo STRING, link STRING, categorias STRING, tags STRING,
# MAGIC   coletado_em STRING, fonte_site STRING, relevancia STRING, temas STRING, erro_relevancia STRING,
# MAGIC   score_relevancia INT, empresa_principal STRING, categoria_principal STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Lote limitado: no máximo 100 notícias por execução. Reexecute esta célula para o próximo lote.
# MAGIC -- Texto enviado ao modelo = título + até 700 caracteres do resumo; modelo menor para o primeiro passe.
# MAGIC INSERT INTO hive_metastore.default.noticias_automotivebusiness_arcelormittal
# MAGIC   (id, data, titulo, resumo, link, categorias, tags, coletado_em, fonte_site,
# MAGIC    relevancia, temas, erro_relevancia, score_relevancia, empresa_principal, categoria_principal)
# MAGIC WITH base AS (
# MAGIC   SELECT s.id, s.data, s.titulo, s.resumo, s.link, s.categorias, s.tags, s.coletado_em, s.fonte_site,
# MAGIC          concat(coalesce(s.titulo,''), '. ', substring(coalesce(s.resumo,''), 1, 700)) AS texto_curto
# MAGIC   FROM hive_metastore.default.noticias_automotivebusiness s
# MAGIC   LEFT ANTI JOIN hive_metastore.default.noticias_automotivebusiness_arcelormittal t
# MAGIC     ON s.id = t.id AND s.fonte_site = t.fonte_site
# MAGIC   ORDER BY s.data DESC
# MAGIC   LIMIT 100
# MAGIC ), respostas AS (
# MAGIC   SELECT *, ai_query('databricks-meta-llama-3-1-8b-instruct', concat(
# MAGIC 'Classifique a notícia sob a ótica da ArcelorMittal Brasil como fornecedora de aço para a cadeia automotiva. Use relevância Alta, Média ou Baixa.
# MAGIC ',
# MAGIC 'ALTA: impacto direto e material na produção física, demanda por aço, capacidade industrial, investimentos/fábricas no Brasil, cadeia de fornecedores, materiais (aço/AHSS/alumínio), dados da Anfavea, tarifas ou política industrial com efeito concreto.
# MAGIC ',
# MAGIC 'MÉDIA: tendências de mercado, lançamentos de montadoras de volume, comércio exterior automotivo geral, ESG/regulação ou mudanças executivas sem impacto industrial direto anunciado.
# MAGIC ',
# MAGIC 'BAIXA: marketing, patrocínio, prêmios, luxo/nicho, curiosidades, trânsito, varejo concessionário e eventos sem anúncio concreto. Não exclua automaticamente notícia só por ser de automóvel leve.
# MAGIC ',
# MAGIC 'Temas permitidos: Eletrificação; Materiais e Lightweighting; Volume e Mercado; Cadeia de Fornecedores; Regulação e Política Industrial; Grandes Montadoras; Sustentabilidade e ESG; Eventos e Entretenimento.
# MAGIC ',
# MAGIC 'Empresa principal: nome curto se houver foco claro; senão null. Responda apenas JSON válido: {"relevancia":"Alta|Média|Baixa","temas":[...],"empresa_principal":null}.
# MAGIC NOTÍCIA: ', texto_curto
# MAGIC   )) AS resp_bruta
# MAGIC   FROM base
# MAGIC ), parseadas AS (
# MAGIC   SELECT *, regexp_extract(resp_bruta, '(?s)(\\{.*\\})', 1) AS resp_json FROM respostas
# MAGIC ), normalizadas AS (
# MAGIC   SELECT *, CASE lower(trim(get_json_object(resp_json,'$.relevancia')))
# MAGIC     WHEN 'alta' THEN 'Alta' WHEN 'média' THEN 'Média' WHEN 'media' THEN 'Média' WHEN 'baixa' THEN 'Baixa' ELSE NULL END AS relevancia_norm
# MAGIC   FROM parseadas
# MAGIC )
# MAGIC SELECT id, data, titulo, resumo, link, categorias, tags, coletado_em, fonte_site,
# MAGIC        relevancia_norm,
# MAGIC        get_json_object(resp_json,'$.temas') AS temas,
# MAGIC        CASE WHEN relevancia_norm IS NULL THEN concat('resposta do modelo rápido não interpretada: ', substring(resp_bruta,1,200)) END AS erro_relevancia,
# MAGIC        CASE relevancia_norm WHEN 'Alta' THEN 9 WHEN 'Média' THEN 5 WHEN 'Baixa' THEN 2 ELSE 0 END AS score_relevancia,
# MAGIC        nullif(get_json_object(resp_json,'$.empresa_principal'),'null') AS empresa_principal,
# MAGIC        get_json_object(resp_json,'$.temas[0]') AS categoria_principal
# MAGIC FROM normalizadas;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fallback caro: só tenta novamente registros cuja resposta do modelo menor não pôde ser interpretada.
# MAGIC -- Limitado a 30 por execução para controlar orçamento. Reexecute para processar os próximos erros.
# MAGIC MERGE INTO hive_metastore.default.noticias_automotivebusiness_arcelormittal AS t
# MAGIC USING (
# MAGIC   WITH pendentes AS (
# MAGIC     SELECT id, fonte_site, concat(coalesce(titulo,''), '. ', substring(coalesce(resumo,''),1,700)) AS texto_curto
# MAGIC     FROM hive_metastore.default.noticias_automotivebusiness_arcelormittal
# MAGIC     WHERE relevancia IS NULL AND score_relevancia = 0
# MAGIC     ORDER BY data DESC
# MAGIC     LIMIT 30
# MAGIC   ), respostas AS (
# MAGIC     SELECT *, ai_query('databricks-meta-llama-3-3-70b-instruct', concat(
# MAGIC       'Classifique a notícia para inteligência competitiva da ArcelorMittal na cadeia automotiva. ',
# MAGIC       'Alta = impacto concreto em produção, demanda por aço, fábrica/capacidade, investimento industrial, fornecedores, materiais ou regulação com efeito direto. ',
# MAGIC       'Média = tendência, lançamento de montadora de volume ou comércio exterior sem efeito industrial direto confirmado. ',
# MAGIC       'Baixa = marketing, prêmio, luxo/nicho, curiosidade, trânsito ou varejo. ',
# MAGIC       'Temas permitidos: Eletrificação; Materiais e Lightweighting; Volume e Mercado; Cadeia de Fornecedores; Regulação e Política Industrial; Grandes Montadoras; Sustentabilidade e ESG; Eventos e Entretenimento. ',
# MAGIC       'Responda só JSON: {"relevancia":"Alta|Média|Baixa","temas":[...],"empresa_principal":null}. NOTÍCIA: ', texto_curto
# MAGIC     )) AS resp_bruta
# MAGIC     FROM pendentes
# MAGIC   ), parsed AS (
# MAGIC     SELECT id, fonte_site, resp_bruta, regexp_extract(resp_bruta, '(?s)(\\{.*\\})', 1) AS j FROM respostas
# MAGIC   )
# MAGIC   SELECT id, fonte_site,
# MAGIC     CASE lower(trim(get_json_object(j,'$.relevancia'))) WHEN 'alta' THEN 'Alta' WHEN 'média' THEN 'Média' WHEN 'media' THEN 'Média' WHEN 'baixa' THEN 'Baixa' ELSE NULL END AS relevancia,
# MAGIC     get_json_object(j,'$.temas') AS temas,
# MAGIC     nullif(get_json_object(j,'$.empresa_principal'),'null') AS empresa_principal,
# MAGIC     get_json_object(j,'$.temas[0]') AS categoria_principal,
# MAGIC     resp_bruta
# MAGIC   FROM parsed
# MAGIC ) AS s
# MAGIC ON t.id = s.id AND t.fonte_site = s.fonte_site
# MAGIC WHEN MATCHED AND s.relevancia IS NOT NULL THEN UPDATE SET
# MAGIC   t.relevancia = s.relevancia,
# MAGIC   t.temas = s.temas,
# MAGIC   t.erro_relevancia = NULL,
# MAGIC   t.score_relevancia = CASE s.relevancia WHEN 'Alta' THEN 9 WHEN 'Média' THEN 5 WHEN 'Baixa' THEN 2 ELSE 0 END,
# MAGIC   t.empresa_principal = s.empresa_principal,
# MAGIC   t.categoria_principal = s.categoria_principal;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Resumo da rodada: última notícia coletada, última classificação e volume por relevância nos últimos 7 dias
# MAGIC SELECT fonte_site,
# MAGIC        MAX(DATE(to_timestamp(data)))        AS ultima_publicacao,
# MAGIC        MAX(DATE(to_timestamp(coletado_em))) AS ultima_coleta,
# MAGIC        SUM(CASE WHEN relevancia = 'Alta'  AND DATE(to_timestamp(data)) >= current_date() - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS alta_7d,
# MAGIC        SUM(CASE WHEN relevancia = 'Média' AND DATE(to_timestamp(data)) >= current_date() - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS media_7d,
# MAGIC        SUM(CASE WHEN relevancia = 'Baixa' AND DATE(to_timestamp(data)) >= current_date() - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS baixa_7d,
# MAGIC        SUM(CASE WHEN erro_relevancia IS NOT NULL THEN 1 ELSE 0 END) AS com_erro
# MAGIC FROM hive_metastore.default.noticias_automotivebusiness_arcelormittal
# MAGIC GROUP BY fonte_site
# MAGIC ORDER BY fonte_site
# MAGIC