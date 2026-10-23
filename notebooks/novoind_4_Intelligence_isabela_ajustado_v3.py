# Databricks notebook source
import requests
import pandas as pd
import time


# COMMAND ----------

# MAGIC %run ./token_4i

# COMMAND ----------

lim = 3999

url = "https://apis.4intelligence.ai/api-feature-store/api/v1/indicators"

headers = get_headers()

parametros = { "limit": lim}
response = requests.get(url,headers=headers, verify = False,  params= {'skip': 0}) # params= parametros

# COMMAND ----------

if response.status_code != 200:
    raise Exception(f"API request failed ({response.status_code}): {response.text}")
total = response.json()['total']

# COMMAND ----------

total

# COMMAND ----------

import urllib3, json
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PAGINA = 500   # páginas menores: cada registro do catálogo tem ~3 KB, então 4000 viravam ~12 MB por resposta

headers = get_headers()   # renova o token se estiver perto de vencer

# total de indicadores no catálogo
r = requests.get(url, headers=headers, verify=False, timeout=60, params={'skip': 0, 'limit': 1})
r.raise_for_status()
total = r.json()['total']
print('total no catálogo:', total)

response_all, consultado = [], 0

while consultado < total:
    pagina = None
    for tentativa in range(1, 6):
        try:
            r = requests.get(url, headers=headers, verify=False, timeout=90,
                             params={'skip': consultado, 'limit': PAGINA})
        except requests.exceptions.RequestException as e:
            print(f'  falha de conexão ({type(e).__name__}), tentativa {tentativa}/5')
            time.sleep(5 * tentativa)
            continue

        if r.status_code == 401:
            headers = get_headers()          # token venceu: renova e tenta de novo
            continue
        if r.status_code == 429 or r.status_code >= 500:
            espera = int(r.headers.get('Retry-After', 10 * tentativa))
            print(f'  [{r.status_code}] aguardando {espera}s')
            time.sleep(espera)
            continue

        try:
            pagina = r.json()['data']
            break
        except (json.JSONDecodeError, KeyError):
            # mostra o que a API realmente devolveu
            print(f'  resposta sem JSON (status {r.status_code}, {len(r.content)} bytes): {r.text[:300]!r}')
            time.sleep(10 * tentativa)

    if pagina is None:
        raise RuntimeError(f'Catálogo: falhou em skip={consultado} após 5 tentativas (último status {r.status_code})')
    if not pagina:
        break
    response_all.append(pagina)
    consultado += len(pagina)
    print(f'consultado {consultado} de {total}')

print('✅ catálogo baixado:', consultado, 'indicadores')

# COMMAND ----------

lista_geral = []
for i in range(len(response_all)):
    for j in range(len(response_all[i])):
        lista_temp = []
        try:
            lista_temp.append(response_all[i][j]['indicator_code'])
        except:
            lista_temp.append('')
        #name
        try:
            lista_temp.append(response_all[i][j]['name']['pt-br'])
        except:
            lista_temp.append('')
        
                #short_name
        try:
            lista_temp.append(response_all[i][j]['short_name']['pt-br'])
        except:
            lista_temp.append('')
        #source
        try:
            lista_temp.append(response_all[i][j]['source'] ['pt-br'])
        except:
            lista_temp.append('')
        #projections
        try:
            lista_temp.append(response_all[i][j]['projections'])
        except:
            lista_temp.append('')
        #description
        try:
            lista_temp.append(response_all[i][j]['description']['pt-br'])   # CORREÇÃO: antes lia ['pt-br'] direto
        except:
            lista_temp.append('')
        #description_full
        try:
            lista_temp.append(response_all[i][j]['description_full']['pt-br'])
        except:
            lista_temp.append('')
        #country
        try:
            lista_temp.append(response_all[i][j]['country'] )
        except:
            lista_temp.append('')
        #updated_at
        try:
            lista_temp.append(response_all[i][j]['updated_at'])
        except:
            lista_temp.append('')
        
        # insercao
        lista_geral.append(lista_temp)
        listatemp = []

df = pd.DataFrame(lista_geral, columns = ['indicador_code','name','short_name','source','projections','description','description_full','country','updated_at'])      

# COMMAND ----------

df.display()

# COMMAND ----------

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------

# MAGIC %run ./indicadores_portal

# COMMAND ----------

# ===================== PARÂMETROS DA COLETA =====================
# A lista oficial (código, nome, nível ou variação, com ou sem projeção) fica no notebook indicadores_portal,
# que veio com o %run acima. Ele é a única fonte: o Export usa a mesma lista. Mexa só lá.
LISTA_INDICADORES = [i["code"] for i in INDICADORES]

# Alternativa: ler a lista de uma tabela (coluna indicator_code). Se preencher, tem prioridade sobre a lista acima.
TABELA_LISTA = None   # ex.: "lcb_qas_sandbox2_comercial.isabela_pereira.lista_indicadores_4i"

SCHEMA        = "lcb_qas_sandbox2_comercial.isabela_pereira"
TABELA_STAGE  = f"{SCHEMA}.indicadores_4i_stage"            # vai salvando em partes durante a coleta
TABELA_FINAL  = f"{SCHEMA}.indicadores_lt_20260113"              # realizado (mesmas colunas de antes)
TABELA_PROJ   = f"{SCHEMA}.indicadores_lt_20260113_projecoes"    # projeções 4i (tabela nova)

RETOMAR      = True   # True: se a rodada anterior quebrou, continua de onde parou (pula o que já está no stage)
LOTE_SALVAR  = 100    # salva no stage a cada N indicadores
LIMIT_MAX    = 4000   # limite de registros por página da API
TIMEOUT      = 60     # segundos por requisição (antes não tinha: uma conexão pendurada travava a célula)
MAX_TENTATIVAS = 4    # pra 429 (rate limit), 5xx e timeout

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

SCHEMA_STAGE = StructType([
    StructField("date", StringType()),
    StructField("value", DoubleType()),
    StructField("indicator_code", StringType()),
    StructField("serie_code", StringType()),
    StructField("tipo", StringType()),            # 'realizado' ou 'projecao'
])

if TABELA_LISTA:
    lista_ind = [r[0] for r in spark.table(TABELA_LISTA).select("indicator_code").distinct().collect()]
elif LISTA_INDICADORES:
    lista_ind = list(dict.fromkeys(LISTA_INDICADORES))
else:
    lista_ind = list(df['indicador_code'])

# Avisa se tem código na lista que não existe no catálogo (erro de digitação ou indicador descontinuado)
nao_no_catalogo = sorted(set(lista_ind) - set(df['indicador_code']))
if nao_no_catalogo:
    print('Atenção, códigos fora do catálogo da 4i:', nao_no_catalogo)

# Retomada: pula o que já foi salvo numa rodada anterior
ja_baixados = set()
# stage antigo (sem a coluna 'tipo') não serve mais: apaga pra começar do zero
if spark.catalog.tableExists(TABELA_STAGE) and 'tipo' not in spark.table(TABELA_STAGE).columns:
    spark.sql(f"DROP TABLE {TABELA_STAGE}")

# stage de uma rodada com a regra antiga (tudo em variação) também não serve: série em nível tem que terminar em L
if spark.catalog.tableExists(TABELA_STAGE):
    _incoerentes = sorted({r[0] for r in spark.table(TABELA_STAGE).select("indicator_code", "serie_code").distinct().collect()
                           if TIPO_SERIE.get(r[0]) == "#" and not str(r[1]).split(" ")[0].endswith("L")})
    if _incoerentes:
        print("Stage com série de variação em indicador de nível (regra antiga), começando do zero:", _incoerentes)
        spark.sql(f"DROP TABLE {TABELA_STAGE}")

if RETOMAR and spark.catalog.tableExists(TABELA_STAGE):
    ja_baixados = {r[0] for r in spark.table(TABELA_STAGE).select("indicator_code").distinct().collect()}
elif spark.catalog.tableExists(TABELA_STAGE):
    spark.sql(f"DROP TABLE {TABELA_STAGE}")

pendentes = [i for i in lista_ind if i not in ja_baixados]
print(f'Indicadores na lista: {len(lista_ind)} | já baixados: {len(ja_baixados & set(lista_ind))} | a baixar: {len(pendentes)}')

log_erros = []
sessao = requests.Session()   # reaproveita a conexão (bem mais rápido que abrir uma nova a cada chamada)

# COMMAND ----------

FS = "https://apis.4intelligence.ai/api-feature-store/api/v1"
ERROS_DEFINITIVOS = ('sem série', 'sem permissão', 'não encontrado')   # não adianta tentar de novo

def _get(url, params=None):
    """GET com timeout, renovação de token e espera em rate limit / instabilidade."""
    global headers
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            r = sessao.get(url, headers=headers, params=params or {}, timeout=TIMEOUT, verify=False)
        except requests.exceptions.RequestException as e:   # timeout, conexão caiu
            if tentativa == MAX_TENTATIVAS:
                raise
            time.sleep(5 * tentativa)
            continue
        if r.status_code == 401 and tentativa == 1:
            headers = get_headers()          # token venceu: renova e tenta de novo
            continue
        if r.status_code == 429 or r.status_code >= 500:
            espera = int(r.headers.get('Retry-After', 10 * tentativa))
            print(f'  [{r.status_code}] aguardando {espera}s')
            time.sleep(espera)
            continue
        return r
    return r


def _salvar_stage(partes):
    if not partes:
        return
    pdf = pd.concat(partes, ignore_index=True)[['date', 'value', 'indicator_code', 'serie_code', 'tipo']]
    pdf['value'] = pd.to_numeric(pdf['value'], errors='coerce')
    spark.createDataFrame(pdf, schema=SCHEMA_STAGE).write.mode("append").saveAsTable(TABELA_STAGE)


def _baixar_paginado(url):
    """Baixa todas as páginas de observations ou projections. A 1a página já traz o total."""
    r = _get(url, {'skip': 0, 'limit': LIMIT_MAX})
    if r.status_code == 404:
        return [], r
    r.raise_for_status()
    resp = r.json()
    total = int(resp.get('total', 0))
    dados = list(resp.get('data') or [])
    while len(dados) < total:
        r = _get(url, {'skip': len(dados), 'limit': LIMIT_MAX})
        r.raise_for_status()
        pagina = r.json().get('data') or []
        if not pagina:
            break
        dados.extend(pagina)
    return dados, r


def _series_com_projecao(indicador):
    """Códigos das séries do indicador que têm projeção ativa na 4i (a padrão primeiro). Vazio se não tiver ou se a chamada falhar."""
    r = _get(f"{FS}/indicators/{indicador}/projections/metadata")
    if r.status_code != 200:
        return []
    projs = [x for x in (r.json().get('data') or []) if x.get('status', 'active') == 'active' and x.get('series_code')]
    projs.sort(key=lambda x: not x.get('is_default'))
    return [x['series_code'] for x in projs]


# ----- Regra de série do portal (planilha de correspondência, coluna Unidade; 22/10/2026) -----
# Código da série 4i: <indicador>000<ajuste><agregação><frequência><transformação>, ex.: BRPRC0033000SOMM.
# Transformação "L" = nível (o número do indicador). "M" = variação contra o período anterior (% m/m, % t/t).
# TIPO_SERIE (do indicadores_portal): "#" -> série em nível (L); "var" -> série de variação (M).
# Sempre com projeção: entre as séries do tipo certo, vence a que tem projeção na 4i. Se só a do outro tipo tiver,
# usamos ela e convertemos (variação calculada a partir do nível, ou nível encadeado a partir da variação).
# Desempate: maior frequência; no nível prefere a série original (O), na variação a dessazonalizada (S).
PREF_FREQ, PREF_AGREG = "MQY", "OAES"
PREF_AJUSTE = {"#": "OSWR", "var": "SWOR"}
SERIE_FORCADA = {}   # para travar uma série específica: {"BRGDP0002": "BRGDP0002000WOQM"}

def _rank(codigo, tipo, com_proj=()):
    suf = codigo[-4:]
    pos = lambda ordem, c: ordem.index(c) if c in ordem else len(ordem)
    return (codigo not in com_proj, pos(PREF_FREQ, suf[2]), pos(PREF_AJUSTE[tipo], suf[0]), pos(PREF_AGREG, suf[1]))

def _melhor(codigos, transf, tipo, com_proj=()):
    c = sorted([x for x in codigos if x.endswith(transf)], key=lambda x: _rank(x, tipo, com_proj))
    return c[0] if c else None

def _par(codigo, codigos, transf):
    """Série 'irmã' de outra transformação, com o mesmo ajuste/agregação/frequência (ex.: ...SOMM <-> ...SOML)."""
    alvo = codigo[:-1] + transf
    return alvo if alvo in codigos else None

def _escolher_series(indicador, codigos, tipo, com_proj):
    """Devolve (série do realizado, série da projeção, conversão), conversão em {None, 'var_de_nivel', 'nivel_de_var'}."""
    codigos = [c for c in codigos if c]
    if indicador in SERIE_FORCADA:
        return SERIE_FORCADA[indicador], SERIE_FORCADA[indicador], None
    alvo, outro = ("L", "M") if tipo == "#" else ("M", "L")
    certa = _melhor(codigos, alvo, tipo, com_proj)
    if certa and certa in com_proj:
        return certa, certa, None                                   # caso normal: série do tipo certo, com projeção
    trocada = _melhor([c for c in codigos if c in com_proj], outro, tipo, com_proj)
    if tipo == "var" and trocada:
        return trocada, trocada, "var_de_nivel"                     # só o nível tem projeção: calcula a variação
    if tipo == "#" and trocada:
        real = _par(trocada, codigos, "L") or certa
        if real:
            return real, trocada, "nivel_de_var"                    # só a variação tem projeção: encadeia a partir do último nível
    if certa:
        return certa, certa, None                                   # sem projeção em lugar nenhum (ou metadata indisponível)
    nivel = _melhor(codigos, "L", tipo)
    if tipo == "var" and nivel:
        return nivel, nivel, "var_de_nivel"                         # não existe série M: calcula a partir do nível
    return (codigos[0], codigos[0], None) if codigos else (None, None, None)

def _limpa(lista):
    return sorted({str(p["date"])[:10]: float(p["value"]) for p in lista if p.get("value") is not None and p.get("date")}.items())

def _variacao(obs, proj):
    """Variação % contra o período anterior, calculada sobre realizado + projeção em sequência."""
    o, p = _limpa(obs), _limpa(proj)
    ultimo = o[-1][0] if o else ""
    todos = o + [x for x in p if x[0] > ultimo]
    var = [{"date": todos[i][0], "value": (todos[i][1] / todos[i - 1][1] - 1) * 100}
           for i in range(1, len(todos)) if todos[i - 1][1]]
    return [x for x in var if x["date"] <= ultimo], [x for x in var if x["date"] > ultimo]

def _nivel_projetado(obs_nivel, proj_var):
    """Projeção em nível: parte do último nível realizado e aplica as variações % projetadas em sequência."""
    o = _limpa(obs_nivel)
    if not o:
        return []
    ultimo, nivel, saida = o[-1][0], o[-1][1], []
    for d, v in _limpa(proj_var):
        if d > ultimo:
            nivel = nivel * (1 + v / 100)
            saida.append({"date": d, "value": nivel})
    return saida


def _formatar(dados, indicador, serie_code, tipo):
    df_temp = pd.DataFrame(dados)
    df_temp['date'] = pd.to_datetime(df_temp['date']).dt.strftime('%d/%m/%Y')
    df_temp['indicator_code'] = indicador
    df_temp['serie_code'] = serie_code
    df_temp['tipo'] = tipo
    return df_temp


def consulta_dados(listaind):
    """Baixa as observações de cada indicador e vai salvando no stage a cada LOTE_SALVAR indicadores."""
    global headers
    partes, erros_temporarios = [], []
    inicio = time.time()

    for n, indicador_selected in enumerate(listaind, start=1):
        r = None
        if n % 20 == 0:
            headers = get_headers()   # renova o token se estiver perto de vencer
            ritmo = (time.time() - inicio) / n
            print(f'{n}/{len(listaind)} | {ritmo:.1f}s por indicador | faltam ~{ritmo * (len(listaind) - n) / 60:.0f} min')

        try:
            # 1) série do indicador
            r = _get(f"{FS}/indicators/{indicador_selected}/series")
            if r.status_code == 403:
                raise LookupError('sem permissão')
            if r.status_code == 404:
                raise LookupError('não encontrado')
            series = r.json().get('data') or []
            if not series:
                raise LookupError('sem série')
            codigos_series = [x.get('code') for x in series]

            # série: regra do portal (nível ou variação, conforme a planilha), dando preferência à que tem projeção
            tipo = TIPO_SERIE.get(indicador_selected, "var")
            sem_proj = indicador_selected in SEM_PROJECAO
            com_proj = [] if sem_proj else _series_com_projecao(indicador_selected)
            serie_code, serie_proj, conversao = _escolher_series(indicador_selected, codigos_series, tipo, com_proj)
            if serie_code is None:
                raise LookupError('sem série')

            # 2) realizado
            dados, r = _baixar_paginado(f"{FS}/indicators/{indicador_selected}/series/{serie_code}/observations")
            if not dados:
                log_erros.append({'indicador': indicador_selected, 'etapa': 'observations', 'status': r.status_code, 'motivo': 'sem observações'})
                continue

            # 3) projeção. Vazio = indicador sem projeção na 4i, não é erro
            dados_p = []
            if not sem_proj:
                dados_p, r = _baixar_paginado(f"{FS}/indicators/{indicador_selected}/series/{serie_proj}/projections")
                if not dados_p:
                    log_erros.append({'indicador': indicador_selected, 'etapa': 'projections', 'status': r.status_code, 'motivo': f'sem projeção na série {serie_proj}'})

            if conversao == "var_de_nivel":
                dados, dados_p = _variacao(dados, dados_p)
                log_erros.append({'indicador': indicador_selected, 'etapa': 'serie', 'status': None, 'motivo': f'variação calculada a partir do nível {serie_code}'})
                serie_code = f"{serie_code} (var. calculada)"
            elif conversao == "nivel_de_var":
                dados_p = _nivel_projetado(dados, dados_p)
                log_erros.append({'indicador': indicador_selected, 'etapa': 'serie', 'status': None, 'motivo': f'projeção em nível encadeada a partir de {serie_proj}'})
                serie_code = f"{serie_code} (proj. de {serie_proj})"

            if dados:
                partes.append(_formatar(dados, indicador_selected, serie_code, 'realizado'))
            if dados_p:
                partes.append(_formatar(dados_p, indicador_selected, serie_code, 'projecao'))

        except LookupError as e:
            log_erros.append({'indicador': indicador_selected, 'etapa': 'series', 'status': r.status_code, 'motivo': str(e)})
        except Exception as e:
            status = getattr(locals().get('r'), 'status_code', None)
            print('erro', indicador_selected, status, type(e).__name__)
            log_erros.append({'indicador': indicador_selected, 'etapa': 'series/observations', 'status': status, 'motivo': f'{type(e).__name__}: {str(e)[:200]}'})
            erros_temporarios.append(indicador_selected)

        if len(partes) >= LOTE_SALVAR:
            _salvar_stage(partes)
            partes = []

    _salvar_stage(partes)
    print(f'Fim: {len(listaind)} indicadores em {(time.time() - inicio) / 60:.1f} min')
    return erros_temporarios


lista_erro = consulta_dados(pendentes)

# Retry só nos erros temporários (timeout, 5xx). Sem série / sem permissão não muda tentando de novo.
if lista_erro:
    print('Retry em', len(lista_erro), 'indicadores')
    time.sleep(30)
    lista_erro = consulta_dados(lista_erro)
print('Indicadores com erro após o retry:', lista_erro)

# COMMAND ----------

# filtra pela lista atual (se sobrou stage de uma rodada antiga com outros indicadores, eles não entram)
df_stage = (spark.table(TABELA_STAGE)
            .filter(F.col('indicator_code').isin(lista_ind))
            .dropDuplicates(['indicator_code', 'tipo', 'date']))

# resumo por indicador: tem realizado? tem projeção? até quando?
resumo = (df_stage
          .withColumn('dt', F.to_date('date', 'dd/MM/yyyy'))
          .groupBy('indicator_code', 'serie_code')
          .agg(F.max(F.when(F.col('tipo') == 'realizado', F.col('dt'))).alias('ultimo_realizado'),
               F.min(F.when(F.col('tipo') == 'projecao', F.col('dt'))).alias('inicio_projecao'),
               F.max(F.when(F.col('tipo') == 'projecao', F.col('dt'))).alias('fim_projecao'),
               F.sum(F.when(F.col('tipo') == 'projecao', 1).otherwise(0)).alias('pontos_projecao'))
          .orderBy('indicator_code'))
resumo.display()

# COMMAND ----------

baixados = {r[0] for r in df_stage.filter("tipo = 'realizado'").select('indicator_code').distinct().collect()}
com_proj = {r[0] for r in df_stage.filter("tipo = 'projecao'").select('indicator_code').distinct().collect()}
print('Com realizado:', len(baixados), 'de', len(lista_ind))
print('Com projeção:', len(com_proj), 'de', len(lista_ind))
print('Sem realizado:', sorted(set(lista_ind) - baixados))
print('Sem projeção:', sorted(set(lista_ind) - com_proj))
# Conferência da regra da planilha: '#' tem que vir em nível (série terminando em L) e 'var' em variação (M ou calculada)
for r in df_stage.select('indicator_code', 'serie_code').distinct().collect():
    tipo, serie = TIPO_SERIE.get(r[0]), str(r[1])
    ok = serie.split(' ')[0].endswith('L') if tipo == '#' else (serie.endswith('M') or 'var. calculada' in serie)
    if not ok:
        print(f"⚠️ {r[0]}: planilha pede {'nível' if tipo == '#' else 'variação'}, mas a série usada foi {serie}")
print('Pediram pra ficar sem projeção:', sorted(SEM_PROJECAO))
if log_erros:
    display(pd.DataFrame(log_erros).drop_duplicates())

# COMMAND ----------

# Junta com o catálogo no Spark (antes era merge no pandas com a base inteira na memória do driver)
catalogo = spark.createDataFrame(df.astype(str))

def _com_catalogo(sdf):
    return sdf.join(catalogo, sdf.indicator_code == catalogo.indicador_code, 'left')

# Realizado: mesmas colunas de antes, pra não quebrar quem já lê essa tabela
df_all = _com_catalogo(df_stage.filter("tipo = 'realizado'").drop('tipo', 'serie_code'))
df_all.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABELA_FINAL)
print('Realizado gravado em', TABELA_FINAL, '|', df_all.count(), 'linhas')

# Projeções: tabela separada, com a série usada
df_proj = _com_catalogo(df_stage.filter("tipo = 'projecao'").drop('tipo'))
df_proj.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABELA_PROJ)
print('Projeções gravadas em', TABELA_PROJ, '|', df_proj.count(), 'linhas')

# Rodada completa e gravada: limpa o stage pra próxima vez começar do zero
spark.sql(f"DROP TABLE IF EXISTS {TABELA_STAGE}")

