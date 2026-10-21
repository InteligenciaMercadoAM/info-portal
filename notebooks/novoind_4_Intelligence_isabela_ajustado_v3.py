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

# ===================== PARÂMETROS DA COLETA =====================
# Lista oficial: planilha indicadores_4i_correspondencia_6.xlsx (docs/), aba objetivo, 'entra no portal' = Sim.
# Ao incluir/tirar indicador aqui, atualize também o INDICADORES_PORTAL do Export e o indicadores_nomes do config.json.
# Deixe None pra baixar o catálogo inteiro (demora horas).
LISTA_INDICADORES = [
    'BRGDP0002',  # PIB total
    'BRGDP0017',  # PIB Indústria
    'BRGDP0019',  # PIB Transformação
    'BRGDP0021',  # PIB Construção
    'BRGDP0013',  # PIB FBKF
    'BRIND0001',  # Indústria Geral
    'BRIND0003',  # Indústrias de Transformação
    'BRIND0023',  # Fabricação de Máquinas e Equipamentos
    'BRIND0029',  # Bens de Capital
    'BRIND0368',  # Produção de Linha Branca
    'BRIND0420',  # Insumos da Construção
    'BRLDG0054',  # ICEI - Índice de Confiança do Empresário Industrial
    'BRIND0445',  # Produção de Motocicletas - Total
    'BRIND0055',  # Produção de Veículos - Total (Anfavea; substitui BRIND0446, descontinuado em dez/20)
    'BRIND0070',  # Produção de Caminhões - Total
    'BRIND0075',  # Produção de Ônibus - Total
    'BRRTL0053',  # Licenciamento - Total
    'BRRTL0062',  # Licenciamento de Caminhões - Total
    'BRRTL0065',  # Licenciamento de Ônibus - Total
    'BRIND1015',  # ICIA - Índice de Confiança da Indústria do Aço
    'BRRTL0249',  # Unidades Lançadas - Todos os Empreendimentos
    'BRRTL0250',  # Unidades Lançadas - Médio e Alto Padrão MAP
    'BRRTL0251',  # Unidades Lançadas - Programa Casa Verde e Amarela CVA
    'BRRTL0253',  # Unidades Vendidas - Todos os Empreendimentos
    'BRRTL0254',  # Unidades Vendidas - Médio e Alto Padrão MAP
    'BRRTL0255',  # Unidades Vendidas - Programa Casa Verde e Amarela CVA
    'BRRLE0007',  # Secovi SP - Vendas de Imóveis Residenciais Novos
    'BRRLE0010',  # Secovi SP - Lançamentos Residenciais
    'BRIND0125',  # Vendas de Cimento - Total
    'BREMP0059',  # Taxa de Participação na Força de Trabalho
    'BRLDG0003',  # FGV - Índice de Confiança do Consumidor
    'BRLDG0002',  # FGV - Índice de Confiança da Indústria (ICI)
    'BRLDG0034',  # FGV ICSTV - Índice de Confiança da Construção
    'BRGDP0001',  # IBC-Br - Índice de Atividade do Banco Central
    'BRFXR0032',  # Dólar Comercial - PTAX Média Mensal, Compra (R$/US$)
    'BRPRC0046',  # Inflação (IPCA)
    'BRPRC0033',  # INCC-M Índice Nacional de Custo da Construção
    'BRPRC0035',  # INCC-M Mão de Obra
    'BRPRC0034',  # INCC-M Materiais, Equipamentos e Serviços
    'BRPRC0024',  # IGPM - Índice Geral de Preços do Mercado
    'BRSTK0005',  # IBOVESPA - Fechamento Mensal
    'BRINR0012',  # Selic a.a.
]

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


def _serie_da_projecao(indicador):
    """Série da projeção padrão (default + active). Usar a mesma série no realizado garante que
    as duas linhas do gráfico tenham a mesma unidade/transformação. None se não houver projeção."""
    r = _get(f"{FS}/indicators/{indicador}/projections/metadata")
    if r.status_code != 200:
        return None
    projs = r.json().get('data') or []
    ativas = [x for x in projs if x.get('status', 'active') == 'active']
    padrao = [x for x in ativas if x.get('is_default')] or ativas
    return padrao[0].get('series_code') if padrao else None


# ----- Regra de série do portal (definida em 19/10/2026) -----
# Código da série 4i: <indicador>000<ajuste><agregação><frequência><transformação>, ex.: BRPRC0033000SOMM.
# Transformação "M" = variação contra o período anterior (% m/m nos mensais, % t/t nos trimestrais).
# Entre as séries "M", preferimos: maior frequência, depois dessazonalizada (S), depois as demais.
PREF_FREQ, PREF_AJUSTE, PREF_AGREG = "MQY", "SWOR", "OAES"
SERIE_FORCADA = {}   # para travar uma série específica: {"BRGDP0002": "BRGDP0002000WOQM"}

def _rank(codigo):
    suf = codigo[-4:]
    pos = lambda ordem, c: ordem.index(c) if c in ordem else len(ordem)
    return (pos(PREF_FREQ, suf[2]), pos(PREF_AJUSTE, suf[0]), pos(PREF_AGREG, suf[1]))

def _escolher_serie(codigos):
    """Devolve (série, calcular_variação)."""
    codigos = [c for c in codigos if c]
    ind = codigos[0][:9] if codigos else ""
    if ind in SERIE_FORCADA:
        return SERIE_FORCADA[ind], False
    var = sorted([c for c in codigos if c.endswith("M")], key=_rank)
    if var:
        return var[0], False
    nivel = sorted([c for c in codigos if c.endswith("L")], key=_rank)
    if nivel:
        return nivel[0], True
    return (codigos[0], False) if codigos else (None, False)

def _variacao(obs, proj):
    """Variação % contra o período anterior, calculada sobre realizado + projeção em sequência."""
    def limpa(lista):
        return sorted({str(p["date"])[:10]: float(p["value"]) for p in lista if p.get("value") is not None and p.get("date")}.items())
    o, p = limpa(obs), limpa(proj)
    ultimo = o[-1][0] if o else ""
    todos = o + [x for x in p if x[0] > ultimo]
    var = [{"date": todos[i][0], "value": (todos[i][1] / todos[i - 1][1] - 1) * 100}
           for i in range(1, len(todos)) if todos[i - 1][1]]
    return [x for x in var if x["date"] <= ultimo], [x for x in var if x["date"] > ultimo]


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

            # série: regra do portal (variação contra o período anterior), a MESMA no realizado e na projeção
            serie_code, calcular = _escolher_serie(codigos_series)
            if serie_code is None:
                raise LookupError('sem série')
            base = f"{FS}/indicators/{indicador_selected}/series/{serie_code}"

            # 2) realizado
            dados, r = _baixar_paginado(f"{base}/observations")
            if not dados:
                log_erros.append({'indicador': indicador_selected, 'etapa': 'observations', 'status': r.status_code, 'motivo': 'sem observações'})
                continue

            # 3) projeção da mesma série. Vazio = indicador sem projeção na 4i, não é erro
            dados_p, r = _baixar_paginado(f"{base}/projections")
            if not dados_p:
                log_erros.append({'indicador': indicador_selected, 'etapa': 'projections', 'status': r.status_code, 'motivo': f'sem projeção na série {serie_code}'})

            if calcular:
                # não existe série de variação pronta: calcula a partir do nível (realizado + projeção juntos)
                dados, dados_p = _variacao(dados, dados_p)
                serie_code = f"{serie_code} (var. calculada)"
                log_erros.append({'indicador': indicador_selected, 'etapa': 'serie', 'status': None, 'motivo': f'sem série M; variação calculada de {serie_code}'})

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
