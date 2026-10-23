# Databricks notebook source
# MAGIC %md
# MAGIC # Indicadores 4i do portal (lista oficial)
# MAGIC **Único lugar onde se escolhe quais indicadores vão pro portal e como cada um aparece.**
# MAGIC Usado com `%run ./indicadores_portal` pelo `novoind_4_Intelligence...` (o que baixar) e pelo `Export_Portal_Data` (como mostrar).
# MAGIC
# MAGIC Fonte: planilha `docs/indicadores_4i_correspondencia.xlsx`, aba **objetivo**, linhas com **x na coluna Mantém**.
# MAGIC - `nome`: coluna **name** da planilha (é o nome que aparece no site).
# MAGIC - `serie`: coluna **Unidade**. `"#"` = série em nível (o número do indicador); `"var"` = variação contra o período anterior (% m/m, % t/t).
# MAGIC - `projecao`: `False` só quando a planilha pede pra tirar (Dólar). O resto sempre tenta trazer a projeção da 4i.
# MAGIC - `unidade` / `casas`: só para as séries em nível. Nas de variação a unidade sai da frequência (% m/m, % t/t).
# MAGIC
# MAGIC Ao mudar algo aqui, mude também o `indicadores_nomes` do `public/config.json` (o site usa essa lista pra filtrar e nomear).

# COMMAND ----------

INDICADORES = [
    # ---------- PIB (trimestral, variação t/t) ----------
    {"code": "BRGDP0002", "nome": "PIB total",          "serie": "var", "setor": "Macroeconomia",    "fonte_original": "IBGE"},
    {"code": "BRGDP0017", "nome": "PIB Indústria",      "serie": "var", "setor": "Indústria",        "fonte_original": "IBGE"},
    {"code": "BRGDP0019", "nome": "PIB Transformação",  "serie": "var", "setor": "Indústria",        "fonte_original": "IBGE"},
    {"code": "BRGDP0021", "nome": "PIB Construção",     "serie": "var", "setor": "Construção civil", "fonte_original": "IBGE"},
    {"code": "BRGDP0013", "nome": "PIB FBKF",           "serie": "var", "setor": "Macroeconomia",    "fonte_original": "IBGE"},

    # ---------- Produção industrial (PIM-PF, variação m/m) ----------
    {"code": "BRIND0001", "nome": "Produção Industrial Total",          "serie": "var", "setor": "Indústria",        "fonte_original": "IBGE"},
    {"code": "BRIND0003", "nome": "Produção Industrial Transformação",  "serie": "var", "setor": "Indústria",        "fonte_original": "IBGE"},
    {"code": "BRIND0029", "nome": "Bens de Capital",                    "serie": "var", "setor": "Indústria",        "fonte_original": "IBGE"},
    # Planilha traz BRIND0251, mas na 4i esse código é "Produção de Eletrodomésticos". O de fogões/refrigeradores/máquinas de lavar é o BRIND0368.
    {"code": "BRIND0368", "nome": "Produção de Linha Branca",           "serie": "var", "setor": "Indústria",        "fonte_original": "IBGE"},
    {"code": "BRIND0420", "nome": "Insumos da Construção",              "serie": "var", "setor": "Construção civil", "fonte_original": "IBGE"},

    # ---------- Automotivo (nível, unidades) ----------
    # Planilha traz BRIND0446, que a 4i descontinuou em dez/20. BRIND0055 é a mesma série da Anfavea, atualizada.
    {"code": "BRIND0055", "nome": "Anfavea - Produção Total",     "serie": "#", "setor": "Automotivo", "fonte_original": "Anfavea", "unidade": "unidades", "casas": 0},
    {"code": "BRIND0070", "nome": "Anfavea - Produção Caminhões", "serie": "#", "setor": "Automotivo", "fonte_original": "Anfavea", "unidade": "unidades", "casas": 0},
    {"code": "BRIND0075", "nome": "Anfavea - Produção Ônibus",    "serie": "#", "setor": "Automotivo", "fonte_original": "Anfavea", "unidade": "unidades", "casas": 0},
    {"code": "BRRTL0053", "nome": "Anfavea - Vendas Totais",      "serie": "#", "setor": "Automotivo", "fonte_original": "Anfavea", "unidade": "unidades", "casas": 0},
    {"code": "BRRTL0062", "nome": "Anfavea - Vendas Caminhões",   "serie": "#", "setor": "Automotivo", "fonte_original": "Anfavea", "unidade": "unidades", "casas": 0},
    {"code": "BRRTL0065", "nome": "Anfavea - Vendas Ônibus",      "serie": "#", "setor": "Automotivo", "fonte_original": "Anfavea", "unidade": "unidades", "casas": 0},

    # ---------- Mercado imobiliário (nível, unidades) ----------
    {"code": "BRRTL0249", "nome": "ABRAINC - Unidades Lançadas - Todos os Empreendimentos",           "serie": "#", "setor": "Construção civil", "fonte_original": "Abrainc/Fipe", "unidade": "unidades", "casas": 0},
    {"code": "BRRTL0250", "nome": "ABRAINC - Unidades Lançadas - Médio e Alto Padrão MAP",            "serie": "#", "setor": "Construção civil", "fonte_original": "Abrainc/Fipe", "unidade": "unidades", "casas": 0},
    {"code": "BRRTL0251", "nome": "ABRAINC - Unidades Lançadas - Programa Casa Verde e Amarela CVA",  "serie": "#", "setor": "Construção civil", "fonte_original": "Abrainc/Fipe", "unidade": "unidades", "casas": 0},
    {"code": "BRRTL0253", "nome": "ABRAINC - Unidades Vendidas - Todos os Empreendimentos",           "serie": "#", "setor": "Construção civil", "fonte_original": "Abrainc/Fipe", "unidade": "unidades", "casas": 0},
    {"code": "BRRTL0254", "nome": "ABRAINC - Unidades Vendidas - Médio e Alto Padrão MAP",            "serie": "#", "setor": "Construção civil", "fonte_original": "Abrainc/Fipe", "unidade": "unidades", "casas": 0},
    {"code": "BRRTL0255", "nome": "ABRAINC - Unidades Vendidas - Programa Casa Verde e Amarela CVA",  "serie": "#", "setor": "Construção civil", "fonte_original": "Abrainc/Fipe", "unidade": "unidades", "casas": 0},
    {"code": "BRRLE0007", "nome": "Secovi SP - Vendas de Imóveis Residenciais Novos",                 "serie": "#", "setor": "Construção civil", "fonte_original": "Secovi", "unidade": "unidades", "casas": 0},
    {"code": "BRRLE0010", "nome": "Secovi SP - Lançamentos Residenciais",                             "serie": "#", "setor": "Construção civil", "fonte_original": "Secovi", "unidade": "unidades", "casas": 0},
    # Unidade do cimento: conferir na 1a rodada qual é a da série da 4i (t, mil t ou milhões de t) e preencher aqui.
    {"code": "BRIND0125", "nome": "SNIC - Vendas de Cimento Total",                                   "serie": "#", "setor": "Construção civil", "fonte_original": "SNIC"},

    # ---------- Confiança (FGV, variação m/m) ----------
    {"code": "BRLDG0003", "nome": "FGV - Índice de Confiança do Consumidor",  "serie": "var", "setor": "Macroeconomia",    "fonte_original": "FGV"},
    {"code": "BRLDG0002", "nome": "FGV - Índice de Confiança da Indústria",   "serie": "var", "setor": "Indústria",        "fonte_original": "FGV"},
    {"code": "BRLDG0034", "nome": "FGV - Índice de Confiança da Construção",  "serie": "var", "setor": "Construção civil", "fonte_original": "FGV"},

    # ---------- Preços, câmbio, juros e bolsa ----------
    # Dólar: a planilha pede "Tira a projeção".
    {"code": "BRFXR0032", "nome": "Dólar Comercial - Média (R$/US$)", "serie": "#",   "setor": "Macroeconomia",    "fonte_original": "Banco Central do Brasil", "unidade": "R$/US$", "casas": 2, "projecao": False},
    {"code": "BRPRC0046", "nome": "Inflação (IPCA)",                  "serie": "var", "setor": "Macroeconomia",    "fonte_original": "IBGE", "polaridade": -1},
    {"code": "BRPRC0033", "nome": "INCC-M",                           "serie": "var", "setor": "Construção civil", "fonte_original": "FGV",  "polaridade": -1},
    {"code": "BRPRC0024", "nome": "IGP-M",                            "serie": "var", "setor": "Macroeconomia",    "fonte_original": "FGV",  "polaridade": -1},
    {"code": "BRSTK0005", "nome": "IBOVESPA - Fechamento Mensal",     "serie": "#",   "setor": "Macroeconomia",    "fonte_original": "B3", "unidade": "pontos", "casas": 0},
    {"code": "BRINR0012", "nome": "Selic (% a.a.)",                 "serie": "#",   "setor": "Macroeconomia",    "fonte_original": "Banco Central do Brasil", "unidade": "% a.a.", "casas": 2, "polaridade": -1},
]

# Marcados com x na planilha mas ainda sem código na 4i (ficam fora até alguém achar a série):
#   ANIP - Vendas Totais de Pneus

# COMMAND ----------

# Conferências rápidas (rodam junto com o %run)
_codes = [i["code"] for i in INDICADORES]
assert len(_codes) == len(set(_codes)), f"código repetido em INDICADORES: {sorted({c for c in _codes if _codes.count(c) > 1})}"
assert all(i["serie"] in ("#", "var") for i in INDICADORES), "serie tem que ser '#' (nível) ou 'var' (variação)"
TIPO_SERIE = {i["code"]: i["serie"] for i in INDICADORES}                       # code -> '#' ou 'var'
SEM_PROJECAO = {i["code"] for i in INDICADORES if i.get("projecao") is False}    # não baixa/mostra projeção

