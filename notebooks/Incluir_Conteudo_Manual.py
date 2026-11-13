# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ✍️ Incluir conteúdo manual no Portal IM
# MAGIC
# MAGIC Use esta página para **adicionar notícias, alertas e os Highlights da semana** no portal. Não precisa saber programar.
# MAGIC
# MAGIC ## Como usar (3 passos)
# MAGIC
# MAGIC 1. **Preencha as caixas no topo desta página** (logo acima do texto). Primeiro escolha em **"1. O que você quer fazer?"** e depois preencha só os campos daquela ação (tabela abaixo).
# MAGIC 2. Clique em **▶ Run all** (Executar tudo). A página mostra **como vai ficar**, mas **ainda não salva nada**.
# MAGIC 3. Se estiver certo, mude a última caixa **"▶ Salvar de verdade?"** para **"Sim, salvar"** e clique em **Run all** de novo. Pronto: as caixas voltam em branco.
# MAGIC
# MAGIC | O que você quer fazer | Campos que precisa preencher |
# MAGIC |---|---|
# MAGIC | **Adicionar notícia** | Headline, Resumo, Fonte, Data. Opcionais: Link, Setor, Região, Tema |
# MAGIC | **Adicionar alerta** | Headline. Opcionais: Resumo, Data, "Vale até" (sem isso, o alerta some sozinho em 7 dias) |
# MAGIC | **Publicar Highlights da semana** | Highlight 1 a 5 (use quantos quiser). Opcionais: Título e Link |
# MAGIC | **Ver o que está publicado** | Nada |
# MAGIC | **Remover um item** | Cole o ID do item (aparece na lista que a página mostra) |
# MAGIC
# MAGIC ## Quando aparece no portal?
# MAGIC
# MAGIC - Na **próxima atualização automática** do portal (rodada diária), ou
# MAGIC - **na hora**, se você mudar a caixa **"Publicar no portal agora?"** para "Sim". Demora alguns minutos, e o site atualiza cerca de 1 minuto depois.
# MAGIC
# MAGIC ## Dicas
# MAGIC
# MAGIC - **Escreva a Região como "Internacional"** se a notícia for sobre mercado externo: ela aparece na aba *Mercado externo*.
# MAGIC - Se a **Data** ficar em branco, vale a data de hoje. Formato: `dd/mm/aaaa` (ex.: `01/11/2026`).
# MAGIC - Cada nova rodada de **Highlights** substitui a anterior no portal (aparecem até 5 linhas).
# MAGIC - Errou algo depois de salvar? Use **"Remover um item"** e adicione de novo.
# MAGIC - Qualquer dúvida ou mensagem de erro em vermelho: tire um print e mande para a Isabela.

# COMMAND ----------

# ===================== CAMPOS DO FORMULÁRIO =====================
# Os campos aparecem no topo da página, em ordem alfabética do nome (por isso os números).
# Quem mexe aqui é TI: a gerente só preenche as caixas.

TABELA = "lcb_qas_sandbox2_comercial.isabela_pereira.tb_portal_conteudo_manual"   # a mesma MANUAL_TABLE do Export

ACOES = ["Adicionar notícia", "Adicionar alerta", "Publicar Highlights da semana", "Ver o que está publicado", "Remover um item"]
SETORES = ["Siderurgia", "Construção civil", "Agronegócio", "Automotivo", "Indústria", "Energia", "Mineração", "Macroeconomia"]
REGIOES = ["(não informar)", "Nacional", "Sudeste", "Sul", "Nordeste", "Norte", "Centro-Oeste", "Internacional"]
# nome que a pessoa vê -> código que o portal usa (config.json > temas)
TEMAS = {
    "Mercado e volumes": "mercado_e_volumes",
    "Operação e produção": "operacional_e_producao",
    "Investimentos e expansão": "investimentos_e_expansao",
    "Comércio exterior": "comercio_exterior",
    "Players do setor": "players",
    "Construção civil: geral": "construcao_civil_geral",
    "Construção civil: infraestrutura": "construcao_civil_infraestrutura",
    "Energia": "energia",
    "Mineração": "mineracao",
    "Sucata": "sucata",
    "Financeiro": "financeiro",
    "Sustentabilidade e ESG": "sustentabilidade_esg",
    "Outros (não aparece na home)": "outro",
}
SIM_SALVAR = "Sim, salvar"
NAO_SALVAR = "Não, só mostrar como vai ficar"
SIM_PUBLICAR = "Sim, publicar agora"
NAO_PUBLICAR = "Não, esperar a atualização automática"

# (tipo, nome interno, valor padrão, opções, rótulo que a pessoa vê)
CAMPOS = [
    ("dropdown", "01_acao", ACOES[0], ACOES, "1. O que você quer fazer?"),
    ("text", "02_seu_nome", "", None, "2. Seu nome"),
    ("text", "03_headline", "", None, "3. Headline (título)  [notícia / alerta]"),
    ("text", "04_resumo", "", None, "4. Resumo  [notícia / alerta]"),
    ("text", "05_fonte", "", None, "5. Fonte, ex.: Valor Econômico  [notícia]"),
    ("text", "06_data", "", None, "6. Data dd/mm/aaaa (vazio = hoje)  [notícia / alerta]"),
    ("text", "07_link", "", None, "7. Link (opcional)  [notícia / Highlights]"),
    ("dropdown", "08_setor", SETORES[0], SETORES, "8. Setor  [notícia / alerta]"),
    ("dropdown", "09_regiao", REGIOES[0], REGIOES, "9. Região  [notícia / alerta]"),
    ("dropdown", "10_tema", "Mercado e volumes", list(TEMAS), "10. Tema  [notícia]"),
    ("text", "11_vale_ate", "", None, "11. Alerta vale até dd/mm/aaaa (opcional)  [alerta]"),
    ("text", "12_highlights_titulo", "", None, "12. Título dos Highlights (opcional, ex.: Semana de 27/10 a 31/10)"),
    ("text", "13_highlight_1", "", None, "13. Highlight 1"),
    ("text", "14_highlight_2", "", None, "14. Highlight 2"),
    ("text", "15_highlight_3", "", None, "15. Highlight 3"),
    ("text", "16_highlight_4", "", None, "16. Highlight 4"),
    ("text", "17_highlight_5", "", None, "17. Highlight 5"),
    ("text", "18_id_remover", "", None, "18. ID do item a remover  [remover]"),
    ("dropdown", "98_publicar", NAO_PUBLICAR, [NAO_PUBLICAR, SIM_PUBLICAR], "Publicar no portal agora?"),
    ("dropdown", "99_confirmar", NAO_SALVAR, [NAO_SALVAR, SIM_SALVAR], "▶ Salvar de verdade?"),
]
# depois de salvar, tudo volta em branco, menos "ação" e "seu nome"
MANTER = {"01_acao", "02_seu_nome"}

def criar_campo(c):
    tipo, nome, padrao, opcoes, rotulo = c
    if tipo == "dropdown":
        dbutils.widgets.dropdown(nome, padrao, opcoes, rotulo)
    else:
        dbutils.widgets.text(nome, padrao, rotulo)

for c in CAMPOS:
    criar_campo(c)

def limpar_campos():
    for c in CAMPOS:
        if c[1] in MANTER:
            continue
        dbutils.widgets.remove(c[1])
        criar_campo(c)

# COMMAND ----------

# ===================== REGRAS (funções) =====================
import hashlib, html, re
from datetime import date, datetime
from zoneinfo import ZoneInfo

HOJE = datetime.now(ZoneInfo("America/Sao_Paulo")).date()

class Erro(Exception):
    """Mensagem amigável para a pessoa que está preenchendo."""

def texto(valor):
    # tira espaços e quebras sobrando
    return " ".join(str(valor or "").split())

def parse_data(valor, campo, padrao=None):
    t = texto(valor)
    if not t:
        return padrao
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(t, fmt).date()
        except ValueError:
            pass
    raise Erro(f"Não entendi a data do campo \"{campo}\": \"{t}\". Escreva assim: 01/11/2026")

def gerar_id(tipo, titulo, data):
    h = hashlib.sha1(f"{tipo}|{titulo}|{data}".encode("utf-8")).hexdigest()[:8]
    return f"{tipo}-{h}"

def montar_item(acao, v):
    """v = dicionário com os valores dos campos. Devolve o item pronto ou levanta Erro."""
    nome = texto(v["02_seu_nome"]) or "Equipe IM"
    link = texto(v["07_link"])
    if link and not re.match(r"^https?://", link, re.I):
        raise Erro("O Link precisa começar com http:// ou https:// (copie o endereço completo da página).")

    if acao == "Publicar Highlights da semana":
        linhas = [re.sub(r"^[-•*]\s*", "", texto(v[k])) for k in sorted(v) if k.startswith(("13_", "14_", "15_", "16_", "17_"))]
        linhas = [l for l in linhas if l]
        if not linhas:
            raise Erro("Preencha pelo menos o \"Highlight 1\".")
        titulo = texto(v["12_highlights_titulo"])
        if not titulo:
            fim = HOJE
            ini = date.fromordinal(fim.toordinal() - 6)
            titulo = f"Semana de {ini:%d/%m} a {fim:%d/%m}"
        return {"id": gerar_id("resumo", titulo + "".join(linhas), HOJE), "tipo": "resumo", "titulo": titulo,
                "texto": "\n".join(linhas), "url": link or None, "data_publicacao": HOJE, "fonte": None, "tema": None,
                "setor": None, "regiao": None, "empresa_principal": None, "valido_ate": None, "adicionado_por": nome}

    tipo = "noticia" if acao == "Adicionar notícia" else "alerta"
    titulo = texto(v["03_headline"])
    if not titulo:
        raise Erro("Falta o Headline (campo 3).")
    resumo = texto(v["04_resumo"])
    fonte = texto(v["05_fonte"])
    if tipo == "noticia":
        if not resumo:
            raise Erro("Falta o Resumo da notícia (campo 4).")
        if not fonte:
            raise Erro("Falta a Fonte da notícia (campo 5).")
    data = parse_data(v["06_data"], "Data", HOJE)
    if data > HOJE:
        raise Erro(f"A data {data:%d/%m/%Y} está no futuro. Confira o campo 6.")
    vale_ate = parse_data(v["11_vale_ate"], "Alerta vale até") if tipo == "alerta" else None
    if vale_ate and vale_ate < HOJE:
        raise Erro(f"O alerta \"vale até\" {vale_ate:%d/%m/%Y}, que já passou. Ele não apareceria no portal.")
    regiao = texto(v["09_regiao"])
    return {"id": gerar_id(tipo, titulo, data), "tipo": tipo, "titulo": titulo, "texto": resumo, "url": link or None,
            "data_publicacao": data, "fonte": fonte or ("Equipe IM" if tipo == "alerta" else None),
            "tema": TEMAS.get(v["10_tema"]) if tipo == "noticia" else None,
            "setor": texto(v["08_setor"]), "regiao": None if regiao.startswith("(") else regiao,
            "empresa_principal": None, "valido_ate": vale_ate, "adicionado_por": nome}

def previa_html(item):
    e = html.escape
    rotulo = {"noticia": "Notícia", "alerta": "Alerta IM", "resumo": "Highlights da semana"}[item["tipo"]]
    meta = " · ".join(x for x in [e(item["fonte"] or ""), item["data_publicacao"].strftime("%d/%m/%Y"),
                                  e(item["setor"] or ""), e(item["regiao"] or "")] if x)
    corpo = ("<ul>" + "".join(f"<li>{e(l)}</li>" for l in item["texto"].split("\n")) + "</ul>") if item["tipo"] == "resumo" \
        else f"<p>{e(item['texto'])}</p>"
    validade = f"<p><i>Vale até {item['valido_ate']:%d/%m/%Y}</i></p>" if item["valido_ate"] else ""
    link = f"<p>🔗 {e(item['url'])}</p>" if item["url"] else ""
    return (f"<div style='border:1px solid #bbb;border-radius:8px;padding:14px 18px;max-width:760px;font-family:sans-serif'>"
            f"<div style='font-size:12px;color:#666'>{rotulo}</div><h3 style='margin:6px 0'>{e(item['titulo'])}</h3>"
            f"<div style='font-size:12px;color:#666'>{meta}</div>{corpo}{validade}{link}</div>")

# COMMAND ----------

# ===================== EXECUÇÃO =====================
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, TimestampType

SCHEMA = StructType([
    StructField("id", StringType()), StructField("tipo", StringType()), StructField("titulo", StringType()),
    StructField("texto", StringType()), StructField("url", StringType()), StructField("data_publicacao", DateType()),
    StructField("fonte", StringType()), StructField("tema", StringType()), StructField("setor", StringType()),
    StructField("regiao", StringType()), StructField("empresa_principal", StringType()),
    StructField("valido_ate", DateType()), StructField("adicionado_por", StringType()),
    StructField("ativo", BooleanType()), StructField("criado_em", TimestampType()),
])

def criar_tabela():
    colunas = ", ".join(f"{f.name} {f.dataType.simpleString().upper()}" for f in SCHEMA.fields)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {TABELA} ({colunas})")

def mostrar_publicados():
    df = (spark.table(TABELA).filter("ativo")
          .orderBy(F.desc("data_publicacao"), F.desc("criado_em"))
          .select("id", "tipo", "data_publicacao", "titulo", "fonte", "setor", "valido_ate", "adicionado_por"))
    n = df.count()
    print(f"📋 {n} {'item publicado' if n == 1 else 'itens publicados'} (a coluna \"id\" é o que se usa para remover):")
    display(df)

def publicar_agora():
    print("\n🚀 Publicando no portal... (leva alguns minutos, não feche a página)")
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    pasta = ctx.notebookPath().get().rsplit("/", 1)[0]
    try:
        dbutils.notebook.run(f"{pasta}/00_atualizar_portal", 3600, {"so_conteudo_manual": "sim"})
        print("✅ Publicado. O site atualiza em cerca de 1 minuto.")
    except Exception as e:
        print("⚠️ Seu conteúdo foi SALVO, mas não consegui publicar agora (pode ser permissão ou o cluster).")
        print("   Não precisa refazer nada: ele entra no portal na próxima atualização automática.")
        print("   Se for urgente, avise a Vanessa. Detalhe técnico:", str(e).split("\n")[0][:200])

def parar(msg):
    print(msg)
    dbutils.notebook.exit(msg)

acao = dbutils.widgets.get("01_acao")
salvar = dbutils.widgets.get("99_confirmar") == SIM_SALVAR
publicar = dbutils.widgets.get("98_publicar") == SIM_PUBLICAR
valores = {c[1]: dbutils.widgets.get(c[1]) for c in CAMPOS}

criar_tabela()
print(f"▶ {acao}\n")

if acao == "Ver o que está publicado":
    mostrar_publicados()
    parar("")

if acao == "Remover um item":
    mostrar_publicados()
    item_id = texto(valores["18_id_remover"])
    if not item_id:
        parar("👆 Para remover, copie o ID do item na lista acima, cole no campo 18 e rode de novo.")
    if not re.fullmatch(r"[a-z0-9-]+", item_id):
        parar("❌ Esse ID não parece certo. Copie só o ID da lista (ex.: noticia-1a2b3c4d), sem espaços.")
    achou = spark.table(TABELA).filter((F.col("id") == item_id) & F.col("ativo")).select("tipo", "titulo").collect()
    if not achou:
        parar(f"❌ Não encontrei um item publicado com o ID \"{item_id}\". Confira a lista acima.")
    print(f"\nVai remover: {achou[0]['tipo']} · {achou[0]['titulo']}")
    if not salvar:
        parar("\n👉 Se é isso mesmo, mude \"▶ Salvar de verdade?\" para \"Sim, salvar\" e clique em Run all de novo.")
    spark.sql(f"UPDATE {TABELA} SET ativo = false WHERE id = '{item_id}'")
    print("🗑️ Removido.")
    limpar_campos()
    if publicar:
        publicar_agora()
    else:
        print("\nSai do portal na próxima atualização automática.")
    parar("")

# --- adicionar notícia / alerta / Highlights ---
try:
    item = montar_item(acao, valores)
except Erro as e:
    parar(f"❌ {e}\n   Corrija a caixa indicada e clique em Run all de novo.")

print("Assim vai aparecer no portal:\n")
displayHTML(previa_html(item))

ja_existe = spark.table(TABELA).filter((F.col("id") == item["id"]) & F.col("ativo")).count() > 0
if ja_existe:
    parar("\nℹ️ Este mesmo conteúdo já está publicado (mesmo título e data). Nada foi duplicado.")
if not salvar:
    print("\n👉 Está certo? Mude \"▶ Salvar de verdade?\" para \"Sim, salvar\" e clique em Run all de novo.\n"
          "   Quer corrigir? Altere as caixas e clique em Run all.")
else:
    # Highlights: o novo substitui o anterior
    if item["tipo"] == "resumo":
        spark.sql(f"UPDATE {TABELA} SET ativo = false WHERE tipo = 'resumo' AND ativo")

    agora = datetime.now(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
    linha = tuple(item[f.name] if f.name in item else None for f in SCHEMA.fields[:-2]) + (True, agora)
    spark.createDataFrame([linha], SCHEMA).write.mode("append").saveAsTable(TABELA)
    print("\n✅ Salvo!")
    limpar_campos()

    if publicar:
        publicar_agora()
    else:
        print("Aparece no portal na próxima atualização automática.\n"
            "(Quer na hora? Na próxima vez, mude \"Publicar no portal agora?\" para \"Sim\".)")
