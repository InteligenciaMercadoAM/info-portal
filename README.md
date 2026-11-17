# Portal IM

Portal de Inteligência de Mercado: notícias classificadas (Infomet e automotivo), indicadores da 4intelligence com projeção e resumo semanal gerado por IA.

- **Site:** https://imview.netlify.app (Netlify, publica sozinho cerca de 1 minuto depois de cada commit em `main`)
- **Repositório:** `InteligenciaMercadoAM/info-portal` (**público**)
- **Dados:** gerados no Databricks pelo notebook `00_atualizar_portal` e publicados em `public/data/` por commit via API do GitHub

> ⚠️ **Repositório público.** Qualquer pessoa vê o código e o histórico. Nunca coloque token, senha ou URL de webhook em arquivo do repositório. Credenciais ficam só no secret scope `portal-im` do Databricks.

---

## Sumário

1. [Estrutura do repositório](#1-estrutura-do-repositório)
2. [Pipeline diário (`00_atualizar_portal`)](#2-pipeline-diário-00_atualizar_portal)
3. [Indicadores (4intelligence)](#3-indicadores-4intelligence)
4. [Notícias](#4-notícias)
5. [Resumo semanal com IA](#5-resumo-semanal-com-ia)
6. [Configuração do site (`public/config.json`)](#6-configuração-do-site-publicconfigjson)
7. [Ambiente e permissões](#7-ambiente-e-permissões)
8. [Como aplicar um pacote de atualização](#8-como-aplicar-um-pacote-de-atualização)
9. [Pontos de atenção e pendências](#9-pontos-de-atenção-e-pendências)
10. [Histórico de mudanças](#10-histórico-de-mudanças)

---

## 1. Estrutura do repositório

```
info-portal/
├── README.md                     ← este arquivo
├── public/                       ← site (Netlify)
│   ├── index.html
│   ├── app.js
│   ├── styles.css
│   ├── goatcounter.js            ← monitoramento de acessos (inofensivo se não configurado)
│   ├── config.json               ← páginas ocultas, nomes dos indicadores etc.
│   └── data/                     ← GERADO PELO PIPELINE. Não editar nem subir à mão.
│       ├── feed.json, feed_automotivo.json, manual.json, resumo.json,
│       │   meta.json, distinct.json, indicadores.json, ptax.json
│       └── textos/AAAA-MM-DD.json   ← texto completo das notícias, 1 arquivo por dia
├── notebooks/                    ← notebooks Databricks (formato .py "source")
│   ├── 00_atualizar_portal
│   ├── Export_Portal_Data (3)
│   ├── indicadores_portal
│   ├── novoind_4_Intelligence_isabela_ajustado_v3
│   ├── Scraper_Automotivas_Economico
│   ├── Resumo_Semanal_IA
│   ├── token_4i
│   └── utilitarios/
│       ├── listar_indicadores_4i
│       ├── diagnostico_projecoes_4i
│       ├── configurar_token_github
│       └── criar_job_ptax
└── docs/
    └── indicadores_4i_correspondencia.xlsx
```

> **`public/data/` só é escrito pelo pipeline.** Em 21/10 um zip trouxe JSONs antigos por cima dos novos e as projeções sumiram do site. Nenhum pacote de atualização deve conter `public/data/`.

---

## 2. Pipeline diário (`00_atualizar_portal`)

Roda as fontes em sequência, gera os JSONs e publica tudo em **um commit só**.

| # | Etapa | Notebook | Padrão em `RODAR` |
|---|---|---|---|
| 1 | Infomet (scraping → raw → silver → gold → classificação) | pasta `chatbot_v3 (final)/pipeline` | `False`: já roda nos jobs "1- Pipeline Medallion" e "2- Classifica + Envia Teams" |
| 2 | Automotivo (scraping + classificação) | `Scraper_Automotivas_Economico` | `True` |
| 3 | Indicadores 4i (realizado + projeção) | `novoind_4_Intelligence_isabela_ajustado_v3` | `True` |
| 4 | Resumo semanal IA | `Resumo_Semanal_IA` | `True` |
| 5 | PTAX (dólar) | `NB_PTAX` | opcional |
| 6 | Export dos JSONs (inclui `ptax.json`) | `Export_Portal_Data (3)` | sempre |
| 7 | Commit único no GitHub (só arquivos que mudaram, incluindo `textos/`) | — | se `FAZER_COMMIT = True` |

O widget **`rodada`** define o que roda: `completa` (padrão, todas as etapas acima) ou `ptax` (só PTAX + Export + commit, usado pela 2ª rodada das 13:30; ver [seção 7](#7-ambiente-e-permissões)).

**Comportamento**
- Cada fonte é independente: se uma falhar (ex.: login da 4i vencido), as outras seguem e o portal é publicado com o que deu certo. No fim o notebook **falha de propósito** para o agendamento mandar e-mail.
- `FAZER_COMMIT = False` gera os arquivos em `portal_dados` sem publicar. As fontes **gravam nas tabelas mesmo assim**.
- Widget `so_conteudo_manual = sim`: pula as fontes e roda só Export + commit (usado pelo botão "Publicar no portal agora?" do conteúdo manual).
- Se um dia ligar o Infomet no `RODAR`, desligue os jobs 1 e 2, senão tudo roda duas vezes.

**Execução**
- **Num cluster, não no serverless.** DBR **15.4 LTS ou mais novo** (necessário para `ai_query`).
- Agendamento: diário, dias úteis, depois que o job "2- Classifica + Envia Teams" termina. Roda como a Vanessa (ver [seção 7](#7-ambiente-e-permissões)).

---

## 3. Indicadores (4intelligence)

### Lista oficial: `notebooks/indicadores_portal`
- **Único lugar** onde se escolhe quais indicadores vão para o portal e como aparecem (nome, unidade, frequência). O `novoind` e o `Export` leem a lista com `%run`.
- Segue a planilha `docs/indicadores_4i_correspondencia.xlsx`, aba **objetivo**, linhas com **x em Mantém** (34 indicadores na versão de 22/10). Nome exibido = coluna **name**.
- Ao mudar a lista, atualize também `indicadores_nomes` em `public/config.json`. Se ficarem diferentes, o Export avisa com ⚠️.

### Nível ou variação (coluna Unidade da planilha)
| Tipo | Série da 4i | Indicadores |
|---|---|---|
| `#` nível | terminada em `L`, de preferência original (sem ajuste sazonal) | produção e vendas de veículos, ABRAINC, Secovi, cimento, Dólar, Ibovespa, Selic |
| `var` variação vs. período anterior (% m/m, % t/t) | terminada em `M`, de preferência dessazonalizada | PIB, produção industrial, confiança FGV, IPCA, INCC-M, IGP-M |

### Projeção
- Sempre que a 4i tiver: entre as séries do tipo certo, o notebook escolhe a que tem projeção.
- Se só a série do outro tipo tiver projeção, ele converte (variação a partir do nível ou nível encadeado a partir da variação) e registra no log.
- **Dólar sem projeção**, como pede a planilha.
- **`"frequencia": "M"`** trava a série mensal: usado em Bens de Capital, PIM Transformação, PIM Total e Anfavea Vendas Totais, que vinham trimestrais porque a projeção só existia na trimestral. Se a 4i não tiver projeção na mensal, o indicador aparece sem projeção. O Export traduz a frequência travada para o formato do site ("Mensal", unidade "% m/m", períodos "jul/26").
- Ao final, o notebook confere se cada indicador saiu como a planilha pede e mostra ⚠️ se algum não bater. Stages antigos com regra diferente são refeitos sozinhos.

### Gráficos no site: cores, unidades e variação %
- **Cores por tipo de medida:** nível = **grafite**, variação % = **laranja** (azul fica de fora: é a cor da concorrência). Indicadores de variação (IPCA, PIB, PIM, FGV...) têm a linha laranja.
- **Indicadores em nível** (Anfavea, ABRAINC, Secovi, SNIC, Dólar, Ibovespa) ganham uma **2ª linha laranja com a variação %** (m/m por padrão), num painel com eixo próprio logo abaixo da linha do nível, no mesmo eixo de tempo. Todos os gráficos têm a mesma altura.
- **Mês fora da curva** (ex.: ABRAINC jul/25, +3.387%) fica preso na borda do painel com um ponto vazado; o valor real aparece no tooltip.
- O card mostra também a variação do último realizado (ex.: "+2,7% m/m").
- **Selic fica sem a linha de variação** (variação % de uma taxa não diz nada).
- Tipo e layout da variação são configuráveis no `config.json` (`variacao_grafico`, `variacao_layout`; ver [seção 6](#6-configuração-do-site-publicconfigjson)). A linha de variação é calculada no `app.js`: aparece assim que o Netlify publica, sem rodar o pipeline.

### Unidades
- **Cimento (SNIC): milhões de t** (a série da 4i vem ~5,5 por mês). Definida em `"unidade"` no `indicadores_portal`.

### Calendário de divulgações
- Só entram divulgações de **série nacional** (região `000` no código da série). A 4i lista PIMs regionais com o mesmo código do indicador nacional.
- O log imprime "Ignoradas N divulgações regionais".

### Códigos que diferem da planilha
| Indicador | Código usado | Motivo |
|---|---|---|
| Selic | **BRINR0010** (Meta) | a Efetiva (BRINR0012) vinha acumulada em 12 meses e atrasada em relação à taxa vigente |
| Linha branca | BRIND0368 | BRIND0251 é "Produção de Eletrodomésticos" na 4i |
| Produção total Anfavea | BRIND0055 | BRIND0446 foi descontinuada em dez/20 |
| ABRAINC "todos" e Secovi vendas | BRRTL0249, BRRTL0253, BRRLE0007 | — |

---

## 4. Notícias

### Texto completo
- `tb_noticias_classificadas` guarda só o 1º chunk (~1.200 caracteres) de cada notícia, vindo da gold picotada para o chatbot.
- O **Export** junta a silver `tb_silver_news_chatbot_v1` (por `id` + `source_type`). No `feed.json`, cada notícia Infomet ganha `titulo` (título real), `fonte_original` e `texto_completo: true`.
- O texto inteiro vai para `public/data/textos/AAAA-MM-DD.json`. O site só baixa esse arquivo quando alguém abre a notícia. Dias fora da janela de 90 dias são apagados localmente e no GitHub.
- Para desligar: `TEXTO_COMPLETO = False` no Export.

### Automotivo (Automotive Business e AutoData)
- O `Scraper_Automotivas_Economico` grava o texto completo na coluna `conteudo` da base bruta `hive_metastore.default.noticias_automotivebusiness` (criada sozinha com `ALTER TABLE` na 1ª rodada).
- O Export junta pela chave `id` + `fonte_site`.
- O dia a dia pega os 200 posts mais recentes de cada fonte. Para preencher notícias antigas, rode o scraper **uma vez** com `PAGINAS_COLETA = 10` e volte para 2.
- Notícias da AutoData que caem no fallback de HTML ficam sem texto completo.
- Notícias automotivas vão só para o portal, não para o Teams.

### Temas do automotivo
- Todas as notícias do piloto automotivo aparecem num **tema único, "Automotivo"**: uma caixa só na home (com as 3 principais) e uma opção só no filtro de Tema. Antes eram subtemas ("Automotivo: volume e mercado", "Automotivo: montadoras" etc.).
- Categorias novas que o classificador criar com prefixo `auto_` (ex.: `auto_marketing`) também caem em "Automotivo", via `temas_por_prefixo` no `config.json` (lido pela `canonCategory` do `app.js`).
- **"Veículos pesados"** (Infomet) continua como tema separado.
- Para voltar aos subtemas: apagar `temas_por_prefixo` e restaurar os nomes antigos em `temas` no `config.json`.

### Ordem da aba Notícias
- Padrão **"Dia e relevância"**: dia mais recente primeiro; dentro do dia, "Relevante" primeiro, depois maior score e, no empate, horário mais recente. (Por dia porque o automotivo vem sempre com score 9 e ocuparia o topo da semana numa ordem só por score.)
- Seletor **Ordenar por** no topo da lista: "Data e hora" é a ordem antiga.

---

## 5. Resumo semanal com IA

Notebook `notebooks/Resumo_Semanal_IA`, modelo `databricks-meta-llama-3-3-70b-instruct` via `ai_query` (não precisa de token).

1. Junta as notícias relevantes da semana (Infomet score ≥ 5, automotivo "Alta", notícias e alertas manuais), no máximo 60, as de maior score.
2. Gera uma frase por notícia, guardada em `tb_portal_resumo_noticia_ia` (nenhuma notícia é resumida duas vezes).
3. Gera 3 a 5 destaques (manchete + 2 ou 3 frases, com números e o impacto para aços longos), cada um ligado às notícias que o sustentam. Descarta id inventado e destaque sem notícia; tenta de novo (até 3x) se a resposta vier curta ou quebrada.
4. Grava em `tb_portal_resumo_semanal_ia`. O Export lê o mais recente e gera `resumo.json`.

| Configuração | Efeito |
|---|---|
| `MODO = "semana_fechada"` (padrão) | segunda a domingo da semana anterior; gera na 1ª rodada da segunda, nos outros dias sai em segundos |
| `MODO = "ultimos_7_dias"` | gera de novo todo dia |
| widget `forcar = sim` | refaz a mesma semana (ex.: depois de mudar o prompt) |

- **Resumo manual da equipe** continua funcionando e só substitui o da IA se for mais novo que a semana da IA.
- **No site** (home, card "Resumo semanal"): destaque → notícias com uma frase cada → detalhe com texto completo. O card avisa que o texto é gerado por IA.
- **Custo:** cerca de 60 chamadas curtas e 1 maior por semana (centavos).

---

## 6. Configuração do site (`public/config.json`)

| Chave | Uso |
|---|---|
| `portal.paginas_ocultas` | páginas escondidas do menu; link direto cai na home. Hoje: `["external", "competitors"]` (Mercado Externo e Concorrentes; `competitors` também tira o card "Concorrentes no período" da home). Para voltar a mostrar, remova da lista |
| `portal.variacao_grafico` | linha de variação % nos indicadores em nível: `"mom"` (vs. mês anterior, padrão), `"yoy"` (vs. mesmo mês do ano anterior, a/a) ou `""` (sem a linha) |
| `portal.variacao_layout` | `"painel"` (variação num painel logo abaixo do nível, padrão) ou `"eixo_direito"` (as duas linhas juntas, variação no eixo da direita) |
| `analytics_goatcounter` | conta do GoatCounter para o monitoramento de acessos (`goatcounter.js`). Vazio = desligado. Passo a passo de criação da conta no README do pacote de analytics de 17/11 |
| `indicadores_nomes` | nomes exibidos dos indicadores; deve bater com `indicadores_portal` |
| `indicadores_fixos` | indicadores em destaque na home |
| `temas` | nome exibido de cada categoria do classificador (ex.: categorias do automotivo → "Automotivo") |
| `temas_por_prefixo` | agrupa num tema todas as categorias que começam com um prefixo. Hoje: `auto_*` → "Automotivo" |
| `temas_fora_da_home` | temas que não ganham caixa própria na home |

Se o `config.json` tiver sido editado depois de 17/11 e as chaves de variação não estiverem lá, basta acrescentar no bloco `"portal"` (com vírgula antes):

```json
"variacao_grafico": "mom", "variacao_layout": "painel"
```

---

## 7. Ambiente e permissões

### Caminhos no workspace (usuário da Vanessa)
| O quê | Caminho |
|---|---|
| Git folder do repositório | `/Workspace/Users/vanessa.n.silva@arcelormittal.com.br/info-portal` |
| Saída dos JSONs (`OUTPUT_DIR`) | `/Workspace/Users/vanessa.n.silva@arcelormittal.com.br/portal_dados` |
| `config.json` lido pelo Export | `.../info-portal/public/config.json` |
| Pipeline Infomet (`PIPELINE_DIR`, só se ligado no `RODAR`) | `.../chatbot_v3 (final)/pipeline` |

- Os JSONs são gerados **fora** do Git folder de propósito: dentro dele, o Databricks ficaria com "mudanças locais" e o Pull deixaria de funcionar.
- Na 1ª rodada a pasta `portal_dados` está vazia, e isso é normal: o Export busca o `indicadores.json` anterior no GitHub para não perder as datas de "dado novo".

### Tabelas
- Continuam no schema **`lcb_qas_sandbox2_comercial.isabela_pereira`** (ainda não há schema próprio da Vanessa).
- Automotivo: `hive_metastore.default.noticias_automotivebusiness` (bruta) e `..._arcelormittal` (classificada).

### Permissões necessárias para quem roda
- leitura e escrita no schema `lcb_qas_sandbox2_comercial.isabela_pereira`;
- leitura e escrita nas tabelas `hive_metastore.default.noticias_automotivebusiness*`;
- leitura no secret scope `portal-im` (token do GitHub em `github-token`);
- `Can Query` no endpoint `databricks-meta-llama-3-3-70b-instruct`.

### Login da 4i (`notebooks/token_4i`)
- Rodar logada como **quem executa o pipeline**, com o usuário do Mateus. O token fica na pasta pessoal de quem roda e vale **cerca de 30 dias**.
- Se a etapa da 4i falhar por login/timeout, rode o `token_4i` de novo.

### Agendamento
- Criado no usuário da Vanessa (substitui o da Isabela, que deve ficar desligado para não publicarem os dois).
- Os jobs do Infomet ("1- Pipeline Medallion" e "2- Classifica + Envia Teams") são separados e não mudam.
- **2ª rodada (dólar PTAX):** job "4- Atualizar Portal IM - PTAX", dias úteis às 13:30 (Brasília), roda o `00_atualizar_portal` com `rodada = ptax` (só PTAX + Export + commit). Criado pelo `utilitarios/criar_job_ptax`. Se a cotação de hoje não estiver no `ptax.json`, o job publica mesmo assim e falha no fim pra mandar e-mail.

### Utilitários (`notebooks/utilitarios/`)
| Notebook | Uso |
|---|---|
| `listar_indicadores_4i` | busca indicadores no catálogo da 4i por palavra-chave |
| `diagnostico_projecoes_4i` | lista as séries de um indicador e mostra se cada uma tem projeção |
| `configurar_token_github` | guarda ou troca o token do GitHub no secret do Databricks |
| `criar_job_ptax` | cria/ajusta o job da 2ª rodada do dia (dólar PTAX), copiando cluster e e-mails do job "3- Atualizar Portal IM" |

---

## 8. Como aplicar um pacote de atualização

1. **Atualize o repositório:** copie o **conteúdo** da pasta `info-portal/` do zip para a **raiz** do repositório, por cima do que existe.
   - O zip traz só os arquivos alterados, inteiros: ele sobrescreve e **não apaga** nada. Arquivos que precisam sumir são indicados nas notas do pacote e devem ser apagados à mão.
   - Confira se o zip **não** contém `public/data/`.
   - O README de um pacote é só do pacote: **não** suba por cima deste `README.md`; incorpore as mudanças aqui.
   - Notebooks ficam em formato `.py` (source). Se existir um `.ipynb` com o mesmo nome, apague o `.ipynb`: os dois juntos impedem o Pull no Databricks ("conflicting names").
   - **Ordem dos pacotes:** como cada pacote traz arquivos inteiros, subir um pacote mais antigo depois de um mais novo desfaz as mudanças do mais novo nos arquivos em comum (ex.: `app.js`, `config.json`). Suba sempre na ordem cronológica, ou só o mais recente quando ele já incluir o anterior.
2. **Revise o diff** antes do commit (caminhos `/Workspace/Users/...`, `FAZER_COMMIT = True`, só o que as notas descrevem).
3. **Pull** no Git folder do Databricks, fora do horário do agendamento.
4. **Rode os passos manuais** indicados nas notas do pacote (ex.: scraper com `PAGINAS_COLETA = 10`, `Resumo_Semanal_IA` com `forcar = sim`).
5. **Teste sem publicar:** `FAZER_COMMIT = False` no `00_atualizar_portal`; depois descarte essa alteração no Git folder (não edite de volta à mão) e rode publicando.

Mudanças só de site (`public/app.js`, `public/styles.css`, `public/config.json`) não precisam rodar o pipeline: o Netlify publica após o commit.

---

## 9. Pontos de atenção e pendências

- **Direitos do conteúdo.** Infomet é assinatura paga, e o repositório e o site são públicos. Com o texto completo, as matérias inteiras (Infomet, Automotive Business, AutoData) ficam abertas no GitHub e no Netlify. Confirmar com quem cuida do contrato. Se não puder: `TEXTO_COMPLETO = False` ou site com login.
- **Indicadores sem projeção na 4i** (rodada de 21/10): linha branca, Secovi, ABRAINC, Ibovespa, Selic e ICST. Os que continuarem sem projeção não têm projeção na 4i: é conversa com a 4i, não com o código.
- **Salto no 1º ponto da projeção** (21/10): Dólar +10,9%, ICI +8,8% e ICC +6,6% num mês. Parece projeção desatualizada na 4i; perguntar ao Mateus.
- **Unidades das séries em nível** a conferir: "unidades" (veículos, ABRAINC, Secovi), "pontos" (Ibovespa), "R$/US$" (Dólar), "% a.a." (Selic). Cimento resolvido em 17/11 (milhões de t).
- **ANIP (pneus):** marcado na planilha, mas a série ainda não foi encontrada na 4i; fica fora. Usar `listar_indicadores_4i`.
- **Selic:** o portal mostra a Meta (taxa do Copom), não a Efetiva.

---

## 10. Histórico de mudanças

### 17/11/2026
- **Variação % nos indicadores em nível** (Anfavea, ABRAINC, Secovi, SNIC, Dólar, Ibovespa): 2ª linha laranja com a variação m/m num painel abaixo do nível, outliers presos na borda com ponto vazado (valor real no tooltip) e variação do último realizado no card. Selic sem variação. Configurável por `portal.variacao_grafico` e `portal.variacao_layout`.
- **Cores por tipo de medida:** nível = grafite, variação % = laranja. Todos os gráficos com a mesma altura.
- **Unidade do cimento (SNIC):** milhões de t (`notebooks/indicadores_portal.py`).
- **Correção:** Bens de Capital, Produção Industrial Total/Transformação e Anfavea Vendas Totais saíam com frequência "M", unidade "%" e períodos "01/07/26"; passam a "Mensal", "% m/m" e "jul/26" (`notebooks/Export_Portal_Data (3).py`).
- **Monitoramento de acessos (GoatCounter):** `public/goatcounter.js` e chave `analytics_goatcounter` no `config.json`.
- Arquivos: `public/app.js`, `public/styles.css`, `public/config.json`, `public/goatcounter.js`, `notebooks/Export_Portal_Data (3).py`, `notebooks/indicadores_portal.py`.
- **Ordem dos pacotes de 17/11:** o pacote de unidades/variação já contém o de analytics. Se subir os dois, o de analytics vai **primeiro**; subi-lo depois apaga a variação e as cores novas.
- A linha de variação aparece assim que o Netlify publica; a unidade do cimento e a correção do "M" só depois da próxima rodada do `00_atualizar_portal`.

### 13/11/2026
- **Automotivo num tema só:** os subtemas do piloto automotivo viraram o tema único "Automotivo" (home e filtro de Tema). Categorias com prefixo `auto_` caem nele via `temas_por_prefixo`. "Veículos pesados" segue separado.
- `public/config.json`: temas `auto_*` = "Automotivo"; "Automotivo: eventos/outros" saíram de `temas_fora_da_home`.
- `public/app.js`: `canonCategory` lê `temas_por_prefixo`.
- Sem mudança em notebooks nem em `public/data/`; não precisa rodar o pipeline.

### 10/11/2026 (tarde)
- **Card "Dólar PTAX"** na home, no topo da coluna da direita (acima de Próximas divulgações): fechamento do dia (compra/venda, data e hora de São Paulo), média parcial do mês corrente e último mês fechado (média, fim do mês, dias úteis). 4 casas, pt-BR, nulo = "—".
- Export grava `ptax.json` a partir de `isabela_pereira.historico_completo_ptax` e `isabela_pereira.media_mensal_ptax`. Se falhar, mantém o anterior e o resto do Export segue.
- `00_atualizar_portal`: widget `rodada` (`completa`/`ptax`), fonte opcional `PTAX` (`NB_PTAX`), `ptax.json` na lista de arquivos do commit.
- Novo `utilitarios/criar_job_ptax`.

### 10/11/2026
- `LEIA-ME*.md` renomeados para `README*.md` (conteúdo consolidado neste arquivo).
- Aba Notícias ordenada por **dia e relevância**, com seletor "Ordenar por". Arquivos: `public/app.js`, `public/styles.css`.

### 06/11/2026
- Notícias com **texto completo** (Infomet via silver; automotivo via coluna `conteudo`) em `public/data/textos/`.
- Novo **`Resumo_Semanal_IA`** e `resumo.json`; `RODAR` ganhou `"Resumo semanal IA": True`.
- Calendário só com divulgações nacionais; `"frequencia": "M"` em Bens de Capital, PIM Transformação, PIM Total e Anfavea Vendas Totais.
- Selic trocada de Efetiva (BRINR0012) para **Meta (BRINR0010)**.
- Página **Mercado Externo** oculta via `portal.paginas_ocultas`.
- Aba **Concorrentes** e card "Concorrentes no período" da home ocultos via `portal.paginas_ocultas` (11/11/2026).
- Pipeline passou para o usuário da **Vanessa** (caminhos, Git folder, `portal_dados`, agendamento); tabelas seguem em `isabela_pereira`.

### 22/10/2026
- Pacote completo do repositório, sem JSONs em `public/data/` (correção do sumiço de projeções em 21/10, causado por JSONs antigos subidos à mão).
- Novo `indicadores_portal` como lista única (34 indicadores) e regra de nível/variação conforme a planilha, com projeção sempre que existir.
- `GITHUB_DATA_PATH = "public/data"` (o repositório não tem mais a subpasta `info-portal/` desde 21/10).
- Removidos `LEIA-ME_atualizacao_2026-10-19.md` e `docs/indicadores_4i_correspondencia_6.xlsx`.
- Utilitários `listar_indicadores_4i`, `diagnostico_projecoes_4i` e `configurar_token_github`.
- Fora do repositório: `Job_Classificacao_Noticias_v2.py` (corrige `NameError: SETORES`), na pasta do pipeline Infomet, com `MODO_TESTE = True`.
