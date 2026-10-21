# Atualização do Portal IM (19 e 20/10/2026)

Isa, este é o repositório `info-portal` completo e já atualizado. Ele substitui todas as versões e zips anteriores: é só subir no Git e seguir os passos abaixo.

## Resumo do que mudou

### Indicadores 4i
- **Por que a projeção não aparecia:** o `novoind` já baixava as projeções e gravava em `indicadores_lt_20260113_projecoes`, mas o `Export_Portal_Data` só lia a tabela de realizado. Agora o Export lê as duas.
- **Regra nova de série (decisão da Lari):** todos os indicadores usam a série de **variação contra o período anterior** (código terminando em `M`: % m/m nos mensais, % t/t nos trimestrais).
  - A **mesma série** vai no realizado e na projeção.
  - Entre as opções, o notebook prefere a dessazonalizada (`S`).
  - Se um indicador não tiver série `M`, a variação é calculada a partir do nível e isso aparece no log.
  - Para travar uma série específica, use `SERIE_FORCADA` no `novoind`.
- **Lista oficial:** 42 indicadores, conforme `docs/indicadores_4i_correspondencia_6.xlsx`.
  - A BRIND0446 foi trocada pela **BRIND0055 Produção de Veículos (Anfavea)**, porque a 4i descontinuou a 0446 em dez/20.
  - Entraram linha branca (**BRIND0368**; o BRIND0251 estava errado), unidades lançadas e vendidas "todos" (BRRTL0249 e BRRTL0253), Secovi vendas (BRRLE0007), motos (BRIND0445) e ICI (BRLDG0002).
- **Metadados no Export:** o `INDICADORES_PORTAL` agora tem nome, setor (Construção civil, Automotivo, Indústria, Siderurgia ou Macroeconomia), fonte e polaridade de cada indicador.
- **Data de divulgação:** a "última divulgação" deixou de usar o `updated_at` do catálogo, que é a data de cadastro na 4i e não a divulgação do dado. O card mostra o último período com dado.
- **Token:** o código do login virou o notebook `notebooks/token_4i`, usado com `%run` pelo `novoind` e pelos utilitários.

### Pipeline único: `00_atualizar_portal`
- Roda as três fontes, cada uma com o seu processo e o seu prompt de classificação:
  - **Infomet:** scraping, raw, silver, gold e classificação com o prompt do Infomet (`Job - Classificação Relevância Notícias`);
  - **Automotivo:** scraping e classificação com o prompt automotivo;
  - **4intelligence:** indicadores com realizado e projeção. Indicador não passa por classificação.
- Depois gera os JSONs (Export) e faz **um commit único** no GitHub.
- As fontes são independentes: se uma falhar, as outras seguem e o portal é publicado com o que deu certo. No fim o notebook acusa o erro, então chega o e-mail.
- É o único notebook que precisa ser agendado.

### Piloto automotivo
- **Só coleta e classificação:** o envio das notícias automotivas pro Teams saiu (fila, dedup pré-envio, card e resumo diário). Elas aparecem só no portal.
- **Volume de keywords do LinkedIn fora:** a leitura do volume `source=keywords` saiu do notebook automotivo. São posts sobre siderúrgicas, que estavam sendo classificados com o prompt automotivo.
- **Feed no Export:** nova célula gera `feed_automotivo.json`, com setor Automotivo e fonte Automotive Business ou AutoData. O arquivo entra no commit do GitHub. Se a célula falhar, o resto do Export segue normalmente.
- **Correções no `Scraper_e_Classificacao_Noticias_Automotivas.ipynb`:**
  - **ID estável:** o ID agora é gerado por md5. O `hash()` do Python muda a cada execução e duplicava notícias no MERGE.
  - **`TRUNCATE` removido:** ele reclassificava todo o histórico a cada rodada.

### Site
- **Home:** 5 indicadores fixos com mini gráfico (PIB total, PIB Construção, PIB Indústria, INCC-M e Produção de veículos).
- **Cards:** mostram o último realizado e a projeção do próximo período.
- **Filtros:** setor Automotivo para as notícias e para os indicadores.

## Passo a passo

**Agora é um notebook só: `notebooks/00_atualizar_portal`.** Ele roda Infomet, automotivo e 4i, depois o Export, e no fim faz **um commit único** no GitHub, só com os arquivos que mudaram.

1. **Atualizar o repositório.** Substituir o conteúdo do repositório por esta pasta e dar **Pull** no Git folder do Databricks.
2. **Guardar o token do GitHub no Databricks.** A Lari tem o token temporário.
   - Abrir `notebooks/utilitarios/configurar_token_github` e rodar a primeira célula.
   - Colar o token no campo **token_github** que aparece no topo e rodar a segunda célula.
   - Ela guarda o token no secret `portal-im` / `github-token`, testa o acesso ao repositório e mostra qual `GITHUB_DATA_PATH` usar.
   - **Nunca escrever o token no código:** os notebooks ficam no GitHub, o token ficaria exposto e o GitHub costuma revogá-lo automaticamente.
   - Se der erro de permissão ao criar o scope, pedir ao admin do workspace.
3. **Preencher o `00_atualizar_portal`.** O `GITHUB_REPO` já está como `InteligenciaMercadoAM/info-portal`. Na primeira célula:
   - conferir o `GITHUB_DATA_PATH` com o que o passo 2 mostrou;
   - `PIPELINE_DIR` com a pasta do workspace onde está o pipeline do Infomet (`01_stage_ingestion`, `02_raw`, `03_silver`, `04_gold` e `Job - Classificação Relevância Notícias`).

   No Export, preencher o `PORTAL_URL`.
4. **Login da 4i.** Rodar `notebooks/token_4i` na mão e logar com o **usuário do Mateus**.
5. **Primeiro teste, sem publicar.** Rodar o `00_atualizar_portal` com `FAZER_COMMIT = False` e conferir a saída de cada etapa. Dá pra testar uma fonte por vez com o `RODAR` (ex.: `"Infomet": False`).
6. **Rodar publicando.** Voltar `FAZER_COMMIT = True` e rodar de novo. No fim aparece "Commit xxxxxxx" com a lista de arquivos. O site atualiza em cerca de 1 minuto.
7. **Agendar.** No próprio `00_atualizar_portal`, clicar em **Schedule** (canto superior direito), escolher diário, dias úteis, 6h30, fuso de São Paulo, e ativar o e-mail em caso de falha. O agendamento roda como quem criou, ou seja, a Isabela.
8. **Desligar os agendamentos antigos.** Pausar os jobs que hoje rodam o pipeline do Infomet e o scraper automotivo, senão tudo roda duas vezes. Isso inclui o envio automotivo pro Teams, que deixou de existir.

## Pontos de atenção

- **O agendamento precisa rodar como a Isabela**, porque o token da 4i fica salvo na pasta pessoal de quem fez o login.
- **A cada ~30 dias o login da 4i vence.** Quando isso acontecer, a etapa de indicadores falha. As notícias são publicadas mesmo assim, e no fim o notebook acusa erro, então chega o e-mail. Aí é só rodar o `token_4i` na mão e logar com o usuário do Mateus.
- **LinkedIn e chatbot fora do pipeline:** o scraper do LinkedIn (Selenium) e a atualização do índice vetorial do chatbot (`Sync_vector`) não estão no `00_atualizar_portal`. Se eles têm agendamento próprio, continuam como estão. O silver junta o raw do LinkedIn que já existir.
- **Classificação do Infomet:** o pipeline usa o `Job - Classificação Relevância Notícias` (produção). O `Job_Classificacao_Noticias_v2` está com `MODO_TESTE = True` e grava numa tabela de teste. Quando ele for aprovado, é só trocar o caminho no `FONTES` do `00_atualizar_portal`.
- **Git folder:** como o commit dos dados vai direto pela API, o Git folder fica "atrás" do GitHub. Antes de editar código, dar **Pull**. Se reclamar de mudanças locais em `public/data/`, pode descartar, porque o conteúdo é o mesmo que já está no GitHub. Não commitar `public/data/` à mão.
- **Teams:** se ligar o `POST_TO_TEAMS` no Export, a mensagem sai cerca de 1 minuto antes do site atualizar. Na prática não faz diferença.
- **Selic e Taxa de Participação:** variação percentual de uma taxa é pouco intuitiva (a Selic indo de 15% para 14,75% aparece como -1,7%). Vamos avaliar depois da primeira rodada.
- **Segredos expostos:** o webhook do Teams saiu do notebook automotivo, mas ele circulou em versões antigas e vale gerar outro no Power Automate. Há também um token do Databricks em texto no pipeline do chatbot (`secrets_dtb/...`); melhor mover para `dbutils.secrets` e gerar um novo.

## Utilitários (`notebooks/utilitarios/`)
- `listar_indicadores_4i`: busca indicadores no catálogo completo da 4i por palavra-chave.
- `diagnostico_projecoes_4i`: lista as séries de um indicador, até quando vai o realizado e se tem projeção.
- `configurar_token_github`: guarda (ou troca) o token do GitHub no secret do Databricks.
