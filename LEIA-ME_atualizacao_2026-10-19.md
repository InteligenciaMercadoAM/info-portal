# Atualização do Portal IM (19/10/2026)

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
- **Notebook novo `00_atualizar_portal`:** roda tudo em ordem e faz o commit no GitHub. É o único que precisa ser agendado.

### Piloto automotivo
- **Feed no Export:** nova célula gera `feed_automotivo.json`, com setor Automotivo e fonte Automotive Business ou AutoData. O arquivo entra no commit do GitHub. Se a célula falhar, o resto do Export segue normalmente.
- **Correções no `Scraper_e_Classificacao_Noticias_Automotivas.ipynb`:**
  - **ID estável:** o ID agora é gerado por md5. O `hash()` do Python muda a cada execução e duplicava notícias no MERGE.
  - **`TRUNCATE` desligado:** ele reclassificava todo o histórico a cada rodada.

### Site
- **Home:** 5 indicadores fixos com mini gráfico (PIB total, PIB Construção, PIB Indústria, INCC-M e Produção de veículos).
- **Cards:** mostram o último realizado e a projeção do próximo período.
- **Filtros:** setor Automotivo para as notícias e para os indicadores.

## Passo a passo

**Agora é um notebook só: `notebooks/00_atualizar_portal`.** Ele roda os indicadores, depois o Export, e no fim faz **um commit único** no GitHub, só com os arquivos que mudaram. Não precisa mais montar job com várias tarefas.

1. **Atualizar o repositório.** Substituir o conteúdo do repositório por esta pasta e dar **Pull** no Git folder do Databricks.
2. **Criar o secret do GitHub.**
   - No GitHub: Settings > Developer settings > Fine-grained token, só para este repositório, com permissão **Contents: Read and write**.
   - No terminal: `databricks secrets create-scope portal-im` e depois `databricks secrets put-secret portal-im github-token`.
   - Se não tiver permissão para criar o scope, pedir ao admin do workspace.
3. **Preencher o `00_atualizar_portal`.** Na primeira célula, colocar o `GITHUB_REPO` real (como aparece na URL do GitHub). No Export, preencher o `PORTAL_URL`.
4. **Login da 4i.** Rodar `notebooks/token_4i` na mão e logar com o **usuário do Mateus**.
5. **Primeiro teste, sem publicar.** Rodar o `00_atualizar_portal` com `FAZER_COMMIT = False` e conferir a saída de cada etapa.
6. **Rodar publicando.** Voltar `FAZER_COMMIT = True` e rodar de novo. No fim aparece "Commit xxxxxxx" com a lista de arquivos. O site atualiza em cerca de 1 minuto.
7. **Agendar.** No próprio `00_atualizar_portal`, clicar em **Schedule** (canto superior direito), escolher diário, dias úteis, 6h30, fuso de São Paulo, e ativar o e-mail em caso de falha. O agendamento roda como quem criou, ou seja, a Isabela.

## Pontos de atenção

- **O agendamento precisa rodar como a Isabela**, porque o token da 4i fica salvo na pasta pessoal de quem fez o login.
- **A cada ~30 dias o login da 4i vence.** Quando isso acontecer, a etapa de indicadores falha. As notícias são publicadas mesmo assim, e no fim o notebook acusa erro, então chega o e-mail. Aí é só rodar o `token_4i` na mão e logar com o usuário do Mateus.
- **O scraper automotivo não entra no `00_atualizar_portal`**, porque ele tem cadência própria (1 envio por hora no Teams, até 6 por dia). O agendamento dele deve apontar para a versão corrigida, que está em `notebooks/`.
- **Git folder:** como o commit dos dados vai direto pela API, o Git folder fica "atrás" do GitHub. Antes de editar código, dar **Pull**. Se reclamar de mudanças locais em `public/data/`, pode descartar, porque o conteúdo é o mesmo que já está no GitHub. Não commitar `public/data/` à mão.
- **Teams:** se ligar o `POST_TO_TEAMS` no Export, a mensagem sai cerca de 1 minuto antes do site atualizar. Na prática não faz diferença.
- **Selic e Taxa de Participação:** variação percentual de uma taxa é pouco intuitiva (a Selic indo de 15% para 14,75% aparece como -1,7%). Vamos avaliar depois da primeira rodada.
- **Segredos expostos:** o webhook do Teams está escrito no notebook do scraper automotivo, e há um token do Databricks em texto no pipeline do chatbot (`secrets_dtb/...`). Vale mover os dois para `dbutils.secrets` e gerar credenciais novas.

## Utilitários (`notebooks/utilitarios/`)
- `listar_indicadores_4i`: busca indicadores no catálogo completo da 4i por palavra-chave.
- `diagnostico_projecoes_4i`: lista as séries de um indicador, até quando vai o realizado e se tem projeção.
