# Portal IM: resumo semanal com IA + notícias completas (06/11/2026)

Isa, este pacote traz quatro blocos de mudanças. Ele leva só os arquivos alterados e **nenhum JSON de `public/data/`**.

## 1. Notícias completas no portal

**Por que estava cortado:** a tabela `tb_noticias_classificadas` guarda só o 1º pedaço (chunk) de cada notícia, que tem uns 1.200 caracteres.
- Ele vem da gold, que é picotada pro chatbot (`CHUNK_SIZE = 1200`).
- O texto inteiro, o título de verdade e a fonte original (Valor, Estadão etc.) estão na silver, `tb_silver_news_chatbot_v1`.

**O que mudou:**
- O **Export** junta a silver (por `id` + `source_type`). No `feed.json`, cada notícia Infomet ganha:
  - `titulo`: o título real, no lugar da 1ª frase;
  - `fonte_original`;
  - `texto_completo: true`.
- O texto inteiro vai para **`public/data/textos/AAAA-MM-DD.json`**, um arquivo por dia de publicação.
  - O portal só baixa esse arquivo quando alguém abre a notícia, e por isso o `feed.json` continua leve.
  - Os dias que saem da janela de 90 dias são apagados, aqui e no GitHub.
- O **00_atualizar_portal** agora também publica a pasta `textos/`, no mesmo commit único.
- Pra desligar: `TEXTO_COMPLETO = False` no Export.

**Automotivo (Automotive Business e AutoData):**
- O `Scraper_Automotivas_Economico` agora pede também o campo `content` da API do WordPress e grava o texto completo numa coluna nova, **`conteudo`**, na base bruta `noticias_automotivebusiness`.
  - A coluna é criada sozinha (`ALTER TABLE`) na 1ª rodada.
  - A tabela classificada e a classificação não mudam.
- O Export junta a base bruta pela chave `id` + `fonte_site` e põe o texto nos mesmos arquivos `textos/`.
- **Notícias antigas:** só ganham o texto quando o scraper passa por elas de novo. O dia a dia pega os 200 posts mais recentes de cada fonte. Pra preencher as anteriores, rode o scraper **uma vez** com `PAGINAS_COLETA = 10` e depois volte para 2.
- Quando a AutoData cai no fallback de HTML, essas notícias seguem sem o texto completo.

## 2. Resumo semanal com IA (Llama)

Novo notebook **`notebooks/Resumo_Semanal_IA`**. Ele usa o `databricks-meta-llama-3-3-70b-instruct` via `ai_query`, o mesmo modelo e o mesmo jeito que o job de classificação já usa. Não precisa de token.

1. Junta as notícias relevantes da semana:
   - Infomet com score >= 5;
   - automotivo "Alta";
   - notícias e alertas manuais.

   Entram no máximo 60, as de maior score.
2. Faz uma frase por notícia. Fica guardada em `tb_portal_resumo_noticia_ia`, então nenhuma notícia é resumida duas vezes.
3. Gera de 3 a 5 destaques. Cada um tem uma manchete informativa e 2 ou 3 frases, com os números e o que isso significa para aços longos, e aponta as notícias que o sustentam. Se vier curto demais, tenta de novo (até 3 vezes).
   - O notebook descarta id inventado e destaque sem notícia.
   - Se a resposta vier quebrada, tenta de novo.
4. Grava em `tb_portal_resumo_semanal_ia`. O Export lê o mais recente e gera o `resumo.json`.

**Já rodou a versão antiga do resumo?** Rode o `Resumo_Semanal_IA` de novo com `forcar = sim`. Ele gera a mesma semana com o prompt novo, e o Export passa a usar essa versão.

**Semana:** `MODO = "semana_fechada"` cobre segunda a domingo da semana anterior. O resumo é gerado na 1ª rodada da segunda e, nos outros dias, o notebook sai em segundos. A outra opção, `"ultimos_7_dias"`, gera de novo todo dia.

**Resumo manual da equipe:** continua funcionando. Ele só aparece no lugar do da IA se for mais novo que a semana da IA.

**No portal (home, card "Resumo semanal"):** cada destaque mostra o título e uma frase. Clicando, ele abre as notícias, cada uma com uma frase. Clicando na notícia, abre o detalhe com o texto completo.

## 3. Ajustes de indicadores e páginas

- **Calendário (próximas divulgações):**
  - Em 11/11 aparecia "Produção Industrial Total", mas a divulgação desse dia é a PIM **regional**. A 4i lista ela com o mesmo código do indicador nacional (BRIND0001).
  - O `novoind` agora só guarda divulgações de série nacional: no código da série, a região é `000`.
  - Ele imprime a linha "Ignoradas N divulgações regionais", pra dar pra conferir.
- **Indicadores mensais que vinham trimestrais** (Bens de Capital, PIM Transformação, Anfavea Vendas Totais):
  - A regra preferia a série com projeção, e na 4i, nesses casos, ela só existia na trimestral.
  - No `indicadores_portal`, esses indicadores e a PIM Total ganharam `"frequencia": "M"`, que trava a série mensal. Os demais não mudaram.
  - Se a 4i não tiver projeção na mensal, o indicador aparece **sem projeção**.
  - O `novoind` refaz sozinho o stage dos indicadores que estavam na frequência errada.
- **Selic:** trocada da Efetiva (BRINR0012) para a **Meta (BRINR0010)**.
  - A Efetiva vinha na série acumulada em 12 meses: em out/26 dava 14,38%, atrasada em relação à taxa vigente.
  - A Meta é a taxa do Copom, o "15% a.a." que todo mundo cita, e a planilha já indicava ela como alternativa.
  - O `config.json` (`indicadores_nomes`) foi atualizado junto.
- **Mercado Externo oculto:**
  - Fica no `public/config.json`, em `portal.paginas_ocultas: ["external"]`.
  - Pra voltar a mostrar, é só tirar `"external"` da lista. O código da página continua lá.

## 4. Pipeline do portal no usuário da Vanessa

- O `00_atualizar_portal` e o `Export` agora apontam para `/Workspace/Users/vanessa.n.silva@arcelormittal.com.br/`:
  - `info-portal/notebooks` (a pasta do Git);
  - `portal_dados` (onde saem os JSONs);
  - `info-portal/public/config.json`;
  - `chatbot_v3 (final)/pipeline` (só é usado se o Infomet for ligado no `RODAR`).
- **As tabelas continuam no schema `isabela_pereira`**, porque a Vanessa ainda não tem schema próprio.
- **O que precisa existir antes da 1ª rodada como Vanessa:**
  1. **Git folder** do repositório clonado em `/Workspace/Users/vanessa.n.silva@arcelormittal.com.br/info-portal`.
  2. **Permissões da Vanessa:**
     - leitura e escrita no schema `lcb_qas_sandbox2_comercial.isabela_pereira`;
     - acesso às tabelas `hive_metastore.default.noticias_automotivebusiness*`;
     - leitura no secret scope `portal-im` (token do GitHub).
  3. **Login da 4i feito por ela:** rodar o `token_4i` logada como Vanessa, com o usuário do Mateus. O token fica na pasta pessoal de quem roda.
  4. **Agendamento** do `00_atualizar_portal` criado no usuário dela.
     - Desligue o agendamento antigo da Isa, senão os dois publicam.
     - Os jobs do Infomet ("1- Pipeline Medallion" e "2- Classifica + Envia Teams") não mudam.
- Na 1ª rodada a pasta `portal_dados` está vazia, e isso é normal. O Export busca o `indicadores.json` anterior no GitHub, então as datas de "dado novo" não se perdem.

## Passo a passo

1. **Atualizar o repositório.** Copie o conteúdo da pasta `info-portal/` deste zip para a raiz do repositório, por cima do que existe. **Não mexa em `public/data/`.**
2. **Pull** no Git folder do Databricks.
   - Rode o `Scraper_Automotivas_Economico` uma vez na mão com `PAGINAS_COLETA = 10` e confira a linha "Com texto completo nesta coleta".
   - Depois volte para 2.
3. **Testar o resumo:** abra `notebooks/Resumo_Semanal_IA`, coloque o widget `forcar = sim` e rode.
   - No fim ele imprime os destaques e as notícias de cada um. Manda um print pra Lari.
   - Se der erro de `ai_query` não encontrado, use o mesmo cluster/runtime do job de classificação (DBR 15.4 LTS ou mais novo).
4. **Rodar sem publicar:** no `00_atualizar_portal`, use `FAZER_COMMIT = False` e rode. No Export, confira duas linhas:
   - `✅ textos/: N notícias com texto completo (N automotivas) em ~90 arquivos · N com título da fonte`;
   - `resumo.json: Semana de ...`.
5. **Publicar:** volte `FAZER_COMMIT = True` e rode de novo.
   - A 1ª publicação sobe uns 90 arquivos de texto. Depois disso, só os do dia mudam.
   - O `RODAR` ganhou `"Resumo semanal IA": True`.

## Pontos de atenção

- **Direitos do conteúdo.** Vale também para as matérias da Automotive Business e da AutoData. O Infomet é assinatura paga, e o repositório e o site são públicos. Com o texto completo, as matérias inteiras ficam abertas no GitHub e no Netlify. Vale confirmar com quem cuida do contrato do Infomet antes de publicar. Se não puder, use `TEXTO_COMPLETO = False`, ou deixe o site com login.
- **Custo da IA.** Por semana são umas 60 chamadas curtas e 1 maior: centavos.
- **Imprecisão.** O card avisa que o texto é gerado por IA. Cada destaque leva às notícias de origem, então dá pra conferir.
