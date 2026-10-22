# Portal IM: pacote final (22/10/2026)

Isa, este pacote substitui todos os zips anteriores. É o código do repositório `InteligenciaMercadoAM/info-portal` já com a lista final de indicadores.

## Por que sumiram as projeções no dia 21/10

O pipeline funcionou. Às 16h06 o `00_atualizar_portal` publicou um `indicadores.json` com 42 indicadores e as projeções. Às 16h44 entrou o commit "Integra versao atualizada do portal", que trouxe de volta os JSONs antigos de um zip, gerados em 16/10, ainda sem projeção e com os números em nível. **O problema foi o arquivo de dados antigo por cima do novo, não o código.**

Por isso este pacote **não traz nenhum JSON em `public/data/`**. Esses arquivos só devem ser gerados pelo pipeline. Não suba `public/data/*.json` à mão.

## O que mudou nesta versão

### Lista de indicadores: um lugar só
- Novo notebook **`notebooks/indicadores_portal`**. Ele é a lista oficial e o único lugar onde se mexe.
  - Tanto o `novoind_4_Intelligence...` quanto o `Export_Portal_Data` leem a lista com `%run`.
  - Antes a lista estava repetida em três lugares, e era fácil um ficar diferente do outro.
- A lista segue a planilha `docs/indicadores_4i_correspondencia.xlsx`, aba **objetivo**:
  - entram só as linhas com **x em Mantém**: são **34 indicadores**;
  - o nome exibido é a coluna **name**.
- O `indicadores_nomes` do `public/config.json` foi atualizado com os mesmos nomes. Se um dia ficar diferente, o Export avisa.

### Nível ou variação, conforme a planilha (coluna Unidade)
- **`#`: série em nível**, ou seja, o número do indicador: produção e vendas de veículos, ABRAINC, Secovi, cimento, Dólar, Ibovespa e Selic.
  - Usa a série da 4i terminada em `L`, de preferência a original (sem ajuste sazonal).
- **`var`: variação contra o período anterior** (% m/m ou % t/t): PIB, produção industrial, confiança FGV, IPCA, INCC-M e IGP-M.
  - Usa a série terminada em `M`, de preferência a dessazonalizada.
- **Projeção sempre que a 4i tiver.** Entre as séries do tipo certo, o notebook escolhe a que tem projeção.
  - Se só a série do outro tipo tiver projeção, ele converte: calcula a variação a partir do nível, ou encadeia o nível a partir da variação.
  - Isso aparece no log.
- **Dólar sem projeção**, como pede a planilha ("Tira a projeção").
- Ao final da coleta, o notebook confere se cada indicador saiu do jeito que a planilha pede e mostra um ⚠️ se algum não bater.
- Se tiver sobrado um stage de uma rodada antiga, com a regra de "tudo variação", o notebook apaga e começa do zero.

### Caminho dos dados no GitHub
- **`GITHUB_DATA_PATH = "public/data"`** no `00_atualizar_portal` e no Export. Desde 21/10 o repositório não tem mais a subpasta `info-portal/`.
- Se você já tinha ajustado isso direto no Databricks, agora o código do repositório também está certo, e o Pull não desfaz o ajuste.

## Passo a passo

1. **Atualizar o repositório.**
   - Copie o **conteúdo** da pasta `info-portal/` deste zip para a **raiz** do repositório, por cima do que existe.
   - Apague do repositório `LEIA-ME_atualizacao_2026-10-19.md` e `docs/indicadores_4i_correspondencia_6.xlsx`, que foram substituídos.
   - **Não mexa em `public/data/`.**
2. **Pull** no Git folder do Databricks.
3. **Login da 4i**, se estiver vencido: rode `notebooks/token_4i` e logue com o usuário do Mateus.
4. **Rodar só os indicadores e o Export, sem publicar:**
   - No `00_atualizar_portal`, use `RODAR = {"Infomet": False, "Automotivo": False, "4intelligence": True}` e `FAZER_COMMIT = False`.
   - Confira duas saídas:
     - **No `novoind`:** a tabela de resumo e as linhas "Com projeção" e "Sem projeção". Não pode aparecer nenhum ⚠️ de nível ou variação.
     - **No Export:** a lista final, com unidade, último valor e número de projeções de cada indicador.
   - Manda um print das duas pra Lari.
5. **Publicar:** volte `FAZER_COMMIT = True` e `RODAR` com tudo `True`, depois rode de novo. O site atualiza em cerca de 1 minuto.

## Pontos de atenção

- **Indicadores sem projeção na 4i.** Na rodada de 21/10 vieram sem projeção:
  - linha branca, Secovi, ABRAINC, Ibovespa, Selic e ICST.

  Agora o notebook procura projeção em todas as séries do indicador, então alguns podem passar a ter. Os que continuarem sem projeção não têm projeção na 4i. Isso é conversa com a 4i, não com o código.
- **Primeiro ponto da projeção estranho.** Em 21/10, Dólar, ICI e ICC começavam a projeção com um salto: +10,9%, +8,8% e +6,6% num mês. Isso parece projeção desatualizada na 4i. Vale perguntar ao Mateus.
- **Unidade do cimento.** Ficou sem unidade porque não sei se a série da 4i vem em toneladas, mil toneladas ou milhões de toneladas. Veja o valor no print do Export e preencha `"unidade"` no `indicadores_portal`.
- **Unidades das séries em nível.** Pus "unidades" para veículos, ABRAINC e Secovi, "pontos" para o Ibovespa, "R$/US$" para o Dólar e "% a.a." para a Selic. Confira na primeira rodada.
- **Códigos que diferem da planilha:**
  - **Linha branca:** usa BRIND0368. O BRIND0251 da planilha é "Produção de Eletrodomésticos" na 4i.
  - **Produção total da Anfavea:** usa BRIND0055. A BRIND0446 foi descontinuada em dez/20.
  - **ABRAINC "todos" e Secovi vendas:** usam BRRTL0249, BRRTL0253 e BRRLE0007, que já tinham sido localizados.
- **ANIP (pneus)** está marcado na planilha, mas a série ainda não foi encontrada na 4i, então fica fora.
- **Repositório público.** O `info-portal` é público no GitHub: qualquer pessoa vê o código e o histórico. **Nunca coloque token, senha ou webhook em arquivo do repositório.** O token do GitHub fica só no secret do Databricks.
- As demais orientações continuam valendo: agendamento como Isabela, login da 4i a cada ~30 dias, `PIPELINE_DIR`, desligar os jobs antigos e não commitar `public/data/` à mão.

## Fora do repositório (`fora_do_repo/`)
- `Job_Classificacao_Noticias_v2.py`: versão corrigida, sem o `NameError: SETORES`. Ela vai na pasta do pipeline do Infomet no workspace, não no repositório. Continua com `MODO_TESTE = True` e não afeta o portal.

## Utilitários (`notebooks/utilitarios/`)
- `listar_indicadores_4i`: busca indicadores no catálogo da 4i por palavra-chave. Use pra achar o ANIP.
- `diagnostico_projecoes_4i`: lista as séries de um indicador e mostra se cada uma tem projeção.
- `configurar_token_github`: guarda ou troca o token do GitHub no secret do Databricks.
