# Unidades + variação % nos indicadores em nível — 17/11/2026

> Este README é só do pacote: NÃO subir por cima do README.md da raiz do repositório.

O que muda no portal (aba Indicadores):
1. Cimento (SNIC) passa a mostrar a unidade: **milhões de t** (a série da 4i vem ~5,5 por mês).
2. Indicadores em nível (Anfavea, ABRAINC, Secovi, SNIC, Dólar, Ibovespa) ganham uma **2ª linha laranja com a variação % m/m**,
   num painel com eixo próprio logo abaixo da linha do nível (mesmo eixo de tempo). Todos os gráficos ficam com a mesma altura. Mês fora da curva (ex.: ABRAINC jul/25, +3.387%) fica preso na borda com um ponto vazado;
   o valor real aparece no tooltip. O card também mostra a variação do último realizado (ex.: "+2,7% m/m").
3. Cores por tipo de medida: **nível = grafite**, **variação % = laranja** (azul fica de fora: é a cor da concorrência). Indicadores de variação (IPCA, PIB, PIM, FGV...) ficam com a linha laranja.
   Selic fica sem a linha de variação (variação % de uma taxa não diz nada).
4. Correção de bug: Bens de Capital, Produção Industrial Total/Transformação e Anfavea Vendas Totais apareciam com frequência "M",
   unidade "%" e períodos "01/07/26". Passam a sair como "Mensal", "% m/m" e "jul/26".

## Arquivos do pacote (só código, NENHUM JSON de public/data)
- public/app.js, public/styles.css, public/config.json — base: o que está no GitHub + o pacote de analytics (GoatCounter) de 17/11.
  ATENÇÃO, ordem dos pacotes: este pacote JÁ CONTÉM o monitoramento de acessos. Pode subir só este (não precisa do de analytics).
  Se for subir os dois, suba o de analytics PRIMEIRO e este DEPOIS. Subir o de analytics depois apaga a variação e as cores novas.
  O passo a passo do GoatCounter (criar conta, preencher analytics_goatcounter no config) continua no README do pacote de analytics.
- public/goatcounter.js — do pacote de analytics (inofensivo se não for usado)
- notebooks/Export_Portal_Data (3).py — correção da frequência travada (M -> Mensal)
- notebooks/indicadores_portal.py — unidade do cimento

## Passo a passo (Isabela/Vanessa)
1. No GitHub (InteligenciaMercadoAM/info-portal), subir os 4 arquivos de public/ deste pacote por cima dos atuais.
   NÃO subir nada de public/data.
   Se alguém mexeu no config.json depois de 17/11: em vez de substituir, só acrescentar no bloco "portal" (com vírgula antes):
       "variacao_grafico": "mom", "variacao_layout": "painel"
2. Subir os 2 notebooks em notebooks/ (ou atualizar o Git folder no Databricks com pull, se subiram pelo GitHub).
3. Rodar o 00_atualizar_portal normalmente (ou esperar a rodada agendada). A unidade do cimento e a correção do "M"
   só aparecem depois dessa rodada. A linha de variação aparece assim que o Netlify publicar o app.js (1–2 min).
4. Conferir em https://imview.netlify.app → Indicadores: SNIC com "milhões de t" e a linha laranja da variação no painel de baixo;
   Bens de Capital com "Mensal" e períodos tipo "out/26".

## Ajustes pelo config.json (sem mexer no código)
- "variacao_grafico": "mom"  → variação contra o mês anterior (padrão)
- "variacao_grafico": "yoy"  → variação contra o mesmo mês do ano anterior (a/a)
- "variacao_grafico": ""     → sem a linha de variação
- "variacao_layout": "painel"       → variação num painel logo abaixo do nível (padrão; preview_indicadores.png)
- "variacao_layout": "eixo_direito" → as duas linhas juntas, variação no eixo da direita
