// Portal de Inteligência de Mercado: site estático que lê config.json e os JSONs em data/ (gerados pelo notebook no Databricks).
//   config.json      temas, setores, empresas, regiões e indicadores fixos (editável pela equipe, sem mexer no código)
//   data/feed.json        notícias do bot (obrigatório)
//   data/manual.json      notícias e alertas incluídos pela equipe (opcional)
//   data/indicadores.json indicadores 4intelligence: realizado + projeção (opcional)
//   data/resumo.json      último resumo semanal, elaborado pela equipe (opcional)
//   data/meta.json        data/hora da última exportação (opcional)

let C = {};            // config.json
const PAGE_SIZE = 50;
const FLAT_BAND = 0.02; // termômetro: variação relativa abaixo da qual o sinal é "estável"
const NO_COMPANY = new Set(['', 'nenhuma', 'nenhum', 'outro', 'outros', 'na', 'none', 'nan', 'null']);
const REL_LABELS = { RELEVANTE: 'Relevante', NAO_RELEVANTE: 'Não relevante', IRRELEVANTE: 'Irrelevante' };
const MONTHS = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'];
const DAY = 86400000;
const TITLES = {
  home: ['Início', 'O que mudou no mercado, em uma tela.'],
  feed: ['Notícias', 'Todas as publicações monitoradas, por data.'],
  external: ['Mercado externo', 'Notícias sobre aço e demanda fora do Brasil.'],
  indicators: ['Indicadores', 'Realizado e projeções 4intelligence. Linha cheia = realizado, tracejada = projeção.'],
  competitors: ['Concorrentes', 'Volume e temas das notícias de cada concorrente.'],
  detail: ['Detalhe da notícia', '']
};

const state = { rows: [], alerts: [], indicators: [], indicatorsSample: false, resumo: null, meta: null, page: 'home', selected: null, limit: PAGE_SIZE };
const $ = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>'"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[c]));
const norm = s => String(s ?? '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
const key = s => norm(s).replace(/[^a-z0-9]/g, '');
const titleCase = s => String(s).trim().toLowerCase().replace(/(^|\s)\S/g, c => c.toUpperCase());
const nf = (v, d = 1) => (Math.abs(+v) < 0.5 * 10 ** -d ? 0 : Number(v)).toLocaleString('pt-BR', { minimumFractionDigits: d, maximumFractionDigits: d });
const reEsc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// Datas chegam como 'YYYY-MM-DD' e 'YYYY-MM-DD HH:MM:SS'. Não usamos new Date(string) para exibir,
// pois ele trata 'YYYY-MM-DD' como UTC e mostraria o dia anterior no Brasil.
const dparts = v => String(v ?? '').match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?/);
function fmtDate(v, withTime = false) {
  const m = dparts(v); if (!m) return '—';
  return `${m[3]}/${m[2]}/${m[1]}` + (withTime && m[4] ? ` ${m[4]}:${m[5]}` : '');
}
const shortDate = v => { const m = dparts(v); return m ? `${m[3]}/${m[2]}` : ''; };
const toTs = v => { const m = dparts(v); return m ? Date.UTC(+m[1], +m[2] - 1, +m[3]) : 0; };
const todayTs = () => { const d = new Date(); return Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()); };

// ---------- regras de texto (região e mercado externo), montadas a partir do config ----------
// Região: nomes de estados com maiúscula e acento exatos ("Pará", não "para"); siglas só entre
// parênteses, depois de barra/hífen/vírgula ("Contagem (MG)", "Serra/ES"), para não confundir "SE" com "se".
let REGION_RULES = [], EXT_RE = null, BR_RE = null;
function buildRules() {
  REGION_RULES = Object.entries(C.regioes_por_estado || {}).map(([reg, terms]) => {
    const names = terms.filter(t => t.length > 2).map(reEsc), ufs = terms.filter(t => t.length === 2).map(reEsc);
    const parts = [];
    if (names.length) parts.push(`(?<![\\p{L}])(?:${names.join('|')})(?![\\p{L}])`);
    if (ufs.length) parts.push(`(?:[(/,]|\\s[-–]\\s?|-)\\s?(?:${ufs.join('|')})(?![\\p{L}])`);
    return [reg, new RegExp(parts.join('|'), 'u')];
  });
  const prefixRe = list => list?.length ? new RegExp(`(?<![\\p{L}])(?:${list.map(t => reEsc(norm(t))).join('|')})`, 'u') : null;
  EXT_RE = prefixRe(C.termos_mercado_externo); BR_RE = prefixRe(C.termos_brasil);
}
function inferRegions(text) { return REGION_RULES.filter(([, re]) => re.test(text)).map(([r]) => r); }

// ---------- notícias ----------
function cleanText(raw) {
  return String(raw ?? '').replace(/\\n/g, '\n').replace(/\\"/g, '"')
    .replace(/^\s*\[Data de publica[cç][aã]o:[^\]]*\]\s*\n*/i, '').replace(/\s*(?:…|\.\.\.)\s*mais\s*$/i, '')
    .replace(/[ \t]+/g, ' ').replace(/\n{3,}/g, '\n\n').trim();
}
function makeTitle(t) {
  if (!t) return '(sem texto)';
  const first = t.split(/(?<=[.!?])\s+(?=[A-ZÁÉÍÓÚÂÊÔÃÕÇ“"'(])/)[0];
  return first.length <= 130 ? first.replace(/[ .;:]+$/, '') : first.slice(0, 130).replace(/\s+\S*$/, '') + '…';
}
function canonCompany(raw) { const k = key(raw); return NO_COMPANY.has(k) ? 'Sem empresa' : ((C.empresas_alias || {})[k] || titleCase(raw)); }
function canonCategory(raw) {
  const k = norm(raw).trim();
  return (C.temas || {})[k] || (k ? k.replace(/_/g, ' ').replace(/^./, c => c.toUpperCase()) : 'Outros');
}
function canonSector(raw, category) {
  if (raw) { const hit = (C.setores || []).find(s => key(s) === key(raw)); return hit || titleCase(raw); }
  return (C.setor_por_tema || {})[norm(category).trim()] || C.setor_padrao || 'Outros';
}
function canonRegion(raw) {
  const k = key(raw); if (!k) return '';
  const hit = [...(C.regioes_ordem || []), 'Nacional'].find(r => key(r) === k);
  return hit || (['brasil', 'br'].includes(k) ? 'Nacional' : ['global', 'mundo'].includes(k) ? 'Internacional' : titleCase(raw));
}
function canonRelevance(raw) {
  const s = String(raw ?? '').trim();
  return REL_LABELS[s.toUpperCase()] || (s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : 'Não classificada');
}
function prepare(raw, manual = false) {
  const texto = cleanText(raw.text ?? raw.texto), titulo = raw.titulo ? String(raw.titulo).trim() : makeTitle(texto);
  const full = `${titulo}\n${texto}`;
  const regioes = raw.regiao ? String(raw.regiao).split(/[;,]/).map(canonRegion).filter(Boolean) : inferRegions(full);
  const n = norm(full);
  // Mercado externo: campo "mercado" se existir; conteúdo manual só se a região for Internacional;
  // notícias do bot por regra de texto (fala de outro país e não cita Brasil nem estado brasileiro).
  const externo = raw.mercado ? norm(raw.mercado).startsWith('ext')
    : (regioes.includes('Internacional') || (!manual && !!EXT_RE && EXT_RE.test(n) && !(BR_RE && BR_RE.test(n)) && !regioes.length));
  const r = {
    ...raw, id: String(raw.id ?? ''), texto, titulo, manual, regioes, externo,
    fonte: manual ? (raw.fonte || 'Equipe IM') : ((C.fontes || {})[norm(raw.source_type).trim()] || titleCase(raw.source_type || '') || 'Não informada'),
    origem: manual ? 'Equipe IM' : (raw.projeto || 'Bot'),
    empresa: canonCompany(raw.empresa_principal), categoria: canonCategory(raw.categoria_evento ?? raw.tema),
    setor: canonSector(raw.setor, raw.categoria_evento ?? raw.tema),
    rel: manual ? 'Relevante' : canonRelevance(raw.relevancia),
    score: manual ? Number(raw.score_relevancia ?? 100) : (Number(raw.score_relevancia) || 0),
    ts: toTs(raw.data_publicacao)
  };
  r.busca = norm(`${titulo} ${texto} ${r.empresa} ${r.categoria} ${r.fonte} ${r.setor} ${regioes.join(' ')}`);
  return r;
}
function prepareAlert(raw) {
  const r = prepare(raw, true);
  const until = raw.valido_ate ? toTs(raw.valido_ate) : r.ts + ((C.portal?.dias_validade_alerta ?? 7) * DAY);
  return { ...r, alerta: true, ativo: until >= todayTs() };
}

// ---------- indicadores ----------
function prepareIndicator(raw) {
  const serie = (raw.serie || []).filter(p => p && p.valor != null && !isNaN(+p.valor)).map(p => ({ ...p, valor: +p.valor, proj: norm(p.tipo).startsWith('proj') }));
  const real = serie.filter(p => !p.proj), proj = serie.filter(p => p.proj);
  // Projeção exibida = próximo período (as séries são variação contra o período anterior; a de 12 meses à frente diz pouco).
  const last = real[real.length - 1], horizon = proj[0];
  const ind = { ...raw, serie, last, horizon, setor: canonSector(raw.setor, ''), regiao: canonRegion(raw.regiao) || 'Nacional', polaridade: Number(raw.polaridade) === -1 ? -1 : 1 };
  // Sinal = direção da projeção vs último realizado, ajustada pela polaridade (ex.: juros subindo é desfavorável).
  if (last && horizon) {
    const delta = horizon.valor - last.valor;
    const band = raw.limiar_estavel != null ? +raw.limiar_estavel : Math.abs(last.valor) * FLAT_BAND;
    const dir = Math.abs(delta) <= band ? 0 : Math.sign(delta);
    ind.direcao = dir; ind.sinal = dir * ind.polaridade;
  } else { ind.direcao = null; ind.sinal = null; }
  ind.busca = norm(`${raw.nome} ${ind.setor} ${ind.regiao} ${raw.fonte || ''}`);
  return ind;
}
const SIGNAL = { 1: ['up', 'Favorável'], 0: ['flat', 'Estável'], '-1': ['down', 'Desfavorável'] };
const signalTag = s => s == null ? '' : `<span class="signal ${SIGNAL[s][0]}">${SIGNAL[s][1]}</span>`;
const fmtVal = (v, ind) => `${nf(v, ind.casas ?? 1)}${ind.unidade && ind.unidade.startsWith('%') ? '%' : ''}`;
const unitSuffix = ind => ind.unidade && !ind.unidade.startsWith('%') ? ind.unidade : (ind.unidade || '').replace(/^%\s*/, '');

// ---------- filtros ----------
const ADV = [['company', 'empresa'], ['source', 'fonte'], ['category', 'categoria'], ['relevance', 'rel'], ['origin', 'origem']];
const latest = rows => rows.reduce((m, r) => Math.max(m, r.ts), 0);

function filteredNews(base = state.rows) {
  let a = base;
  const max = latest(state.rows), p = $('period').value;
  if (p !== 'all') a = a.filter(r => max - r.ts < +p * DAY);
  const sec = $('sector').value, reg = $('region').value;
  if (sec) a = a.filter(r => r.setor === sec);
  if (reg) a = a.filter(r => r.regioes.includes(reg));
  ADV.forEach(([id, k]) => { const v = $(id).value; if (v) a = a.filter(r => r[k] === v); });
  const min = +$('score').value || 0, q = norm($('search').value).trim();
  a = a.filter(r => r.score >= min);
  if (q) a = a.filter(r => r.busca.includes(q));
  return [...a];
}
function filteredIndicators() {
  let a = state.indicators;
  const sec = $('sector').value, q = norm($('search').value).trim();
  if (sec) a = a.filter(i => i.setor === sec);
  if (q) a = a.filter(i => i.busca.includes(q));
  return a;
}
function setOptions(id, vals, allLabel) {
  const cur = $(id).value;
  $(id).innerHTML = `<option value="">${allLabel}</option>` + vals.map(v => `<option>${esc(v)}</option>`).join('');
  if (vals.includes(cur)) $(id).value = cur;
}
const uniq = arr => [...new Set(arr.filter(Boolean))].sort((x, y) => x.localeCompare(y, 'pt-BR'));
function fillOptions() {
  setOptions('sector', uniq([...state.rows.map(r => r.setor), ...state.indicators.map(i => i.setor)]), 'Todos os setores');
  const present = new Set(state.rows.flatMap(r => r.regioes));
  const order = C.regioes_ordem || [];
  const regions = [...order.filter(r => present.has(r)), ...uniq([...present].filter(r => !order.includes(r)))];
  setOptions('region', regions, 'Todas as regiões');
  $('region').hidden = regions.length === 0;
  ADV.forEach(([id, k]) => setOptions(id, uniq(state.rows.map(r => r[k])), id === 'category' ? 'Todos' : 'Todas'));
  const def = C.portal?.relevancia_padrao;
  if (def && [...$('relevance').options].some(o => o.value === def)) $('relevance').value = def;
}
function resetFilters() {
  ['search', 'sector', 'region', 'company', 'source', 'category', 'origin', 'relevance'].forEach(id => $(id).value = '');
  $('period').value = String(C.portal?.periodo_padrao_dias ?? 7); $('score').value = 0;
}

// ---------- componentes ----------
const byScore = (x, y) => y.score - x.score;
const byImportance = (x, y) => (y.rel === 'Relevante') - (x.rel === 'Relevante') || (y.manual - x.manual) || byScore(x, y) || y.ts - x.ts;
// Região não vira etiqueta no card (decisão de reunião): só aparece no filtro e no detalhe.
function tags(r) {
  return `<div class="tags"><span class="tag sector">${esc(r.setor)}</span><span class="tag">${esc(r.categoria)}</span>${r.empresa !== 'Sem empresa' ? `<span class="tag">${esc(r.empresa)}</span>` : ''}${r.alerta ? '<span class="tag alert">Alerta IM</span>' : r.manual ? '<span class="tag manual">Incluída pela equipe</span>' : r.projeto ? `<span class="tag manual">${esc(r.projeto)}</span>` : `<span class="tag score">score ${r.score}</span>`}</div>`;
}
function newsItem(r, withText = false) {
  return `<article class="news${withText ? ' news-card' : ''}${r.alerta ? ' is-alert' : ''}" tabindex="0" data-id="${esc(r.id)}">
    <div class="meta">${esc(r.fonte)} · ${fmtDate(r.data_publicacao)}</div>
    <h3>${esc(r.titulo)}</h3>${withText ? `<p>${esc(r.texto.slice(0, 280))}${r.texto.length > 280 ? '…' : ''}</p>` : ''}${tags(r)}</article>`;
}
function bars(rows, k, limit = 10) {
  const m = {}; rows.forEach(r => { m[r[k]] = (m[r[k]] || 0) + 1; });
  const a = Object.entries(m).sort((x, y) => y[1] - x[1]).slice(0, limit), mx = a[0]?.[1] || 1;
  return `<div class="bars">${a.map(([n, v]) => `<div class="bar"><span>${esc(n)}</span><i style="width:${Math.max(3, v / mx * 100)}%"></i><b>${v}</b></div>`).join('') || '<p class="muted">Sem dados.</p>'}</div>`;
}
const sampleBadge = () => state.indicatorsSample ? '<span class="sample" title="indicadores.json marcado como exemplo">Dados de exemplo</span>' : '';

function alertsStrip() {
  const act = state.alerts.filter(a => a.ativo).sort((x, y) => y.ts - x.ts);
  if (!act.length) return '';
  return `<section class="alerts" aria-label="Alertas IM"><div class="alerts-head"><span class="alerts-title">Alertas IM</span><span class="alerts-sub">Curadoria da equipe de Inteligência de Mercado</span></div>
    ${act.map(a => `<article class="alert-item" tabindex="0" data-id="${esc(a.id)}"><div class="alert-date">${shortDate(a.data_publicacao)}</div>
      <div><h3>${esc(a.titulo)}</h3>${a.texto ? `<p>${esc(a.texto.slice(0, 220))}${a.texto.length > 220 ? '…' : ''}</p>` : ''}
      <div class="meta">${esc(a.adicionado_por || 'Equipe IM')}${a.setor ? ' · ' + esc(a.setor) : ''}</div></div></article>`).join('')}</section>`;
}
function thermometer(inds) {
  const s = inds.filter(i => i.sinal != null);
  if (!s.length) return '';
  const up = s.filter(i => i.sinal === 1).length, down = s.filter(i => i.sinal === -1).length, flat = s.length - up - down;
  const idx = (up - down) / s.length;
  const [cls, label] = idx > 0.25 ? ['up', 'Aquecido'] : idx < -0.25 ? ['down', 'Desaquecido'] : ['flat', 'Estável'];
  return `<div class="card thermo"><div class="card-title"><h2>Termômetro de mercado</h2>${sampleBadge()}</div>
    <div class="reading" style="color:var(--${cls})">${label}</div>
    <div class="gauge">${[...s].sort((a, b) => b.sinal - a.sinal).map(i => `<i class="${SIGNAL[i.sinal][0]}" data-tip="${esc(i.nome)}: ${SIGNAL[i.sinal][1].toLowerCase()}"></i>`).join('')}</div>
    <div class="legend">${up} favoráveis · ${flat} estáveis · ${down} desfavoráveis</div></div>`;
}
// Período do último realizado ('set/26' ou '2T26') -> meses de atraso em relação a hoje.
function monthsBehind(periodo) {
  const p = String(periodo || '').toLowerCase().trim();
  let y, m, mm;
  if ((mm = p.match(/^([a-zç]{3})\/(\d{2,4})$/)) && MONTHS.includes(mm[1])) { m = MONTHS.indexOf(mm[1]) + 1; y = +mm[2]; }
  else if ((mm = p.match(/^([1-4])t(\d{2,4})$/))) { m = +mm[1] * 3; y = +mm[2]; }
  else return null;
  if (y < 100) y += 2000;
  const now = new Date();
  return (now.getFullYear() - y) * 12 + (now.getMonth() + 1 - m);
}
// Defasado = último realizado mais antigo do que a defasagem normal de divulgação (mensal ~2 meses, trimestral ~1 trimestre).
function isStale(ind) {
  const lag = ind.last ? monthsBehind(ind.last.periodo) : null;
  if (lag == null) return false;
  return lag > (norm(ind.frequencia).startsWith('trim') ? 7 : 4);
}
function kpi(ind) {
  const u = unitSuffix(ind), prev = ind.serie.filter(p => !p.proj).slice(-2)[0];
  const stale = isStale(ind);
  const sub = ind.horizon ? `proj. ${esc(ind.horizon.periodo)}: ${fmtVal(ind.horizon.valor, ind)}`
    : prev && prev !== ind.last ? `ant. ${esc(prev.periodo)}: ${fmtVal(prev.valor, ind)}` : '';
  return `<div class="kpi${stale ? ' stale' : ''}" data-go="indicators" tabindex="0" title="${esc(ind.nome)}"><span class="label">${esc(ind.nome_card || ind.nome)}</span>
    <span class="value">${ind.last ? fmtVal(ind.last.valor, ind) : '—'}${u ? `<small>${esc(u)}</small>` : ''}</span>
    <span class="proj">${ind.last ? `<b>${esc(ind.last.periodo)}</b>` : ''}${ind.last && sub ? ' · ' : ''}${sub}${stale ? ' <span class="stale-tag">desatualizado</span>' : ''}</span>
    <svg class="spark" data-chart="${esc(ind.id)}" data-mode="spark" role="img" aria-label="Série de ${esc(ind.nome)}"></svg></div>`;
}
// Indicadores fixos: sempre os mesmos na home, independentemente dos filtros.
// No config, cada item pode ser só o código ("BRGDP0002") ou um objeto { id, nome, unidade, casas }.
const fixedCfg = () => (C.indicadores_fixos || []).map(f => typeof f === 'string' ? { id: f } : f).filter(f => f && f.id);
function fixedIndicators() {
  let list = fixedCfg().map(f => state.indicators.find(i => key(i.id) === key(f.id))).filter(Boolean);
  if (!list.length) list = state.indicators.filter(i => i.destaque).slice(0, 4);
  return list;
}
// Aplica nome amigável, unidade e casas decimais do config (vale para a home e para a página de indicadores).
function applyFixedOverrides() {
  // 1) Nomes oficiais da planilha de correspondência (config.indicadores_nomes: código -> nome).
  const nomes = Object.fromEntries(Object.entries(C.indicadores_nomes || {}).map(([k, v]) => [key(k), v]));
  if (Object.keys(nomes).length) {
    if (C.indicadores_somente_lista) {
      const fora = state.indicators.filter(i => !nomes[key(i.id)]).map(i => i.id);
      if (fora.length) console.info('Indicadores fora da lista (ocultos):', fora.join(', '));
      state.indicators = state.indicators.filter(i => nomes[key(i.id)]);
    }
    const tem = new Set(state.indicators.map(i => key(i.id)));
    const faltando = Object.keys(C.indicadores_nomes).filter(k => !tem.has(key(k)));
    if (faltando.length) console.warn('Indicadores da lista sem dados em data/indicadores.json:', faltando.join(', '));
    state.indicators.forEach(i => { const n = nomes[key(i.id)]; if (n) { i.nome_original = i.nome; i.nome = n; i.busca += ' ' + norm(n); } });
  }
  // 2) Ajustes dos indicadores fixos da home (nome curto do card, unidade, casas decimais).
  fixedCfg().forEach(f => {
    const ind = state.indicators.find(i => key(i.id) === key(f.id));
    if (!ind) { console.warn(`Indicador fixo ${f.id} não encontrado em data/indicadores.json`); return; }
    if (f.nome) { ind.nome_original ??= ind.nome; ind.nome_card = f.nome; ind.busca += ' ' + norm(f.nome); }
    if (f.unidade != null) ind.unidade = f.unidade;
    if (f.casas != null) ind.casas = f.casas;
  });
}
function agenda(inds) {
  const t = todayTs();
  const next = inds.filter(i => toTs(i.proxima_divulgacao) >= t).sort((a, b) => toTs(a.proxima_divulgacao) - toTs(b.proxima_divulgacao)).slice(0, C.portal?.itens_agenda ?? 5);
  const item = i => { const m = dparts(i.proxima_divulgacao);
    return `<div class="agenda-item"><div class="date-box"><b>${m[3]}</b><span>${MONTHS[+m[2] - 1]}</span></div><div><div class="name">${esc(i.nome)}</div><div class="sub">${esc(i.fonte_original || i.fonte || '')}${i.referencia_proxima ? ' · ref. ' + esc(i.referencia_proxima) : ''}</div></div></div>`; };
  return `<div class="card"><div class="card-title"><h2>Próximas divulgações</h2>${sampleBadge()}</div>${next.map(item).join('') || '<p class="muted">Nenhuma divulgação agendada.</p>'}</div>`;
}
function summaryCard() {
  const r = state.resumo;
  if (!r) return `<div class="card summary"><div class="card-title"><h2>Resumo semanal</h2></div><p>O resumo da semana aparece aqui assim que a equipe publicar.</p></div>`;
  const bullets = Array.isArray(r.destaques) ? r.destaques : String(r.texto || '').split('\n').map(s => s.replace(/^[-•*]\s*/, '').trim()).filter(Boolean);
  return `<div class="card summary"><div class="card-title"><h2>Resumo semanal</h2><span class="muted" style="font-size:12px">${esc(r.semana || r.titulo || '')}</span></div>
    ${bullets.length ? `<ul>${bullets.slice(0, 5).map(d => `<li>${esc(d)}</li>`).join('')}</ul>` : ''}
    ${r.pdf || r.url ? `<a class="btn" href="${esc(r.pdf || r.url)}" target="_blank" rel="noopener">Abrir resumo completo</a>` : ''}
    <p class="muted" style="font-size:11px;margin:8px 0 0">Elaborado pela equipe IM${r.adicionado_por && r.adicionado_por !== 'Equipe IM' ? ' (' + esc(r.adicionado_por) + ')' : ''}${r.data_publicacao ? ' em ' + fmtDate(r.data_publicacao) : ''}.</p></div>`;
}
function competitorSnapshot(a) {
  const rows = [C.empresa_propria, ...(C.concorrentes || [])].filter(Boolean).map(n => [n, a.filter(r => r.empresa === n).length]).sort((x, y) => y[1] - x[1]);
  return `<div class="card"><div class="card-title"><h2>Concorrentes no período</h2><button class="linkbtn" data-go="competitors">Ver todos</button></div>
    ${rows.map(([n, v]) => `<div class="comp-row" data-company="${esc(n)}"><span>${esc(n)}</span><b>${v}</b></div>`).join('')}</div>`;
}
// A mesma notícia costuma chegar por várias fontes (Infomet, LinkedIn, X): agrupa pelo título e mostra uma só.
function dedupeByTitle(list) {
  const seen = new Map();
  list.forEach(r => { const k = key(r.titulo).slice(0, 80); if (seen.has(k)) seen.get(k)[1]++; else seen.set(k, [r, 0]); });
  return [...seen.values()];
}
// Caixas por tema: os temas com mais notícias no período (ou a ordem fixa do config), com as principais de cada um.
function themeBoxes(news, max = C.portal?.temas_na_home ?? 6) {
  const hide = new Set(C.temas_fora_da_home || []), groups = {};
  news.forEach(r => { if (!hide.has(r.categoria)) (groups[r.categoria] ||= []).push(r); });
  const fixed = (C.temas_ordem_home || []).filter(t => groups[t]);
  const rest = Object.keys(groups).filter(t => !fixed.includes(t)).sort((a, b) => groups[b].length - groups[a].length);
  const themes = [...fixed, ...rest].slice(0, max), per = C.portal?.noticias_por_tema ?? 3;
  if (!themes.length) return '<div class="card empty">Nenhuma notícia para os filtros selecionados.</div>';
  return `<div class="themes">${themes.map(t => { const g = groups[t].sort(byImportance), top = dedupeByTitle(g).slice(0, per);
    return `<div class="card theme"><div class="card-title"><h2>${esc(t)}</h2><button class="linkbtn" data-theme="${esc(t)}">${g.length} ${g.length === 1 ? 'notícia' : 'notícias'}</button></div>
      ${top.map(([r, extra]) => `<article class="theme-item${r.alerta ? ' is-alert' : ''}" tabindex="0" data-id="${esc(r.id)}"><h3>${esc(r.titulo)}</h3><div class="meta">${esc(r.fonte)} · ${shortDate(r.data_publicacao)}${r.empresa !== 'Sem empresa' ? ' · ' + esc(r.empresa) : ''}${r.manual ? ' · Equipe IM' : ''}${extra ? ` · +${extra} ${extra === 1 ? 'fonte' : 'fontes'}` : ''}</div></article>`).join('')}</div>`; }).join('')}</div>`;
}

// ---------- páginas ----------
function home() {
  const news = filteredNews(), fixed = fixedIndicators();
  const showThermo = C.portal?.mostrar_termometro && state.indicators.length;
  const kpis = fixed.length ? `<div class="kpis">${fixed.map(kpi).join('')}</div>` : '';
  return `${alertsStrip()}
  ${showThermo ? `<div class="hero">${thermometer(state.indicators)}${kpis}</div>` : kpis}
  <div class="cols">
    <div><div class="section-head"><h2>Principais notícias por tema</h2><button class="linkbtn" data-go="feed">Ver todas as ${news.length} notícias</button></div>${themeBoxes(news)}</div>
    <div class="stack">${state.indicators.length ? agenda(state.indicators) : ''}${summaryCard()}${competitorSnapshot(news)}</div>
  </div>`;
}
function feed() {
  const a = filteredNews().sort((x, y) => y.ts - x.ts || byScore(x, y));
  const shown = a.slice(0, state.limit);
  return `<p class="muted" style="margin:0 0 10px"><b style="color:var(--ink)">${a.length}</b> notícias encontradas</p>
    ${shown.map(r => newsItem(r, true)).join('') || '<div class="card empty">Nenhuma notícia para os filtros selecionados.</div>'}
    ${a.length > shown.length ? `<button class="btn more" id="more">Mostrar mais (${a.length - shown.length})</button>` : ''}`;
}
function external() {
  const news = filteredNews(state.rows.filter(r => r.externo));
  return `<div class="card note">Por enquanto esta página reúne as notícias do bot que falam de outros países sem citar o Brasil. Novas fontes internacionais entram aqui quando forem definidas.</div>
    <div class="section">${themeBoxes(news, 12)}</div>`;
}
function indicatorCard(ind) {
  const u = unitSuffix(ind);
  return `<div class="card ind-card"><div class="head"><div><h3>${esc(ind.nome)}</h3><div class="tags"><span class="tag sector">${esc(ind.setor)}</span>${ind.frequencia ? `<span class="tag">${esc(ind.frequencia)}</span>` : ''}</div></div>${C.portal?.mostrar_termometro ? signalTag(ind.sinal) : ''}</div>
    <div class="nums"><div>Último realizado${ind.last ? ` (${esc(ind.last.periodo)})` : ''}<b>${ind.last ? fmtVal(ind.last.valor, ind) : '—'} <small style="font-size:12px;font-weight:500;color:var(--muted)">${esc(u)}</small></b></div>
      <div>Projeção${ind.horizon ? ` (${esc(ind.horizon.periodo)})` : ''}<b>${ind.horizon ? fmtVal(ind.horizon.valor, ind) : '—'}</b></div></div>
    <svg class="chart" data-chart="${esc(ind.id)}" role="img" aria-label="Gráfico de ${esc(ind.nome)}: realizado e projeção"></svg>
    <div class="chart-legend"><span><i></i>Realizado</span><span><i class="dash"></i>Projeção</span></div>
    <div class="foot"><span>Fonte: ${esc(ind.fonte || '4intelligence')}${ind.fonte_original ? ' / ' + esc(ind.fonte_original) : ''}</span><span>${ind.ultima_divulgacao ? `Última divulgação ${fmtDate(ind.ultima_divulgacao)}` : `Último dado: ${esc(ind.last?.periodo || '—')}`}${ind.proxima_divulgacao ? ` · próxima ${fmtDate(ind.proxima_divulgacao)}` : ''}</span></div></div>`;
}
function indicators() {
  if (!state.indicators.length) return `<div class="card empty">Nenhum indicador publicado ainda. Exporte <code>data/indicadores.json</code> pelo notebook.</div>`;
  const a = filteredIndicators();
  return `${state.indicatorsSample ? `<p style="margin:0 0 10px">${sampleBadge()}</p>` : ''}<div class="ind-grid">${a.map(indicatorCard).join('') || '<div class="card empty">Nenhum indicador para os filtros selecionados.</div>'}</div>`;
}
function competitors() {
  const a = filteredNews();
  const card = n => {
    const x = a.filter(r => r.empresa === n).sort(byScore);
    return `<div class="company" data-company="${esc(n)}" tabindex="0"><h3>${esc(n)}</h3><strong>${x.length}</strong> <span class="muted">notícias</span><div class="muted" style="font-size:12px;margin-top:4px">${x.length ? 'Tema principal: ' + esc(x[0].categoria) : 'Sem notícias no filtro'}</div></div>`;
  };
  return `<div class="cards">${[C.empresa_propria, ...(C.concorrentes || [])].filter(Boolean).map(card).join('')}</div><div class="section card"><div class="card-title"><h2>Volume por empresa</h2></div>${bars(a, 'empresa', 15)}</div>`;
}
function detail(r) {
  if (!r) return '<div class="card empty">Selecione uma notícia para ver os detalhes.</div>';
  const facts = [['Fonte', r.fonte], ['Origem', r.alerta ? 'Alerta IM' : r.origem], ['Setor', r.setor], ['Região', r.regioes.join(', ') || 'Não identificada'], ['Tema', r.categoria], ['Empresa principal', r.empresa],
    ['Relevância', r.rel], ['Score', r.score], ['Publicação', fmtDate(r.data_publicacao)], [r.manual ? 'Incluída por' : 'Classificada em', r.manual ? (r.adicionado_por || 'Equipe IM') : fmtDate(r.classificado_em, true)], ['ID', r.id]];
  const link = /^https?:\/\//i.test(r.url || '') ? `<a href="${esc(r.url)}" target="_blank" rel="noopener">Abrir fonte original</a>` : '';
  return `<button class="ghost back" data-back>Voltar</button><div class="detail-grid"><div class="card${r.alerta ? ' is-alert' : ''}"><div class="meta">${esc(r.fonte)} · ${fmtDate(r.data_publicacao)}</div><h2>${esc(r.titulo)}</h2>${tags(r)}
    <p class="detail-text">${esc(r.texto || 'Texto indisponível.')}</p>${link}</div>
    <div class="card"><div class="card-title"><h2>Informações</h2></div>${facts.map(([k, v]) => `<div class="fact"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join('')}</div></div>`;
}

// ---------- gráficos (SVG desenhado com a largura real do container) ----------
function drawCharts() {
  document.querySelectorAll('svg[data-chart]').forEach(svg => {
    const ind = state.indicators.find(i => i.id === svg.dataset.chart);
    if (ind && ind.serie.length > 1) drawChart(svg, ind, svg.dataset.mode === 'spark');
  });
}
function drawChart(svg, ind, spark) {
  const W = svg.clientWidth || 300, H = svg.clientHeight || (spark ? 30 : 150);
  const pad = spark ? { l: 1, r: 1, t: 3, b: 3 } : { l: 40, r: 10, t: 10, b: 22 };
  const pts = ind.serie, n = pts.length;
  let lo = Math.min(...pts.map(p => p.valor)), hi = Math.max(...pts.map(p => p.valor));
  if (lo === hi) { lo -= 1; hi += 1; }
  // Escala "redonda": passo 1, 2, 2,5 ou 5 x 10^n, com limites alinhados ao passo
  const raw = (hi - lo) / 3, mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map(f => f * mag).find(v => v >= raw);
  const span = hi - lo;
  lo = spark ? lo - span * 0.08 : Math.floor(lo / step) * step;
  hi = spark ? hi + span * 0.08 : Math.ceil(hi / step) * step;
  const dec = Number.isInteger(+step.toFixed(6)) ? 0 : Number.isInteger(+(step * 10).toFixed(6)) ? 1 : 2;
  // Margem esquerda cresce com o tamanho do rótulo do eixo (ex.: 175.000 no Ibovespa).
  if (!spark) pad.l = Math.max(40, Math.max(nf(lo, dec).length, nf(hi, dec).length) * 6 + 12);
  const x = i => pad.l + (W - pad.l - pad.r) * (n === 1 ? 0.5 : i / (n - 1));
  const y = v => pad.t + (H - pad.t - pad.b) * (1 - (v - lo) / (hi - lo));
  const lastReal = pts.reduce((k, p, i) => (!p.proj ? i : k), -1);
  const path = idx => idx.map((i, k) => `${k ? 'L' : 'M'}${x(i).toFixed(1)},${y(pts[i].valor).toFixed(1)}`).join('');
  const realIdx = pts.map((p, i) => i).filter(i => !pts[i].proj);
  const projIdx = pts.map((p, i) => i).filter(i => pts[i].proj);
  if (lastReal >= 0 && projIdx.length) projIdx.unshift(lastReal);
  let s = '';
  if (!spark) {
    if (lastReal >= 0 && lastReal < n - 1) s += `<rect x="${x(lastReal)}" y="${pad.t}" width="${W - pad.r - x(lastReal)}" height="${H - pad.t - pad.b}" fill="#f5f5f4"/>`;
    for (let v = lo; v <= hi + step / 2; v += step) {
      const yy = y(v);
      s += `<line x1="${pad.l}" x2="${W - pad.r}" y1="${yy}" y2="${yy}" stroke="#e5e7eb" stroke-width="1"/>`;
      s += `<text x="${pad.l - 6}" y="${yy + 3.5}" text-anchor="end" font-size="10" fill="#6b7280">${nf(v, dec)}</text>`;
    }
    new Set([0, n - 1, lastReal].filter(i => i >= 0)).forEach(i => {
      const anchor = i === 0 ? 'start' : i === n - 1 ? 'end' : 'middle';
      s += `<text x="${x(i)}" y="${H - 6}" text-anchor="${anchor}" font-size="10" fill="#6b7280">${esc(pts[i].periodo)}</text>`;
    });
  }
  const sw = spark ? 1.5 : 2;
  if (realIdx.length > 1) s += `<path d="${path(realIdx)}" fill="none" stroke="#1f2328" stroke-width="${sw}" stroke-linejoin="round" stroke-linecap="round"/>`;
  if (projIdx.length > 1) s += `<path d="${path(projIdx)}" fill="none" stroke="#1f2328" stroke-width="${sw}" stroke-dasharray="4 3" stroke-linejoin="round"/>`;
  if (lastReal >= 0) s += `<circle cx="${x(lastReal)}" cy="${y(pts[lastReal].valor)}" r="${spark ? 2.5 : 4}" fill="#1f2328" stroke="#fff" stroke-width="2"/>`;
  if (!spark) {
    s += `<line class="xh" x1="0" x2="0" y1="${pad.t}" y2="${H - pad.b}" stroke="#9ca3af" stroke-width="1" visibility="hidden"/><circle class="xd" r="4" fill="#fff" stroke="#1f2328" stroke-width="2" visibility="hidden"/>`;
    const stepX = (W - pad.l - pad.r) / Math.max(1, n - 1);
    pts.forEach((p, i) => { s += `<rect class="hit" data-i="${i}" x="${x(i) - stepX / 2}" y="0" width="${stepX}" height="${H}" fill="transparent"/>`; });
  }
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.innerHTML = s;
  if (spark) return;
  const xh = svg.querySelector('.xh'), xd = svg.querySelector('.xd'), tip = $('tooltip');
  svg.querySelectorAll('.hit').forEach(h => {
    h.addEventListener('mousemove', e => {
      const p = pts[+h.dataset.i], cx = x(+h.dataset.i), cy = y(p.valor);
      xh.setAttribute('x1', cx); xh.setAttribute('x2', cx); xh.setAttribute('visibility', 'visible');
      xd.setAttribute('cx', cx); xd.setAttribute('cy', cy); xd.setAttribute('visibility', 'visible');
      showTip(e, `<b>${esc(p.periodo)}</b> · ${fmtVal(p.valor, ind)} ${esc(unitSuffix(ind))} · ${p.proj ? 'projeção' : 'realizado'}`);
    });
    h.addEventListener('mouseleave', () => { xh.setAttribute('visibility', 'hidden'); xd.setAttribute('visibility', 'hidden'); tip.hidden = true; });
  });
}
function showTip(e, html) {
  const tip = $('tooltip'); tip.innerHTML = html; tip.hidden = false;
  const w = tip.offsetWidth, left = Math.min(e.clientX + 12, window.innerWidth - w - 8);
  tip.style.left = left + 'px'; tip.style.top = (e.clientY - 34) + 'px';
}

// ---------- navegação ----------
function render() {
  const [t, s] = TITLES[state.page];
  $('page-title').textContent = t; $('page-subtitle').textContent = s;
  $('content').innerHTML = { home, feed, external, indicators, competitors }[state.page]?.() ?? detail(state.selected);
  document.querySelectorAll('.nav').forEach(x => x.classList.toggle('active', x.dataset.page === state.page));
  drawCharts();
}
function go(page, push = true) {
  state.page = page; state.limit = PAGE_SIZE;
  if (push) history.pushState(null, '', page === 'detail' && state.selected ? `#noticia/${encodeURIComponent(state.selected.id)}` : `#${page}`);
  render(); window.scrollTo(0, 0);
}
function fromHash() {
  const h = decodeURIComponent(location.hash.slice(1));
  if (h.startsWith('noticia/')) { const id = h.slice(8); state.selected = [...state.rows, ...state.alerts].find(r => r.id === id) || null; state.page = 'detail'; }
  else state.page = TITLES[h] && h !== 'detail' ? h : 'home';
  render();
}

// ---------- carga ----------
async function getJSON(path, required = false) {
  const res = await fetch(`${path}?v=${Date.now()}`, { cache: 'no-store' });
  if (res.status === 404 && !required) return null;
  if (res.status === 404) throw new Error(`O arquivo ${path} ainda não existe. Rode o notebook "Export Portal Data" no Databricks e faça commit + push.`);
  if (!res.ok) throw new Error(`Falha ao carregar ${path} (HTTP ${res.status}).`);
  return res.json();
}
async function load() {
  $('updated').textContent = 'Carregando...';
  try {
    C = await getJSON('config.json', true); buildRules(); resetFilters();
    const opt = n => getJSON('data/' + n).catch(e => { console.warn(`data/${n}:`, e.message); return null; });
    const [bot, manual, ind, resumo, meta, auto] = await Promise.all([getJSON('data/feed.json', true), opt('manual.json'), opt('indicadores.json'), opt('resumo.json'), opt('meta.json'), opt('feed_automotivo.json')]);
    if (!Array.isArray(bot)) throw new Error('data/feed.json deve ser uma lista de notícias.');
    // Piloto automotivo (Automotive Business + AutoData): arquivo separado, já vem com setor "Automotivo".
    // Se ele falhar ou não existir, o portal segue só com o feed principal.
    const raw = Array.isArray(auto) ? [...bot, ...auto] : bot;
    const man = Array.isArray(manual) ? manual.filter(m => m.ativo !== false) : [];
    const isAlert = m => norm(m.tipo) === 'alerta';
    const seen = new Set(), dedupe = r => !(r.id && seen.has(r.id)) && seen.add(r.id);
    state.alerts = man.filter(isAlert).map(prepareAlert);
    // Alertas também entram no feed (marcados), para aparecerem na busca e nas caixas por tema.
    state.rows = [...state.alerts, ...man.filter(m => !isAlert(m)).map(m => prepare(m, true)), ...raw.map(r => prepare(r))].filter(dedupe);
    state.indicators = (ind?.indicadores || []).map(prepareIndicator);
    applyFixedOverrides();
    state.indicatorsSample = !!ind?.exemplo;
    state.resumo = resumo; state.meta = meta;
    fillOptions();
    const upd = meta?.atualizado_em || state.rows.map(r => r.classificado_em).filter(Boolean).sort().pop();
    const m = dparts(upd);
    $('updated').innerHTML = m ? `Atualizado em <b>${m[3]}/${m[2]}</b>${m[4] ? ` às <b>${m[4]}:${m[5]}</b>` : ''}` : 'Atualização não informada';
    fromHash();
  } catch (e) {
    $('updated').textContent = 'Erro ao carregar';
    $('content').innerHTML = `<div class="error"><b>Não foi possível carregar os dados.</b><p>${esc(e.message)}</p></div>`;
  }
}

// ---------- eventos ----------
document.querySelectorAll('.nav').forEach(b => b.onclick = () => go(b.dataset.page));
['search', 'sector', 'region', 'period', 'company', 'source', 'category', 'relevance', 'origin', 'score']
  .forEach(id => $(id).addEventListener('input', () => { state.limit = PAGE_SIZE; if (state.page === 'detail') go('feed'); else render(); }));
const openAdvanced = () => { $('advanced').hidden = false; $('more-filters').classList.add('on'); $('more-filters').setAttribute('aria-expanded', 'true'); };
$('more-filters').onclick = () => {
  const adv = $('advanced'); adv.hidden = !adv.hidden;
  $('more-filters').classList.toggle('on', !adv.hidden); $('more-filters').setAttribute('aria-expanded', String(!adv.hidden));
};
$('clear').onclick = () => { resetFilters(); render(); };
function onActivate(e) {
  const t = e.target;
  if (t.id === 'more') { state.limit += PAGE_SIZE; render(); return; }
  if (t.closest('[data-back]')) { history.length > 1 ? history.back() : go('feed'); return; }
  const goEl = t.closest('[data-go]'); if (goEl) { go(goEl.dataset.go); return; }
  const th = t.closest('[data-theme]'); if (th) { $('category').value = th.dataset.theme; openAdvanced(); go('feed'); return; }
  const comp = t.closest('[data-company]'); if (comp) { $('company').value = comp.dataset.company; openAdvanced(); go('feed'); return; }
  const n = t.closest('[data-id]'); if (n) { const id = n.dataset.id; state.selected = [...state.rows, ...state.alerts].find(r => r.id === id); go('detail'); }
}
$('content').addEventListener('click', onActivate);
$('content').addEventListener('keydown', e => { if (e.key === 'Enter') onActivate(e); });
$('content').addEventListener('mouseover', e => { const g = e.target.closest('[data-tip]'); if (g) showTip(e, esc(g.dataset.tip)); });
$('content').addEventListener('mouseout', e => { if (e.target.closest('[data-tip]')) $('tooltip').hidden = true; });
window.addEventListener('popstate', fromHash);
let rt; window.addEventListener('resize', () => { clearTimeout(rt); rt = setTimeout(drawCharts, 150); });
load();
