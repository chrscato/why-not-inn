/* Why Not In-Network? — single-page app */

const API = '/api';
const view = document.getElementById('view');
const dataStatusEl = document.getElementById('data-status');

/* ---------- Utilities ---------- */
const fmt = {
  int(n) {
    if (n === null || n === undefined) return '—';
    return Number(n).toLocaleString('en-US');
  },
  pct(n, digits = 0) {
    if (n === null || n === undefined || Number.isNaN(n)) return '—';
    return Number(n).toFixed(digits) + '%';
  },
  ratio(n, digits = 1) {
    if (n === null || n === undefined || Number.isNaN(n)) return '—';
    return (Number(n) / 100).toFixed(digits) + '×';
  },
  dollar(n) {
    if (n === null || n === undefined || Number.isNaN(n)) return '—';
    return '$' + Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 });
  },
  days(n) {
    if (n === null || n === undefined) return '—';
    return Math.round(n) + 'd';
  },
};

function el(tag, attrs = {}, ...children) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') e.className = v;
    else if (k === 'html') e.innerHTML = v;
    else if (k.startsWith('on') && typeof v === 'function') e.addEventListener(k.slice(2), v);
    else if (v !== null && v !== undefined) e.setAttribute(k, v);
  }
  for (const c of children) {
    if (c === null || c === undefined || c === false) continue;
    e.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return e;
}

async function getJSON(path) {
  const r = await fetch(API + path);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText} on ${path}`);
  return r.json();
}

function spinner() { return el('div', { class: 'spinner' }, 'Loading…'); }
function errorBox(msg) { return el('div', { class: 'error' }, msg); }

function setActiveNav(route) {
  document.querySelectorAll('.topnav a').forEach(a => {
    a.classList.toggle('active', a.dataset.route === route);
  });
}

function buildKpis(items) {
  const row = el('div', { class: 'kpi-row' });
  items.forEach(({ label, value, sub }) => {
    row.append(
      el('div', { class: 'kpi' },
        el('div', { class: 'label' }, label),
        el('div', { class: 'value' }, value),
        sub ? el('div', { class: 'sub' }, sub) : null,
      )
    );
  });
  return row;
}

function rateBadge(stat) {
  const total = (stat.provider_wins || 0) + (stat.issuer_wins || 0) + (stat.split_decisions || 0);
  if (!total) return '—';
  const pct = ((stat.provider_wins || 0) / total) * 100;
  return fmt.pct(pct, 0);
}

function winRate(provider, issuer, splits = 0) {
  const total = (provider || 0) + (issuer || 0) + (splits || 0);
  if (!total) return null;
  return ((provider || 0) / total) * 100;
}

/* Atlas-flavored chart palette */
const COLOR = {
  navy: '#14304d',
  navy2: '#1a3f5e',
  red: '#c5372e',
  warning: '#d4a017',
  success: '#2d8a4e',
  text: '#3a4a5a',
  textDim: '#7a8a9a',
  grid: '#e8ecf0',
};

const CHART_COMMON = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: { labels: { color: COLOR.text, font: { family: 'Inter', size: 11, weight: '500' } } },
    tooltip: {
      backgroundColor: '#ffffff',
      titleColor: COLOR.navy,
      bodyColor: '#1a2a3a',
      borderColor: '#dde3ea',
      borderWidth: 1,
      titleFont: { family: 'Inter', size: 12, weight: '700' },
      bodyFont: { family: 'Inter', size: 12 },
      padding: 10,
      boxPadding: 4,
    },
  },
  scales: {
    x: {
      ticks: { color: COLOR.textDim, font: { family: 'Inter', size: 11 } },
      grid: { color: COLOR.grid },
    },
    y: {
      ticks: { color: COLOR.textDim, font: { family: 'Inter', size: 11 } },
      grid: { color: COLOR.grid },
    },
  },
};

/* ---------- Data status pinger ---------- */
async function refreshStatus() {
  try {
    const d = await getJSON('/health');
    dataStatusEl.textContent = `${fmt.int(d.disputes)} dispute line items · ${fmt.int(d.offers)} offer rows`;
  } catch (e) {
    dataStatusEl.textContent = 'API offline';
  }
}
refreshStatus();

/* ---------- Router ---------- */
const routes = [
  { re: /^#\/?$/,                         render: renderDashboard, name: 'dashboard' },
  { re: /^#\/explorer\/?$/,               render: renderExplorer,  name: 'explorer' },
  { re: /^#\/cpt\/(.+)$/,                 render: renderCpt,       name: 'explorer' },
  { re: /^#\/insurer\/(.+)$/,             render: renderInsurer,   name: 'explorer' },
  { re: /^#\/state\/(.+)$/,               render: renderState,     name: 'explorer' },
  { re: /^#\/about\/?$/,                  render: renderAbout,     name: 'about' },
];

async function route() {
  const hash = window.location.hash || '#/';
  for (const r of routes) {
    const m = hash.match(r.re);
    if (m) {
      setActiveNav(r.name);
      view.innerHTML = '';
      view.append(spinner());
      try {
        await r.render(...m.slice(1));
      } catch (e) {
        view.innerHTML = '';
        view.append(errorBox('Error: ' + e.message));
        console.error(e);
      }
      return;
    }
  }
  view.innerHTML = '';
  view.append(errorBox('Page not found'));
}
window.addEventListener('hashchange', route);
window.addEventListener('load', route);

/* ---------- Dashboard ---------- */
async function renderDashboard() {
  const d = await getJSON('/dashboard');
  view.innerHTML = '';

  const o = d.overall || {};
  const winPct = winRate(o.provider_wins, o.issuer_wins, o.split_decisions);

  view.append(
    el('div', { class: 'page-header-banner' },
      el('div', { class: 'crumbs' }, 'Dashboard'),
      el('h1', {}, 'Federal IDR outcomes — all-time'),
      el('p', { class: 'sub' },
        'Aggregate signal across every closed federal Independent Dispute Resolution case the CMS Public Use Files cover. Things really are more expensive than they have to be.'),
    ),
    buildKpis([
      { label: 'Dispute line items', value: fmt.int(o.n_line_items),
        sub: `${fmt.int(o.n_disputes)} unique disputes` },
      { label: 'Provider win rate', value: fmt.pct(winPct),
        sub: `${fmt.int(o.provider_wins)} of ${fmt.int((o.provider_wins||0)+(o.issuer_wins||0)+(o.split_decisions||0))}` },
      { label: 'Median award / QPA', value: fmt.ratio(o.median_prevailing_pct_qpa),
        sub: `IQR ${fmt.ratio(o.p25_prevailing_pct_qpa)} – ${fmt.ratio(o.p75_prevailing_pct_qpa)}` },
      { label: 'Median time to close', value: fmt.days(o.median_days),
        sub: `${fmt.int(o.defaults)} default decisions` },
    ]),
  );

  // Quarterly chart
  const trendCard = el('div', { class: 'card' },
    el('h2', {}, 'Disputes resolved by quarter'),
    el('div', { class: 'card-sub' }, 'Stacked by determination outcome'),
    el('div', { class: 'chart-wrap' }, el('canvas', { id: 'q-trend' })),
  );
  view.append(trendCard);

  const labels = d.by_quarter.map(q => q.quarter);
  const provider = d.by_quarter.map(q => q.provider_wins || 0);
  const issuer = d.by_quarter.map(q => q.issuer_wins || 0);
  const splits = d.by_quarter.map(q => q.split_decisions || 0);

  new Chart(document.getElementById('q-trend'), {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'Provider win', data: provider, backgroundColor: COLOR.navy },
        { label: 'Issuer win',   data: issuer,   backgroundColor: COLOR.red },
        { label: 'Split',        data: splits,   backgroundColor: COLOR.warning },
      ],
    },
    options: { ...CHART_COMMON, scales: {
      x: { ...CHART_COMMON.scales.x, stacked: true },
      y: { ...CHART_COMMON.scales.y, stacked: true },
    }},
  });

  // Median % of QPA over time
  const ratioCard = el('div', { class: 'card' },
    el('h2', {}, 'Median prevailing offer as % of QPA'),
    el('div', { class: 'card-sub' }, 'Across all line items, by quarter (excluding outliers > 1000%)'),
    el('div', { class: 'chart-wrap' }, el('canvas', { id: 'q-ratio' })),
  );
  view.append(ratioCard);
  new Chart(document.getElementById('q-ratio'), {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Median (% of QPA)',
          data: d.by_quarter.map(q => q.median_prevailing_pct_qpa),
          borderColor: COLOR.navy,
          backgroundColor: 'rgba(20,48,77,0.12)',
          fill: true,
          tension: 0.25,
          pointRadius: 3,
        },
      ],
    },
    options: CHART_COMMON,
  });

  // Top CPTs / Insurers side-by-side
  view.append(
    el('div', { class: 'card-grid' },
      buildTopCard('Top CPT codes by dispute volume', d.top_cpts, 'service_code', '#/cpt/', cpt => cpt.description || ''),
      buildTopCard('Top insurers by dispute volume', d.top_insurers, 'insurer', '#/insurer/'),
    ),
    el('div', { class: 'card-grid' },
      buildTopCard('Top states by dispute volume', d.top_states, 'state', '#/state/'),
      buildTopCard('Top specialties by dispute volume', d.top_specialties, 'specialty', null),
    ),
  );
}

function buildTopCard(title, rows, valueKey, hrefPrefix, descFn = null) {
  const card = el('div', { class: 'card' },
    el('h2', {}, title),
  );
  const table = el('table');
  const thead = el('thead', {}, el('tr', {},
    el('th', {}, '#'),
    el('th', {}, valueKey === 'service_code' ? 'CPT / HCPCS' : valueKey.replace(/^./, c => c.toUpperCase())),
    el('th', { class: 'num' }, 'Lines'),
    el('th', { class: 'num' }, 'Provider win'),
    el('th', { class: 'num' }, 'Med % QPA'),
  ));
  table.append(thead);
  const tbody = el('tbody');
  rows.forEach((r, i) => {
    const winPct = winRate(r.provider_wins, r.issuer_wins);
    const valueText = r[valueKey] || '—';
    const linkText = descFn
      ? el('span', { class: 'list-link' },
          el('span', { class: 'code' }, valueText),
          descFn(r) ? el('span', {}, descFn(r)) : '')
      : el('span', { class: 'list-link' }, valueText);
    const cell = hrefPrefix
      ? el('a', { href: hrefPrefix + encodeURIComponent(valueText), class: 'list-link' }, linkText)
      : linkText;
    tbody.append(el('tr', {},
      el('td', { class: 'num muted' }, String(i + 1)),
      el('td', {}, cell),
      el('td', { class: 'num' }, fmt.int(r.n_line_items)),
      el('td', { class: 'num' }, fmt.pct(winPct)),
      el('td', { class: 'num' }, fmt.ratio(r.median_prevailing_pct_qpa)),
    ));
  });
  table.append(tbody);
  card.append(table);
  return card;
}

/* ---------- Explorer ---------- */
const EXPLORER_STATE = {
  service_code: '',
  state: '',
  insurer: '',
  quarter: '',
  outcome: '',
  specialty: '',
  page: 1,
  limit: 50,
  sort: 'prevailing_offer_pct_qpa',
  order: 'desc',
};

async function renderExplorer() {
  view.innerHTML = '';

  view.append(
    el('div', { class: 'page-header-banner' },
      el('div', { class: 'crumbs' }, 'Explorer'),
      el('h1', {}, 'Filter every dispute line item'),
      el('p', { class: 'sub' }, 'Slice by CPT, insurer, state, specialty, quarter, or outcome. Sortable, paginated, exportable.'),
    ),
  );

  // Filter bar
  const states = (await getJSON('/search/states')).results;
  const specialties = (await getJSON('/search/specialties')).results;
  const quarters = (await getJSON('/quarters')).results;

  const f = EXPLORER_STATE;

  const cptInput = el('input', { type: 'text', value: f.service_code, placeholder: 'e.g. 99285' });
  const insurerInput = el('input', { type: 'text', value: f.insurer, placeholder: 'e.g. UnitedHealthcare' });

  const stateSel = el('select', {}, el('option', { value: '' }, 'All states'));
  states.forEach(s => stateSel.append(el('option', { value: s.state }, `${s.state} (${fmt.int(s.n_line_items)})`)));
  stateSel.value = f.state;

  const specSel = el('select', {}, el('option', { value: '' }, 'All specialties'));
  specialties.forEach(s => specSel.append(el('option', { value: s.specialty }, `${s.specialty} (${fmt.int(s.n_line_items)})`)));
  specSel.value = f.specialty;

  const qSel = el('select', {}, el('option', { value: '' }, 'All quarters'));
  quarters.forEach(q => qSel.append(el('option', { value: q }, q)));
  qSel.value = f.quarter;

  const outSel = el('select', {},
    el('option', { value: '' }, 'Any outcome'),
    el('option', { value: 'provider' }, 'Provider win'),
    el('option', { value: 'issuer' }, 'Issuer win'),
    el('option', { value: 'split' }, 'Split'),
    el('option', { value: 'default' }, 'Default decision'),
  );
  outSel.value = f.outcome;

  const apply = () => {
    f.service_code = cptInput.value.trim();
    f.insurer = insurerInput.value.trim();
    f.state = stateSel.value;
    f.specialty = specSel.value;
    f.quarter = qSel.value;
    f.outcome = outSel.value;
    f.page = 1;
    loadDisputes();
  };
  const reset = () => {
    cptInput.value = ''; insurerInput.value = '';
    stateSel.value = ''; specSel.value = ''; qSel.value = ''; outSel.value = '';
    apply();
  };
  const exportBtn = el('a', { class: 'btn ghost', href: '#', onclick: ev => {
    ev.preventDefault();
    const params = new URLSearchParams();
    Object.entries({
      service_code: f.service_code, state: f.state, insurer: f.insurer,
      quarter: f.quarter, outcome: f.outcome, specialty: f.specialty,
    }).forEach(([k, v]) => v && params.set(k, v));
    window.location.assign('/api/export?' + params.toString());
  } }, 'Export CSV');

  const filterBar = el('div', { class: 'filter-bar' },
    el('div', { class: 'field' }, el('label', {}, 'CPT / HCPCS'), cptInput),
    el('div', { class: 'field' }, el('label', {}, 'Insurer'), insurerInput),
    el('div', { class: 'field' }, el('label', {}, 'State'), stateSel),
    el('div', { class: 'field' }, el('label', {}, 'Specialty'), specSel),
    el('div', { class: 'field' }, el('label', {}, 'Quarter'), qSel),
    el('div', { class: 'field' }, el('label', {}, 'Outcome'), outSel),
    el('div', { class: 'field' },
      el('label', { html: '&nbsp;' }),
      el('div', {},
        el('button', { class: 'btn', onclick: apply }, 'Apply'),
        ' ',
        el('button', { class: 'btn ghost', onclick: reset }, 'Reset'),
        ' ',
        exportBtn,
      ),
    ),
  );

  cptInput.addEventListener('keydown', e => { if (e.key === 'Enter') apply(); });
  insurerInput.addEventListener('keydown', e => { if (e.key === 'Enter') apply(); });

  view.append(filterBar);

  const resultsCard = el('div', { class: 'card' });
  resultsCard.id = 'results-card';
  view.append(resultsCard);

  loadDisputes();

  async function loadDisputes() {
    resultsCard.innerHTML = '';
    resultsCard.append(spinner());

    const params = new URLSearchParams();
    Object.entries(f).forEach(([k, v]) => {
      if (v !== '' && v !== null && v !== undefined) params.set(k, v);
    });
    const data = await getJSON('/disputes?' + params.toString());
    resultsCard.innerHTML = '';

    resultsCard.append(
      el('div', { class: 'card-sub' },
        `${fmt.int(data.total)} matching line items · showing ${data.rows.length} · page ${data.page}`),
    );

    const table = el('table');
    const head = el('thead', {}, el('tr', {},
      el('th', {}, 'CPT'),
      el('th', {}, 'Description'),
      el('th', {}, 'State'),
      el('th', {}, 'Insurer'),
      el('th', {}, 'Specialty'),
      el('th', { class: 'num' }, 'Provider %QPA'),
      el('th', { class: 'num' }, 'Issuer %QPA'),
      el('th', { class: 'num' }, 'Prev %QPA'),
      el('th', {}, 'Outcome'),
      el('th', {}, 'Quarter'),
    ));
    table.append(head);
    const tbody = el('tbody');
    data.rows.forEach(r => {
      tbody.append(el('tr', {},
        el('td', { class: 'code' },
          r.service_code
            ? el('a', { href: '#/cpt/' + encodeURIComponent(r.service_code) }, r.service_code)
            : '—'),
        el('td', { class: 'muted' }, (r.item_description || '').slice(0, 80)),
        el('td', { class: 'code' },
          r.location_of_service
            ? el('a', { href: '#/state/' + encodeURIComponent(r.location_of_service) }, r.location_of_service)
            : '—'),
        el('td', {},
          r.health_plan_name_normalized
            ? el('a', { href: '#/insurer/' + encodeURIComponent(r.health_plan_name_normalized) }, r.health_plan_name_normalized)
            : (r.health_plan_name || '—')),
        el('td', { class: 'muted' }, r.provider_specialty || '—'),
        el('td', { class: 'num' }, fmt.pct(r.provider_offer_pct_qpa)),
        el('td', { class: 'num' }, fmt.pct(r.issuer_offer_pct_qpa)),
        el('td', { class: 'num' }, fmt.pct(r.prevailing_offer_pct_qpa)),
        el('td', {}, outcomeTag(r.payment_determination_outcome, r.default_decision)),
        el('td', { class: 'code muted' }, r.quarter),
      ));
    });
    table.append(tbody);
    resultsCard.append(table);

    const totalPages = Math.max(1, Math.ceil(data.total / data.limit));
    resultsCard.append(
      el('div', { class: 'paginator' },
        el('div', {}, `Page ${data.page} of ${fmt.int(totalPages)}`),
        el('div', { class: 'controls' },
          el('button', { class: 'btn ghost', onclick: () => { f.page = Math.max(1, f.page - 1); loadDisputes(); } }, '← Prev'),
          el('button', { class: 'btn ghost', onclick: () => { f.page = Math.min(totalPages, f.page + 1); loadDisputes(); } }, 'Next →'),
        ),
      ),
    );
  }
}

function outcomeTag(outcome, def) {
  outcome = outcome || '';
  const o = outcome.toLowerCase();
  let cls = 'tag', label = outcome || '—';
  if (o.includes('provider')) cls += ' win';
  else if (o.includes('plan') || o.includes('issuer')) cls += ' loss';
  else if (o.includes('split')) cls += ' split';
  const tag = el('span', { class: cls }, label || '—');
  if ((def || '').toLowerCase().startsWith('yes')) {
    return el('span', {}, tag, ' ', el('span', { class: 'tag' }, 'default'));
  }
  return tag;
}

/* ---------- CPT detail ---------- */
async function renderCpt(rawCode) {
  const code = decodeURIComponent(rawCode);
  const d = await getJSON('/cpt/' + encodeURIComponent(code));
  view.innerHTML = '';

  const o = d.overall || {};
  const winPct = winRate(o.provider_wins, o.issuer_wins, o.split_decisions);

  view.append(
    el('div', { class: 'page-header-banner' },
      el('div', { class: 'crumbs' },
        el('a', { href: '#/' }, 'Dashboard'), ' › ',
        el('a', { href: '#/explorer' }, 'Explorer'), ' › CPT ' + code),
      el('h1', {}, `CPT ${code}` + (d.description ? ` — ${d.description}` : '')),
    ),
    buildKpis([
      { label: 'Dispute line items', value: fmt.int(o.n_line_items),
        sub: `${fmt.int(o.n_disputes)} unique disputes` },
      { label: 'Provider win rate', value: fmt.pct(winPct) },
      { label: 'Median award / QPA', value: fmt.ratio(o.median_prevailing_pct_qpa),
        sub: `IQR ${fmt.ratio(o.p25_prevailing_pct_qpa)} – ${fmt.ratio(o.p75_prevailing_pct_qpa)}` },
      { label: 'Avg QPA → Award', value:
        (o.avg_qpa && o.avg_prevailing) ? `${fmt.dollar(o.avg_qpa)} → ${fmt.dollar(o.avg_prevailing)}` : '—' },
    ]),
  );

  // Histogram
  const histCard = el('div', { class: 'card' },
    el('h2', {}, 'Distribution: prevailing offer as % of QPA'),
    el('div', { class: 'chart-wrap' }, el('canvas', { id: 'cpt-hist' })),
  );
  view.append(histCard);
  const labels = d.histogram.map(b => b.bucket);
  const counts = d.histogram.map(b => b.count);
  new Chart(document.getElementById('cpt-hist'), {
    type: 'bar',
    data: { labels, datasets: [{ label: 'Line items', data: counts, backgroundColor: COLOR.navy }] },
    options: CHART_COMMON,
  });

  // Quarterly volume + median
  if (d.by_quarter.length) {
    const trendCard = el('div', { class: 'card' },
      el('h2', {}, 'Volume and median %QPA by quarter'),
      el('div', { class: 'chart-wrap' }, el('canvas', { id: 'cpt-trend' })),
    );
    view.append(trendCard);
    const qLabels = d.by_quarter.map(q => q.quarter);
    new Chart(document.getElementById('cpt-trend'), {
      type: 'bar',
      data: {
        labels: qLabels,
        datasets: [
          { label: 'Line items', data: d.by_quarter.map(q => q.n_line_items),
            backgroundColor: COLOR.navy, yAxisID: 'y' },
          { type: 'line', label: 'Median % of QPA',
            data: d.by_quarter.map(q => q.median_prevailing_pct_qpa),
            borderColor: '#fbbf24', backgroundColor: COLOR.warning,
            yAxisID: 'y1', tension: 0.25, pointRadius: 3 },
        ],
      },
      options: {
        ...CHART_COMMON,
        scales: {
          x: CHART_COMMON.scales.x,
          y: { ...CHART_COMMON.scales.y, position: 'left' },
          y1: {
            position: 'right', beginAtZero: true,
            ticks: { color: COLOR.textDim, font: { family: 'Inter', size: 11 } },
            grid: { drawOnChartArea: false },
          },
        },
      },
    });
  }

  view.append(el('div', { class: 'card-grid' },
    insurerListCard('Top insurers for this code', d.top_insurers, true),
    stateListCard('Top states for this code', d.top_states),
  ));
}

function insurerListCard(title, rows, withWin = false) {
  const card = el('div', { class: 'card' }, el('h2', {}, title));
  const table = el('table');
  table.append(el('thead', {}, el('tr', {},
    el('th', {}, 'Insurer'),
    el('th', { class: 'num' }, 'Lines'),
    withWin ? el('th', { class: 'num' }, 'Provider win') : null,
  )));
  const tbody = el('tbody');
  rows.forEach(r => {
    const winPct = withWin ? winRate(r.provider_wins, r.issuer_wins) : null;
    tbody.append(el('tr', {},
      el('td', {}, el('a', { href: '#/insurer/' + encodeURIComponent(r.insurer) }, r.insurer || '—')),
      el('td', { class: 'num' }, fmt.int(r.n)),
      withWin ? el('td', { class: 'num' }, fmt.pct(winPct)) : null,
    ));
  });
  table.append(tbody);
  card.append(table);
  return card;
}

function stateListCard(title, rows) {
  const card = el('div', { class: 'card' }, el('h2', {}, title));
  const table = el('table');
  table.append(el('thead', {}, el('tr', {},
    el('th', {}, 'State'),
    el('th', { class: 'num' }, 'Lines'),
    el('th', { class: 'num' }, 'Avg %QPA'),
  )));
  const tbody = el('tbody');
  rows.forEach(r => {
    tbody.append(el('tr', {},
      el('td', { class: 'code' },
        el('a', { href: '#/state/' + encodeURIComponent(r.state) }, r.state)),
      el('td', { class: 'num' }, fmt.int(r.n)),
      el('td', { class: 'num' }, fmt.ratio(r.mean_pct)),
    ));
  });
  table.append(tbody);
  card.append(table);
  return card;
}

/* ---------- Insurer detail ---------- */
async function renderInsurer(rawName) {
  const name = decodeURIComponent(rawName);
  const d = await getJSON('/insurer/' + encodeURIComponent(name));
  view.innerHTML = '';

  const o = d.overall || {};
  const winPct = winRate(o.provider_wins, o.issuer_wins, o.split_decisions);

  view.append(
    el('div', { class: 'page-header-banner' },
      el('div', { class: 'crumbs' },
        el('a', { href: '#/' }, 'Dashboard'), ' › Insurer'),
      el('h1', {}, name),
    ),
    buildKpis([
      { label: 'Dispute line items', value: fmt.int(o.n_line_items),
        sub: `${fmt.int(o.n_disputes)} unique disputes` },
      { label: 'Provider win rate', value: fmt.pct(winPct),
        sub: `${fmt.int(o.issuer_wins)} insurer wins` },
      { label: 'Median award / QPA', value: fmt.ratio(o.median_prevailing_pct_qpa),
        sub: `IQR ${fmt.ratio(o.p25_prevailing_pct_qpa)} – ${fmt.ratio(o.p75_prevailing_pct_qpa)}` },
      { label: 'Median time to close', value: fmt.days(o.median_days),
        sub: `${fmt.int(o.defaults)} defaults` },
    ]),
  );

  if (d.by_quarter.length) {
    const trendCard = el('div', { class: 'card' },
      el('h2', {}, 'Win/loss by quarter'),
      el('div', { class: 'chart-wrap' }, el('canvas', { id: 'ins-trend' })),
    );
    view.append(trendCard);
    new Chart(document.getElementById('ins-trend'), {
      type: 'bar',
      data: {
        labels: d.by_quarter.map(q => q.quarter),
        datasets: [
          { label: 'Provider win', data: d.by_quarter.map(q => q.provider_wins), backgroundColor: COLOR.navy },
          { label: 'Issuer win',   data: d.by_quarter.map(q => q.issuer_wins),   backgroundColor: COLOR.red },
          { label: 'Split',        data: d.by_quarter.map(q => q.split_decisions), backgroundColor: COLOR.warning },
        ],
      },
      options: { ...CHART_COMMON, scales: {
        x: { ...CHART_COMMON.scales.x, stacked: true },
        y: { ...CHART_COMMON.scales.y, stacked: true },
      }},
    });
  }

  view.append(el('div', { class: 'card-grid' },
    cptListCard('Top CPT codes', d.top_cpts),
    stateListCardSimple('Top states', d.top_states),
  ));
}

function cptListCard(title, rows) {
  const card = el('div', { class: 'card' }, el('h2', {}, title));
  const table = el('table');
  table.append(el('thead', {}, el('tr', {},
    el('th', {}, 'CPT'),
    el('th', {}, 'Description'),
    el('th', { class: 'num' }, 'Lines'),
  )));
  const tbody = el('tbody');
  rows.forEach(r => {
    tbody.append(el('tr', {},
      el('td', { class: 'code' },
        el('a', { href: '#/cpt/' + encodeURIComponent(r.service_code) }, r.service_code)),
      el('td', { class: 'muted' }, (r.description || '').slice(0, 70)),
      el('td', { class: 'num' }, fmt.int(r.n)),
    ));
  });
  table.append(tbody);
  card.append(table);
  return card;
}

function stateListCardSimple(title, rows) {
  const card = el('div', { class: 'card' }, el('h2', {}, title));
  const table = el('table');
  table.append(el('thead', {}, el('tr', {},
    el('th', {}, 'State'),
    el('th', { class: 'num' }, 'Lines'),
  )));
  const tbody = el('tbody');
  rows.forEach(r => {
    tbody.append(el('tr', {},
      el('td', { class: 'code' },
        el('a', { href: '#/state/' + encodeURIComponent(r.state) }, r.state)),
      el('td', { class: 'num' }, fmt.int(r.n)),
    ));
  });
  table.append(tbody);
  card.append(table);
  return card;
}

/* ---------- State detail ---------- */
async function renderState(rawCode) {
  const code = decodeURIComponent(rawCode);
  const d = await getJSON('/state/' + encodeURIComponent(code));
  view.innerHTML = '';
  const o = d.overall || {};
  const winPct = winRate(o.provider_wins, o.issuer_wins, o.split_decisions);

  view.append(
    el('div', { class: 'page-header-banner' },
      el('div', { class: 'crumbs' },
        el('a', { href: '#/' }, 'Dashboard'), ' › State'),
      el('h1', {}, 'State: ' + code),
    ),
    buildKpis([
      { label: 'Dispute line items', value: fmt.int(o.n_line_items),
        sub: `${fmt.int(o.n_disputes)} unique disputes` },
      { label: 'Provider win rate', value: fmt.pct(winPct) },
      { label: 'Median award / QPA', value: fmt.ratio(o.median_prevailing_pct_qpa) },
      { label: 'Median time to close', value: fmt.days(o.median_days) },
    ]),
    el('div', { class: 'card-grid' },
      cptListCard('Top CPT codes', d.top_cpts),
      insurerListCard('Top insurers', d.top_insurers.map(r => ({...r}))),
    ),
  );
}

/* ---------- About ---------- */
async function renderAbout() {
  view.innerHTML = '';
  view.append(
    el('div', { class: 'page-header-banner' },
      el('div', { class: 'crumbs' }, 'About'),
      el('h1', {}, 'About this dashboard'),
    ),
    el('div', { class: 'card' },
      el('p', { html: `
        <strong>Why Not In-Network?</strong> is a local-first explorer for the federal Independent Dispute Resolution
        (IDR) Public Use Files released by CMS under the No Surprises Act. The PUF reports every IDR dispute
        the federal portal closed, including which party initiated, the Qualifying Payment Amount (QPA),
        each side's offer, and which offer prevailed.
      `}),
      el('p', { html: `
        The thesis: out-of-network bills are systematically more expensive than they have to be.
        IDR arbitrators tend to side with providers — and the prevailing offers tend to be multiples of the QPA
        (the network rate the insurer would have paid in-network). This site lets you slice the public data
        and see the patterns yourself.
      `}),
      el('p', { html: `
        <strong>Data quality notes.</strong> CMS warns that some initiating parties report nominal QPA values
        or unit prices, producing extreme percent-of-QPA outliers. This dashboard excludes line items where
        QPA &lt; $1.00 or prevailing offer &gt; 1000% of QPA from medians/percentiles. Component DLIs from bundled
        disputes are excluded from outcome counts (they don't receive offers).
      `}),
      el('p', { html: `
        <strong>Source:</strong> <a href="https://www.cms.gov/nosurprises/policies-and-resources/Reports-data-and-resources" target="_blank" rel="noopener">CMS — No Surprises Act Reports, Data, and Resources</a>.
      `}),
    ),
  );
}
