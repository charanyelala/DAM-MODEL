const app = document.getElementById("app");

const uploadStates = {};
const fetchStates  = {};
const datasetModes = {};

const apiConnections = {
  entso: {
    key:    localStorage.getItem("conn_entso_key") || "",
    status: localStorage.getItem("conn_entso_key") ? "saved" : "idle",
    error:  null,
  },
  custom: {
    url:    localStorage.getItem("conn_custom_url")   || "",
    token:  localStorage.getItem("conn_custom_token") || "",
    status: localStorage.getItem("conn_custom_url")  ? "saved" : "idle",
    error:  null,
  },
};

function getDatasetMode(key) {
  if (datasetModes[key] !== undefined) return datasetModes[key];
  return localStorage.getItem("dm_" + key) || "manual";
}
function setDatasetMode(key, mode) {
  datasetModes[key] = mode;
  localStorage.setItem("dm_" + key, mode);
}

const UPLOAD_DATASETS = [
  {
    key: "prices", label: "DAM Prices", tier: "required", apiSource: "entso",
    description: "Official Day-Ahead Market clearing prices for all 96 quarter-hour intervals. The model anchors its P50 forecast to these MCP values when present.",
    source: "HEnEx — Hellenic Energy Exchange",
    sourceUrl: "https://www.enexgroup.gr/web/guest/market-publications-dam-results",
    apiNote: "HEnEx publishes DAM results daily ~14:00 EET as CSV/XLSX. Use ENTSO-E Transparency (document type A44) for a programmatic REST API — free after registration.",
    entsoRef: "ENTSO-E A44 · BZN|GR Day-Ahead Prices",
    format: "timestamp,price",
    example: "2026-05-01T00:00:00Z,87.42\n2026-05-01T00:15:00Z,84.10\n... (96 rows total)",
  },
  {
    key: "load", label: "Load Forecast", tier: "required", apiSource: "entso",
    description: "System load forecast and actual consumption. Residual demand error is computed from this, directly driving stress score and spike risk.",
    source: "ADMIE / IPTO — Independent Power Transmission Operator",
    sourceUrl: "https://www.admie.gr/en/market/market-statistics/system-load",
    apiNote: "ADMIE publishes 24h-ahead load forecasts on their portal. ENTSO-E A65 (Total Load Actual) and A71 (Day-Ahead Total Load Forecast) cover Greece via REST API.",
    entsoRef: "ENTSO-E A65 + A71 · BZN|GR",
    format: "timestamp,load_forecast,load_actual",
    example: "2026-05-01T00:00:00Z,5820,5790\n2026-05-01T00:15:00Z,5710,\n... (96 rows, load_actual optional)",
  },
  {
    key: "res", label: "RES Generation", tier: "required", apiSource: "entso",
    description: "Renewable energy forecast and actual generation (wind + solar combined). This drives surplus pressure and negative-price risk calculations.",
    source: "ADMIE / IPTO RES Transparency Portal",
    sourceUrl: "https://www.admie.gr/en/market/market-statistics/renewable-energy-sources",
    apiNote: "ADMIE posts day-ahead RES forecasts per type. ENTSO-E A69 (Actual Generation) and B1 (Day-Ahead Forecast) for Wind (B19) and Solar (B16) in Greece.",
    entsoRef: "ENTSO-E A69 + B1 · PSR B16 Solar + B19 Wind · GR",
    format: "timestamp,res_forecast,res_actual",
    example: "2026-05-01T00:00:00Z,1840,\n2026-05-01T06:00:00Z,3200,2980\n... (96 rows, res_actual optional)",
  },
  {
    key: "cross_border", label: "Cross-Border Flows", tier: "important", apiSource: "entso",
    description: "Import/export flows and NTC limits across Greek interconnections with Bulgaria, North Macedonia, Albania, and Turkey. Regional pressure directly affects price.",
    source: "ENTSO-E Transparency Platform",
    sourceUrl: "https://transparency.entsoe.eu/",
    apiNote: "ENTSO-E free REST API. Document types A09 (Scheduled Commercial Exchanges) and A11 (Net Transfer Capacities). Python: pip install entsoe-py.",
    entsoRef: "ENTSO-E A09 + A11 · GR border interconnections",
    format: "timestamp,imports_mw,exports_mw,ntc_import_mw,ntc_export_mw",
    example: "2026-05-01T00:00:00Z,420,180,800,600\n2026-05-01T00:15:00Z,440,160,800,600",
  },
  {
    key: "market_signals", label: "Market Signals", tier: "important", apiSource: null,
    description: "Bid curve steepness, imbalance settlement risk, and price impact signals. Improves bid-clear probability and optimizer edge quality.",
    source: "HEnEx bid/offer curve publications or trading desk",
    sourceUrl: "https://www.enexgroup.gr/",
    apiNote: "No public API. Derive from HEnEx DAM bid curve depth reports post-clearing, or supply manually from trading desk estimates. Default zeros are accepted — the model falls back gracefully.",
    entsoRef: null,
    format: "timestamp,bid_clear_probability,imbalance_risk,curve_steepness,price_impact",
    example: "2026-05-01T00:00:00Z,0.88,0.12,0.42,2.1\n2026-05-01T00:15:00Z,0.85,0.14,0.38,1.9",
  },
  {
    key: "grid_outages", label: "Grid Outages", tier: "important", apiSource: "entso",
    description: "Planned and unplanned transmission and generation unavailability. Large outages raise scarcity risk and shift optimizer dispatch priorities.",
    source: "ADMIE Outage Transparency Portal",
    sourceUrl: "https://www.admie.gr/en/market/market-statistics/outages-and-unavailabilities",
    apiNote: "ADMIE publishes planned and unplanned outages on their portal. ENTSO-E A80 (Transmission Unavailability) and B15 (Generation Unit Unavailability) for Greece.",
    entsoRef: "ENTSO-E A80 + B15 · GR control area",
    format: "timestamp,unavailable_mw,outage_type,affected_area",
    example: "2026-05-01T06:00:00Z,350,planned,North\n2026-05-01T10:00:00Z,120,unplanned,Peloponnese",
  },
  {
    key: "asset_availability", label: "Asset Availability", tier: "optional", apiSource: "custom",
    description: "BESS power derating from maintenance schedules, grid connection limits, or inverter constraints. Reduces usable MW before the optimizer runs.",
    source: "Internal EMS / plant SCADA or manual schedule",
    sourceUrl: null,
    apiNote: "Provide via your EMS REST endpoint or export from your maintenance planning tool. If omitted, the optimizer assumes 100% availability for all 96 intervals.",
    entsoRef: null,
    format: "timestamp,availability_factor,thermal_derating_factor",
    example: "2026-05-01T00:00:00Z,1.0,0.95\n2026-05-01T08:00:00Z,0.8,1.0\n... (both factors 0.0–1.0)",
  },
  {
    key: "battery_health", label: "Battery Health (BMS)", tier: "optional", apiSource: "custom",
    description: "Real BMS/EMS telemetry replacing the synthetic digital twin. Unlocks accurate SOH, available capacity, cell temperature, and cumulative cycle count.",
    source: "BMS / EMS telemetry — Metlen-Karatzis SCADA or vendor API",
    sourceUrl: null,
    apiNote: "Connect via REST webhook, MQTT, or Modbus-to-CSV export. Real telemetry raises twin confidence from 62% (synthetic) to 88% (manual BMS). See Battery Twin tab for format details.",
    entsoRef: null,
    format: "timestamp,soh_pct,available_capacity_mwh,cell_temp_c,daily_throughput_mwh,cumulative_cycles",
    example: "2026-05-01T00:00:00Z,96.4,761.6,28.3,0,12840\n... (repeat per MTU or once per day)",
  },
];

const state = {
  date: new Date().toISOString().slice(0, 10),
  activeTab: "overview",
  loading: false,
  lastRefresh: null,
  scheduler: null,
  health: null,
  data: null,
  error: null,
  uploadMessage: null,
  groq: null,
  groqLoading: false,
  liveMode: true,
  currentSocPct: localStorage.getItem("currentSocPct") ? Number(localStorage.getItem("currentSocPct")) : null,
  socOverride: localStorage.getItem("socOverride") === "true",
};

const colors = {
  blue:   "#145c9e",
  cyan:   "#0087a8",
  green:  "#147d5b",
  amber:  "#a96800",
  red:    "#b42318",
  violet: "#6540a3",
  gray:   "#73828c",
  faint:  "#d8e1e7",
};

bootstrap();

async function bootstrap() {
  renderShell();
  bindShell();
  await hydrateInitialDate();
  await refreshAll({ runPipeline: false, quiet: true });
  startRealtimePolling();
}

function renderShell() {
  app.innerHTML = `
    <div class="app-shell">
      <header class="topbar">
        <div class="topbar-inner">
          <div class="brand">
            <div class="brand-wordmark">
              <h1 class="brand-name">Hellas <span>DAM</span></h1>
              <p class="brand-sub">Day-Ahead Market Intelligence &middot; 96-MTU BESS Dispatch</p>
            </div>
          </div>
          <form class="controls" id="run-form">
            <input id="date-input" type="date" value="${escapeHtml(state.date)}" />
            <button type="button" id="load-btn">Load</button>
            <button type="submit" class="primary" id="run-btn">Run Model</button>
            <button type="button" id="live-btn" aria-pressed="true">Live On</button>
          </form>
        </div>
      </header>
      <main class="layout">
        <section class="status-strip">
          <div class="status-main" id="status-main"></div>
          <div class="status-side" id="status-side"></div>
        </section>
        <nav class="tabs" id="tabs"></nav>
        <section id="view"></section>
      </main>
    </div>
  `;
}

function bindShell() {
  document.getElementById("run-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    state.date = document.getElementById("date-input").value;
    await refreshAll({ runPipeline: true });
  });
  document.getElementById("load-btn").addEventListener("click", async () => {
    state.date = document.getElementById("date-input").value;
    await refreshAll({ runPipeline: false });
  });
  document.getElementById("live-btn").addEventListener("click", () => {
    state.liveMode = !state.liveMode;
    document.getElementById("live-btn").textContent = state.liveMode ? "Live On" : "Live Off";
    document.getElementById("live-btn").setAttribute("aria-pressed", String(state.liveMode));
    render();
  });
}

async function hydrateInitialDate() {
  try {
    const health = await api("/api/health");
    state.health = health;
    const latest = latestStoredDate(health.storedFiles || []);
    if (latest) {
      state.date = latest;
      document.getElementById("date-input").value = latest;
    }
  } catch (error) {
    state.error = error.message;
  }
}

function startRealtimePolling() {
  setInterval(async () => {
    if (!state.liveMode || state.loading) return;
    await refreshAll({ runPipeline: false, quiet: true });
  }, 15000);

  setInterval(async () => {
    if (!state.liveMode) return;
    await pollScheduler();
  }, 30000);
}

async function refreshAll({ runPipeline, quiet = false } = {}) {
  state.loading = true;
  state.error = null;
  if (!quiet) render();

  try {
    const runQuery = `date=${encodeURIComponent(state.date)}${state.socOverride && state.currentSocPct != null ? `&initialSocPct=${encodeURIComponent(state.currentSocPct)}` : ""}`;
    const result = runPipeline
      ? await api(`/api/pipeline/run?${runQuery}`, { method: "POST" })
      : await loadCachedResult(state.date);
    state.data = normalizeResult(result);
    state.lastRefresh = new Date();
    await pollScheduler();
  } catch (error) {
    state.error = error.message;
  } finally {
    state.loading = false;
    render();
  }
}

async function pollScheduler() {
  try {
    state.scheduler = await api("/api/scheduler/status");
  } catch (_) {
    state.scheduler = null;
  }
}

async function loadCachedResult(date) {
  const [summary, forecasts, forecastErrors, dispatch, features] = await Promise.all([
    api(`/api/summary/${encodeURIComponent(date)}`),
    api(`/api/forecasts/${encodeURIComponent(date)}`),
    api(`/api/forecast-errors/${encodeURIComponent(date)}`),
    api(`/api/dispatch/${encodeURIComponent(date)}`),
    api(`/api/features/${encodeURIComponent(date)}`).catch(() => []),
  ]);
  return { summary, forecasts, forecastErrors, dispatch, features };
}

async function api(path, options) {
  const response = await fetch(path, options);
  if (!response.ok) {
    let message = await response.text();
    try {
      const parsed = JSON.parse(message);
      message = parsed.error || message;
    } catch (_) {}
    throw new Error(message || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

function normalizeResult(result) {
  const summary = result.summary || {};
  const forecasts = Array.isArray(result.forecasts) ? result.forecasts : [];
  const forecastErrors = Array.isArray(result.forecastErrors) ? result.forecastErrors : [];
  const dispatch = Array.isArray(result.dispatch) ? result.dispatch : [];
  const features = Array.isArray(result.features) ? result.features : [];
  return {
    summary,
    forecasts,
    forecastErrors,
    dispatch,
    features,
    rows: forecasts.map((forecast, index) => ({
      forecast,
      error: forecastErrors[index] || {},
      dispatch: dispatch[index] || {},
      feature: features[index] || {},
    })),
  };
}

function render() {
  renderStatus();
  renderTabs();
  const view = document.getElementById("view");

  const savedWindowY   = window.scrollY;
  const prevTableWrap  = view.querySelector(".table-wrap");
  const savedTableTop  = prevTableWrap?.scrollTop  ?? 0;
  const savedTableLeft = prevTableWrap?.scrollLeft ?? 0;

  if (!state.data) {
    view.innerHTML = `
      <div class="panel empty">
        ${state.error
          ? `<strong>Error</strong><br>${escapeHtml(state.error)}`
          : "No model run loaded yet. Select a date and click Run Model or Load."}
      </div>`;
    return;
  }

  if (state.activeTab === "overview")      renderOverview(view);
  if (state.activeTab === "forecast")      renderForecast(view);
  if (state.activeTab === "risk")          renderRisk(view);
  if (state.activeTab === "optimization")  renderOptimization(view);
  if (state.activeTab === "twin")          renderTwin(view);
  if (state.activeTab === "quality")       renderQuality(view);
  if (state.activeTab === "intervals")     renderIntervals(view);
  if (state.activeTab === "upload")        renderUpload(view);
  bindViewControls();
  if (state.activeTab === "upload")        bindUploadControls();

  if (savedWindowY > 0) window.scrollTo(0, savedWindowY);
  const newTableWrap = view.querySelector(".table-wrap");
  if (newTableWrap) {
    newTableWrap.scrollTop  = savedTableTop;
    newTableWrap.scrollLeft = savedTableLeft;
  }
}

function renderStatus() {
  const summary = state.data?.summary || {};
  const qualityClass = summary.dataQuality?.isTradeReady ? "" : "warn";
  const statusText = state.loading
    ? "Updating model output"
    : state.error
      ? "Needs attention"
      : "Dashboard ready";

  document.getElementById("status-main").innerHTML = `
    <span class="status-pill">
      <span class="dot ${state.error ? "bad" : qualityClass}"></span>
      ${escapeHtml(statusText)}
    </span>
    <span>${escapeHtml(summary.date || state.date)}</span>
    <span>${summary.dataQuality?.isTradeReady ? "Trade-ready inputs" : "Demo or partial inputs"}</span>
    ${state.error ? `<span class="chip"><span class="dot bad"></span>${escapeHtml(state.error)}</span>` : ""}
  `;

  const scheduler   = state.scheduler;
  const nextRun     = scheduler?.nextRun ? formatTime(scheduler.nextRun) : "not scheduled";
  const lastRefresh = state.lastRefresh
    ? state.lastRefresh.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
    : "never";

  document.getElementById("status-side").innerHTML = `
    <span class="chip">Refreshed ${escapeHtml(lastRefresh)}</span>
    <span class="chip">Auto ${escapeHtml(nextRun)}</span>
    <span class="chip">${state.liveMode ? "Polling · 15s" : "Manual"}</span>
  `;

  const runBtn  = document.getElementById("run-btn");
  const loadBtn = document.getElementById("load-btn");
  if (runBtn)  runBtn.disabled  = state.loading;
  if (loadBtn) loadBtn.disabled = state.loading;
}

function renderTabs() {
  const tabs = [
    ["overview",     "Overview"],
    ["forecast",     "Forecast"],
    ["risk",         "Risk"],
    ["optimization", "Optimization"],
    ["twin",         "Battery Twin"],
    ["quality",      "Data Quality"],
    ["intervals",    "Intervals"],
    ["upload",       "Upload Data"],
  ];
  document.getElementById("tabs").innerHTML = tabs
    .map(([id, label]) =>
      `<button class="tab ${state.activeTab === id ? "active" : ""}" data-tab="${id}">${label}</button>`)
    .join("");
  document.querySelectorAll("[data-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeTab = button.dataset.tab;
      render();
      window.scrollTo({ top: 0, behavior: "smooth" });
    });
  });
}
function renderOverview(view) {
  const { summary, forecasts, forecastErrors, dispatch } = state.data;
  const topRisk  = maxBy(forecastErrors, (row) => row.anomalyScore || 0);
  const topPrice = maxBy(forecasts,      (row) => row.priceP50 || 0);
  const revenue  = sum(dispatch,         (row) => row.expectedRevenueEur || 0);
  view.innerHTML = `
    ${kpiGrid([
      ["Expected Revenue",    fmt(summary.expectedRevenue ?? revenue, 0), "EUR",        "Net model value from the optimized battery schedule.", "Revenue = Σ Δt[DischargeMW*(DischargeValue-HoldValue) - ChargeMW*ChargeCost - (ChargeMW+DischargeMW)*DegCost]"],
      ["Avg P50 Price",       fmt(summary.avgPrice, 1),                   "EUR/MWh",    "Average central DAM price forecast across all 96 intervals.", "AvgP50 = mean(P50_t), t=1..96", ""],
      ["Price Range",         `${fmt(summary.minPrice,1)} / ${fmt(summary.maxPrice,1)}`, "EUR/MWh", "Lowest and highest forecast price; spread drives arbitrage.", "Range = min(P50_t) / max(P50_t)"],
      ["Risk Intervals",      fmt(summary.anomalyIntervals, 0),            "of 96 MTUs", "Intervals flagged for unusual forecast-error or market risk.", "RiskIntervals = count(Anomaly_t > 0.65)"],
      ["Cycles",              fmt(summary.equivalentCycles, 2),            "eq. daily",  "Battery throughput used by the optimizer today.", "EquivalentCycles = Σ(ChargeMW_t*0.25) / CapacityMWh"],
      ["Capacity After Day",  fmt(summary.remainingCapacityMwhAfterDay, 1),"MWh",        "Estimated battery capacity after daily degradation.", "RemainingCapacity = CapacityMWh*(1 - CapacityFadePct/100)", ""],
    ])}
    <div class="grid two-col">
      <article class="panel">
        ${panelHeader("Prediction Surface", "P10/P50/P90 prices with market stress and dispatch value adjustments.", legend([
          ["P10", colors.cyan], ["P50", colors.blue], ["P90", colors.amber],
          ["Discharge value", colors.red], ["Charge cost", colors.green]
        ]))}
        <canvas id="overview-price" class="chart"></canvas>
      </article>
      <article class="panel">
        ${panelHeader("Decision Summary", "The LP optimizer charges in low-value intervals and discharges when net value clears degradation and risk costs.")}
        <div class="split">
          ${miniStat("Charge",          `${fmt(summary.chargeIntervals,0)} MTUs`,    `${fmt(summary.chargeMwh,1)} MWh`,        "Low-value intervals selected for storing energy.",        "ChargeMWh = Σ ChargeMW_t*0.25")}
          ${miniStat("Discharge",       `${fmt(summary.dischargeIntervals,0)} MTUs`, `${fmt(summary.dischargeMwh,1)} MWh`,     "High-value intervals selected for selling energy.",       "DischargeMWh = Σ DischargeMW_t*0.25")}
          ${miniStat("Peak Price MTU",  `MTU ${topPrice?.mtu ?? "-"}`,               `${fmt(topPrice?.priceP50,1)} EUR/MWh`,   "Highest central price forecast in the day.",              "Peak = argmax_t(P50_t)")}
          ${miniStat("Highest Risk MTU",`MTU ${topRisk?.mtu ?? "-"}`,                `${pct(topRisk?.anomalyScore)} anomaly`,  "Interval most likely affected by forecast-error risk.",   "HighestRisk = argmax_t(Anomaly_t)")}
        </div>
        <div class="insight-list" style="margin-top:10px">${renderInsights()}</div>
      </article>
    </div>
    <article class="panel" style="margin-top:14px">
      ${panelHeader("Groq Market Brief", "AI explanation generated from model outputs only. Does not replace the numeric forecast engine.",
        `<button id="groq-btn" type="button">${state.groqLoading ? "Generating…" : "Generate Brief"}</button>`)}
      <div class="insight-list">${renderGroqBrief()}</div>
    </article>
    <div class="grid three-col" style="margin-top:14px">
      <article class="panel">
        ${panelHeader("Forecast-Error Intelligence", "Residual-demand underforecast, overforecast, anomaly, and weather spread.")}
        <canvas id="overview-risk" class="chart compact"></canvas>
        ${renderRiskExplanation(forecasts, forecastErrors)}
      </article>
      <article class="panel">
        ${panelHeader("Battery Dispatch", "Positive bars discharge, negative bars charge; line is state of charge.")}
        <canvas id="overview-dispatch" class="chart compact"></canvas>
        ${renderDispatchExplanation(dispatch, summary)}
      </article>
      <article class="panel">
        ${panelHeader("Input Coverage", "How much of the 96-MTU day is backed by live or manual source data.")}
        <div class="source-grid">${renderSourceCoverage()}</div>
        <div style="margin-top:10px">${formulaBlock("Coverage = non-missing, non-synthetic source rows / 96")}</div>
      </article>
    </div>
  `;
  drawPriceChart("overview-price", forecasts);
  drawRiskChart("overview-risk", forecasts, forecastErrors);
  drawDispatchChart("overview-dispatch", dispatch, summary.battery);
}
function renderForecast(view) {
  const { forecasts } = state.data;
  const spikeRows = [...forecasts].sort((a, b) => (b.probabilitySpike || 0) - (a.probabilitySpike || 0)).slice(0, 6);
  view.innerHTML = `
    ${kpiGrid([
      ["Mean Confidence",    pct(avg(forecasts, (r) => r.confidence)),           "model confidence", "Average confidence in the 96 interval price forecast.",           "Confidence_t = clamp(1 - 0.55*Anomaly_t + EnsembleBonus, 0.2, 0.95)", ""],
      ["Spike Probability",  pct(max(forecasts, (r) => r.probabilitySpike)),      "max interval",     "Highest upside-tail price spike risk in the day.",                "SpikeProb_t = clamp((CorrectedNetLoad_t-4200)/1800 + Evening*0.25 + max(0,ThermalMC-80)/300 + 0.18*Stress_t, 0.01, 0.97)"],
      ["Negative Probability",pct(max(forecasts, (r) => r.probabilityNegative)), "max interval",     "Highest chance of zero or negative price conditions.",            "NegProb_t = clamp((RESShare_t-0.62)*2.2 + SolarHourAdj + 0.15*(SolarAnomaly_t-1) + 0.18*Surplus_t, 0.01, 0.95)"],
      ["Mean Stress",        pct(avg(forecasts, (r) => r.stressScore)),           "market regime",    "Average tight-system pressure supporting higher prices.",         "Stress = .35*LoadErrScore + .25*ImportStress + .20*GasCarbon + .12*Scarcity + .03*Outage + .05*BlockFragility"],
      ["Mean Surplus",       pct(avg(forecasts, (r) => r.surplusScore)),          "RES pressure",     "Average renewable surplus pressure supporting lower prices.",      "Surplus = .45*RESSurplus + .30*ExportConstraint + .25*CurtailmentRisk + .15*max(0,-LoadErrScore)"],
      ["Thermal MC",         fmt(avg(forecasts, (r) => r.thermalMarginalCost), 1),"EUR/MWh",          "Gas and carbon based thermal marginal cost reference.",           "ThermalMC = GasEUR/MWh / 0.45 + CarbonEUR/t * 0.37"],
    ])}
    <div class="grid two-col">
      <article class="panel">
        ${panelHeader("Probabilistic Price Forecast", "The shaded range is represented by P10 and P90 lines around the P50 decision forecast.", legend([
          ["P10", colors.cyan], ["P50", colors.blue], ["P90", colors.amber], ["Thermal MC", colors.gray]
        ]))}
        <canvas id="forecast-price" class="chart"></canvas>
        ${formulaList([
          ["P50",             "P50_t = Price_t if available; otherwise ThermalMC_t + ScarcityPremium_t - RESSurplusDiscount_t + EveningPremium_t",                                          "HEnEx Results MCP is used as Price_t when present."],
          ["P10 / P90",       ["P10_t = clamp(P50_t - Uncertainty_t, -100, 500)", "P90_t = clamp(P50_t + Uncertainty_t*(1+SpikeProb_t), -100, 700)"],                                      "Uncertainty expands when residual-demand error, anomaly, or weather spread is high."],
          ["Corrected Net Load","CorrectedNetLoad_t = LoadForecast_t - RESForecast_t + ResidualDemandError_t",                                                                               "This drives scarcity and spike risk."],
        ])}
      </article>
      <article class="panel">
        ${panelHeader("Spike Watchlist", "Intervals where the model sees elevated upside-tail price risk.")}
        <div class="risk-list">
          ${spikeRows.map((row) => riskRow(`MTU ${row.mtu}`, pct(row.probabilitySpike), row.note || row.regime || "Price spike watch", row.probabilitySpike || 0)).join("")}
        </div>
      </article>
    </div>
  `;
  drawPriceChart("forecast-price", forecasts, { includeThermal: true });
}
function renderRisk(view) {
  const { forecasts, forecastErrors } = state.data;
  const top = [...forecastErrors].sort((a, b) => (b.anomalyScore || 0) - (a.anomalyScore || 0)).slice(0, 12);
  view.innerHTML = `
    <article class="panel" style="margin-bottom:14px">
      ${panelHeader("Risk Intelligence Edge", "This tab estimates where residual demand may deviate and how that changes battery value.")}
      <div class="explain-grid">
        <div class="explain-box">
          <strong>Why this is the edge</strong>
          <span>Simple arbitrage buys low and sells high. This layer asks whether a price is structurally stressed or structurally cheap before the battery commits energy.</span>
          ${formulaBlock(["DischargeValue = P50 + StressPremium - costs", "ChargeCost = P50 - SurplusDiscount + costs"])}
        </div>
        <div class="explain-box">
          <strong>Stress changes dispatch</strong>
          <span>Positive residual-demand pressure raises spike risk and makes stored energy more valuable. Surplus pressure makes charging more attractive.</span>
          ${formulaBlock(["Stress up -> DischargeValue up", "Surplus up -> ChargeCost down"])}
        </div>
      </div>
    </article>
    ${kpiGrid([
      ["Max Anomaly",        pct(max(forecastErrors, (r) => r.anomalyScore)),              "forecast-error score", "Highest detected risk that official forecasts are wrong.",          "Anomaly_t = clamp(|ResidualDemandError_t|/900 + CloudVolatility_t/100 + EnsemblePenalty_t + CloudPenalty_t, 0, 1)"],
      ["Underforecast Risk", pct(max(forecastErrors, (r) => r.probabilityUnderforecast)), "max interval",         "Highest chance actual residual demand is above forecast.",          "P_under_t = clamp(0.25 + ResidualDemandError_t/1200, 0.02, 0.98)"],
      ["Overforecast Risk",  pct(max(forecastErrors, (r) => r.probabilityOverforecast)),  "max interval",         "Highest chance actual residual demand is below forecast.",         "P_over_t = clamp(0.25 - ResidualDemandError_t/1400, 0.02, 0.98)"],
      ["Residual Error",     fmt(avgAbs(forecastErrors, (r) => r.expectedResidualDemandErrorMw), 0), "avg abs MW", "Average expected error in load minus renewable generation.",        "ResidualDemandError_t = LoadError_t - RESError_t", ""],
      ["Cloud Uncertainty",  fmt(avg(forecastErrors, (r) => r.cloudUncertainty), 1),      "index",                "Weather uncertainty affecting solar output and residual demand.",   "CloudUncertainty_t = CloudCoverP90_t - CloudCoverP10_t"],
      ["Solar Spread",       fmt(max(forecastErrors, (r) => r.ensembleSolarSpreadWm2), 0),"W/m² max",             "Largest ensemble disagreement in solar irradiance.",               "SolarSpread_t = SolarRadiationP90_t - SolarRadiationP10_t"],
    ])}
    <div class="grid two-col">
      <article class="panel">
        ${panelHeader("Forecast-Error Risk Surface", "The model detects where official load, RES, and residual-demand forecasts are likely wrong.", legend([
          ["Anomaly", colors.red], ["Underforecast", colors.violet], ["Stress", colors.amber], ["Solar spread", colors.cyan]
        ]))}
        <canvas id="risk-chart" class="chart"></canvas>
        ${formulaList([
          ["Load Error",  "LoadError_t = ActualLoad_t - LoadForecast_t; if actual missing, Heat + Cold + MorningRamp + Cloud - 120",       "Positive values mean demand may be higher than expected."],
          ["RES Error",   "RESError_t = ActualRES_t - RESForecast_t; if actual missing, solar/weather proxy is used",                      "Negative RES error raises residual-demand risk."],
          ["Risk Label",  "underforecast-risk if Anomaly_t > 0.65 and ResidualDemandError_t > 0; overforecast-risk if < 0",                "This is why high-risk MTUs appear in the list."],
        ])}
        ${renderRiskExplanation(forecasts, forecastErrors)}
      </article>
      <article class="panel">
        ${panelHeader("Top Risk Intervals", "Largest anomaly scores, with expected residual-demand error.")}
        <div class="risk-list">
          ${top.map((row) => riskRow(`MTU ${row.mtu}`, pct(row.anomalyScore), `${row.note || row.label || "Risk"}; residual error ${fmt(row.expectedResidualDemandErrorMw, 0)} MW`, row.anomalyScore || 0)).join("")}
        </div>
      </article>
    </div>
  `;
  drawRiskChart("risk-chart", forecasts, forecastErrors);
}
function renderOptimization(view) {
  const { summary, forecasts, dispatch } = state.data;
  const best        = maxBy(dispatch, (row) => row.expectedRevenueEur || 0);
  const worstCharge = maxBy(dispatch, (row) => row.chargeMw || 0);
  view.innerHTML = `
    ${kpiGrid([
      ["Optimizer Revenue",  fmt(summary.expectedRevenue, 0),            "EUR net expected",  "Expected value after the dispatch optimization.",                          "max Σ Δt[DischargeMW*(DischargeValue-HoldValue) - ChargeMW*ChargeCost - (ChargeMW+DischargeMW)*DegCost]"],
      ["Wear Cost",          fmt(summary.degradationCostEur, 0),         "EUR",               "Estimated battery degradation cost from cycling and conditions.",          "WearCost = Σ(ChargeMW_t + DischargeMW_t)*0.25*DegCost_t"],
      ["Capacity Fade",      `${fmt(summary.capacityFadePct, 4)}%`,      "this run",          "Estimated capacity loss caused by this day of operation.",                 "CapacityFade = Σ(CycleFade_t + CalendarFade_t)"],
      ["Charge Energy",      fmt(summary.chargeMwh, 1),                  "MWh",               "Total energy bought and stored by the battery.",                          "ChargeMWh = Σ ChargeMW_t*0.25"],
      ["Discharge Energy",   fmt(summary.dischargeMwh, 1),               "MWh",               "Total energy sold back into the market.",                                  "DischargeMWh = Σ DischargeMW_t*0.25", ""],
      ["Max Daily Cycles",   fmt(summary.battery?.maxDailyCycles, 2),    "constraint",        "Daily cycle limit used to protect long-term asset value.",                 "Σ ChargeMW_t*0.25 <= MaxDailyCycles*CapacityMWh"],
    ])}
    <div class="grid two-col">
      <article class="panel">
        ${panelHeader("Dispatch and State of Charge", "Schedule chosen from forecast value, degradation cost, power limits, SOC bounds, and daily-cycle limit.", legend([
          ["Charge", colors.green], ["Discharge", colors.amber], ["SOC", colors.blue]
        ]))}
        <canvas id="opt-dispatch" class="chart"></canvas>
        ${formulaList([
          ["SOC Dynamics", "SOC_t = SOC_(t-1) + ChargeMW_t*η_c*0.25 - DischargeMW_t*0.25/η_d",                                                                                                 "Every dispatch row follows this energy balance."],
          ["Power Bounds",  ["0 <= ChargeMW_t <= MaxChargeMW*Availability_t*ThermalDerating_t", "0 <= DischargeMW_t <= MaxDischargeMW*Availability_t*ThermalDerating_t"],                       "Derating reduces usable power before optimization."],
          ["SOC Bounds",    "MinSOC <= SOC_t <= MaxSOC and SOC_final >= SOC_initial",                                                                                                            "The battery cannot empty itself for one-day profit."],
        ])}
        ${renderDispatchExplanation(dispatch, summary)}
      </article>
      <article class="panel">
        ${panelHeader("Optimization Explainability", "What the schedule is doing and why.")}
        <div class="split">
          ${miniStat("Battery",        `${fmt(summary.battery?.maxChargeMw,0)} MW`,          `${fmt(summary.battery?.capacityMwh,0)} MWh / ${fmt(summary.battery?.durationHours,2)} h`, "Power, energy, and duration used in optimization.", "DurationHours = CapacityMWh / min(MaxChargeMW, MaxDischargeMW)")}
          ${miniStat("Best Revenue MTU",`MTU ${best?.mtu ?? "-"}`,                           `${fmt(best?.expectedRevenueEur,0)} EUR`,                                                  "Single interval with highest expected dispatch value.", "BestMTU = argmax_t(Revenue_t)")}
          ${miniStat("Max Charge MTU", `MTU ${worstCharge?.mtu ?? "-"}`,                     `${fmt(worstCharge?.chargeMw,0)} MW`,                                                      "Interval where the optimizer charges at highest power.", "MaxChargeMTU = argmax_t(ChargeMW_t)")}
          ${miniStat("Bid Clearance",  pct(avg(forecasts, (r) => r.bidClearProbability)),    "mean probability",                                                                        "Average chance model-adjusted bids clear the market.", "MeanBidClear = mean(BidClearProbability_t)")}
        </div>
        <div class="insight-list" style="margin-top:10px">${renderDispatchExplanations()}</div>
      </article>
    </div>
  `;
  drawDispatchChart("opt-dispatch", dispatch, summary.battery);
}
function renderTwin(view) {
  const { summary, forecasts, dispatch, features } = state.data;
  const twin = buildTwinStats(summary, forecasts, dispatch, features);
  view.innerHTML = `
    ${kpiGrid([
      ["Twin Confidence",  `${fmt(twin.confidence,0)}%`,                            "synthetic accuracy", "Confidence in the twin inputs. Real BMS data would raise this materially.",                            "Confidence = 88 if manual BMS, 62 if synthetic/predicted twin, otherwise 35", ""],
      ["Asset Scale",      "330 / 790",                                              "MW / MWh",           "Metlen-Karatzis Thessaly standalone BESS power and energy rating.",                                   "Scale = MaxPowerMW / CapacityMWh"],
      ["Usable Duration",  fmt(twin.usableDuration, 2),                              "hours",              "Usable energy window divided by 330 MW operating power.",                                             "UsableDuration = CapacityMWh*(SOCmax-SOCmin)/PowerMW", ""],
      ["Cell Temp Range",  `${fmt(twin.minCellTemp,1)} / ${fmt(twin.maxCellTemp,1)}`,"°C",                 "Weather-driven estimated cell temperature range for the selected day.",                               "CellTemp = AmbientTemp + ThermalManagementDelta + 4*C-rate"],
      ["Capacity Health",  pct(twin.capacityHealth),                                 "factor",             "Synthetic SoH/capacity factor feeding optimizer constraints.",                                        "CapacityHealth = mean(CapacityHealthFactor_t)"],
      ["Round-Trip Eff.",  `${fmt(twin.rtePct,1)}%`,                                 "RTE",                "Baseline LFP RTE adjusted down when live cell temperature is hotter than the curve.",                 "RTE = ChargeEfficiency*DischargeEfficiency adjusted by twin assumptions", ""],
      ["Daily Fade",       `${fmt(twin.calendarFadePct,5)}%`,                        "calendar estimate",  "Estimated weather/SOC calendar aging before real telemetry.",                                         "CalendarFade = Σ AnnualFade*0.25/8760*TempFactor_t*SOCStress_t"],
    ])}
    <div class="twin-hero">
      <article class="panel">
        ${panelHeader("Metlen-Karatzis Thessaly Digital Twin", "Animated planning replica for the 330 MW / 790 MWh BESS in Thessaly.")}
        <div class="battery-visual">
          <div class="thermal-ring"></div>
          <div class="battery-pack" style="--soc:${clamp(twin.currentSocPct, 8, 100)}%">
            <div class="battery-fill"></div>
            <div class="battery-lines">${Array.from({ length: 8 }, () => "<span></span>").join("")}</div>
          </div>
          <div class="battery-label">
            ${miniStat("Current Battery", `${fmt(twin.currentSocPct,0)}%`, `${fmt(twin.currentSocMwh,1)} MWh`, "Starting SOC used by the optimizer. Lower SOC reduces how much the battery can discharge.", "SOC_initial = CurrentBattery% * CapacityMWh")}
            ${miniStat("State of Charge", `${fmt(twin.endSocPct,0)}%`,   `${fmt(twin.endSocMwh,1)} MWh`,  "End-of-day SOC from optimizer output.",                                           "SOC% = SOCMWh / CapacityMWh * 100")}
            ${miniStat("Thermal State",   `${fmt(twin.avgCellTemp,1)} °C`,`${twin.thermalStatus}`,         "Estimated cell temperature from weather plus thermal-management assumptions.",     "AvgCellTemp = mean(CellTemp_t)")}
          </div>
        </div>
        <div class="soc-control">
          <label for="current-soc-input">Current battery level</label>
          <input id="current-soc-input" type="range" min="${fmt(twin.minSocPct,0)}" max="${fmt(twin.maxSocPct,0)}" step="1" value="${fmt(twin.currentSocPct,0)}" />
          <input id="current-soc-number" type="number" min="${fmt(twin.minSocPct,0)}" max="${fmt(twin.maxSocPct,0)}" step="1" value="${fmt(twin.currentSocPct,0)}" />
          <button id="apply-soc-btn" type="button" class="primary">Override SOC & Run</button>
          <button id="auto-soc-btn" type="button">Auto SOC</button>
        </div>
        <p class="source-note">Optimizer start constraint: SOC starts at ${fmt(twin.currentSocMwh,1)} MWh from ${escapeHtml(twin.currentSocSourceLabel)} and cannot go below ${fmt(twin.minSocMwh,1)} MWh. This prevents over-discharge when the battery is already low.</p>
      </article>
      <article class="panel">
        ${panelHeader("Project Identity", "Project assumptions used to make the UI and model explainable.")}
        <div class="twin-pill-grid">
          ${miniStat("Location",   "Thessaly, Greece",  "Mediterranean grid climate",  "Weather nodes should be refined around Thessaly, Evia, Western Macedonia, Crete, and Peloponnese.", "")}
          ${miniStat("Investment", "EUR 170m",          "announced project cost",       "Used only as project context, not as dispatch economics.", "")}
          ${miniStat("Ownership",  "49% / 51%",         "Metlen / Karatzis",           "Metlen handles construction, operation, maintenance, and energy management through M Renewables.", "")}
          ${miniStat("Timeline",   "Q2 2026",           "target completion",            "The twin is for planning until real operational telemetry exists.", "")}
          ${miniStat("Chemistry",  "LFP",               "assumption",                  "Based on Metlen/PPC regional BESS announcements using liquid-cooled LFP systems.", "")}
          ${miniStat("Role",       "Grid stability",    "RES integration",             "Value comes from arbitrage, reserve optionality, and renewable curtailment avoidance.", "")}
        </div>
      </article>
    </div>
    <div class="grid two-col" style="margin-top:14px">
      <article class="panel">
        ${panelHeader("Twin Health and Degradation Logic", "What the synthetic model estimates before real BMS data exists.")}
        <div class="explain-grid">
          <div class="explain-box"><strong>Temperature model</strong><span>Ambient weather + solar heating - wind cooling + thermal-management delta gives estimated cell temperature.</span>${formulaBlock("CellTemp_t = AmbientTemp_t + ThermalDelta + PowerHeatProxy_t")}</div>
          <div class="explain-box"><strong>Capacity model</strong><span>Uses baseline year ${twin.baselineYear}: ${fmt(twin.baselineSohPct,2)}% SoH, ${fmt(twin.baselineCycles,0)} cumulative EFC, ${fmt(twin.baselineCapLossPct,2)}% annual capacity loss.</span>${formulaBlock("AvailableCapacity_t = NominalCapacity*(SOH_t/100)")}</div>
          <div class="explain-box"><strong>Derating model</strong><span>High or low estimated cell temperature reduces thermalDeratingFactor before optimizer dispatch.</span>${formulaBlock("PowerLimit_t = MaxPower*Availability_t*ThermalDerating_t")}</div>
          <div class="explain-box"><strong>Live climate effect</strong><span>Current weather is ${fmt(twin.climateTempDeltaC,2)} °C versus the baseline operating-temperature curve.</span></div>
          <div class="explain-box"><strong>Operating window</strong><span>Uses ${escapeHtml(twin.socWindow || "20–80%")} as the planning SoC band from the provided degradation assumptions.</span></div>
          <div class="explain-box"><strong>Production upgrade</strong><span>Replace twin rows with BMS/EMS telemetry: SoH, available capacity, cell temp, cycles, throughput.</span></div>
        </div>
      </article>
      <article class="panel">
        ${panelHeader("Similar Battery Evidence", "Why the synthetic assumptions are reasonable but not final.")}
        <div class="insight-list">
          <div class="insight">METLEN confirms the Thessaly unit is 330 MW / 790 MWh, about 2.39 hours nominal duration, with completion targeted for Q2 2026.</div>
          <div class="insight">METLEN and PPC's regional BESS pipeline in Romania, Bulgaria, and Italy uses two-hour liquid-cooled LFP systems, supporting LFP as the planning chemistry assumption.</div>
          <div class="insight">Mediterranean operation makes temperature and solar exposure important, so the twin uses live weather, solar radiation, wind cooling, and cell-temperature derating.</div>
          <div class="insight">This twin is useful for pre-COD planning and UI explainability. It is not a warranty model and should not replace vendor data, SCADA, EMS, or BMS telemetry.</div>
        </div>
        <p class="source-note" style="margin-top:10px">Sources: METLEN project release, METLEN/PPC regional BESS release, Open-Meteo/PVGIS weather-source documentation.</p>
      </article>
    </div>
    <article class="panel" style="margin-top:14px">
      ${panelHeader("15-Year LFP Degradation Curve", "Planning curve used by the synthetic twin. Live weather adjusts the selected baseline.")}
      <div class="table-wrap" style="max-height:360px">
        <table>
          <thead><tr><th>Year</th><th>SOH</th><th>Cycles</th><th>RTE</th><th>Avg Temp</th><th>Cap Loss</th><th>SoC Window</th></tr></thead>
          <tbody>${renderTwinCurveRows()}</tbody>
        </table>
      </div>
    </article>
    <article class="panel" style="margin-top:14px">
      ${panelHeader("Twin Timeline", "How the replica should mature from synthetic planning to production operations.")}
      <div class="explain-grid">
        <div class="explain-box">
          <strong>Level 1: Synthetic twin</strong>
          <span>Current state. Uses public project size, weather, LFP assumptions, and optimizer output.</span>
          ${formulaBlock([
            "CellTemp_t = AmbientTemp_t + ThermalDelta + 4*(PowerMW_t/CapacityMWh)",
            "CycleFade_t = ThroughputMWh_t * DegCost_t / (ReplacementEURPerMWh*CapacityMWh) * 100",
            "CalendarFade_t = AnnualFade * 0.25/8760 * TempFactor_t * SOCStress_t",
          ])}
        </div>
        <div class="explain-box">
          <strong>Level 2: Calibrated twin</strong>
          <span>Add vendor curves, warranty limits, HVAC data, measured cell temperature, and historical dispatch.</span>
          ${formulaBlock([
            "Fit θ = argmin Σ(ObservedSOH_t - PredictedSOH_t(θ))^2",
            "PredictedSOH_t = 100 - Σ(CycleFade_t(θ) + CalendarFade_t(θ))",
            "Derating_t = min(VendorThermalLimit_t, WarrantyLimit_t, Availability_t)",
          ])}
        </div>
        <div class="explain-box">
          <strong>Level 3: Operational twin</strong>
          <span>Live SCADA/BMS/EMS telemetry updates SoH, derating, cycle cost, and dispatch limits every interval.</span>
          ${formulaBlock([
            "SOH_t = BMS_SOH_t or filtered(SOH_(t-1), BMS_SOH_t)",
            "AvailableCapacity_t = NominalCapacity * SOH_t/100 * Availability_t",
            "PowerLimit_t = min(PCSLimit_t, BMSLimit_t, ThermalLimit_t, GridLimit_t)",
          ])}
        </div>
      </div>
    </article>
  `;
}

function buildTwinStats(summary, forecasts, dispatch, features) {
  const battery         = summary.battery || {};
  const capacity        = Number(battery.capacityMwh || 790);
  const power           = Number(battery.maxDischargeMw || battery.maxChargeMw || 330);
  const minSocMwh       = Number(battery.minSocMwh ?? capacity * 0.10);
  const maxSocMwh       = Number(battery.maxSocMwh ?? capacity * 0.95);
  const minSocFraction  = minSocMwh / Math.max(1, capacity);
  const maxSocFraction  = maxSocMwh / Math.max(1, capacity);
  const minSocPct       = minSocFraction * 100;
  const maxSocPct       = maxSocFraction * 100;
  const autoSocPct      = Number(battery.initialSocFraction ?? 0.5) * 100;
  const manualSocPct    = Number(state.currentSocPct);
  const requestedSocPct = state.socOverride && Number.isFinite(manualSocPct) ? manualSocPct : autoSocPct;
  const currentSocPct   = clamp(requestedSocPct, minSocPct, maxSocPct);
  const currentSocMwh   = capacity * currentSocPct / 100;
  const currentSocSourceLabel = state.socOverride
    ? "manual override"
    : socSourceLabel(battery.initialSocSource);
  const usableDuration  = capacity * (maxSocFraction - minSocFraction) / Math.max(1, power);
  const cellTemps       = [
    ...features.map((row) => row.healthCellTempC).filter(Number.isFinite),
    ...forecasts.map((row) => row.cellTempC).filter(Number.isFinite),
  ];
  const capacityFactors    = features.map((row) => row.capacityHealthFactor).filter(Number.isFinite);
  const twinCalendarFade   = sum(features, (row) => row.batteryTwinCalendarFadePct || 0);
  const rtePct             = avg(features, (row) => Number(row.batteryTwinRtePct));
  const baselineYear       = firstFinite(features, (row) => Number(row.batteryTwinBaselineYear));
  const baselineSohPct     = firstFinite(features, (row) => Number(row.batteryTwinBaselineSohPct));
  const baselineCapLossPct = firstFinite(features, (row) => Number(row.batteryTwinBaselineAnnualCapacityLossPct));
  const baselineAvgTempC   = firstFinite(features, (row) => Number(row.batteryTwinBaselineAvgOpTempC));
  const climateTempDeltaC  = avg(features, (row) => Number(row.batteryTwinClimateTempDeltaC));
  const socWindow          = features.find((row) => row.batteryTwinOperatingSocWindow)?.batteryTwinOperatingSocWindow;
  const sourceCounts       = summary.sources?.batteryHealth || {};
  const coverage           = Object.values(sourceCounts).reduce((acc, value) => acc + Number(value || 0), 0);
  const sourceLabel        = Object.keys(sourceCounts)[0] || "missing";
  const endSocMwh          = dispatch.at(-1)?.socMwh ?? capacity * 0.5;
  const endSocPct          = endSocMwh / Math.max(1, capacity) * 100;
  const avgCellTemp        = avg(cellTemps.map((value) => ({ value })), (row) => row.value);
  const maxCellTemp        = max(cellTemps.map((value) => ({ value })), (row) => row.value);
  const minCellTemp        = min(cellTemps.map((value) => ({ value })), (row) => row.value);
  const capacityHealth     = avg(capacityFactors.map((value) => ({ value })), (row) => row.value) || 1;
  const hasTwin            = Object.keys(sourceCounts).some((key) => key.includes("twin") || key.includes("synthetic"));
  const hasManual          = Object.keys(sourceCounts).some((key) => key.includes("manual"));
  const confidence         = hasManual ? 88 : hasTwin ? 62 : 35;
  const thermalStatus      = maxCellTemp >= 40 ? "hot derating watch" : minCellTemp <= 0 ? "cold derating watch" : "normal band";
  return {
    confidence, usableDuration, minCellTemp, maxCellTemp, avgCellTemp,
    capacityHealth, rtePct, calendarFadePct: twinCalendarFade,
    baselineYear, baselineSohPct, baselineCycles: (baselineYear || 1) * 300,
    baselineCapLossPct, baselineAvgTempC, climateTempDeltaC,
    socWindow, sourceLabel, coverage, minSocMwh, maxSocMwh, minSocPct, maxSocPct,
    currentSocPct, currentSocMwh, currentSocSourceLabel, endSocMwh, endSocPct, thermalStatus,
  };
}

function socSourceLabel(source) {
  if (source === "previous-dispatch") return "previous day dispatch";
  if (source === "default-min-soc") return "minimum SOC default";
  if (source === "manual-override") return "manual override";
  return source || "automatic SOC";
}

const TWIN_CURVE = [
  [1,  98.00, 300,  88.6, 24.5, 2.00],
  [2,  96.04, 600,  88.2, 24.6, 3.96],
  [3,  94.12, 900,  87.8, 24.8, 5.88],
  [4,  92.24, 1200, 87.4, 25.0, 7.76],
  [5,  90.39, 1500, 87.0, 25.2, 9.61],
  [6,  88.58, 1800, 86.6, 25.4, 11.42],
  [7,  86.81, 2100, 86.2, 25.7, 13.19],
  [8,  85.08, 2400, 85.8, 26.0, 14.92],
  [9,  83.37, 2700, 85.4, 26.3, 16.63],
  [10, 81.71, 3000, 85.0, 26.6, 18.29],
  [11, 80.07, 3300, 84.6, 27.0, 19.93],
  [12, 78.47, 3600, 84.2, 27.3, 21.53],
  [13, 76.90, 3900, 83.8, 27.7, 23.10],
  [14, 75.36, 4200, 83.4, 28.1, 24.64],
  [15, 73.86, 4500, 83.0, 28.5, 26.14],
];

function renderTwinCurveRows() {
  return TWIN_CURVE.map(([year, soh, cycles, rte, temp, loss]) => `
    <tr>
      <td>${year}</td>
      <td>${fmt(soh, 2)}%</td>
      <td>${fmt(cycles, 0)}</td>
      <td>${fmt(rte, 1)}%</td>
      <td>${fmt(temp, 1)} °C</td>
      <td>${fmt(loss, 2)}%</td>
      <td>20–80%</td>
    </tr>
  `).join("");
}
function renderQuality(view) {
  const { summary } = state.data;
  const warnings  = [...(summary.dataQuality?.warnings || []), ...(summary.warnings || [])];
  const readiness = buildReadinessSummary(summary);
  view.innerHTML = `
    ${kpiGrid([
      ["Trade Ready",      summary.dataQuality?.isTradeReady ? "Yes" : "No", "input status", "Whether backend inputs are strong enough for serious use.",            "TradeReady = no synthetic price/load/RES fallback fields"],
      ["Price Coverage",   sourceCount(summary.sources?.price),               "MTUs",         "Intervals with usable DAM price input data.",                          "PriceCoverage = count(source.price not missing/synthetic)", ""],
      ["Load Coverage",    sourceCount(summary.sources?.load),                "MTUs",         "Intervals with usable load forecast or actual load data.",             "LoadCoverage = count(source.load not missing/synthetic)", ""],
      ["RES Coverage",     sourceCount(summary.sources?.res),                 "MTUs",         "Intervals with usable renewable generation data.",                     "RESCoverage = count(source.res not missing/synthetic)", ""],
      ["Weather Coverage", sourceCount(summary.sources?.weather),             "MTUs",         "Intervals backed by weather or ensemble weather data.",                "WeatherCoverage = count(source.weather not missing/synthetic)", ""],
      ["Fallback Warnings",warnings.length,                                   "items",        "Data-quality warnings that limit trust in the run.",                   "Warnings = dataQuality.warnings + backend connector warnings"],
    ])}
    <div class="grid two-col">
      <article class="panel">
        ${panelHeader("Why This Is Not Market Ready", "The dashboard separates model output from production-grade trading input quality.")}
        <div class="insight-list">
          ${readiness.blockers.map((item) => `<div class="insight">${escapeHtml(item)}</div>`).join("")}
        </div>
      </article>
      <article class="panel">
        ${panelHeader("What To Improve Next", "Upload or connect these inputs, then run the model again for the selected date.")}
        <div class="insight-list">
          ${readiness.actions.map((item) => `<div class="insight">${escapeHtml(item)}</div>`).join("")}
        </div>
      </article>
      <article class="panel">
        ${panelHeader("Upload Next-Day Inputs", "No database required. Upload CSV rows into data/manual, then run the model for the selected date.")}
        <form id="manual-upload-form" class="form-grid">
          <div class="field">
            <label for="upload-dataset">Dataset</label>
            <select id="upload-dataset" name="dataset">
              <option value="prices">DAM prices: timestamp, price</option>
              <option value="load">Load: timestamp, load_forecast, load_actual</option>
              <option value="res">RES: timestamp, res_forecast, res_actual</option>
              <option value="cross_border">Cross-border: imports/exports/NTC</option>
              <option value="market_signals">Market signals: bid/risk adjustments</option>
              <option value="grid_outages">Grid outages: unavailable MW</option>
              <option value="asset_availability">Asset availability: derating factors</option>
              <option value="battery_health">Battery health: SOH/SOC temperature</option>
            </select>
          </div>
          <div class="field">
            <label for="upload-mode">Mode</label>
            <select id="upload-mode" name="mode">
              <option value="append">Append rows</option>
              <option value="replace">Replace file</option>
            </select>
          </div>
          <div class="field">
            <label for="upload-file">CSV file</label>
            <input id="upload-file" name="file" type="file" accept=".csv,text/csv" required />
          </div>
          <button class="primary" type="submit">Upload CSV</button>
        </form>
        <div class="insight-list" style="margin-top:10px">
          <div class="insight">Minimum useful next-day upload: 96 rows each for price, load forecast, and RES forecast. Cross-border, outages, market signals, and battery health improve quality but are optional.</div>
          <div class="insight">Use ISO timestamps such as 2026-04-30T00:00:00Z, or include an mtu column from 0 to 95.</div>
          ${state.uploadMessage ? `<div class="insight">${escapeHtml(state.uploadMessage)}</div>` : ""}
        </div>
      </article>
      <article class="panel">
        ${panelHeader("Source Coverage", "Source counts by collection. Any missing or synthetic rows should be treated as non-tradeable.")}
        <div class="source-grid">${renderSourceCoverage(true)}</div>
      </article>
      <article class="panel">
        ${panelHeader("Warnings and Model Limits", "These are blockers before treating the output as executable trading advice.")}
        <div class="insight-list">
          ${warnings.length
            ? warnings.map((warning) => `<div class="insight">${escapeHtml(warning)}</div>`).join("")
            : `<div class="insight">No warnings reported by the backend.</div>`}
        </div>
      </article>
    </div>
  `;
}

function buildReadinessSummary(summary) {
  const sources  = summary.sources || {};
  const blockers = [];
  const actions  = [];
  const required = [
    ["price", "DAM price",    "prices.csv with timestamp,price"],
    ["load",  "load forecast","load.csv with timestamp,load_forecast"],
    ["res",   "RES forecast", "res.csv with timestamp,res_forecast"],
  ];

  required.forEach(([key, label, upload]) => {
    const counts    = sources[key] || {};
    const real      = sourceCount(counts);
    const synthetic = sourceSyntheticCount(counts);
    const missing   = Number(counts.missing || 0);
    if (real < 96 || synthetic > 0 || missing > 0) {
      blockers.push(`${label}: ${real}/96 real intervals. ${synthetic} synthetic and ${missing} missing intervals reduce trust.`);
      actions.push(`Upload ${upload} for all 96 quarter-hour intervals for ${summary.date || state.date}.`);
    }
  });

  const weatherReal    = sourceCount(sources.weather);
  const weatherMissing = Number(sources.weather?.missing || 0);
  if (weatherReal < 96 || weatherMissing > 0) {
    blockers.push(`Weather: ${weatherReal}/96 intervals available. ${weatherMissing} missing intervals were filled by defaults.`);
    actions.push("Rerun the model when weather access works, or add a weather/manual forecast feed for all 96 intervals.");
  }

  if (sourceCount(sources.crossBorder) < 96) {
    actions.push("Add cross_border.csv with imports_mw,exports_mw,ntc_import_mw,ntc_export_mw to improve regional pressure and spike risk.");
  }

  if (sourceCount(sources.marketSignals) < 96) {
    actions.push("Add market_signals.csv for bid-clear probability, curve steepness, imbalance risk, and price impact if you want trader-grade optimization.");
  }

  const batteryHealthCounts = sources.batteryHealth || {};
  const twinRows = Object.entries(batteryHealthCounts)
    .filter(([key]) => key.includes("twin") || key.includes("synthetic"))
    .reduce((acc, [, value]) => acc + Number(value || 0), 0);
  if (twinRows > 0) {
    blockers.push(`Battery health: ${twinRows}/96 intervals are supplied by predicted battery-twin data, not real BMS/EMS telemetry. Treat precision as approximate.`);
    actions.push("Replace the twin with battery_health.csv from BMS/EMS data when available: timestamp,soh_pct,available_capacity_mwh,cell_temp_c,daily_throughput_mwh,cumulative_cycles.");
  }

  if (summary.dataQuality?.isTradeReady && blockers.length === 0) {
    blockers.push("Required price, load, and RES inputs are complete enough for serious analysis.");
  }

  return {
    blockers: blockers.length ? blockers : ["No hard blockers reported by the backend."],
    actions:  actions.length  ? actions  : ["No immediate input fixes required. Next improvement is backtesting against actual settled prices."],
  };
}
function renderIntervals(view) {
  const rows = state.data.rows;
  view.innerHTML = `
    <article class="panel">
      ${panelHeader("96-MTU Detail Table", "Every interval with forecast, risk, optimizer decision, SOC, expected revenue, and explanation.")}
      ${formulaList([
        ["Price Columns",   ["P50_t = central price; P10_t = P50_t - Uncertainty_t; P90_t = P50_t + Uncertainty_t*(1+SpikeProb_t)", "When HEnEx Results MCP exists, P50_t is anchored to official MCP_t."],   "These formulas correspond to the P10/P50/P90 table columns."],
        ["Risk Columns",    ["Anomaly_t = clamp(|ResidualDemandError_t|/900 + weather uncertainty terms, 0, 1)", "Under_t = clamp(0.25 + ResidualDemandError_t/1200, 0.02, 0.98)"],                          "These formulas correspond to Anomaly, Under, Load Err, and RES Err."],
        ["Dispatch Columns",["SOC_t = SOC_(t-1) + ChargeMW_t*η_c*0.25 - DischargeMW_t*0.25/η_d", "Revenue_t = 0.25[DischargeMW_t*(DischargeValue_t-HoldValue_t) - ChargeMW_t*ChargeCost_t - ThroughputMW_t*DegCost_t]"], "These formulas correspond to Action, Charge, Discharge, SOC, and Revenue."],
      ])}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Action</th>
              <th>P10</th>
              <th>P50</th>
              <th>P90</th>
              <th>Anomaly</th>
              <th>Under</th>
              <th>Load Err</th>
              <th>RES Err</th>
              <th>Charge</th>
              <th>Discharge</th>
              <th>SOC</th>
              <th>Revenue</th>
              <th>Confidence</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map(intervalRow).join("")}
          </tbody>
        </table>
      </div>
    </article>
  `;
}

function intervalRow(row) {
  const f      = row.forecast;
  const e      = row.error;
  const d      = row.dispatch;
  const action = d.action || "idle";
  return `
    <tr title="${escapeHtml(d.explanation || f.note || "")}">
      <td>${escapeHtml(formatMtuTime(f.timestamp, f.mtu))}</td>
      <td><span class="action ${escapeHtml(action)}">${escapeHtml(action)}</span></td>
      <td>${fmt(f.priceP10, 1)}</td>
      <td>${fmt(f.priceP50, 1)}</td>
      <td>${fmt(f.priceP90, 1)}</td>
      <td>${pct(e.anomalyScore)}</td>
      <td>${pct(e.probabilityUnderforecast)}</td>
      <td>${fmt(e.expectedLoadErrorMw, 0)}</td>
      <td>${fmt(e.expectedResErrorMw, 0)}</td>
      <td>${fmt(d.chargeMw, 1)}</td>
      <td>${fmt(d.dischargeMw, 1)}</td>
      <td>${fmt(d.socMwh, 1)}</td>
      <td>${fmt(d.expectedRevenueEur, 0)}</td>
      <td>${pct(f.confidence)}</td>
    </tr>
  `;
}
function renderUpload(view) {
  const sources  = state.data?.summary?.sources || {};
  const tomorrow = nextDay(state.date);

  const checkItems = [
    { key: "prices", label: "Prices" }, { key: "load", label: "Load" },
    { key: "res", label: "RES" }, { key: "cross_border", label: "Cross-Border" },
    { key: "market_signals", label: "Signals" }, { key: "grid_outages", label: "Outages" },
    { key: "asset_availability", label: "Availability" }, { key: "battery_health", label: "BMS" },
  ].map(({ key, label }) => {
    const st  = uploadStates[key]?.status === "success" ? "ready"
              : fetchStates[key]?.status  === "success" ? "ready"
              : sourceCount(sources[key] || {}) > 0     ? "partial"
              : "missing";
    const icon = st === "ready" ? "✓" : st === "partial" ? "~" : "·";
    return `<span class="check-item ${st}">${icon} ${escapeHtml(label)}</span>`;
  });

  const entsoConn  = apiConnections.entso;
  const customConn = apiConnections.custom;
  const entsoConnected  = entsoConn.status === "connected";
  const customConnected = customConn.status === "connected";
  const entsoSaved   = entsoConn.key  !== "";
  const customSaved  = customConn.url !== "";

  const connStatusText = (conn, savedLabel) => {
    if (conn.status === "connected") return `<span class="conn-status connected">● Connected</span>`;
    if (conn.status === "testing")   return `<span class="conn-status testing">● Testing…</span>`;
    if (conn.status === "error")     return `<span class="conn-status error">● ${escapeHtml(conn.error || "Error")}</span>`;
    if (conn.status === "saved")     return `<span class="conn-status saved">● ${savedLabel}</span>`;
    return `<span class="conn-status idle">Not configured</span>`;
  };

  const required  = UPLOAD_DATASETS.filter((d) => d.tier === "required");
  const important = UPLOAD_DATASETS.filter((d) => d.tier === "important");
  const optional  = UPLOAD_DATASETS.filter((d) => d.tier === "optional");

  view.innerHTML = `
    <div class="upload-intro">
      <div class="upload-intro-text">
        <h2>Prepare Next-Day Inputs</h2>
        <p>Connect your API keys for one-click fetching, or upload CSV files manually. Required datasets (prices, load, RES) unlock the trade-ready model. Important datasets sharpen risk and optimizer edge. Optional datasets replace the synthetic battery twin with real telemetry.</p>
        <p style="margin-top:8px">Files land in <code style="font-size:11px;background:rgba(255,255,255,.7);padding:1px 5px;border-radius:3px;font-family:monospace">data/manual/</code> and persist between sessions. After uploading, select tomorrow's date and click <strong>Run Model</strong>.</p>
      </div>
      <div class="upload-day-badge">
        <div class="upload-day-label">Target date</div>
        <div class="upload-day-value">${escapeHtml(tomorrow)}</div>
        <div style="height:1px;background:var(--faint);width:100%;margin:6px 0"></div>
        <div class="upload-day-label">Model loaded</div>
        <div style="font-size:12px;color:var(--muted)">${escapeHtml(state.date)}</div>
      </div>
    </div>

    <div class="upload-checklist">
      <span class="upload-checklist-label">Today's coverage</span>
      ${checkItems.join("")}
    </div>

    <div class="connections-panel">
      <div class="dataset-section-title">API Connections — configure once, fetch automatically</div>
      <div class="connections-grid">

        <div class="connection-card ${entsoConnected ? "active" : ""}">
          <div class="connection-card-head">
            <div>
              <div class="connection-card-name">ENTSO-E Transparency</div>
              <div class="connection-card-desc">Free REST API covering prices, load, RES, cross-border flows, and grid outages for all of Greece. One security token covers all five datasets.</div>
            </div>
            <span class="api-card-type free">Free API</span>
          </div>
          <div class="connection-covers">
            <strong>Covers:</strong> DAM Prices · Load Forecast · RES Generation · Cross-Border Flows · Grid Outages
          </div>
          <div class="connection-form">
            <div class="connection-field">
              <label for="entso-key-input">Security Token</label>
              <input id="entso-key-input" type="password" autocomplete="off"
                placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                value="${escapeHtml(entsoConn.key)}" />
            </div>
            <div class="connection-actions">
              <button type="button" id="entso-save-btn" class="primary">Save Key</button>
              <button type="button" id="entso-test-btn" ${!entsoSaved ? "disabled" : ""}>Test Connection</button>
              ${connStatusText(entsoConn, "Key saved")}
            </div>
            <div class="connection-note">Register free at <a href="https://transparency.entsoe.eu/" target="_blank" rel="noopener">transparency.entsoe.eu</a> → My Account → API Security Token. The token is stored in your browser only.</div>
          </div>
        </div>

        <div class="connection-card active">
          <div class="connection-card-head">
            <div>
              <div class="connection-card-name">Open-Meteo Weather</div>
              <div class="connection-card-desc">Weather is fetched automatically on every pipeline run — temperature, solar irradiance, wind speed, and cloud cover for Thessaly.</div>
            </div>
            <span class="api-card-type free">Auto · Active</span>
          </div>
          <div class="connection-covers">
            <strong>Covers:</strong> Temperature · Solar Irradiance · Wind Speed · Cloud Cover · Ensemble Spread
          </div>
          <div class="connection-form">
            <div style="display:flex;align-items:center;gap:8px;padding:10px 0">
              <div style="width:8px;height:8px;border-radius:50%;background:var(--green);flex-shrink:0"></div>
              <span style="font-size:12px;font-weight:600;color:var(--green)">Connected automatically</span>
            </div>
            <div class="connection-note">No key required for standard resolution. Pipeline queries <code style="font-size:10px;background:var(--panel-3);padding:1px 4px;border-radius:3px">api.open-meteo.com</code> at latitude 39.36, longitude 22.94 (Thessaly) every run. Upgrade to Open-Meteo Pro for extended forecast horizons.</div>
            <a href="https://open-meteo.com/en/docs" target="_blank" rel="noopener" style="font-size:11px;color:var(--blue);text-decoration:none;font-weight:500">Open-Meteo documentation →</a>
          </div>
        </div>

        <div class="connection-card ${customConnected ? "active" : ""}">
          <div class="connection-card-head">
            <div>
              <div class="connection-card-name">Custom / BMS Endpoint</div>
              <div class="connection-card-desc">Internal EMS, SCADA, or vendor API for asset availability and real battery health telemetry. Replaces the synthetic twin.</div>
            </div>
            <span class="api-card-type internal">Internal</span>
          </div>
          <div class="connection-covers">
            <strong>Covers:</strong> Asset Availability · Battery Health (BMS) — raises twin confidence from 62% → 88%
          </div>
          <div class="connection-form">
            <div class="connection-field">
              <label for="custom-url-input">Endpoint URL</label>
              <input id="custom-url-input" type="url"
                placeholder="https://your-ems.internal/api/telemetry"
                value="${escapeHtml(customConn.url)}" />
            </div>
            <div class="connection-field">
              <label for="custom-token-input">Bearer Token <span style="font-weight:400;text-transform:none">(optional)</span></label>
              <input id="custom-token-input" type="password" autocomplete="off"
                placeholder="Bearer token or API key"
                value="${escapeHtml(customConn.token)}" />
            </div>
            <div class="connection-actions">
              <button type="button" id="custom-save-btn" class="primary">Save Endpoint</button>
              <button type="button" id="custom-test-btn" ${!customSaved ? "disabled" : ""}>Test</button>
              ${connStatusText(customConn, "Endpoint saved")}
            </div>
            <div class="connection-note">Supports REST (JSON or CSV response), MQTT export, or Modbus-to-HTTP bridge. The URL and token are stored in your browser only.</div>
          </div>
        </div>

      </div>
    </div>

    <div class="dataset-section">
      <div class="dataset-section-title">Required — unlock trade-ready output</div>
      <div class="dataset-card-grid cols-3">
        ${required.map((d) => renderDatasetCard(d, tomorrow)).join("")}
      </div>
    </div>

    <div class="dataset-section">
      <div class="dataset-section-title">Important — improve risk and optimizer edge</div>
      <div class="dataset-card-grid cols-3">
        ${important.map((d) => renderDatasetCard(d, tomorrow)).join("")}
      </div>
    </div>

    <div class="dataset-section">
      <div class="dataset-section-title">Optional — refine battery twin and dispatch limits</div>
      <div class="dataset-card-grid cols-2">
        ${optional.map((d) => renderDatasetCard(d, tomorrow)).join("")}
      </div>
    </div>
  `;
}

function nextDay(dateStr) {
  const d = new Date(dateStr);
  d.setDate(d.getDate() + 1);
  return d.toISOString().slice(0, 10);
}

function renderDatasetCard(ds, tomorrow) {
  const mode    = getDatasetMode(ds.key);
  const uSt     = uploadStates[ds.key] || { status: "idle" };
  const fSt     = fetchStates[ds.key]  || { status: "idle" };
  const hasApi  = ds.apiSource !== null;

  const overallStatus = uSt.status === "success" || fSt.status === "success" ? "success"
                      : uSt.status === "error"   || fSt.status === "error"   ? "error"
                      : uSt.status === "uploading" || fSt.status === "fetching" ? "uploading"
                      : "idle";

  const dropIcon  = uSt.status === "success"   ? "✓"
                  : uSt.status === "error"     ? "✕"
                  : uSt.status === "uploading" ? "⟳"
                  : "↑";
  const dropLabel = uSt.status === "uploading" ? "Uploading…"
                  : uSt.status === "success"   ? "Uploaded — drop another to replace"
                  : uSt.status === "error"     ? "Failed — drop to retry"
                  : "Drop CSV here or click to browse";
  const dropSub   = (uSt.status === "success" || uSt.status === "error")
                  ? escapeHtml(uSt.message || "") : ".csv · 96 rows recommended";

  const conn = ds.apiSource === "entso" ? apiConnections.entso : apiConnections.custom;
  const connOk = ds.apiSource && (conn?.status === "connected" || conn?.status === "saved");
  const connLabel = ds.apiSource === "entso" ? "ENTSO-E Transparency"
                  : ds.apiSource === "custom" ? "Custom / BMS Endpoint"
                  : "";
  const connBadgeCls = connOk ? "connected" : "not-configured";
  const connBadgeText = connOk ? "Connected" : "Key required";
  const fetchIcon   = fSt.status === "success"  ? "✓"
                    : fSt.status === "error"    ? "✕"
                    : fSt.status === "fetching" ? "⟳"
                    : "";
  const fetchStatusCls   = fSt.status === "idle" ? "" : fSt.status;
  const fetchStatusText  = fSt.message ? escapeHtml(fSt.message) : "";

  return `
    <div class="dataset-card">
      <div class="dataset-card-top">
        <div class="dataset-card-head">
          <div class="dataset-card-name">${escapeHtml(ds.label)}</div>
          <div style="display:flex;align-items:center;gap:7px;flex-shrink:0">
            <span class="tier-badge ${ds.tier}">${escapeHtml(ds.tier)}</span>
            <div class="upload-status-dot ${overallStatus}"></div>
          </div>
        </div>
        <div class="dataset-card-desc">${escapeHtml(ds.description)}</div>
        ${hasApi ? `
        <div style="margin-top:10px">
          <div class="mode-toggle">
            <button class="mode-tab ${mode === "manual" ? "active" : ""}" data-mode="manual" data-key="${escapeHtml(ds.key)}">Manual CSV</button>
            <button class="mode-tab ${mode === "auto"   ? "active" : ""}" data-mode="auto"   data-key="${escapeHtml(ds.key)}">Auto Fetch</button>
          </div>
        </div>` : ""}
      </div>

      ${mode === "manual" || !hasApi ? `
      <div class="dataset-source">
        <div class="source-name">Where to get it</div>
        ${ds.sourceUrl
          ? `<a class="source-url" href="${escapeHtml(ds.sourceUrl)}" target="_blank" rel="noopener">${escapeHtml(ds.source)}</a>`
          : `<div style="font-size:11px;color:var(--muted)">${escapeHtml(ds.source)}</div>`}
        ${ds.entsoRef ? `<div class="source-api-note">ENTSO-E ref: ${escapeHtml(ds.entsoRef)}</div>` : ""}
        <div class="source-api-note">${escapeHtml(ds.apiNote)}</div>
      </div>
      <div class="dataset-format">
        <div class="format-label">CSV columns</div>
        <code class="format-code">${escapeHtml(ds.format)}\n\n${escapeHtml(ds.example)}</code>
      </div>
      <div class="drop-zone ${uSt.status}" data-dataset="${escapeHtml(ds.key)}" role="button" tabindex="0" aria-label="Upload ${escapeHtml(ds.label)} CSV">
        <input type="file" accept=".csv,text/csv" class="drop-input" style="display:none" />
        <div class="drop-icon">${dropIcon}</div>
        <div class="drop-label">${dropLabel}</div>
        <div class="drop-sub drop-status ${uSt.status}">${dropSub}</div>
      </div>
      ` : `
      <div class="fetch-zone">
        <div class="fetch-source">
          <strong>${escapeHtml(connLabel)}</strong>
          <span class="fetch-conn-badge ${connBadgeCls}">${escapeHtml(connBadgeText)}</span>
        </div>
        ${ds.entsoRef ? `<div class="fetch-note">Document: ${escapeHtml(ds.entsoRef)}</div>` : ""}
        <div class="fetch-note">${escapeHtml(ds.apiNote)}</div>
        <div class="fetch-actions">
          <button type="button" class="primary fetch-btn" data-key="${escapeHtml(ds.key)}" data-source="${escapeHtml(ds.apiSource || "")}"
            ${!connOk || fSt.status === "fetching" ? "disabled" : ""}>
            ${fSt.status === "fetching" ? "Fetching…" : `Fetch for ${escapeHtml(tomorrow || "")}`}
          </button>
          ${fetchStatusText ? `<span class="fetch-status ${fetchStatusCls}">${fetchIcon ? fetchIcon + " " : ""}${fetchStatusText}</span>` : ""}
        </div>
        ${!connOk ? `<div class="fetch-note" style="color:var(--amber)">Configure and save the ${escapeHtml(connLabel)} connection above first.</div>` : ""}
      </div>
      `}
    </div>
  `;
}

function bindUploadControls() {
  const bind = (id, handler) => {
    const el = document.getElementById(id);
    if (el && !el.dataset.bound) { el.dataset.bound = "true"; el.addEventListener("click", handler); }
  };

  bind("entso-save-btn", () => {
    const key = document.getElementById("entso-key-input")?.value?.trim() || "";
    apiConnections.entso.key    = key;
    apiConnections.entso.status = key ? "saved" : "idle";
    apiConnections.entso.error  = null;
    localStorage.setItem("conn_entso_key", key);
    render();
  });

  bind("entso-test-btn", async () => {
    apiConnections.entso.status = "testing";
    apiConnections.entso.error  = null;
    render();
    try {
      const r = await fetch(`/api/test/entso?key=${encodeURIComponent(apiConnections.entso.key)}`);
      const b = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(b.error || `HTTP ${r.status}`);
      apiConnections.entso.status = "connected";
    } catch (err) {
      apiConnections.entso.status = err.message.includes("404") ? "saved" : "error";
      apiConnections.entso.error  = err.message.includes("404")
        ? "Key saved — test endpoint not yet wired up on backend"
        : err.message;
    }
    render();
  });

  bind("custom-save-btn", () => {
    const url   = document.getElementById("custom-url-input")?.value?.trim()   || "";
    const token = document.getElementById("custom-token-input")?.value?.trim() || "";
    apiConnections.custom.url    = url;
    apiConnections.custom.token  = token;
    apiConnections.custom.status = url ? "saved" : "idle";
    apiConnections.custom.error  = null;
    localStorage.setItem("conn_custom_url",   url);
    localStorage.setItem("conn_custom_token", token);
    render();
  });

  bind("custom-test-btn", async () => {
    apiConnections.custom.status = "testing";
    apiConnections.custom.error  = null;
    render();
    try {
      const r = await fetch("/api/test/custom", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: apiConnections.custom.url, token: apiConnections.custom.token }),
      });
      const b = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(b.error || `HTTP ${r.status}`);
      apiConnections.custom.status = "connected";
    } catch (err) {
      apiConnections.custom.status = err.message.includes("404") ? "saved" : "error";
      apiConnections.custom.error  = err.message.includes("404")
        ? "Endpoint saved — test route not yet wired up on backend"
        : err.message;
    }
    render();
  });

  document.querySelectorAll(".mode-tab").forEach((btn) => {
    if (btn.dataset.bound === "true") return;
    btn.dataset.bound = "true";
    btn.addEventListener("click", () => {
      setDatasetMode(btn.dataset.key, btn.dataset.mode);
      render();
    });
  });

  document.querySelectorAll(".fetch-btn").forEach((btn) => {
    if (btn.dataset.bound === "true") return;
    btn.dataset.bound = "true";
    btn.addEventListener("click", () => fetchDataset(btn.dataset.key, btn.dataset.source));
  });

  document.querySelectorAll(".drop-zone").forEach((zone) => {
    if (zone.dataset.bound === "true") return;
    zone.dataset.bound = "true";
    const input = zone.querySelector(".drop-input");
    const key   = zone.dataset.dataset;

    zone.addEventListener("click", () => {
      if (uploadStates[key]?.status !== "uploading") input.click();
    });
    zone.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); input.click(); }
    });
    zone.addEventListener("dragover", (e) => { e.preventDefault(); zone.classList.add("dragover"); });
    zone.addEventListener("dragleave", () => zone.classList.remove("dragover"));
    zone.addEventListener("drop", (e) => {
      e.preventDefault();
      zone.classList.remove("dragover");
      const file = e.dataTransfer?.files?.[0];
      if (file) uploadDataset(key, file);
    });
    input.addEventListener("change", () => {
      const file = input.files?.[0];
      if (file) uploadDataset(key, file);
      input.value = "";
    });
  });
}

async function uploadDataset(key, file) {
  uploadStates[key] = { status: "uploading", message: "Uploading…" };
  if (state.activeTab === "upload") render();
  try {
    const formData = new FormData();
    formData.append("dataset", key);
    formData.append("mode", "replace");
    formData.append("file", file);
    const result = await fetch("/api/manual/upload", { method: "POST", body: formData }).then(async (r) => {
      const body = await r.json();
      if (!r.ok) throw new Error(body.error || "Upload failed");
      return body;
    });
    uploadStates[key] = { status: "success", message: `${result.rows} rows loaded` };
  } catch (err) {
    uploadStates[key] = { status: "error", message: err.message };
  }
  if (state.activeTab === "upload") render();
}

async function fetchDataset(key, apiSource) {
  fetchStates[key] = { status: "fetching", message: "Fetching from API…" };
  if (state.activeTab === "upload") render();
  try {
    const tomorrow = nextDay(state.date);
    const conn     = apiSource === "entso" ? apiConnections.entso : apiConnections.custom;
    const body     = apiSource === "entso"
      ? { dataset: key, date: tomorrow, apiKey: conn.key }
      : { dataset: key, date: tomorrow, url: conn.url, token: conn.token };
    const r = await fetch(`/api/fetch/${encodeURIComponent(apiSource)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const result = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(result.error || `HTTP ${r.status}`);
    fetchStates[key]  = { status: "success", message: `${result.rows ?? "?"} rows fetched` };
    uploadStates[key] = { status: "success", message: `${result.rows ?? "?"} rows via ${apiSource}` };
  } catch (err) {
    fetchStates[key] = {
      status: "error",
      message: err.message.includes("404") ? "Backend fetch endpoint not yet configured" : err.message,
    };
  }
  if (state.activeTab === "upload") render();
}
function bindViewControls() {
  const uploadForm = document.getElementById("manual-upload-form");
  if (uploadForm && uploadForm.dataset.bound !== "true") {
    uploadForm.dataset.bound = "true";
    uploadForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const file = document.getElementById("upload-file").files[0];
      if (!file) return;
      const formData = new FormData(uploadForm);
      state.uploadMessage = "Uploading CSV…";
      render();
      try {
        const result = await fetch("/api/manual/upload", { method: "POST", body: formData }).then(async (response) => {
          const body = await response.json();
          if (!response.ok) throw new Error(body.error || "Upload failed.");
          return body;
        });
        state.uploadMessage = `Uploaded ${result.rows} rows into ${result.dataset}. Click Run Model for ${state.date}.`;
        await refreshAll({ runPipeline: false, quiet: true });
      } catch (error) {
        state.uploadMessage = `Upload failed: ${error.message}`;
        render();
      }
    });
  }

  const groqBtn = document.getElementById("groq-btn");
  if (groqBtn && groqBtn.dataset.bound !== "true") {
    groqBtn.dataset.bound = "true";
    groqBtn.disabled = state.groqLoading;
    groqBtn.addEventListener("click", generateGroqBrief);
  }

  const socRange = document.getElementById("current-soc-input");
  const socNumber = document.getElementById("current-soc-number");
  const applySoc = document.getElementById("apply-soc-btn");
  const autoSoc = document.getElementById("auto-soc-btn");
  if (socRange && socNumber && applySoc && autoSoc && applySoc.dataset.bound !== "true") {
    applySoc.dataset.bound = "true";
    autoSoc.dataset.bound = "true";
    const syncSoc = (value) => {
      const min = Number(socNumber.min || 0);
      const max = Number(socNumber.max || 100);
      const next = clamp(Number(value), min, max);
      state.currentSocPct = next;
      socRange.value = String(next);
      socNumber.value = String(next);
      return next;
    };
    socRange.addEventListener("input", () => syncSoc(socRange.value));
    socNumber.addEventListener("input", () => syncSoc(socNumber.value));
    applySoc.addEventListener("click", async () => {
      const next = syncSoc(socNumber.value);
      state.socOverride = true;
      localStorage.setItem("socOverride", "true");
      localStorage.setItem("currentSocPct", String(next));
      await refreshAll({ runPipeline: true });
    });
    autoSoc.addEventListener("click", async () => {
      state.socOverride = false;
      state.currentSocPct = null;
      localStorage.setItem("socOverride", "false");
      localStorage.removeItem("currentSocPct");
      await refreshAll({ runPipeline: true });
    });
  }
}

async function generateGroqBrief() {
  state.groqLoading = true;
  state.groq        = null;
  render();
  try {
    state.groq = await api(`/api/groq/explain?date=${encodeURIComponent(state.date)}`);
  } catch (error) {
    state.groq = { enabled: false, summary: `Groq explanation failed: ${error.message}` };
  } finally {
    state.groqLoading = false;
    render();
  }
}

function renderGroqBrief() {
  if (state.groqLoading) {
    return `<div class="insight">Generating Groq explanation from the current forecast, risk, dispatch, and data-quality output…</div>`;
  }
  if (!state.groq) {
    return `
      <div class="insight">Click Generate Brief to ask Groq for a concise market explanation, risk summary, and improvement checklist.</div>
      <div class="insight">The app sends model outputs to Groq; Groq is not used to invent P10/P50/P90 prices.</div>
    `;
  }
  return `<div class="insight">${escapeHtml(state.groq.summary).replaceAll("\n", "<br>")}</div>`;
}
function kpiGrid(items) {
  return `<div class="grid kpi-grid">${items.map(([label, value, unit, help, formula]) => `
    <div class="kpi">
      <div class="kpi-label">${escapeHtml(label)}</div>
      <div class="kpi-value">${escapeHtml(String(value ?? "-"))}</div>
      <div class="kpi-unit">${escapeHtml(unit || "")}</div>
      ${help    ? `<div class="kpi-help">${escapeHtml(help)}</div>` : ""}
      ${formula ? formulaBlock(formula) : ""}
    </div>
  `).join("")}</div>`;
}

function panelHeader(title, subtitle, right = "") {
  return `
    <div class="panel-header">
      <div>
        <h2 class="panel-title">${escapeHtml(title)}</h2>
        ${subtitle ? `<p class="panel-subtitle">${escapeHtml(subtitle)}</p>` : ""}
      </div>
      ${right || ""}
    </div>
  `;
}

function miniStat(label, value, unit, help, formula) {
  return `
    <div class="mini-stat">
      <div class="mini-label">${escapeHtml(label)}</div>
      <div class="mini-value">${escapeHtml(String(value))}</div>
      <div class="kpi-unit">${escapeHtml(String(unit || ""))}</div>
      ${help    ? `<div class="kpi-help">${escapeHtml(help)}</div>` : ""}
      ${formula ? formulaBlock(formula) : ""}
    </div>`;
}

function formulaBlock(formula) {
  const lines = Array.isArray(formula) ? formula : [formula];
  return `<div class="formula-box">${lines.map((line) => `<code>${escapeHtml(line)}</code>`).join("")}</div>`;
}

function formulaList(items) {
  return `<div class="formula-list">${items.map(([label, formula, note]) => `
    <div class="formula-row">
      <strong>${escapeHtml(label)}</strong>
      ${formulaBlock(formula)}
      ${note ? `<span>${escapeHtml(note)}</span>` : ""}
    </div>
  `).join("")}</div>`;
}

function legend(items) {
  return `<div class="legend">${items.map(([label, color]) =>
    `<span class="legend-item" style="color:${color}"><span class="swatch"></span>${escapeHtml(label)}</span>`
  ).join("")}</div>`;
}

function riskRow(label, score, detail, rawScore) {
  const status = rawScore >= 0.7 ? "bad" : rawScore >= 0.45 ? "warn" : "good";
  return `
    <div class="risk-row">
      <strong>${escapeHtml(label)}</strong>
      <div>
        <div style="font-size:12px">${escapeHtml(detail)}</div>
        <div class="bar" style="margin-top:7px">
          <div class="bar-fill ${status}" style="width:${clamp((rawScore || 0) * 100, 0, 100)}%"></div>
        </div>
      </div>
      <span class="risk-score">${escapeHtml(score)}</span>
    </div>
  `;
}

function renderInsights() {
  const { summary, forecasts, forecastErrors } = state.data;
  const warnings   = summary.dataQuality?.warnings || [];
  const topSpike   = maxBy(forecasts,      (row) => row.probabilitySpike || 0);
  const topPrice   = maxBy(forecasts,      (row) => row.priceP50 || 0);
  const topAnomaly = maxBy(forecastErrors, (row) => row.anomalyScore || 0);
  const items = [
    `Gate view: ${summary.dataQuality?.isTradeReady ? "inputs are marked trade-ready" : "output is demo-grade until missing/synthetic inputs are replaced"}.`,
    `Price edge: highest P50 interval is MTU ${topPrice?.mtu ?? "-"} at ${fmt(topPrice?.priceP50, 1)} EUR/MWh; max spike risk is MTU ${topSpike?.mtu ?? "-"} at ${pct(topSpike?.probabilitySpike)}.`,
    `Forecast-error edge: highest residual-demand anomaly is MTU ${topAnomaly?.mtu ?? "-"} at ${pct(topAnomaly?.anomalyScore)}.`,
    ...warnings.slice(0, 2),
  ];
  return items.map((item) => `<div class="insight">${escapeHtml(item)}</div>`).join("");
}

function renderDispatchExplanations() {
  const rows = state.data.dispatch.filter((row) => row.action && row.action !== "idle").slice(0, 6);
  if (!rows.length) return `<div class="insight">No active charge or discharge intervals in this schedule.</div>`;
  return rows.map((row) =>
    `<div class="insight"><strong>MTU ${row.mtu} ${escapeHtml(row.action)}:</strong> ${escapeHtml(row.explanation || "Optimizer selected this interval.")}</div>`
  ).join("");
}

function renderRiskExplanation(forecasts, forecastErrors) {
  const topAnomaly      = maxBy(forecastErrors, (row) => row.anomalyScore || 0);
  const topUnder        = maxBy(forecastErrors, (row) => row.probabilityUnderforecast || 0);
  const topOver         = maxBy(forecastErrors, (row) => row.probabilityOverforecast || 0);
  const highRiskCount   = forecastErrors.filter((row) => (row.anomalyScore || 0) >= 0.65).length;
  const avgResidualError= avgAbs(forecastErrors, (row) => row.expectedResidualDemandErrorMw);
  const avgStress       = avg(forecasts, (row) => row.stressScore);
  const avgSurplus      = avg(forecasts, (row) => row.surplusScore);
  const regime = avgStress > avgSurplus * 1.25 ? "stress-led day"
               : avgSurplus > avgStress * 1.25 ? "surplus-led day"
               : "mixed regime day";
  return `
    <div class="explain-grid">
      <div class="explain-box"><strong>${highRiskCount} high-risk MTUs</strong><span>Intervals with anomaly score above 65%. These are the first places to review official load, RES, and weather assumptions.</span></div>
      <div class="explain-box"><strong>MTU ${topAnomaly?.mtu ?? "-"} is highest anomaly</strong><span>${pct(topAnomaly?.anomalyScore)} anomaly with ${fmt(topAnomaly?.expectedResidualDemandErrorMw, 0)} MW expected residual-demand error.</span></div>
      <div class="explain-box"><strong>${fmt(avgResidualError, 0)} MW avg error</strong><span>Average absolute residual-demand error. Larger values mean price can move away from the central forecast.</span></div>
      <div class="explain-box"><strong>Underforecast peak: MTU ${topUnder?.mtu ?? "-"}</strong><span>${pct(topUnder?.probabilityUnderforecast)} chance actual residual demand is higher than forecast, which can lift prices.</span></div>
      <div class="explain-box"><strong>Overforecast peak: MTU ${topOver?.mtu ?? "-"}</strong><span>${pct(topOver?.probabilityOverforecast)} chance actual residual demand is lower than forecast, which can pressure prices down.</span></div>
      <div class="explain-box"><strong>${regime}</strong><span>Stress average ${pct(avgStress)} vs surplus average ${pct(avgSurplus)}. This explains whether scarcity or renewable pressure dominates.</span></div>
    </div>
  `;
}

function renderDispatchExplanation(dispatch, summary) {
  const chargeRows    = dispatch.filter((row) => (row.chargeMw    || 0) > 0.05);
  const dischargeRows = dispatch.filter((row) => (row.dischargeMw || 0) > 0.05);
  const maxCharge     = maxBy(dispatch, (row) => row.chargeMw    || 0);
  const maxDischarge  = maxBy(dispatch, (row) => row.dischargeMw || 0);
  const bestRevenue   = maxBy(dispatch, (row) => row.expectedRevenueEur || 0);
  const startSoc      = dispatch[0]?.socMwh;
  const endSoc        = dispatch.at(-1)?.socMwh;
  const minSoc        = min(dispatch, (row) => row.socMwh);
  const maxSoc        = max(dispatch, (row) => row.socMwh);
  const netMwh        = (summary.dischargeMwh || 0) - (summary.chargeMwh || 0);
  const posture       = dischargeRows.length > chargeRows.length ? "net seller posture"
                      : chargeRows.length > dischargeRows.length  ? "charging-heavy posture"
                      : "balanced posture";
  return `
    <div class="explain-grid">
      <div class="explain-box"><strong>${chargeRows.length} charge MTUs</strong><span>The optimizer buys energy in low-value intervals. Total charged energy is ${fmt(summary.chargeMwh, 1)} MWh.</span></div>
      <div class="explain-box"><strong>${dischargeRows.length} discharge MTUs</strong><span>The optimizer sells energy in high-value intervals. Total discharged energy is ${fmt(summary.dischargeMwh, 1)} MWh.</span></div>
      <div class="explain-box"><strong>${posture}</strong><span>Net energy position is ${fmt(netMwh, 1)} MWh before considering efficiency and end-of-day SOC constraints.</span></div>
      <div class="explain-box"><strong>Max charge: MTU ${maxCharge?.mtu ?? "-"}</strong><span>${fmt(maxCharge?.chargeMw, 1)} MW at forecast price ${fmt(maxCharge?.priceForecast, 1)} EUR/MWh.</span></div>
      <div class="explain-box"><strong>Max discharge: MTU ${maxDischarge?.mtu ?? "-"}</strong><span>${fmt(maxDischarge?.dischargeMw, 1)} MW at forecast price ${fmt(maxDischarge?.priceForecast, 1)} EUR/MWh.</span></div>
      <div class="explain-box"><strong>SOC ${fmt(startSoc, 1)} → ${fmt(endSoc, 1)} MWh</strong><span>State of charge ranges from ${fmt(minSoc, 1)} to ${fmt(maxSoc, 1)} MWh; best interval revenue is MTU ${bestRevenue?.mtu ?? "-"}.</span></div>
    </div>
  `;
}

function renderSourceCoverage(expanded = false) {
  const sources = state.data?.summary?.sources || {};
  const entries = Object.entries(sources);
  if (!entries.length) return `<div class="empty">No source metadata reported.</div>`;
  return entries.map(([name, counts]) => {
    const count     = sourceCount(counts);
    const missing   = (counts && counts.missing) || 0;
    const synthetic = Object.entries(counts || {}).filter(([key]) => key.includes("synthetic")).reduce((acc, [, value]) => acc + Number(value || 0), 0);
    const pctValue  = clamp((count / 96) * 100, 0, 100);
    const status    = missing || synthetic ? "warn" : "good";
    const details   = Object.entries(counts || {}).map(([key, value]) => `${key}:${value}`).join(" ");
    return `
      <div class="source-row">
        <strong>${escapeHtml(titleCase(name))}</strong>
        <div>
          <div class="bar"><div class="bar-fill ${status}" style="width:${pctValue}%"></div></div>
          ${expanded ? `<div class="panel-subtitle">${escapeHtml(details)}</div>` : ""}
          ${formulaBlock(`${titleCase(name)} coverage = ${count}/96 non-missing source intervals`)}
        </div>
        <span class="chip">${count}/96</span>
      </div>
    `;
  }).join("");
}
function drawPriceChart(id, forecasts, options = {}) {
  const series = [
    { label: "P10",             color: colors.cyan,   values: forecasts.map((r) => r.priceP10) },
    { label: "P50",             color: colors.blue,   values: forecasts.map((r) => r.priceP50), width: 2.5 },
    { label: "P90",             color: colors.amber,  values: forecasts.map((r) => r.priceP90) },
    { label: "Discharge value", color: colors.red,    values: forecasts.map((r) => r.adjustedDischargeValue), dashed: true },
    { label: "Charge cost",     color: colors.green,  values: forecasts.map((r) => r.adjustedChargeCost),    dashed: true },
  ];
  if (options.includeThermal) {
    series.push({ label: "Thermal MC", color: colors.gray, values: forecasts.map((r) => r.thermalMarginalCost), dashed: true });
  }
  drawLineSeries(id, series, { yLabel: "EUR/MWh" });
}

function drawRiskChart(id, forecasts, forecastErrors) {
  drawLineSeries(id, [
    { label: "Anomaly",      color: colors.red,    values: forecastErrors.map((r) => (r.anomalyScore || 0) * 100), width: 2.2 },
    { label: "Underforecast",color: colors.violet, values: forecastErrors.map((r) => (r.probabilityUnderforecast || 0) * 100) },
    { label: "Stress",       color: colors.amber,  values: forecasts.map((r) => (r.stressScore || 0) * 100) },
    { label: "Solar spread", color: colors.cyan,   values: forecastErrors.map((r) => Math.min((r.ensembleSolarSpreadWm2 || 0) / 10, 100)), dashed: true },
  ], { min: 0, max: 100, yLabel: "%" });
}

function drawDispatchChart(id, dispatch, battery = {}) {
  const canvas = setupCanvas(id);
  if (!canvas) return;
  const { ctx, width, height } = canvas;
  const pad    = { left: 48, right: 16, top: 18, bottom: 34 };
  const plotW  = width  - pad.left - pad.right;
  const plotH  = height - pad.top  - pad.bottom;
  const mid    = pad.top + plotH / 2;
  const maxPower = Math.max(1, ...dispatch.map((r) => Math.max(r.chargeMw || 0, r.dischargeMw || 0)));
  const maxSoc   = Math.max(battery.capacityMwh || 1, ...dispatch.map((r) => r.socMwh || 0));

  drawGrid(ctx, width, height, pad, -maxPower, maxPower);
  dispatch.forEach((row, index) => {
    const x    = pad.left + index * (plotW / 96);
    const barW = Math.max(2, plotW / 130);
    if (row.chargeMw > 0) {
      const h = (row.chargeMw / maxPower) * (plotH / 2 - 8);
      ctx.fillStyle = colors.green;
      ctx.fillRect(x, mid, barW, h);
    }
    if (row.dischargeMw > 0) {
      const h = (row.dischargeMw / maxPower) * (plotH / 2 - 8);
      ctx.fillStyle = colors.amber;
      ctx.fillRect(x, mid - h, barW, h);
    }
  });

  ctx.beginPath();
  ctx.strokeStyle = colors.blue;
  ctx.lineWidth   = 2.2;
  dispatch.forEach((row, index) => {
    const x = pad.left + index * (plotW / 95);
    const y = pad.top + plotH - ((row.socMwh || 0) / maxSoc) * plotH;
    if (index === 0) ctx.moveTo(x, y);
    else             ctx.lineTo(x, y);
  });
  ctx.stroke();
  labelAxes(ctx, pad, width, height, `${fmt(maxPower, 0)} MW`, `-${fmt(maxPower, 0)} MW`);
}

function drawLineSeries(id, series, options = {}) {
  const canvas = setupCanvas(id);
  if (!canvas) return;
  const { ctx, width, height } = canvas;
  const pad    = { left: 48, right: 16, top: 18, bottom: 34 };
  const values = series.flatMap((s) => s.values).filter(Number.isFinite);
  if (!values.length) {
    drawEmpty(ctx, width, height);
    return;
  }
  const minVal = options.min ?? Math.min(...values, 0);
  const maxVal = options.max ?? Math.max(...values, 1);
  drawGrid(ctx, width, height, pad, minVal, maxVal);

  series.forEach((item) => {
    ctx.beginPath();
    ctx.strokeStyle = item.color;
    ctx.lineWidth   = item.width || 1.8;
    ctx.setLineDash(item.dashed ? [6, 5] : []);
    item.values.forEach((value, index) => {
      if (!Number.isFinite(value)) return;
      const x = pad.left + index * ((width - pad.left - pad.right) / Math.max(1, item.values.length - 1));
      const y = pad.top  + (1 - (value - minVal) / (maxVal - minVal || 1)) * (height - pad.top - pad.bottom);
      if (index === 0) ctx.moveTo(x, y);
      else             ctx.lineTo(x, y);
    });
    ctx.stroke();
    ctx.setLineDash([]);
  });
  labelAxes(ctx, pad, width, height, fmt(maxVal, 0), fmt(minVal, 0));
}

function setupCanvas(id) {
  const canvas = document.getElementById(id);
  if (!canvas) return null;
  const rect = canvas.getBoundingClientRect();
  const dpr  = window.devicePixelRatio || 1;
  canvas.width  = Math.max(320, Math.floor(rect.width  * dpr));
  canvas.height = Math.max(220, Math.floor(rect.height * dpr));
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, rect.width, rect.height);
  return { canvas, ctx, width: rect.width, height: rect.height };
}

function drawGrid(ctx, width, height, pad, minVal, maxVal) {
  ctx.strokeStyle = colors.faint;
  ctx.lineWidth   = 1;
  ctx.font        = "11px Inter, Arial";
  ctx.fillStyle   = "#8a9bab";
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + i * ((height - pad.top - pad.bottom) / 4);
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(width - pad.right, y);
    ctx.stroke();
  }
  for (let hour = 0; hour <= 24; hour += 6) {
    const x = pad.left + (hour / 24) * (width - pad.left - pad.right);
    ctx.beginPath();
    ctx.moveTo(x, pad.top);
    ctx.lineTo(x, height - pad.bottom);
    ctx.stroke();
    ctx.fillText(`${String(hour).padStart(2, "0")}:00`, x - 14, height - 12);
  }
  ctx.strokeStyle = "#b0bec9";
  ctx.beginPath();
  ctx.moveTo(pad.left, pad.top);
  ctx.lineTo(pad.left, height - pad.bottom);
  ctx.lineTo(width - pad.right, height - pad.bottom);
  ctx.stroke();
}

function labelAxes(ctx, pad, width, height, maxLabel, minLabel) {
  ctx.fillStyle = "#8a9bab";
  ctx.font      = "11px Inter, Arial";
  ctx.fillText(maxLabel, 8, pad.top + 4);
  ctx.fillText(minLabel, 8, height - pad.bottom + 4);
}

function drawEmpty(ctx, width, height) {
  ctx.fillStyle = "#8a9bab";
  ctx.font      = "13px Inter, Arial";
  ctx.fillText("No chart data available.", 24, 40);
}
function latestStoredDate(files) {
  const dates = files
    .map((name) => (name.match(/^summary-(\d{4}-\d{2}-\d{2})\.json$/) || [])[1])
    .filter(Boolean)
    .sort();
  return dates.at(-1);
}

function sourceCount(counts) {
  return Object.entries(counts || {})
    .filter(([key]) => key !== "missing" && !key.includes("synthetic"))
    .reduce((acc, [, value]) => acc + Number(value || 0), 0);
}

function sourceSyntheticCount(counts) {
  return Object.entries(counts || {})
    .filter(([key]) => key.includes("synthetic"))
    .reduce((acc, [, value]) => acc + Number(value || 0), 0);
}
function fmt(value, digits = 1) {
  if (!Number.isFinite(Number(value))) return "-";
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function pct(value) {
  if (!Number.isFinite(Number(value))) return "-";
  return `${Math.round(Number(value) * 100)}%`;
}
function avg(rows, fn) {
  const values = rows.map(fn).filter(Number.isFinite);
  return values.length ? values.reduce((a, b) => a + b, 0) / values.length : NaN;
}

function avgAbs(rows, fn) {
  const values = rows.map((row) => Math.abs(fn(row))).filter(Number.isFinite);
  return values.length ? values.reduce((a, b) => a + b, 0) / values.length : NaN;
}

function max(rows, fn) {
  const values = rows.map(fn).filter(Number.isFinite);
  return values.length ? Math.max(...values) : NaN;
}

function min(rows, fn) {
  const values = rows.map(fn).filter(Number.isFinite);
  return values.length ? Math.min(...values) : NaN;
}

function sum(rows, fn) {
  return rows.map(fn).filter(Number.isFinite).reduce((a, b) => a + b, 0);
}

function maxBy(rows, fn) {
  return rows.reduce((best, row) => (best == null || fn(row) > fn(best) ? row : best), null);
}

function firstFinite(rows, fn) {
  for (const row of rows) {
    const value = fn(row);
    if (Number.isFinite(value)) return value;
  }
  return NaN;
}

function clamp(value, lo, hi) {
  return Math.min(hi, Math.max(lo, value));
}
function formatTime(value) {
  return new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", timeZoneName: "short" });
}

function formatMtuTime(timestamp, mtu) {
  if (timestamp) {
    return new Date(timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  }
  const minutes = (Number(mtu) || 0) * 15;
  return `${String(Math.floor(minutes / 60)).padStart(2, "0")}:${String(minutes % 60).padStart(2, "0")}`;
}
function titleCase(value) {
  return String(value).replace(/([A-Z])/g, " $1").replace(/^./, (c) => c.toUpperCase());
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

window.addEventListener("resize", () => {
  if (state.data) render();
});

