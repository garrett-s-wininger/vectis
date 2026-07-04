const data = JSON.parse(document.getElementById("perf-data").textContent);
const state = { benchmark: "all", metric: "" };
const results = data.results || [];
const pgStats = data.pg_stat_statements || [];
const benchmarkFilter = document.getElementById("benchmark-filter");
const metricFilter = document.getElementById("metric-filter");
const queryFilter = document.getElementById("query-filter");
const pgControls = document.getElementById("pg-controls");
const pgFilterCount = document.getElementById("pg-filter-count");

function metricNumber(metric) {
  const value = Number(metric.value);
  return Number.isFinite(value) ? value : null;
}

function allMetricUnits() {
  const units = new Set();
  results.forEach(result => result.metrics.forEach(metric => units.add(metric.unit)));
  return Array.from(units).sort((a, b) => formatUnit(a).localeCompare(formatUnit(b)));
}

function selectedResults() {
  if (state.benchmark === "all") return results;
  return results.filter(result => result.name === state.benchmark);
}

function finalPGStats() {
  const maxIterations = new Map();
  pgStats.forEach(stat => {
    const key = (stat.database || "") + "\n" + stat.benchmark;
    maxIterations.set(key, Math.max(maxIterations.get(key) || 0, stat.iterations || 0));
  });
  return pgStats.filter(stat => {
    const key = (stat.database || "") + "\n" + stat.benchmark;
    return (stat.iterations || 0) === maxIterations.get(key);
  });
}

function formatUnit(unit) {
  return unit.replace(/_/g, " ");
}

function formatPhase(phase) {
  return phase.replace(/_/g, " ");
}

function formatValue(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return value;
  if (Math.abs(number) >= 1000) return number.toLocaleString(undefined, { maximumFractionDigits: 1 });
  if (Math.abs(number) >= 10) return number.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return number.toLocaleString(undefined, { maximumFractionDigits: 4 });
}

function esc(value) {
  return String(value === null || value === undefined ? "" : value).replace(/[&<>"']/g, ch => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;"
  }[ch]));
}

function shortName(name) {
  return name.replace(/^Benchmark/, "");
}

function displayBenchmarkName(name) {
  return shortName(name).replace(/_/g, " / ").replace(/([a-z0-9])([A-Z])/g, "$1 $2");
}

function compactName(name, max) {
  if (name.length <= max) return name;
  const keep = Math.max(2, Math.floor((max - 3) / 2));
  return name.slice(0, keep) + "..." + name.slice(name.length - keep);
}

function metricByUnit(result, unit) {
  return result.metrics.find(metric => metric.unit === unit);
}

function numericMetricByUnit(result, unit) {
  const metric = metricByUnit(result, unit);
  if (!metric) return null;
  const value = metricNumber(metric);
  if (value === null) return null;
  return { metric, value };
}

function primaryMetric(result) {
  return result.metrics.find(metric => metric.unit.endsWith("/s")) ||
    result.metrics.find(metric => metric.unit === "ns/op") ||
    result.metrics[0];
}

function latencyMatch(unit) {
  return unit.match(/^(.*)_p(50|95|99)_ms$/);
}

function preferredLatency(result) {
  const preferred = [
    "dequeued_to_terminal",
    "trigger_to_terminal",
    "accepted_to_terminal",
    "claimed_to_terminal",
    "execute",
    "log_flush"
  ];
  for (const phase of preferred) {
    const metric = metricByUnit(result, phase + "_p95_ms") || metricByUnit(result, phase + "_p50_ms");
    if (metric) return metric;
  }
  return result.metrics.find(metric => latencyMatch(metric.unit));
}

function metricCategory(unit) {
  if (unit.endsWith("/s")) return "throughput";
  if (unit === "ns/op") return "cost";
  if (unit === "B/op" || unit === "allocs/op") return "memory";
  if (latencyMatch(unit)) return "latency";
  if (unit.endsWith("_count") || unit === "total_runs" || unit === "worker_count" || unit === "trigger_clients") return "counts";
  return "other";
}

function metricClass(unit) {
  return metricCategory(unit).replace("throughput", "");
}

function selectedMetricRows() {
  const rows = [];
  selectedResults().forEach(result => {
    const metric = metricByUnit(result, state.metric);
    if (!metric) return;
    const value = metricNumber(metric);
    if (value === null) return;
    rows.push({
      label: displayBenchmarkName(result.name),
      value,
      unit: metric.unit,
      className: metricClass(metric.unit)
    });
  });
  rows.sort((a, b) => b.value - a.value);
  return rows;
}

function metricRowsForCards(result) {
  const rows = [];
  const primary = primaryMetric(result);
  if (primary) rows.push({ label: formatUnit(primary.unit), metric: primary, className: metricClass(primary.unit) });
  const cost = metricByUnit(result, "ns/op");
  if (cost && cost !== primary) rows.push({ label: "nanoseconds per op", metric: cost, className: "cost" });
  const latency = preferredLatency(result);
  if (latency && latency !== primary) rows.push({ label: formatUnit(latency.unit), metric: latency, className: "latency" });
  const bytes = metricByUnit(result, "B/op");
  if (bytes) rows.push({ label: "bytes per op", metric: bytes, className: "memory" });
  const allocs = metricByUnit(result, "allocs/op");
  if (allocs) rows.push({ label: "allocs per op", metric: allocs, className: "memory" });
  return rows.slice(0, 5);
}

function latencyGroups() {
  const groups = new Map();
  selectedResults().forEach(result => {
    result.metrics.forEach(metric => {
      const match = latencyMatch(metric.unit);
      if (!match) return;
      const value = metricNumber(metric);
      if (value === null) return;
      const phase = match[1];
      const percentile = "p" + match[2];
      const key = result.name + "\n" + phase;
      if (!groups.has(key)) {
        groups.set(key, { benchmark: result.name, phase, values: {} });
      }
      groups.get(key).values[percentile] = value;
    });
  });
  return Array.from(groups.values()).sort((a, b) => {
    const left = Math.max(a.values.p99 || 0, a.values.p95 || 0, a.values.p50 || 0);
    const right = Math.max(b.values.p99 || 0, b.values.p95 || 0, b.values.p50 || 0);
    return right - left;
  });
}

function renderMetadata() {
  const metadata = data.metadata || {};
  const dirty = metadata.git_dirty ? " dirty" : "";
  const rows = [
    ["Suite", metadata.suite],
    ["Package", metadata.pkg],
    ["Commit", (metadata.git_commit || "") + dirty],
    ["Go", metadata.go_version],
    ["CPU", metadata.cpu],
    ["Started", metadata.started_at],
    ["Duration", String(metadata.duration_seconds || 0) + "s"],
    ["Status", metadata.status]
  ].filter(([, value]) => value !== undefined && value !== "");
  document.getElementById("metadata").innerHTML = rows.map(([key, value]) =>
    "<span><strong>" + esc(key) + ":</strong> " + esc(value) + "</span>"
  ).join("");
}

function renderRunSummary() {
  const metadata = data.metadata || {};
  const throughput = [];
  const p95 = [];
  results.forEach(result => {
    result.metrics.forEach(metric => {
      const value = metricNumber(metric);
      if (value === null) return;
      if (metric.unit.endsWith("/s")) throughput.push({ result, metric, value });
      if (metric.unit.endsWith("_p95_ms")) p95.push({ result, metric, value });
    });
  });
  throughput.sort((a, b) => b.value - a.value);
  p95.sort((a, b) => b.value - a.value);
  const tiles = [
    ["Suite", metadata.suite || "-", "status " + String(metadata.status || 0)],
    ["Benchmarks", String(results.length), (metadata.pkg || "")],
    ["Best throughput", throughput[0] ? formatValue(throughput[0].value) : "-", throughput[0] ? formatUnit(throughput[0].metric.unit) : ""],
    ["Slowest p95", p95[0] ? formatValue(p95[0].value) + " ms" : "-", p95[0] ? formatUnit(p95[0].metric.unit) : ""]
  ];
  document.getElementById("run-summary").innerHTML = tiles.map(tile =>
    "<div class=\"run-tile\">" +
    "<div class=\"label\">" + esc(tile[0]) + "</div>" +
    "<div class=\"value\">" + esc(tile[1]) + "</div>" +
    "<div class=\"sub\">" + esc(tile[2]) + "</div>" +
    "</div>"
  ).join("");
}

function renderFilters() {
  benchmarkFilter.innerHTML = "<option value=\"all\">All benchmarks</option>" + results.map(result =>
    "<option value=\"" + esc(result.name) + "\">" + esc(displayBenchmarkName(result.name)) + "</option>"
  ).join("");
  const units = allMetricUnits();
  const preferred = units.find(unit => unit.endsWith("/s")) ||
    units.find(unit => unit.endsWith("_p95_ms")) ||
    units.find(unit => unit === "ns/op") ||
    units[0] ||
    "";
  state.metric = state.metric || preferred;
  metricFilter.innerHTML = units.map(unit =>
    "<option value=\"" + esc(unit) + "\">" + esc(formatUnit(unit)) + "</option>"
  ).join("");
  metricFilter.value = state.metric;
  const hasPGStats = pgStats.length > 0;
  pgControls.classList.toggle("is-hidden", !hasPGStats);
  queryFilter.disabled = !hasPGStats;
  queryFilter.placeholder = hasPGStats ? "Filter statement text" : "No Postgres stats";
}

function renderBenchmarkOverview() {
  const cards = [];
  const selected = selectedResults();
  const el = document.getElementById("benchmark-overview");
  el.className = "benchmark-list" + (selected.length === 1 ? " single" : "");
  selected.forEach(result => {
    const chips = metricRowsForCards(result).map(row =>
      "<div class=\"metric-chip " + esc(row.className) + "\">" +
      "<div class=\"label\">" + esc(row.label) + "</div>" +
      "<div class=\"value\">" + esc(formatValue(row.metric.value)) + "</div>" +
      "<div class=\"sub\">" + esc(formatUnit(row.metric.unit)) + "</div>" +
      "</div>"
    );
    cards.push(
      "<article class=\"benchmark-card\">" +
      "<div class=\"benchmark-title\">" +
      "<div class=\"benchmark-name\" title=\"" + esc(result.name) + "\">" + esc(displayBenchmarkName(result.name)) + "</div>" +
      "<div class=\"badge\">" + esc(result.iterations) + " iterations</div>" +
      "</div>" +
      "<div class=\"metric-chip-grid\">" + chips.join("") + "</div>" +
      "</article>"
    );
  });
  el.innerHTML = cards.join("") || "<div class=\"empty\">No benchmark rows parsed.</div>";
}

function metricFill(className) {
  if (className === "cost") return "var(--accent-2)";
  if (className === "memory") return "var(--warn)";
  if (className === "latency") return "var(--accent-3)";
  return "var(--accent)";
}

function niceMax(value) {
  if (!Number.isFinite(value) || value <= 0) return 1;
  const magnitude = Math.pow(10, Math.floor(Math.log10(value)));
  const scaled = value / magnitude;
  const nice = scaled <= 1 ? 1 : scaled <= 2 ? 2 : scaled <= 5 ? 5 : 10;
  return nice * magnitude;
}

function tickValues(max, count) {
  const ticks = [];
  for (let i = 0; i <= count; i += 1) ticks.push((max / count) * i);
  return ticks;
}

function chartLayout(target, kind) {
  const el = document.getElementById(target);
  const measuredWidth = el ? Math.floor(el.getBoundingClientRect().width) : 0;
  const width = Math.max(320, measuredWidth || 900);
  const compact = width < 620;
  const tight = width < 420;
  let left = kind === "pg" ? 300 : kind === "latency" ? 250 : 220;
  let right = kind === "pg" ? 100 : 92;
  let labelMax = kind === "pg" ? 56 : 34;
  let ticks = 4;
  let showTrack = true;

  if (compact) {
    left = kind === "pg" ? 124 : kind === "latency" ? 112 : 92;
    right = kind === "selected" ? 62 : 72;
    labelMax = kind === "pg" ? 22 : kind === "latency" ? 12 : 16;
    ticks = 2;
    showTrack = false;
  }
  if (tight) {
    left = kind === "pg" ? 106 : kind === "latency" ? 96 : 80;
    right = kind === "selected" ? 58 : 68;
    labelMax = kind === "pg" ? 17 : kind === "latency" ? 9 : 12;
  }

  const margin = { top: 18, right, bottom: compact ? 30 : 34, left };
  return {
    width,
    compact,
    tight,
    margin,
    innerWidth: Math.max(60, width - margin.left - margin.right),
    labelMax,
    ticks,
    showTrack
  };
}

function chartValueAttributes(layout) {
  if (!layout.compact) {
    return { x: layout.width - layout.margin.right + 12, anchor: "start" };
  }
  return { x: layout.width - 2, anchor: "end" };
}

function renderHorizontalBars(target, rows, emptyText) {
  const el = document.getElementById(target);
  if (!rows.length) {
    el.innerHTML = "<div class=\"empty\">" + esc(emptyText) + "</div>";
    return;
  }
  const visibleRows = rows.slice(0, 24);
  const layout = chartLayout(target, "selected");
  const width = layout.width;
  const margin = layout.margin;
  const rowHeight = 34;
  const innerWidth = layout.innerWidth;
  const max = niceMax(Math.max(...visibleRows.map(row => row.value), 0));
  const height = margin.top + margin.bottom + visibleRows.length * rowHeight;
  const ticks = tickValues(max, layout.ticks);
  const valueAttrs = chartValueAttributes(layout);
  const grid = ticks.map(tick => {
    const x = margin.left + (tick / max) * innerWidth;
    return "<line class=\"grid-line\" x1=\"" + x + "\" y1=\"" + margin.top + "\" x2=\"" + x + "\" y2=\"" + (height - margin.bottom + 4) + "\"></line>" +
      "<text class=\"axis-label\" x=\"" + x + "\" y=\"" + (height - 10) + "\" text-anchor=\"middle\">" + esc(formatValue(tick)) + "</text>";
  }).join("");
  const bars = visibleRows.map((row, index) => {
    const y = margin.top + index * rowHeight + 6;
    const barWidth = Math.max(3, (row.value / max) * innerWidth);
    const label = compactName(row.label, layout.labelMax);
    const track = layout.showTrack ? "<rect class=\"bar-bg\" x=\"" + margin.left + "\" y=\"" + y + "\" width=\"" + innerWidth + "\" height=\"20\" rx=\"5\"></rect>" : "";
    return "<g>" +
      "<text class=\"bar-label\" x=\"0\" y=\"" + (y + 15) + "\" title=\"" + esc(row.label) + "\">" + esc(label) + "</text>" +
      track +
      "<rect class=\"bar\" x=\"" + margin.left + "\" y=\"" + y + "\" width=\"" + barWidth + "\" height=\"20\" rx=\"5\" fill=\"" + metricFill(row.className || "") + "\"></rect>" +
      "<text class=\"chart-value\" x=\"" + valueAttrs.x + "\" y=\"" + (y + 15) + "\" text-anchor=\"" + valueAttrs.anchor + "\">" + esc(formatValue(row.value)) + "</text>" +
      "</g>";
  }).join("");
  el.innerHTML = "<div class=\"viz\"><svg viewBox=\"0 0 " + width + " " + height + "\" role=\"img\" aria-label=\"" + esc(formatUnit(visibleRows[0].unit || "")) + " chart\" preserveAspectRatio=\"xMinYMin meet\">" +
    grid +
    "<line class=\"axis-line\" x1=\"" + margin.left + "\" y1=\"" + (height - margin.bottom + 4) + "\" x2=\"" + (width - margin.right) + "\" y2=\"" + (height - margin.bottom + 4) + "\"></line>" +
    bars +
    "</svg></div>" +
    (rows.length > visibleRows.length ? "<div class=\"note\">Showing the 24 largest rows for this metric.</div>" : "");
}

function renderSelectedMetric() {
  document.getElementById("selected-metric-label").textContent = formatUnit(state.metric || "");
  renderHorizontalBars("metric-chart", selectedMetricRows(), "No numeric metrics for this selection.");
}

function renderLatencyBreakdown() {
  const groups = latencyGroups();
  if (!groups.length) {
    document.getElementById("latency-breakdown").innerHTML = "<div class=\"empty\">No latency metrics for this selection.</div>";
    return;
  }
  const visibleGroups = groups.slice(0, 16);
  const max = niceMax(Math.max(...visibleGroups.flatMap(group => [group.values.p50 || 0, group.values.p95 || 0, group.values.p99 || 0]), 0));
  const layout = chartLayout("latency-breakdown", "latency");
  const width = layout.width;
  const margin = layout.margin;
  const innerWidth = layout.innerWidth;
  const rowHeight = 24;
  const groupGap = 14;
  const labelRowHeight = layout.compact ? 18 : 0;
  let cursor = margin.top;
  const ticks = tickValues(max, layout.ticks);
  const valueAttrs = chartValueAttributes(layout);
  const gridHeight = visibleGroups.reduce((height, group) => {
    const count = ["p50", "p95", "p99"].filter(percentile => group.values[percentile] !== undefined).length;
    return height + labelRowHeight + count * rowHeight + groupGap;
  }, margin.top);
  const height = gridHeight + margin.bottom;
  const grid = ticks.map(tick => {
    const x = margin.left + (tick / max) * innerWidth;
    return "<line class=\"grid-line\" x1=\"" + x + "\" y1=\"" + margin.top + "\" x2=\"" + x + "\" y2=\"" + (height - margin.bottom + 4) + "\"></line>" +
      "<text class=\"axis-label\" x=\"" + x + "\" y=\"" + (height - 10) + "\" text-anchor=\"middle\">" + esc(formatValue(tick)) + "</text>";
  }).join("");
  const includeBenchmark = selectedResults().length > 1;
  const rendered = visibleGroups.map(group => {
    const label = includeBenchmark ? displayBenchmarkName(group.benchmark) + " / " + formatPhase(group.phase) : formatPhase(group.phase);
    const groupStart = cursor;
    cursor += labelRowHeight;
    const bars = ["p50", "p95", "p99"].map(percentile => {
      const value = group.values[percentile];
      if (value === undefined) return "";
      const y = cursor + 4;
      cursor += rowHeight;
      const barWidth = Math.max(3, (value / max) * innerWidth);
      const track = layout.showTrack ? "<rect class=\"bar-bg\" x=\"" + margin.left + "\" y=\"" + y + "\" width=\"" + innerWidth + "\" height=\"17\" rx=\"5\"></rect>" : "";
      return "<g>" +
        "<text class=\"axis-label\" x=\"" + (margin.left - (layout.compact ? 10 : 16)) + "\" y=\"" + (y + 14) + "\" text-anchor=\"end\">" + percentile + "</text>" +
        track +
        "<rect class=\"bar\" x=\"" + margin.left + "\" y=\"" + y + "\" width=\"" + barWidth + "\" height=\"17\" rx=\"5\" fill=\"var(--accent-3)\"></rect>" +
        "<text class=\"chart-value\" x=\"" + valueAttrs.x + "\" y=\"" + (y + 14) + "\" text-anchor=\"" + valueAttrs.anchor + "\">" + esc(formatValue(value)) + " ms</text>" +
        "</g>";
    }).join("");
    cursor += groupGap;
    return "<g>" +
      "<text class=\"group-label\" x=\"0\" y=\"" + (groupStart + (layout.compact ? 12 : 17)) + "\" title=\"" + esc(label) + "\">" + esc(compactName(label, layout.labelMax)) + "</text>" +
      bars +
      "</g>";
  });
  document.getElementById("latency-breakdown").innerHTML =
    "<div class=\"viz\"><svg viewBox=\"0 0 " + width + " " + height + "\" role=\"img\" aria-label=\"Latency by phase chart\" preserveAspectRatio=\"xMinYMin meet\">" +
    grid +
    "<line class=\"axis-line\" x1=\"" + margin.left + "\" y1=\"" + (height - margin.bottom + 4) + "\" x2=\"" + (width - margin.right) + "\" y2=\"" + (height - margin.bottom + 4) + "\"></line>" +
    rendered.join("") +
    "</svg></div>" +
    (groups.length > 16 ? "<div class=\"note\">Showing the 16 slowest latency groups.</div>" : "");
}

function renderMetricRows(metrics, category) {
  const values = metrics.map(metric => metricNumber(metric)).filter(value => value !== null).map(value => Math.abs(value));
  const max = Math.max(...values, 0);
  return metrics.map(metric => {
    const value = metricNumber(metric);
    const pct = value !== null && max > 0 ? Math.max(3, (Math.abs(value) / max) * 100) : 0;
    const track = value !== null ?
      "<span class=\"metric-row-track\"><span class=\"metric-row-fill\" style=\"width:" + pct + "%\"></span></span>" :
      "<span></span>";
    return (
    "<span class=\"metric-pill\">" +
    "<span class=\"label\">" + esc(formatUnit(metric.unit)) + "</span> " +
    track +
    "<span class=\"value\">" + esc(formatValue(metric.value)) + "</span>" +
    "</span>"
    );
  }).join("");
}

function renderLatencyMetricDetails(metrics) {
  const groups = new Map();
  metrics.forEach(metric => {
    const match = latencyMatch(metric.unit);
    const value = metricNumber(metric);
    if (!match || value === null) return;
    const phase = match[1];
    const percentile = "p" + match[2];
    if (!groups.has(phase)) groups.set(phase, { phase, values: {} });
    groups.get(phase).values[percentile] = value;
  });
  const grouped = Array.from(groups.values()).sort((a, b) => {
    const left = Math.max(a.values.p99 || 0, a.values.p95 || 0, a.values.p50 || 0);
    const right = Math.max(b.values.p99 || 0, b.values.p95 || 0, b.values.p50 || 0);
    return right - left;
  });
  if (!grouped.length) {
    return "<div class=\"metric-list\">" + renderMetricRows(metrics, "latency") + "</div>";
  }
  const max = Math.max(...grouped.flatMap(group => [group.values.p50 || 0, group.values.p95 || 0, group.values.p99 || 0]), 0);
  const items = grouped.map(group => {
    const rows = ["p50", "p95", "p99"].map(percentile => {
      const value = group.values[percentile];
      if (value === undefined) return "";
      const pct = max > 0 ? Math.max(3, (value / max) * 100) : 0;
      return "<div class=\"latency-percentile\">" +
        "<span>" + percentile + "</span>" +
        "<div class=\"mini-track\"><div class=\"mini-fill\" style=\"width:" + pct + "%\"></div></div>" +
        "<strong>" + esc(formatValue(value)) + " ms</strong>" +
        "</div>";
    }).join("");
    return "<div class=\"latency-detail-item\">" +
      "<div class=\"latency-phase\">" + esc(formatPhase(group.phase)) + "</div>" +
      "<div class=\"latency-percentiles\">" + rows + "</div>" +
      "</div>";
  });
  return "<div class=\"latency-detail-list\">" + items.join("") + "</div>";
}

function renderMetricDetails() {
  const cards = [];
  selectedResults().forEach(result => {
    const buckets = new Map([
      ["throughput", []],
      ["cost", []],
      ["memory", []],
      ["counts", []],
      ["other", []],
      ["latency", []]
    ]);
    result.metrics.forEach(metric => buckets.get(metricCategory(metric.unit)).push(metric));
    const categories = [];
    buckets.forEach((metrics, category) => {
      if (!metrics.length) return;
      const isLatency = category === "latency";
      const body = isLatency ?
        renderLatencyMetricDetails(metrics) :
        "<div class=\"metric-list\">" + renderMetricRows(metrics, category) + "</div>";
      categories.push(
        "<div class=\"metric-category category-" + esc(category) + (isLatency ? " latency-detail-category" : "") + "\">" +
        "<div class=\"group-title\">" + esc(isLatency ? "latency phases" : category) + "</div>" +
        body +
        "</div>"
      );
    });
    cards.push(
      "<article class=\"metric-detail-card\">" +
      "<h3 title=\"" + esc(result.name) + "\">" + esc(displayBenchmarkName(result.name)) + "</h3>" +
      "<div class=\"metric-category-grid\">" + categories.join("") + "</div>" +
      "</article>"
    );
  });
  document.getElementById("metric-details").innerHTML = cards.join("") || "<div class=\"empty\">No metrics for this selection.</div>";
}

function renderPGChart(rows, emptyText) {
  const el = document.getElementById("pg-chart");
  if (!rows.length) {
    el.innerHTML = "<div class=\"empty\">" + esc(emptyText || "No Postgres statement stats for this selection.") + "</div>";
    return;
  }
  const visibleRows = rows.slice().sort((a, b) => (b.total_ms || 0) - (a.total_ms || 0)).slice(0, 10);
  const layout = chartLayout("pg-chart", "pg");
  const width = layout.width;
  const margin = layout.margin;
  const rowHeight = 34;
  const innerWidth = layout.innerWidth;
  const max = niceMax(Math.max(...visibleRows.map(row => row.total_ms || 0), 0));
  const height = margin.top + margin.bottom + visibleRows.length * rowHeight;
  const ticks = tickValues(max, layout.ticks);
  const valueAttrs = chartValueAttributes(layout);
  const grid = ticks.map(tick => {
    const x = margin.left + (tick / max) * innerWidth;
    return "<line class=\"grid-line\" x1=\"" + x + "\" y1=\"" + margin.top + "\" x2=\"" + x + "\" y2=\"" + (height - margin.bottom + 4) + "\"></line>" +
      "<text class=\"axis-label\" x=\"" + x + "\" y=\"" + (height - 10) + "\" text-anchor=\"middle\">" + esc(formatValue(tick)) + "</text>";
  }).join("");
  const bars = visibleRows.map((row, index) => {
    const y = margin.top + index * rowHeight + 6;
    const total = row.total_ms || 0;
    const barWidth = Math.max(3, (total / max) * innerWidth);
    const label = "#" + row.rank + " " + compactName(row.query || "", layout.labelMax);
    const fill = index < 3 ? "var(--warn)" : "var(--accent)";
    const track = layout.showTrack ? "<rect class=\"bar-bg\" x=\"" + margin.left + "\" y=\"" + y + "\" width=\"" + innerWidth + "\" height=\"20\" rx=\"5\"></rect>" : "";
    return "<g>" +
      "<text class=\"bar-label\" x=\"0\" y=\"" + (y + 15) + "\" title=\"" + esc(row.query || "") + "\">" + esc(label) + "</text>" +
      track +
      "<rect class=\"bar\" x=\"" + margin.left + "\" y=\"" + y + "\" width=\"" + barWidth + "\" height=\"20\" rx=\"5\" fill=\"" + fill + "\"></rect>" +
      "<text class=\"chart-value\" x=\"" + valueAttrs.x + "\" y=\"" + (y + 15) + "\" text-anchor=\"" + valueAttrs.anchor + "\">" + esc(formatValue(total)) + " ms</text>" +
      "</g>";
  }).join("");
  el.innerHTML = "<div class=\"viz\"><svg viewBox=\"0 0 " + width + " " + height + "\" role=\"img\" aria-label=\"Postgres statement total time chart\" preserveAspectRatio=\"xMinYMin meet\">" +
    grid +
    "<line class=\"axis-line\" x1=\"" + margin.left + "\" y1=\"" + (height - margin.bottom + 4) + "\" x2=\"" + (width - margin.right) + "\" y2=\"" + (height - margin.bottom + 4) + "\"></line>" +
    bars +
    "</svg></div>" +
    (rows.length > visibleRows.length ? "<div class=\"note\">Showing the 10 statements with the highest total time.</div>" : "");
}

function renderPGTable() {
  const filter = queryFilter.value.trim().toLowerCase();
  let rows = finalPGStats();
  if (state.benchmark !== "all") rows = rows.filter(stat => stat.benchmark === state.benchmark);
  const availableRows = rows.length;
  if (filter) rows = rows.filter(stat => stat.query.toLowerCase().includes(filter));
  rows.sort((a, b) => (a.benchmark.localeCompare(b.benchmark) || (a.database || "").localeCompare(b.database || "") || a.rank - b.rank));
  pgFilterCount.textContent = availableRows ?
    (filter ? String(rows.length) + " of " + String(availableRows) + " statements" : String(rows.length) + " statements") :
    "";
  const tableEl = document.getElementById("pg-table");
  if (!rows.length) {
    const emptyText = availableRows ? "No Postgres statements match this SQL filter." : "No Postgres statement stats for this selection.";
    renderPGChart(rows, emptyText);
    tableEl.classList.add("is-empty");
    tableEl.innerHTML = "";
    return;
  }
  tableEl.classList.remove("is-empty");
  renderPGChart(rows);
  const maxTotal = Math.max(...rows.map(row => row.total_ms || 0), 0);
  const body = rows.map(row => {
    const pct = maxTotal > 0 ? Math.max(4, (row.total_ms || 0) / maxTotal * 100) : 0;
    return "<tr>" +
      "<td>" + esc(displayBenchmarkName(row.benchmark)) + "</td>" +
      "<td>" + esc(row.database || "") + "</td>" +
      "<td>" + esc(row.iterations) + "</td>" +
      "<td>" + esc(row.rank) + "</td>" +
      "<td>" + esc(row.calls) + "</td>" +
      "<td class=\"bar-bg\" style=\"background-size:" + pct + "% 100%; background-repeat:no-repeat;\">" + esc(formatValue(row.total_ms)) + "</td>" +
      "<td>" + esc(formatValue(row.mean_ms)) + "</td>" +
      "<td>" + esc(row.rows) + "</td>" +
      "<td>" + esc(row.plans || 0) + "</td>" +
      "<td>" + esc(formatValue(row.total_plan_ms || 0)) + "</td>" +
      "<td>" + esc(row.shared_blks_hit || 0) + "</td>" +
      "<td>" + esc(row.shared_blks_read || 0) + "</td>" +
      "<td>" + esc(row.shared_blks_dirtied || 0) + "</td>" +
      "<td>" + esc(row.shared_blks_written || 0) + "</td>" +
      "<td>" + esc(row.temp_blks_read || 0) + "</td>" +
      "<td>" + esc(row.temp_blks_written || 0) + "</td>" +
      "<td>" + esc(row.wal_bytes || 0) + "</td>" +
      "<td class=\"query\">" + esc(row.query) + "</td>" +
      "</tr>";
  });
  tableEl.innerHTML =
    "<table><thead><tr><th>Benchmark</th><th>DB</th><th>Iter</th><th>Rank</th><th>Calls</th><th>Total ms</th><th>Mean ms</th><th>Rows</th><th>Plans</th><th>Total plan ms</th><th>Shared hit</th><th>Shared read</th><th>Shared dirtied</th><th>Shared written</th><th>Temp read</th><th>Temp written</th><th>WAL bytes</th><th>Query</th></tr></thead><tbody>" +
    body.join("") +
    "</tbody></table>";
}

function renderAll() {
  renderBenchmarkOverview();
  renderSelectedMetric();
  renderLatencyBreakdown();
  renderMetricDetails();
  renderPGTable();
}

benchmarkFilter.addEventListener("change", event => {
  state.benchmark = event.target.value;
  renderAll();
});
metricFilter.addEventListener("change", event => {
  state.metric = event.target.value;
  renderAll();
});
queryFilter.addEventListener("input", renderPGTable);
let resizeTimer = null;
window.addEventListener("resize", () => {
  window.clearTimeout(resizeTimer);
  resizeTimer = window.setTimeout(renderAll, 120);
});

renderMetadata();
renderRunSummary();
renderFilters();
renderAll();
