const $ = (s) => document.querySelector(s);
let events = [];
let rateHistory = [];

async function loadUpstreams() {
  const d = await (await fetch("/api/upstreams")).json();
  $("#s-upstreams").textContent = d.upstreams.length;
}

function renderKey(k, v) {
  const total = v.allowed + v.denied || 1;
  return `<div class="keyrow"><b>${esc(k)}</b>
    <div class="bars">
      <div class="bar a" style="width:${Math.max(2, v.allowed / total * 120)}px"></div>
      <div class="bar d" style="width:${Math.max(0, v.denied / total * 120)}px"></div>
      <span class="muted">${v.allowed} ok / ${v.denied} throttled</span>
    </div></div>`;
}

function addEvent(ev) {
  events.unshift(ev);
  if (events.length > 120) events.pop();
  const el = document.createElement("div");
  el.className = "ev" + (ev.allowed ? "" : " denied");
  el.innerHTML = `<span class="verdict ${ev.allowed ? "a" : "d"}">${ev.allowed ? "ALLOW" : "DENY "}</span>
    <span>${esc(ev.key)}</span><span class="muted">${esc(ev.path)}</span>
    <span class="muted">${ev.latency_ms.toFixed(1)} ms</span><span class="muted">${ev.status_code}</span>`;
  const stream = $("#stream");
  stream.prepend(el);
  while (stream.children.length > 120) stream.lastChild.remove();
}

function drawChart() {
  const c = $("#chart"), ctx = c.getContext("2d");
  ctx.clearRect(0, 0, c.width, c.height);
  if (rateHistory.length < 2) return;
  const max = Math.max(...rateHistory.map(p => p[1]), 1);
  ctx.strokeStyle = "#58a6ff"; ctx.lineWidth = 2; ctx.beginPath();
  rateHistory.forEach((p, i) => {
    const x = i / (rateHistory.length - 1) * c.width;
    const y = c.height - (p[1] / max) * (c.height - 20) - 10;
    i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
  });
  ctx.stroke();
  ctx.fillStyle = "#8b949e"; ctx.font = "11px sans-serif";
  ctx.fillText(`peak ${max.toFixed(1)} req/s`, 8, 14);
}

async function tick() {
  const s = await (await fetch("/api/stats")).json();
  $("#s-total").textContent = s.total;
  $("#s-allowed").textContent = s.allowed;
  $("#s-denied").textContent = s.denied;
  rateHistory.push([Date.now(), s.total / Math.max(s.uptime_sec, 1)]);
  if (rateHistory.length > 60) rateHistory.shift();
  // instantaneous rps over last 5s of events
  const cutoff = Date.now() - 5000;
  const recent = events.filter(e => new Date(e.at).getTime() > cutoff).length;
  $("#s-rps").textContent = (recent / 5).toFixed(1);
  $("#keys").innerHTML = Object.entries(s.per_key).map(([k, v]) => renderKey(k, v)).join("") || '<span class="muted">no traffic yet</span>';
  drawChart();
}

function connect() {
  const es = new EventSource("/api/stream");
  es.onopen = () => { $("#live-dot").classList.add("on"); $("#live-label").textContent = "live"; };
  es.onerror = () => { $("#live-dot").classList.remove("on"); $("#live-label").textContent = "reconnecting…"; };
  es.onmessage = (m) => { if (m.data && m.data !== "{}") { addEvent(JSON.parse(m.data)); tick(); } };
}

$("#fire").onclick = async () => {
  $("#fire").disabled = true;
  await fetch("/api/loadtest", { method: "POST", body: JSON.stringify({ clients: 3, rps: 20, seconds: 10 }) });
  setTimeout(() => { $("#fire").disabled = false; }, 11000);
};

function esc(s) { return String(s ?? "").replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

loadUpstreams(); tick(); connect();
setInterval(tick, 2000);
