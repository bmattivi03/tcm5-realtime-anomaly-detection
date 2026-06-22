/* ════════════════════════════════════════════════════════════════════════
   TCM-5 control room client.
   Buildless: vendored RxJS UMD (global `rxjs`) + this file, nothing else.
   One SSE stream (/stream, event "snapshot") drives the whole board.
   ════════════════════════════════════════════════════════════════════════ */
"use strict";

const { Observable, from, fromEvent, of, timer } = rxjs;
const { map, scan, filter, share, distinctUntilChanged, exhaustMap, switchMap, catchError } = rxjs;

const $ = (id) => document.getElementById(id);
const FAM_COLOR = { electric: "#79c0ff", bearing: "#ffa657", workroll: "#d2a8ff", reduction: "#56d4dd" };
const FAM_NAME = { electric: "ELECTRIC", bearing: "BEARING", workroll: "WORKROLL", reduction: "REDUCTION" };
const WEAR_FULL_KM = 120;        // observed work-roll mileage saw-tooths 0..120 km
const NOMINAL_EPS = 1500;        // full-speed reference for the animations
const IDLE_AFTER_S = 8;          // sink freshness beyond this => IDLE

/* ───────────────────────── 1 · build the mill schematic ─────────────────
   Parametric inline SVG: pay-off reel, 5 stand housings (work + backup roll
   pairs), thinning strip path, tension reel. Generated here so the geometry
   lives in one place. */

const STAND_X = [272, 440, 608, 776, 944];
const STRIP_Y = 154;
const STRIP_W = [9, 7.4, 6, 4.9, 4, 3.3];   // entry, after S1 … after S5

const rolls = [];   // {el, cx, cy, dir, k} for the rAF spin engine

function rollSvg(cls, cx, cy, r, dir, k) {
  return `<g class="rollg">
    <circle class="${cls}" cx="${cx}" cy="${cy}" r="${r}"/>
    <g class="roll-rot" data-cx="${cx}" data-cy="${cy}" data-dir="${dir}" data-k="${k}">
      <line class="roll-spoke" x1="${cx - r * 0.62}" y1="${cy}" x2="${cx + r * 0.62}" y2="${cy}"/>
      <line class="roll-spoke" x1="${cx}" y1="${cy - r * 0.62}" x2="${cx}" y2="${cy + r * 0.62}"/>
      <circle class="roll-hub" cx="${cx}" cy="${cy}" r="${Math.max(2.4, r * 0.14)}"/>
    </g></g>`;
}

function reelSvg(cx, label) {
  return `
    <circle class="reel" cx="${cx}" cy="${STRIP_Y}" r="42"/>
    <circle class="reel-wrap" cx="${cx}" cy="${STRIP_Y}" r="32"/>
    <circle class="reel-wrap" cx="${cx}" cy="${STRIP_Y}" r="22"/>
    ${rollSvg("roll-hub", cx, STRIP_Y, 8, cx < 600 ? -1 : 1, 0.35)}
    <text class="caption" x="${cx}" y="218">${label}</text>`;
}

function standSvg(i) {
  const cx = STAND_X[i], n = i + 1;
  return `
  <g id="stand-${n}">
    <title id="tip-${n}">stand ${n}</title>
    <text id="fam-${n}" class="fam-label" x="${cx}" y="13"></text>
    <circle id="lamp-${n}" class="lamp off" cx="${cx}" cy="26" r="6.5"/>
    <rect x="${cx - 2}" y="33" width="4" height="7" fill="#2a3442"/>
    <path class="housing" d="M${cx - 44} 256 V52 L${cx - 32} 40 H${cx + 32} L${cx + 44} 52 V256 Z"/>
    <rect class="housing-cap" x="${cx - 14}" y="40" width="28" height="10"/>
    <text class="stand-id" x="${cx}" y="70">S${n}</text>
    ${rollSvg("roll-bu", cx, 102, 24, -1, 0.54)}
    ${rollSvg("roll-wr", cx, 138, 13, 1, 1)}
    ${rollSvg("roll-wr", cx, 170, 13, -1, 1)}
    ${rollSvg("roll-bu", cx, 206, 24, 1, 0.54)}
    <circle class="wear-track" cx="${cx - 22}" cy="241" r="9"/>
    <circle id="wear-${n}" class="wear-arc" cx="${cx - 22}" cy="241" r="9" pathLength="100"
            stroke-dasharray="0 100" stroke="#3fb950"/>
    <text id="wearkm-${n}" class="wear-km" x="${cx - 8}" y="245">— km</text>
    <rect class="plate" x="${cx - 43}" y="270" width="86" height="44"/>
    <text id="rate-${n}" class="plate-text plate-alm" x="${cx}" y="285">ALARM —</text>
    <text id="force-${n}" class="plate-text" x="${cx}" y="298">FORCE —</text>
    <text id="ens-${n}" class="plate-text" x="${cx}" y="311">ENS —</text>
  </g>`;
}

function stripSvg() {
  const xs = [126, ...STAND_X.flatMap((cx) => [cx - 13, cx + 13]), 1114];
  let out = "";
  for (let s = 0; s < 6; s++) {
    const x1 = xs[s * 2], x2 = xs[s * 2 + 1], w = STRIP_W[s];
    out += `<line class="strip-base" x1="${x1}" y1="${STRIP_Y}" x2="${x2}" y2="${STRIP_Y}" stroke-width="${w}"/>`;
    out += `<line class="strip-flow" x1="${x1}" y1="${STRIP_Y}" x2="${x2}" y2="${STRIP_Y}" stroke-width="${Math.max(1.6, w - 3)}"/>`;
  }
  return out;
}

(function buildMill() {
  const svg = $("mill");
  svg.innerHTML = `
    <defs><pattern id="gridpat" width="28" height="28" patternUnits="userSpaceOnUse">
      <path d="M28 0H0V28" fill="none" stroke="#141d2a" stroke-width="1"/>
    </pattern></defs>
    <rect class="blueprint" x="0" y="0" width="1240" height="262"/>
    <line x1="0" y1="262" x2="1240" y2="262" stroke="#222b38" stroke-width="1.5"/>
    ${reelSvg(84, "PAY-OFF REEL")}
    ${stripSvg()}
    ${STAND_X.map((_, i) => standSvg(i)).join("")}
    ${reelSvg(1156, "TENSION REEL")}`;
  svg.querySelectorAll(".roll-rot").forEach((el) =>
    rolls.push({ el, cx: +el.dataset.cx, cy: +el.dataset.cy, dir: +el.dataset.dir, k: +el.dataset.k }));
})();

/* ───────────────────────── 2 · the snapshot stream ─────────────────────── */

function fromSSE(url, name) {
  return new Observable((sub) => {
    const es = new EventSource(url);
    es.addEventListener(name, (e) => sub.next(JSON.parse(e.data)));
    es.onerror = () => setPill("link-lost");     // EventSource reconnects on its own
    return () => es.close();
  });
}

const snap$ = fromSSE("/stream", "snapshot").pipe(share());

let lastSnap = null;       // latest snapshot, for drill-downs and the rAF engine
let epsTarget = 0;         // animation speed target (0 when idle)

/* ───────────────────────── 3 · header vitals + stands ──────────────────── */

const fmtAge = (s) => (s < 1 ? "now" : s < 60 ? `${Math.round(s)}s` : s < 3600 ? `${Math.round(s / 60)}m` : `${Math.round(s / 3600)}h`);

function setPill(state) {
  const pill = $("status-pill"), txt = $("status-text");
  pill.className = "pill " + (state === "live" ? "is-live" : "is-idle");
  txt.textContent = state === "live" ? "STREAMING" : state === "link-lost" ? "LINK LOST" : "IDLE";
}

function renderVitals(s) {
  const r = s.rates, live = r.freshness_s != null && r.freshness_s < IDLE_AFTER_S;
  setPill(live ? "live" : "idle");
  epsTarget = live ? r.events_per_s : 0;        // odometer + strip ease to 0 when idle
  $("v-coils").textContent = r.coils_total == null ? "—" : r.coils_total.toLocaleString("en");
  $("v-unsup").textContent = r.unsup_rate_10s == null ? "—" : (r.unsup_rate_10s * 100).toFixed(1);
  const f = r.freshness_s;
  if (f == null) { $("v-fresh").textContent = "—"; $("v-fresh-u").textContent = "s"; }
  else if (f < 120) { $("v-fresh").textContent = f.toFixed(1); $("v-fresh-u").textContent = "s"; }
  else if (f < 7200) { $("v-fresh").textContent = Math.round(f / 60); $("v-fresh-u").textContent = "min"; }
  else { $("v-fresh").textContent = (f / 86400).toFixed(1); $("v-fresh-u").textContent = "d"; }
  $("v-p95").textContent = r.latency_ms_p95 == null ? "—" : Math.round(r.latency_ms_p95).toLocaleString("en");
}

function renderStands(s) {
  for (const st of s.stands) {
    const n = st.stand, rate = st.alarm_rate || 0, alarmed = st.events > 0 && rate >= 0.02;
    const lamp = $(`lamp-${n}`);
    lamp.setAttribute("class", "lamp " +
      (st.events === 0 ? "off" : rate >= 0.15 ? "r" : rate >= 0.02 ? "a" : "g"));
    const fam = $(`fam-${n}`);
    if (alarmed && st.dominant) {
      fam.textContent = FAM_NAME[st.dominant] || st.dominant.toUpperCase();
      fam.setAttribute("fill", FAM_COLOR[st.dominant] || "#dfe7f1");
      fam.classList.add("on");
    } else fam.classList.remove("on");
    $(`rate-${n}`).textContent = st.events === 0 ? "ALARM —" : `ALARM ${(rate * 100).toFixed(1)}%`;
    $(`force-${n}`).textContent = st.force == null ? "FORCE —" : `FORCE ${(st.force / 1e6).toFixed(1)} MN`;
    const ur = st.u_rate || 0, ens = $(`ens-${n}`);
    ens.textContent = st.events === 0 ? "ENS —" : `ENS ${(ur * 100).toFixed(1)}%`;
    ens.setAttribute("fill", st.events === 0 ? "#6b7888" : ur >= 0.15 ? "#f85149" : ur >= 0.02 ? "#d29922" : "#6b7888");
    $(`tip-${n}`).textContent = `Stand ${n} · ${st.events.toLocaleString("en")} events in the last 10 s of stream time`
      + (st.force == null ? "" : ` · avg roll force ${(st.force / 1e6).toFixed(2)} MN`)
      + ` · unsup 2-of-3 ${(ur * 100).toFixed(1)}% (maha ${((st.u_maha || 0) * 100).toFixed(0)}% ·`
      + ` spc ${((st.u_spc || 0) * 100).toFixed(0)}% · knn ${((st.u_knn || 0) * 100).toFixed(0)}%)`;
    const wear = $(`wear-${n}`), km = st.work_roll_mileage;
    if (km == null) { wear.setAttribute("stroke-dasharray", "0 100"); $(`wearkm-${n}`).textContent = "— km"; }
    else {
      const pct = Math.min(1, Math.max(0, km / WEAR_FULL_KM)) * 100;
      wear.setAttribute("stroke-dasharray", `${pct} ${100 - pct}`);
      wear.setAttribute("stroke", pct < 50 ? "#3fb950" : pct < 80 ? "#d29922" : "#f85149");
      $(`wearkm-${n}`).textContent = `${Math.round(km)} km`;
    }
  }
}

snap$.subscribe((s) => { lastSnap = s; renderVitals(s); renderStands(s); });

/* ───────────────────────── 4 · model banner + hot-swap ──────────────────
   latest.version comes from the model register the moment a retrain lands;
   live_version is max(model_version) in the last 10 s of scored rows. The
   gap between them IS the in-flight hot-reload inside Flink. */

const kfmt = (txt) => String(txt).replace(/\d{4,}/g, (n) => Math.round(+n / 1000) + "K");

function bannerText(m) {
  if (!m.latest) return "NO MODEL YET — click TRAIN MODEL to build v1 from the stream";
  const L = m.latest;
  return `MODEL v${L.version} · THR ${(L.threshold ?? 0).toFixed(2)} · ` +
         `${kfmt(L.trained_on || "?").toUpperCase()} · F1 ${L.overall_f1 == null ? "—" : L.overall_f1.toFixed(2)}`;
}

let flashTimer = null;
function setSwapUI(phase, live, latest) {
  const banner = $("model-banner"), hs = $("hotswap");
  const cls = phase === "swap" ? "is-swap" : phase === "flash" ? "is-flash" : "is-sync";
  banner.className = "model-banner " + cls;
  hs.className = "hotswap " + cls;
  $("hs-live").textContent = live == null ? "v–" : "v" + live;
  $("hs-latest").textContent = latest == null ? "v–" : "v" + latest;
  if (phase === "swap") {
    $("mb-text").textContent = `HOT-SWAP v${live ?? "?"} → v${latest} IN PROGRESS`;
    $("hs-line").textContent = `HOT-SWAP IN PROGRESS — Flink UDF reloading, no restart`;
    $("hs-sub").textContent = `registered v${latest} · live scores still v${live ?? "?"}`;
  } else if (phase === "flash") {
    $("mb-text").textContent = `MODEL v${latest} LIVE — HOT-SWAP COMPLETE`;
    $("hs-line").textContent = `v${latest} IS LIVE — swapped with zero downtime`;
    $("hs-sub").textContent = "every new score below now carries the new version";
  } else {
    $("mb-text").textContent = lastSnap ? bannerText(lastSnap.model) : "AWAITING MODEL REGISTER…";
    $("hs-line").textContent = live == null ? "no scored rows in the last 10 s"
                                            : `live scores on v${live} — register in sync`;
    $("hs-sub").textContent = "live scores vs registered version";
  }
}

snap$.pipe(
  map((s) => ({ live: s.model.live_version, latest: s.model.latest ? s.model.latest.version : null })),
  distinctUntilChanged((a, b) => a.live === b.live && a.latest === b.latest),
  scan((acc, cur) => {
    const swapping = cur.latest != null && cur.live != null && cur.latest > cur.live;
    const phase = swapping ? "swap" : (acc.phase === "swap" ? "flash" : "sync");
    return { ...cur, phase };
  }, { phase: "sync", live: null, latest: null })
).subscribe(({ phase, live, latest }) => {
  clearTimeout(flashTimer);
  setSwapUI(phase, live, latest);
  if (phase === "flash") flashTimer = setTimeout(() => setSwapUI("sync", live, latest), 4500);
});

/* recall bars: tween on version change, ghost tick = previous version */
snap$.pipe(
  map((s) => s.model),
  filter((m) => !!m.latest),
  distinctUntilChanged((a, b) => a.latest.version === b.latest.version)
).subscribe((m) => {
  const prev = m.versions.length > 1 ? m.versions[m.versions.length - 2] : null;
  $("model-meta").textContent =
    `v${m.latest.version} · ${m.versions.length} version${m.versions.length > 1 ? "s" : ""} registered · thr ${m.threshold.toFixed(2)}`;
  const f2 = (v) => (v == null ? "—" : v.toFixed(2));
  $("mk-f1").textContent = f2(m.latest.overall_f1);
  $("mk-prauc").textContent = f2(m.latest.overall_pr_auc);
  $("mk-prec").textContent = f2(m.latest.precision);
  $("mk-rec").textContent = f2(m.latest.recall);
  $("mb-sub").textContent = m.latest.trained_at
    ? `v${m.latest.version} registered ${m.latest.trained_at.replace("T", " ").slice(0, 19)} UTC · PR-AUC ${m.latest.overall_pr_auc == null ? "—" : m.latest.overall_pr_auc.toFixed(2)}`
    : "scoring model · threshold drives every lamp on this board";
  document.querySelectorAll(".recall-row").forEach((row) => {
    const famKey = "recall_" + row.dataset.fam;
    const cur = m.latest[famKey], old = prev ? prev[famKey] : null;
    row.querySelector(".recall-fill").style.width = cur == null ? "0%" : (cur * 100).toFixed(1) + "%";
    row.querySelector(".recall-val").textContent = cur == null ? "—" : (cur * 100).toFixed(0) + "%";
    const ghost = row.querySelector(".recall-ghost");
    if (old != null) { ghost.style.left = (old * 100).toFixed(1) + "%"; ghost.classList.add("on"); }
    else ghost.classList.remove("on");
  });
});

/* ───────────────────────── 5 · anomaly ticker ───────────────────────────
   Diff on reading_id with scan; new entries slide in, oldest are trimmed. */

const tickerEl = $("ticker");

function tickerRow(e, delayMs) {
  const li = document.createElement("li");
  li.className = "trow new";
  li.dataset.coil = e.coil_id;
  if (delayMs) li.style.animationDelay = delayMs + "ms";
  li.innerHTML =
    `<span class="t-age num" data-ts="${e.ts}"></span>` +
    `<span class="t-coil num">COIL ${e.coil_id}</span>` +
    `<span class="t-stand">S${e.stand}</span>` +
    `<span class="chip f-${e.family}">${FAM_NAME[e.family] || "?"}</span>` +
    `<span class="t-score num">${Math.round(e.anomaly_score * 100)}%</span>`;
  return li;
}

function updateAges(anchorIso) {
  if (!anchorIso) return;
  const anchor = Date.parse(anchorIso);
  tickerEl.querySelectorAll(".t-age").forEach((el) => {
    el.textContent = fmtAge(Math.max(0, (anchor - Date.parse(el.dataset.ts)) / 1000));
  });
}

snap$.pipe(
  scan((acc, s) => {
    const fresh = s.ticker.filter((e) => e.reading_id > acc.maxId);
    return { initial: acc.maxId === 0, fresh, anchor: s.anchor_ts,
             maxId: fresh.length ? fresh[0].reading_id : acc.maxId, thr: s.model.threshold };
  }, { maxId: 0 })
).subscribe(({ initial, fresh, anchor, thr }) => {
  if (fresh.length) {
    $("ticker-empty")?.remove();
    [...fresh].reverse().forEach((e, i) =>          // oldest first so newest lands on top
      tickerEl.insertBefore(tickerRow(e, initial ? (fresh.length - 1 - i) * 45 : 0), tickerEl.firstChild));
    while (tickerEl.children.length > 40) tickerEl.lastChild.remove();
  }
  $("ticker-meta").textContent = `confidence ≥ ${Math.round(thr * 100)}% · newest ${Math.min(40, tickerEl.querySelectorAll(".trow").length)}`;
  updateAges(anchor);
});

/* click a row → inline mini-profile of that coil across the 5 stands */
/* shared by the ticker and the inspection table: fetch a coil's per-stand profile */
async function coilProfileHTML(coil) {
  const r = await fetch(`/api/coil/${coil}`);
  const j = await r.json();
  if (!r.ok || !j.stands) throw new Error(j.error || r.statusText);
  const thr = lastSnap ? lastSnap.model.threshold : 0.85;
  return `<div class="p-head">COIL ${coil} — PER-STAND PROFILE (latest rows, model v${j.stands[0]?.model_version ?? "?"})</div>` +
    (j.stands.length ? j.stands.map((st) => {
      const hot = st.anomaly_score >= thr ? " hot" : "";
      const tags = ["electric", "bearing", "workroll", "reduction"]
        .filter((f) => st["y_" + f])
        .map((f) => `<span class="ytag f-${f}">${FAM_NAME[f]}</span>`).join("");
      return `<div class="p-stand">
        <span class="ps-id">S${st.stand}</span>
        <span class="ps-bar${hot}"><i style="width:${Math.min(100, st.anomaly_score * 100).toFixed(0)}%"></i></span>
        <span class="ps-score num${hot}">${Math.round(st.anomaly_score * 100)}%</span>
        <span class="ps-meta num">F ${(st.force / 1e6).toFixed(1)}MN · T ${(st.torque / 1e3).toFixed(0)}kNm · G ${st.gap.toFixed(2)}mm${tags}</span>
      </div>`;
    }).join("") : `<div class="p-err">no rows for this coil</div>`);
}

tickerEl.addEventListener("click", async (ev) => {
  const row = ev.target.closest(".trow");
  if (!row) return;
  const open = tickerEl.querySelector(".profile");
  const wasOurs = open && open.dataset.coil === row.dataset.coil && open.previousElementSibling === row;
  open?.remove();
  if (wasOurs) return;                               // second click closes
  const li = document.createElement("li");
  li.className = "profile";
  li.dataset.coil = row.dataset.coil;
  li.innerHTML = `<div class="p-head">COIL ${row.dataset.coil} — PER-STAND PROFILE</div><div class="p-err">loading…</div>`;
  row.after(li);
  try { li.innerHTML = await coilProfileHTML(row.dataset.coil); }
  catch (e) { li.querySelector(".p-err").textContent = "fetch failed: " + e.message; }
});

/* ───────────────────────── 5b · coils-to-inspect table ──────────────────
   Full flagged list (not just the streaming ticker): predicted fault per coil,
   sortable headers, click a row to drill into the same per-stand profile. */

const inspectBody = $("inspect-body");
let inspectSort = { k: "confidence", dir: -1 };

function renderInspect(rows) {
  const k = inspectSort.k;
  const keyOf = (c) => (k === "confidence" && !c.model_flag) ? null : c[k];  // ensemble-only conf shows "—"
  const sorted = [...rows].sort((a, b) => {
    const va = keyOf(a), vb = keyOf(b);
    if (va == null) return 1;
    if (vb == null) return -1;
    return (va > vb ? 1 : va < vb ? -1 : 0) * inspectSort.dir;
  });
  if (!sorted.length) {
    inspectBody.innerHTML = `<tr class="inspect-empty"><td colspan="8">no flagged coils yet — line is clean</td></tr>`;
    return;
  }
  const sel = $("inspect-detail").dataset.coil;
  inspectBody.innerHTML = sorted.map((c) => {
    const conf = Math.round((c.confidence ?? 0) * 100);
    const sev = conf >= 85 ? "hot" : conf >= 50 ? "warn" : "";
    const fault = c.predicted_fault
      ? `<span class="chip f-${c.predicted_fault}">${FAM_NAME[c.predicted_fault]}</span>`
      : `<span class="chip chip-unsup">UNSUP</span>`;
    const by = c.flagged_by;
    const byTxt = by === "model" ? "MODEL"
      : by === "both" ? `MODEL · ENS ${c.u_votes ?? "?"}/3`
      : `ENS ${c.u_votes ?? "?"}/3`;
    return `<tr class="irow${String(c.coil_id) === sel ? " sel" : ""}" data-coil="${c.coil_id}">` +
      `<td class="num t-coil">${c.coil_id}</td>` +
      `<td>${fault}</td>` +
      `<td><span class="by by-${by}">${byTxt}</span></td>` +
      `<td class="num i-conf ${sev}">${c.model_flag ? conf + "%" : "—"}</td>` +
      `<td class="num">S${c.worst_stand}</td>` +
      `<td class="num">${c.first_stand == null ? "—" : "S" + c.first_stand}</td>` +
      `<td class="num">${c.stands_alarmed}</td>` +
      `<td class="num">${c.model_version ? "v" + c.model_version : "—"}</td>` +
      `</tr>`;
  }).join("");
}

snap$.subscribe((s) => {
  renderInspect(s.inspect || []);
  const thr = s.model.threshold || 0.85;
  $("inspect-meta").textContent =
    `${(s.inspect || []).length} flagged · model ≥ ${Math.round(thr * 100)}% or 2-of-3 ensemble · click to drill in`;
});

$("inspect").querySelector("thead").addEventListener("click", (ev) => {
  const th = ev.target.closest("th");
  if (!th) return;
  const k = th.dataset.k;
  inspectSort = { k, dir: inspectSort.k === k ? -inspectSort.dir : (k === "coil_id" || k === "predicted_fault" ? 1 : -1) };
  th.parentElement.querySelectorAll("th").forEach((h) => h.classList.remove("sortdir", "asc", "desc"));
  th.classList.add("sortdir", inspectSort.dir > 0 ? "asc" : "desc");
  if (lastSnap) renderInspect(lastSnap.inspect || []);
});

inspectBody.addEventListener("click", async (ev) => {
  const row = ev.target.closest(".irow");
  if (!row) return;
  const detail = $("inspect-detail"), coil = row.dataset.coil;
  if (detail.dataset.coil === coil && !detail.hidden) {          // second click closes
    detail.hidden = true; detail.dataset.coil = "";
    inspectBody.querySelectorAll(".irow.sel").forEach((r) => r.classList.remove("sel"));
    return;
  }
  inspectBody.querySelectorAll(".irow.sel").forEach((r) => r.classList.remove("sel"));
  row.classList.add("sel");
  detail.dataset.coil = coil; detail.hidden = false;
  detail.className = "inspect-detail profile";
  detail.innerHTML = `<div class="p-head">COIL ${coil} — PER-STAND PROFILE</div><div class="p-err">loading…</div>`;
  try { detail.innerHTML = await coilProfileHTML(coil); }
  catch (e) { detail.innerHTML = `<div class="p-err">fetch failed: ${e.message}</div>`; }
});

/* ───────────────────────── 6 · retrain button + policy line ─────────────── */

const rbtn = $("retrain-btn"), rstatus = $("retrain-status"), rout = $("retrain-out");

fromEvent(rbtn, "click").pipe(
  exhaustMap(() => {
    rbtn.disabled = true;
    rbtn.querySelector(".rb-label").textContent = "TRAINING…";
    rstatus.className = "retrain-status busy";
    rstatus.textContent = "training on the streamed coils — watch the hot-swap deck when it lands";
    rout.hidden = true;
    return from(fetch("/api/retrain", { method: "POST" })
      .then(async (r) => ({ status: r.status, body: await r.json().catch(() => ({})) })))
      .pipe(catchError((e) => of({ status: 0, body: { error: String(e) } })));
  })
).subscribe(({ status, body }) => {
  rbtn.disabled = false;
  rbtn.querySelector(".rb-label").textContent = "TRAIN MODEL";
  if (status === 200 && body.version != null) {
    rstatus.className = "retrain-status ok";
    rstatus.textContent = `v${body.version} trained (${body.n_coils?.toLocaleString("en") ?? "?"} coils) — hot-swap in flight ▸`;
  } else if (status === 409) {
    rstatus.className = "retrain-status busy";
    rstatus.textContent = "a retrain is already in progress on the sidecar";
  } else {
    rstatus.className = "retrain-status err";
    rstatus.textContent = "retrain failed — see payload below";
  }
  rout.hidden = false;
  rout.textContent = JSON.stringify(body, null, 2);
});

timer(0, 30000).pipe(
  switchMap(() => from(fetch("/api/policy").then((r) => r.json())).pipe(catchError(() => of({ available: false }))))
).subscribe((p) => {
  const el = $("policy-line");
  if (!p.available) { el.innerHTML = "auto-retrain policy: not configured"; return; }
  const en = p.enabled ?? p.active;
  const when = p.last_check ?? p.last_checked ?? p.checked_at;
  const why = p.last_reason ?? p.reason ?? p.last_action;
  el.innerHTML = `auto-retrain: <span class="${en ? "on" : ""}">${en ? "ENABLED" : "DISABLED"}</span>` +
    (when ? ` · last check ${String(when).replace("T", " ").slice(0, 19)}` : "") +
    (why ? ` · ${String(why).slice(0, 60)}` : "");
});

/* ───────────────────────── 7 · rAF engine ───────────────────────────────
   One loop: throughput odometer easing, strip flow, roll rotation.
   Speed is proportional to live events/s and eases to a stop when idle. */

const flows = document.querySelectorAll("#mill .strip-flow");
let epsDisp = 0, flowOffset = 0, rollAngle = 0, tPrev = performance.now();

function frame(tNow) {
  const dt = Math.min(0.1, (tNow - tPrev) / 1000);
  tPrev = tNow;
  epsDisp += (epsTarget - epsDisp) * Math.min(1, dt * 3.5);
  if (Math.abs(epsTarget - epsDisp) < 0.5) epsDisp = epsTarget;
  $("v-eps").textContent = Math.round(epsDisp).toLocaleString("en");

  const speed = epsDisp / NOMINAL_EPS;               // 1.0 at nominal 1500 ev/s
  if (speed > 0.001) {
    flowOffset = (flowOffset - 95 * speed * dt) % 24; // dasharray period 9+15
    flows.forEach((el) => el.setAttribute("stroke-dashoffset", flowOffset.toFixed(2)));
    rollAngle = (rollAngle + 230 * speed * dt) % 360;
    for (const r of rolls)
      r.el.setAttribute("transform", `rotate(${(r.dir * rollAngle * r.k).toFixed(1)} ${r.cx} ${r.cy})`);
  }
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
