"use strict";

/* ------------------------------------------------------------------ *
 * TaskForge dashboard — vanilla JS, no build step.
 * Left: a config builder that mirrors internal/config schema and emits
 * the TASKFORGE_* env vars (including the JSON-encoded ones). Right: a
 * live preview. A second tab calls the running api /v1/admin endpoints.
 * All validation is client-side; the Go config parser stays authoritative.
 * ------------------------------------------------------------------ */

// ---------- tiny DOM helpers ----------
function el(tag, props, children) {
  const node = document.createElement(tag);
  if (props) {
    for (const k in props) {
      if (k === "class") node.className = props[k];
      else if (k === "html") node.innerHTML = props[k];
      else if (k.startsWith("on") && typeof props[k] === "function") node.addEventListener(k.slice(2), props[k]);
      else if (props[k] != null) node.setAttribute(k, props[k]);
    }
  }
  (Array.isArray(children) ? children : children != null ? [children] : []).forEach((c) => {
    if (c == null) return;
    node.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
  });
  return node;
}
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

let toastTimer;
function toast(msg) {
  const t = $("#toast");
  t.textContent = msg;
  t.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove("show"), 1800);
}

// Build a labelled bound input. obj[key] is read/written directly.
function field(obj, key, label, opts = {}) {
  const id = "f_" + Math.random().toString(36).slice(2);
  let input;
  if (opts.type === "select") {
    input = el("select", { id });
    (opts.options || []).forEach((o) => {
      const val = typeof o === "string" ? o : o.value;
      const text = typeof o === "string" ? o : o.label;
      input.appendChild(el("option", { value: val }, text));
    });
    input.value = obj[key] ?? "";
    input.addEventListener("change", () => { obj[key] = input.value; render(); });
  } else if (opts.type === "checkbox") {
    input = el("input", { type: "checkbox" });
    input.checked = !!obj[key];
    input.addEventListener("change", () => { obj[key] = input.checked; render(); });
    return el("label", { class: "field check" }, [input, el("span", null, label)]);
  } else if (opts.type === "textarea") {
    input = el("textarea", { id, placeholder: opts.placeholder || "" });
    input.value = obj[key] ?? "";
    input.addEventListener("input", () => { obj[key] = input.value; render(); });
  } else {
    input = el("input", { id, placeholder: opts.placeholder || "", type: opts.type === "number" ? "number" : "text" });
    if (opts.min != null) input.min = opts.min;
    input.value = obj[key] ?? "";
    input.addEventListener("input", () => {
      obj[key] = opts.type === "number" ? (input.value === "" ? "" : Number(input.value)) : input.value;
      render();
    });
  }
  const lbl = el("label", { class: "field" }, [el("span", null, label)]);
  if (opts.desc) lbl.appendChild(el("span", { class: "desc" }, opts.desc));
  lbl.appendChild(input);
  return lbl;
}

// ---------- application state ----------
const state = {
  core: {
    TASKFORGE_SERVICE_NAME: "taskforge-api",
    TASKFORGE_LOG_LEVEL: "info",
    TASKFORGE_HTTP_ADDR: "127.0.0.1:8080",
    TASKFORGE_REDIS_ADDR: "localhost:6379",
    TASKFORGE_REDIS_PASSWORD: "",
    TASKFORGE_REDIS_DB: 0,
    TASKFORGE_OTEL_ENABLED: false,
  },
  timing: {
    TASKFORGE_POLL_INTERVAL: "1s",
    TASKFORGE_SHUTDOWN_TIMEOUT: "30s",
    TASKFORGE_SCHEDULER_LOCK_TTL: "15s",
    TASKFORGE_SCHEDULER_RENEW_INTERVAL: "5s",
    TASKFORGE_TASK_SUCCESS_RETENTION: "24h",
    TASKFORGE_TASK_FAILURE_RETENTION: "72h",
    TASKFORGE_TASK_PAYLOAD_RETENTION: "24h",
  },
  pools: [],
  budgets: [],
  taskBudgets: [],
  taskLimits: [],
  routing: { enabled: false, defaultQueue: "default", defaultShard: "", rules: [] },
  schedules: [],
};

function newPool() {
  return {
    _open: true,
    name: "", queue: "", concurrency: 4, prefetch: 8, lease_ttl: "30s",
    retry: { max_deliveries: 3, initial_backoff: "1s", max_backoff: "1m", multiplier: 2, jitter: 0.1, max_task_age: "" },
    taskLimits: [],
    fairness: { enabled: false, defaultRule: { weight: 1, reserved_concurrency: 0, soft_quota: 0, hard_quota: 0, burst: 0 }, rules: [] },
    admission: { enabled: false, mode: "defer", max_pending: 0, max_pending_per_fairness_key: 0, max_oldest_ready_age: "", max_retry_backlog: 0, max_dead_letter_size: 0, defer_interval: "1s" },
    adaptive: { enabled: false, min_concurrency: 1, max_concurrency: 16, control_period: "5s", cooldown: "30s", scale_up_step: 1, scale_down_step: 1, latency_threshold: "", error_rate_threshold: 0, backlog_threshold: 0, healthy_windows_required: 0 },
  };
}
function newFairnessRule() { return { name: "", keys: "", weight: 1, reserved_concurrency: 0, soft_quota: 0, hard_quota: 0, burst: 0 }; }
function newRoute() { return { name: "", task_names: "", queues: "", fairness_keys: "", traffic_classes: "", queue: "", shard: "", shards: "", shard_by: "" }; }
function newSchedule() { return { id: "", interval: "1m", queue: "default", fairness_key: "", task_name: "", payload: "{}", enabled: true, start_at: "" }; }

// ---------- render: builder forms ----------
function renderCore() {
  const c = $("#core-fields");
  c.innerHTML = "";
  c.append(
    field(state.core, "TASKFORGE_SERVICE_NAME", "Service name", { desc: "identifies this process in logs/traces" }),
    field(state.core, "TASKFORGE_LOG_LEVEL", "Log level", { type: "select", options: ["debug", "info", "warn", "error"] }),
    field(state.core, "TASKFORGE_HTTP_ADDR", "HTTP address", { placeholder: "127.0.0.1:8080", desc: "loopback by default; operator routes also require TASKFORGE_HTTP_AUTH_TOKEN" }),
    field(state.core, "TASKFORGE_REDIS_ADDR", "Redis address", { placeholder: "localhost:6379", desc: "host:port of the broker/store backend" }),
    field(state.core, "TASKFORGE_REDIS_PASSWORD", "Redis password", { placeholder: "(none)", desc: "leave blank if unauthenticated" }),
    field(state.core, "TASKFORGE_REDIS_DB", "Redis DB", { type: "number", min: 0, desc: "logical DB number" }),
    field(state.core, "TASKFORGE_OTEL_ENABLED", "Enable OTEL tracing", { type: "checkbox" }),
  );
}
function renderTiming() {
  const c = $("#timing-fields");
  c.innerHTML = "";
  const labels = {
    TASKFORGE_POLL_INTERVAL: ["Poll interval", "how often a worker checks Redis for new work"],
    TASKFORGE_SHUTDOWN_TIMEOUT: ["Shutdown timeout", "grace period to drain before force-cancel"],
    TASKFORGE_SCHEDULER_LOCK_TTL: ["Scheduler lock TTL", "leader lease lifetime; must be > renew interval"],
    TASKFORGE_SCHEDULER_RENEW_INTERVAL: ["Scheduler renew interval", "how often the leader refreshes its lock"],
    TASKFORGE_TASK_SUCCESS_RETENTION: ["Success retention", "how long succeeded task records are kept"],
    TASKFORGE_TASK_FAILURE_RETENTION: ["Failure retention", "how long failed task records are kept"],
    TASKFORGE_TASK_PAYLOAD_RETENTION: ["Payload retention", "how long result payloads are kept"],
  };
  Object.keys(labels).forEach((k) => c.append(field(state.timing, k, labels[k][0], { desc: labels[k][1] })));
}

// generic collapsible item
function itemCard(titleText, badgeText, onRemove, bodyEl, open) {
  const item = el("div", { class: "item" + (open ? " open" : "") });
  const header = el("header", null, [
    el("span", { class: "chev" }, "›"),
    el("span", { class: "title" }, titleText || "(unnamed)"),
    badgeText ? el("span", { class: "badge" }, badgeText) : null,
    el("span", { class: "spacer" }),
    el("button", { class: "btn ghost danger", onclick: (e) => { e.stopPropagation(); onRemove(); } }, "Remove"),
  ]);
  header.addEventListener("click", () => item.classList.toggle("open"));
  item.append(header, bodyEl);
  return item;
}

function renderPools() {
  const c = $("#pools-list");
  c.innerHTML = "";
  if (!state.pools.length) c.appendChild(el("div", { class: "empty" }, "No worker pools yet. A running worker needs at least one."));
  state.pools.forEach((p) => {
    const body = el("div", { class: "item-body" });
    const g = el("div", { class: "grid" });
    g.append(
      field(p, "name", "Pool name", { placeholder: "default", desc: "unique pool identifier" }),
      field(p, "queue", "Queue", { placeholder: "default", desc: "Redis stream this pool consumes" }),
      field(p, "concurrency", "Concurrency", { type: "number", min: 1, desc: "tasks run in parallel" }),
      field(p, "prefetch", "Prefetch", { type: "number", min: 0, desc: "deliveries reserved ahead of time" }),
      field(p, "lease_ttl", "Lease TTL", { placeholder: "30s", desc: "time to ack before redelivery" }),
    );
    body.appendChild(g);

    // retry
    body.appendChild(subsection("Retry policy", () => {
      const rg = el("div", { class: "grid three" });
      const r = p.retry;
      rg.append(
        field(r, "max_deliveries", "Max deliveries", { type: "number", min: 0, desc: "broker delivery cap" }),
        field(r, "multiplier", "Backoff multiplier", { type: "number", desc: "growth factor per retry, e.g. 2" }),
        field(r, "initial_backoff", "Initial backoff", { placeholder: "1s", desc: "wait before first retry" }),
        field(r, "max_backoff", "Max backoff", { placeholder: "1m", desc: "cap on retry wait" }),
        field(r, "jitter", "Jitter (0–1)", { type: "number", desc: "randomness to avoid thundering herd" }),
        field(r, "max_task_age", "Max task age", { placeholder: "(optional)", desc: "give up once a task is this old" }),
      );
      return rg;
    }, "How failed deliveries are retried before going to the dead-letter queue. Handlers must be idempotent — duplicate deliveries are expected (at-least-once)."));

    // per-pool task limits
    body.appendChild(subsectionList("Per-pool task-type limits", p.taskLimits,
      () => ({ task_name: "", max_concurrency: 1 }),
      (lim) => {
        const row = el("div", { class: "grid" });
        row.append(field(lim, "task_name", "Task name"), field(lim, "max_concurrency", "Max concurrency", { type: "number", min: 1 }));
        return row;
      }, "+ Add task limit"));

    // fairness
    body.appendChild(toggleSection("Fairness", p.fairness, "enabled", "Enable fairness", () => {
      const wrap = el("div");
      const dr = p.fairness.defaultRule;
      const dg = el("div", { class: "grid three" });
      dg.append(
        field(dr, "weight", "Default weight", { type: "number", min: 0, desc: "share for unmatched keys" }),
        field(dr, "reserved_concurrency", "Reserved conc.", { type: "number", min: 0, desc: "always-available slots" }),
        field(dr, "burst", "Burst", { type: "number", min: 0, desc: "slots above quota briefly" }),
        field(dr, "soft_quota", "Soft quota", { type: "number", min: 0, desc: "target slots; 0 = none" }),
        field(dr, "hard_quota", "Hard quota", { type: "number", min: 0, desc: "absolute cap; 0 = none" }),
      );
      wrap.appendChild(el("div", { class: "subsection" }, [el("div", { class: "sub-h" }, "Default rule"), el("div", { class: "section-desc" }, "Applies to any fairness key not matched by a rule below."), dg]));
      wrap.appendChild(subsectionList("Rules", p.fairness.rules, newFairnessRule, (rule) => {
        const rg = el("div", { class: "grid three" });
        rg.append(
          field(rule, "name", "Name", { desc: "label only" }),
          field(rule, "keys", "Keys (comma sep)", { desc: "required, e.g. tenant:acme" }),
          field(rule, "weight", "Weight", { type: "number", min: 0, desc: "relative share" }),
          field(rule, "reserved_concurrency", "Reserved", { type: "number", min: 0, desc: "guaranteed slots" }),
          field(rule, "soft_quota", "Soft quota", { type: "number", min: 0, desc: "target slots" }),
          field(rule, "hard_quota", "Hard quota", { type: "number", min: 0, desc: "hard cap" }),
          field(rule, "burst", "Burst", { type: "number", min: 0, desc: "slots above quota" }),
        );
        return rg;
      }, "+ Add fairness rule"));
      return wrap;
    }, "Splits a queue's concurrency fairly across tenants/keys so one heavy key can't starve others."));

    // admission
    body.appendChild(toggleSection("Admission control", p.admission, "enabled", "Enable admission control", () => {
      const a = p.admission;
      const ag = el("div", { class: "grid three" });
      ag.append(
        field(a, "mode", "Mode", { type: "select", options: ["defer", "reject", "disabled"], desc: "defer = delay; reject = fail fast" }),
        field(a, "defer_interval", "Defer interval", { placeholder: "1s", desc: "retry delay when deferring" }),
        field(a, "max_pending", "Max pending", { type: "number", min: 0, desc: "cap on ready tasks; 0 = off" }),
        field(a, "max_pending_per_fairness_key", "Max pending / key", { type: "number", min: 0, desc: "per-tenant cap; 0 = off" }),
        field(a, "max_oldest_ready_age", "Max oldest ready age", { placeholder: "(optional)", desc: "shed when backlog is too old" }),
        field(a, "max_retry_backlog", "Max retry backlog", { type: "number", min: 0, desc: "cap on retrying tasks; 0 = off" }),
        field(a, "max_dead_letter_size", "Max DLQ size", { type: "number", min: 0, desc: "shed when DLQ is full; 0 = off" }),
      );
      return ag;
    }, "Sheds or delays new work when the queue is overloaded. A zero limit means that signal is ignored."));

    // adaptive
    body.appendChild(toggleSection("Adaptive concurrency", p.adaptive, "enabled", "Enable adaptive concurrency", () => {
      const a = p.adaptive;
      const ag = el("div", { class: "grid three" });
      ag.append(
        field(a, "min_concurrency", "Min concurrency", { type: "number", min: 1, desc: "never scale below this" }),
        field(a, "max_concurrency", "Max concurrency", { type: "number", min: 1, desc: "never scale above this" }),
        field(a, "control_period", "Control period", { placeholder: "5s", desc: "how often to re-evaluate" }),
        field(a, "cooldown", "Cooldown", { placeholder: "30s", desc: "min wait between changes" }),
        field(a, "scale_up_step", "Scale-up step", { type: "number", min: 1, desc: "slots added per step" }),
        field(a, "scale_down_step", "Scale-down step", { type: "number", min: 1, desc: "slots removed per step" }),
        field(a, "latency_threshold", "Latency threshold", { placeholder: "(optional)", desc: "scale down above this" }),
        field(a, "error_rate_threshold", "Error-rate threshold", { type: "number", desc: "0–1; scale down above this" }),
        field(a, "backlog_threshold", "Backlog threshold", { type: "number", min: 0, desc: "scale up above this" }),
        field(a, "healthy_windows_required", "Healthy windows", { type: "number", min: 0, desc: "good periods before scaling up" }),
      );
      return ag;
    }, "Auto-tunes this pool's concurrency between min and max based on latency, error rate, and backlog."));

    c.appendChild(itemCard(p.name || "(unnamed pool)", p.queue ? "queue: " + p.queue : "", () => { rm(state.pools, p); renderPools(); render(); }, body, p._open));
  });
}

function subsection(title, buildBody, desc) {
  const head = el("div", { class: "sub-h" }, title);
  const children = [head, buildBody()];
  if (desc) children.splice(1, 0, el("div", { class: "section-desc" }, desc));
  return el("div", { class: "subsection" }, children);
}

// A "Enable X" checkbox whose body shows/hides in place when toggled, without
// re-rendering (and collapsing) the surrounding pool card.
function toggleSection(title, obj, key, label, buildBody, desc) {
  const body = el("div");
  function redraw() { body.innerHTML = ""; if (obj[key]) body.appendChild(buildBody()); }
  const cb = el("input", { type: "checkbox" });
  cb.checked = !!obj[key];
  cb.addEventListener("change", () => { obj[key] = cb.checked; redraw(); render(); });
  redraw();
  const children = [el("div", { class: "sub-h" }, title)];
  if (desc) children.push(el("div", { class: "section-desc" }, desc));
  children.push(el("label", { class: "field check" }, [cb, el("span", null, label)]), body);
  return el("div", { class: "subsection" }, children);
}

// A subsection holding an editable list of sub-items with add/remove.
function subsectionList(title, arr, factory, buildRow, addLabel) {
  const wrap = el("div", { class: "subsection" });
  const head = el("div", { class: "sub-h" }, title);
  wrap.appendChild(head);
  const list = el("div");
  function redraw() {
    list.innerHTML = "";
    if (!arr.length) list.appendChild(el("div", { class: "empty" }, "None."));
    arr.forEach((entry) => {
      const row = el("div", { class: "item", style: "margin-bottom:8px" });
      const rb = el("div", { class: "item-body open", style: "display:block" });
      rb.appendChild(buildRow(entry));
      rb.appendChild(el("div", { class: "btn-row", style: "margin-top:8px" }, [
        el("button", { class: "btn ghost danger", onclick: () => { rm(arr, entry); redraw(); render(); } }, "Remove"),
      ]));
      row.appendChild(rb);
      list.appendChild(row);
    });
  }
  redraw();
  wrap.appendChild(list);
  wrap.appendChild(el("div", { class: "btn-row inline-add" }, [
    el("button", { class: "btn", onclick: () => { arr.push(factory()); redraw(); render(); } }, addLabel),
  ]));
  return wrap;
}

function renderBudgets() {
  const c = $("#budgets-list");
  c.innerHTML = "";
  if (!state.budgets.length) c.appendChild(el("div", { class: "empty" }, "No dependency budgets."));
  state.budgets.forEach((b) => {
    const row = el("div", { class: "item" }, [el("div", { class: "item-body open", style: "display:block" }, [
      el("div", { class: "grid" }, [field(b, "name", "Budget name", { desc: "e.g. smtp, thirdparty-api" }), field(b, "capacity", "Capacity", { type: "number", min: 1, desc: "max concurrent tokens cluster-wide" })]),
      el("div", { class: "btn-row", style: "margin-top:8px" }, [el("button", { class: "btn ghost danger", onclick: () => { rm(state.budgets, b); renderBudgets(); render(); } }, "Remove")]),
    ])]);
    c.appendChild(row);
  });
}
function renderTaskBudgets() {
  const c = $("#taskbudgets-list");
  c.innerHTML = "";
  if (!state.taskBudgets.length) c.appendChild(el("div", { class: "empty" }, "No task→budget mappings."));
  const budgetNames = state.budgets.map((b) => b.name).filter(Boolean);
  state.taskBudgets.forEach((m) => {
    const row = el("div", { class: "item" }, [el("div", { class: "item-body open", style: "display:block" }, [
      el("div", { class: "grid three" }, [
        field(m, "task_name", "Task name", { desc: "task type to charge" }),
        field(m, "budget", "Budget", budgetNames.length ? { type: "select", options: ["", ...budgetNames], desc: "must be a budget defined above" } : { desc: "must match a budget name above" }),
        field(m, "tokens", "Tokens", { type: "number", min: 1, desc: "tokens consumed per run; blank = 1" }),
      ]),
      el("div", { class: "btn-row", style: "margin-top:8px" }, [el("button", { class: "btn ghost danger", onclick: () => { rm(state.taskBudgets, m); renderTaskBudgets(); render(); } }, "Remove")]),
    ])]);
    c.appendChild(row);
  });
}
function renderTaskLimits() {
  const c = $("#tasklimits-list");
  c.innerHTML = "";
  if (!state.taskLimits.length) c.appendChild(el("div", { class: "empty" }, "No global task-type limits."));
  state.taskLimits.forEach((l) => {
    const row = el("div", { class: "item" }, [el("div", { class: "item-body open", style: "display:block" }, [
      el("div", { class: "grid" }, [field(l, "task_name", "Task name", { desc: "task type to cap" }), field(l, "max_concurrency", "Max concurrency", { type: "number", min: 1, desc: "max in-flight of this task across all pools" })]),
      el("div", { class: "btn-row", style: "margin-top:8px" }, [el("button", { class: "btn ghost danger", onclick: () => { rm(state.taskLimits, l); renderTaskLimits(); render(); } }, "Remove")]),
    ])]);
    c.appendChild(row);
  });
}

function renderRouting() {
  $("#routing-enabled").checked = state.routing.enabled;
  $("#routing-body").style.display = state.routing.enabled ? "block" : "none";
  $("#routing-default-queue").value = state.routing.defaultQueue;
  $("#routing-default-shard").value = state.routing.defaultShard;
  const c = $("#routing-rules");
  c.innerHTML = "";
  if (!state.routing.rules.length) c.appendChild(el("div", { class: "empty" }, "No rules — only the defaults apply."));
  state.routing.rules.forEach((r) => {
    const body = el("div", { class: "item-body" });
    body.append(
      el("div", { class: "grid" }, [field(r, "name", "Rule name")]),
      subsection("Match (comma-separated lists, all optional)", () => {
        const g = el("div", { class: "grid" });
        g.append(
          field(r, "task_names", "Task names", { desc: "match these task types" }),
          field(r, "queues", "Queues", { desc: "match tasks published to these queues" }),
          field(r, "fairness_keys", "Fairness keys", { desc: "match these tenant/keys" }),
          field(r, "traffic_classes", "Traffic classes", { desc: "match these traffic classes" }),
        );
        return g;
      }, "A task matches this rule when it satisfies every list you fill in. Empty lists are ignored."),
      subsection("Destination", () => {
        const g = el("div", { class: "grid" });
        g.append(
          field(r, "queue", "Queue", { desc: "send matched tasks here" }),
          field(r, "shard", "Shard", { desc: "fixed shard suffix (optional)" }),
          field(r, "shards", "Shards (comma sep)", { desc: "spread across these shards" }),
          field(r, "shard_by", "Shard by", { desc: "field used to pick a shard" }),
        );
        return g;
      }, "Where matched tasks are routed."),
    );
    c.appendChild(itemCard(r.name || "(unnamed rule)", r.queue ? "→ " + r.queue : "", () => { rm(state.routing.rules, r); renderRouting(); render(); }, body, true));
  });
}

function renderSchedules() {
  const c = $("#schedules-list");
  c.innerHTML = "";
  if (!state.schedules.length) c.appendChild(el("div", { class: "empty" }, "No recurring schedules."));
  state.schedules.forEach((s) => {
    const body = el("div", { class: "item-body" });
    const g = el("div", { class: "grid" });
    g.append(
      field(s, "id", "Schedule id", { desc: "required, unique identifier" }),
      field(s, "task_name", "Task name", { desc: "required; task type to enqueue" }),
      field(s, "interval", "Interval", { placeholder: "1m", desc: "how often to fire, e.g. 1m, 24h" }),
      field(s, "queue", "Queue", { placeholder: "default", desc: "queue to publish into" }),
      field(s, "fairness_key", "Fairness key", { placeholder: "(optional)", desc: "tenant/key for fairness" }),
      field(s, "start_at", "Start at (RFC3339)", { placeholder: "(optional)", desc: "first fire time, e.g. 2026-01-01T00:00:00Z" }),
    );
    body.appendChild(g);
    body.appendChild(field(s, "enabled", "Enabled", { type: "checkbox" }));
    body.appendChild(el("label", { class: "field", style: "margin-top:8px" }, [
      el("span", null, "Payload (JSON, required)"),
      (() => {
        const ta = el("textarea", { placeholder: "{}" });
        ta.value = s.payload;
        ta.addEventListener("input", () => { s.payload = ta.value; render(); });
        return ta;
      })(),
    ]));
    c.appendChild(itemCard(s.id || "(unnamed schedule)", s.task_name ? s.task_name : "", () => { rm(state.schedules, s); renderSchedules(); render(); }, body, true));
  });
}

function rm(arr, item) { const i = arr.indexOf(item); if (i >= 0) arr.splice(i, 1); }
function splitList(str) { return (str || "").split(",").map((x) => x.trim()).filter(Boolean); }
function num(v) { return v === "" || v == null ? 0 : Number(v); }

// ---------- serialization + validation ----------
function buildConfig() {
  const errors = [];
  const env = {};

  // plain values
  Object.assign(env, scalarEnv(state.core));
  Object.assign(env, scalarEnv(state.timing));

  // worker pools
  if (state.pools.length) {
    const arr = state.pools.map((p, idx) => {
      if (!p.name.trim()) errors.push(`Pool #${idx + 1}: name is required`);
      if (!p.queue.trim()) errors.push(`Pool "${p.name || idx + 1}": queue is required`);
      if (num(p.concurrency) < 1) errors.push(`Pool "${p.name}": concurrency must be >= 1`);
      const out = { name: p.name, queue: p.queue, concurrency: num(p.concurrency), prefetch: num(p.prefetch) };
      if (p.lease_ttl) out.lease_ttl = p.lease_ttl;
      out.retry = compactRetry(p.retry);
      if (p.taskLimits.length) {
        out.task_limits = p.taskLimits.map((l) => {
          if (!l.task_name.trim()) errors.push(`Pool "${p.name}": task limit needs a task_name`);
          if (num(l.max_concurrency) < 1) errors.push(`Pool "${p.name}": task "${l.task_name}" max_concurrency must be >= 1`);
          return { task_name: l.task_name, max_concurrency: num(l.max_concurrency) };
        });
      }
      if (p.fairness.enabled) {
        const f = { default_rule: compactRule(p.fairness.defaultRule, true) };
        f.rules = p.fairness.rules.map((r) => {
          if (!splitList(r.keys).length) errors.push(`Pool "${p.name}": fairness rule "${r.name}" needs keys`);
          return compactRule(r, false);
        });
        out.fairness = f;
      }
      if (p.admission.enabled) {
        const a = p.admission;
        const adm = { mode: a.mode };
        ["max_pending", "max_pending_per_fairness_key", "max_retry_backlog", "max_dead_letter_size"].forEach((k) => { if (num(a[k]) > 0) adm[k] = num(a[k]); });
        if (a.max_oldest_ready_age) adm.max_oldest_ready_age = a.max_oldest_ready_age;
        if (a.defer_interval) adm.defer_interval = a.defer_interval;
        out.admission = adm;
      }
      if (p.adaptive.enabled) {
        const a = p.adaptive;
        const ad = { enabled: true, min_concurrency: num(a.min_concurrency), max_concurrency: num(a.max_concurrency) };
        if (ad.max_concurrency < ad.min_concurrency) errors.push(`Pool "${p.name}": adaptive max_concurrency < min_concurrency`);
        ["control_period", "cooldown", "latency_threshold"].forEach((k) => { if (a[k]) ad[k] = a[k]; });
        ["scale_up_step", "scale_down_step", "backlog_threshold", "healthy_windows_required"].forEach((k) => { if (num(a[k]) > 0) ad[k] = num(a[k]); });
        if (num(a.error_rate_threshold) > 0) ad.error_rate_threshold = num(a.error_rate_threshold);
        out.adaptive = ad;
      }
      return out;
    });
    env.TASKFORGE_WORKER_POOLS_JSON = JSON.stringify(arr);
  }

  // dependency budgets
  if (state.budgets.length) {
    const seen = new Set();
    const arr = state.budgets.map((b) => {
      if (!b.name.trim()) errors.push("Dependency budget: name is required");
      if (seen.has(b.name)) errors.push(`Duplicate budget "${b.name}"`);
      seen.add(b.name);
      if (num(b.capacity) < 1) errors.push(`Budget "${b.name}": capacity must be >= 1`);
      return { name: b.name, capacity: num(b.capacity) };
    });
    env.TASKFORGE_DEPENDENCY_BUDGETS_JSON = JSON.stringify(arr);
  }

  // task budgets
  if (state.taskBudgets.length) {
    const budgetNames = new Set(state.budgets.map((b) => b.name));
    const arr = state.taskBudgets.map((m) => {
      if (!m.task_name.trim()) errors.push("Task budget: task_name is required");
      if (!m.budget.trim()) errors.push(`Task "${m.task_name}": budget is required`);
      else if (!budgetNames.has(m.budget)) errors.push(`Task "${m.task_name}": references unknown budget "${m.budget}"`);
      const out = { task_name: m.task_name, budget: m.budget };
      if (num(m.tokens) > 0) out.tokens = num(m.tokens);
      return out;
    });
    env.TASKFORGE_TASK_BUDGETS_JSON = JSON.stringify(arr);
  }

  // global task-type limits
  if (state.taskLimits.length) {
    const arr = state.taskLimits.map((l) => {
      if (!l.task_name.trim()) errors.push("Task-type limit: task_name is required");
      if (num(l.max_concurrency) < 1) errors.push(`Task "${l.task_name}": max_concurrency must be >= 1`);
      return { task_name: l.task_name, max_concurrency: num(l.max_concurrency) };
    });
    env.TASKFORGE_TASK_TYPE_LIMITS_JSON = JSON.stringify(arr);
  }

  // routing
  if (state.routing.enabled) {
    const r = state.routing;
    const pol = {};
    if (r.defaultQueue) pol.default_queue = r.defaultQueue;
    if (r.defaultShard) pol.default_shard = r.defaultShard;
    pol.rules = r.rules.map((rule) => {
      const match = {};
      if (splitList(rule.task_names).length) match.task_names = splitList(rule.task_names);
      if (splitList(rule.queues).length) match.queues = splitList(rule.queues);
      if (splitList(rule.fairness_keys).length) match.fairness_keys = splitList(rule.fairness_keys);
      if (splitList(rule.traffic_classes).length) match.traffic_classes = splitList(rule.traffic_classes);
      const dest = {};
      if (rule.queue) dest.queue = rule.queue;
      if (rule.shard) dest.shard = rule.shard;
      if (splitList(rule.shards).length) dest.shards = splitList(rule.shards);
      if (rule.shard_by) dest.shard_by = rule.shard_by;
      return { name: rule.name, match, destination: dest };
    });
    env.TASKFORGE_ROUTING_POLICY_JSON = JSON.stringify(pol);
  }

  // schedules
  if (state.schedules.length) {
    const seen = new Set();
    const arr = state.schedules.map((s) => {
      if (!s.id.trim()) errors.push("Schedule: id is required");
      if (seen.has(s.id)) errors.push(`Duplicate schedule id "${s.id}"`);
      seen.add(s.id);
      if (!s.task_name.trim()) errors.push(`Schedule "${s.id}": task_name is required`);
      if (!s.interval.trim()) errors.push(`Schedule "${s.id}": interval is required`);
      let payload = {};
      try { payload = JSON.parse(s.payload || "{}"); }
      catch (e) { errors.push(`Schedule "${s.id}": payload is not valid JSON`); }
      const out = { id: s.id, interval: s.interval, queue: s.queue || "default", task_name: s.task_name, payload, enabled: !!s.enabled };
      if (s.fairness_key) out.fairness_key = s.fairness_key;
      if (s.start_at) out.start_at = s.start_at;
      return out;
    });
    env.TASKFORGE_SCHEDULES_JSON = JSON.stringify(arr);
  }

  return { env, errors };
}

function compactRetry(r) {
  const out = {};
  if (num(r.max_deliveries) > 0) out.max_deliveries = num(r.max_deliveries);
  if (r.initial_backoff) out.initial_backoff = r.initial_backoff;
  if (r.max_backoff) out.max_backoff = r.max_backoff;
  if (num(r.multiplier) > 0) out.multiplier = num(r.multiplier);
  if (num(r.jitter) > 0) out.jitter = num(r.jitter);
  if (r.max_task_age) out.max_task_age = r.max_task_age;
  return out;
}
function compactRule(r, isDefault) {
  const out = {};
  if (r.name) out.name = r.name;
  if (!isDefault && r.keys) out.keys = splitList(r.keys);
  ["weight", "reserved_concurrency", "soft_quota", "hard_quota", "burst"].forEach((k) => { if (num(r[k]) > 0) out[k] = num(r[k]); });
  return out;
}

// only emit non-default scalars; keep it readable
function scalarEnv(obj) {
  const out = {};
  for (const k in obj) {
    const v = obj[k];
    if (typeof v === "boolean") { if (v) out[k] = "true"; }
    else if (v !== "" && v != null) out[k] = String(v);
  }
  return out;
}

// ---------- output rendering ----------
let outputFormat = "env";
function render() {
  const { env, errors } = buildConfig();
  const keys = Object.keys(env);
  const pre = $("#output");
  pre.innerHTML = "";
  keys.forEach((k) => {
    let line;
    if (outputFormat === "shell") line = `export ${k}=${shellQuote(env[k])}\n`;
    else if (outputFormat === "compose") line = `      - ${k}=${env[k]}\n`;
    else line = `${k}=${env[k]}\n`;
    const eq = line.indexOf("=");
    pre.appendChild(el("span", { class: "key" }, line.slice(0, eq + 1)));
    pre.appendChild(el("span", { class: "val" }, line.slice(eq + 1)));
  });
  if (!keys.length) pre.appendChild(el("span", { class: "cmt" }, "# nothing configured yet\n"));

  const status = $("#output-status");
  if (errors.length) {
    status.className = "status-line err";
    status.textContent = `${errors.length} issue${errors.length > 1 ? "s" : ""}: ${errors[0]}${errors.length > 1 ? ` (+${errors.length - 1} more)` : ""}`;
    status.title = errors.join("\n");
  } else {
    status.className = "status-line ok";
    status.textContent = `Valid · ${keys.length} variable${keys.length === 1 ? "" : "s"}`;
    status.title = "";
  }
}
function shellQuote(v) { return /[^A-Za-z0-9_./:-]/.test(v) ? "'" + v.replace(/'/g, "'\\''") + "'" : v; }
function currentEnvText() {
  const { env } = buildConfig();
  return Object.keys(env).map((k) => {
    if (outputFormat === "shell") return `export ${k}=${shellQuote(env[k])}`;
    if (outputFormat === "compose") return `      - ${k}=${env[k]}`;
    return `${k}=${env[k]}`;
  }).join("\n") + "\n";
}

// ---------- live ops ----------
function opsBase() { return ($("#ops-base").value || "").replace(/\/$/, ""); }
async function fetchJSON(path) {
  const res = await fetch(opsBase() + path, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}: ${(await res.text()).trim()}`);
  return res.json();
}
function pill(text, kind) { return el("span", { class: "pill " + kind }, text); }
function statePill(s) {
  const k = { running: "green", draining: "yellow", drained: "gray", ok: "green", admitting: "green", deferring: "yellow", rejecting: "red", deferred: "yellow", rejected: "red" };
  return pill(s || "—", k[(s || "").toLowerCase()] || "blue");
}
function fmtTime(t) { if (!t) return "—"; try { return new Date(t).toLocaleString(); } catch (e) { return t; } }

async function refreshOps() {
  const status = $("#ops-status");
  status.className = "status-line muted";
  status.textContent = "Loading…";
  let ok = true;
  await Promise.all([loadWorkers(), loadAdmission(), loadAdaptive()].map((p) => p.catch((e) => { ok = false; return e; })));
  status.className = ok ? "status-line ok" : "status-line err";
  status.textContent = ok ? "Updated " + new Date().toLocaleTimeString() : "Some endpoints failed — is the api binary running and the base URL correct?";
}

async function loadWorkers() {
  const c = $("#ops-workers");
  try {
    const data = await fetchJSON("/v1/admin/workers");
    const ws = data.workers || [];
    if (!ws.length) return void (c.innerHTML = "", c.appendChild(el("div", { class: "empty" }, "No workers reporting.")));
    const tbl = el("table", { class: "data" });
    tbl.appendChild(el("tr", null, ["Worker", "Pool", "Queue", "State", "Running", "Pending", "Updated"].map((h) => el("th", null, h))));
    ws.forEach((w) => tbl.appendChild(el("tr", null, [
      el("td", null, w.worker_id), el("td", null, w.pool), el("td", null, w.queue),
      el("td", null, statePill(w.state)), el("td", null, String(w.running)), el("td", null, String(w.pending)),
      el("td", { class: "muted" }, fmtTime(w.updated_at)),
    ])));
    c.innerHTML = ""; c.appendChild(tbl);
  } catch (e) { c.innerHTML = ""; c.appendChild(el("div", { class: "status-line err" }, String(e.message || e))); throw e; }
}

async function loadAdmission() {
  const c = $("#ops-admission");
  try {
    const data = await fetchJSON("/v1/admin/admission");
    const qs = data.queues || [];
    if (!qs.length) return void (c.innerHTML = "", c.appendChild(el("div", { class: "empty" }, "No queues.")));
    const tbl = el("table", { class: "data" });
    tbl.appendChild(el("tr", null, ["Queue", "Mode", "State", "Pending", "Oldest age (s)", "Retry backlog", "DLQ", "Reason"].map((h) => el("th", null, h))));
    qs.forEach((q) => {
      const s = q.signals || {};
      tbl.appendChild(el("tr", null, [
        el("td", null, q.queue), el("td", null, q.mode), el("td", null, statePill(q.state)),
        el("td", null, String(s.queue_pending ?? "—")), el("td", null, String(s.oldest_ready_age_secs ?? "—")),
        el("td", null, String(s.retry_backlog ?? "—")), el("td", null, String(s.dead_letter_size ?? "—")),
        el("td", { class: "muted" }, q.reason || "—"),
      ]));
    });
    c.innerHTML = ""; c.appendChild(tbl);
  } catch (e) { c.innerHTML = ""; c.appendChild(el("div", { class: "status-line err" }, String(e.message || e))); throw e; }
}

async function loadAdaptive() {
  const c = $("#ops-adaptive");
  try {
    const data = await fetchJSON("/v1/admin/adaptive");
    c.innerHTML = "";
    const pools = data.pools || [];
    if (pools.length) {
      const tbl = el("table", { class: "data" });
      tbl.appendChild(el("tr", null, ["Pool", "Queue", "Adaptive", "Configured", "Effective", "Min/Max", "Latency", "Err rate", "Backlog", "Last action"].map((h) => el("th", null, h))));
      pools.forEach((p) => {
        const s = p.signals || {};
        tbl.appendChild(el("tr", null, [
          el("td", null, p.pool), el("td", null, p.queue),
          el("td", null, p.adaptive_enabled ? pill("on", "green") : pill("off", "gray")),
          el("td", null, String(p.configured_concurrency)), el("td", null, String(p.effective_concurrency)),
          el("td", null, `${p.min_concurrency}/${p.max_concurrency}`),
          el("td", null, (s.avg_latency_seconds ?? 0).toFixed ? (s.avg_latency_seconds).toFixed(3) + "s" : "—"),
          el("td", null, String(s.error_rate ?? "—")), el("td", null, String(s.backlog ?? "—")),
          el("td", { class: "muted" }, (p.last_adjustment_action || "—") + (p.last_adjustment_reason ? " · " + p.last_adjustment_reason : "")),
        ]));
      });
      c.appendChild(el("div", { class: "sub-h" }, "Pools"));
      c.appendChild(tbl);
    } else {
      c.appendChild(el("div", { class: "empty" }, "No pools."));
    }
    const budgets = data.budgets || [];
    if (budgets.length) {
      const tbl = el("table", { class: "data" });
      tbl.appendChild(el("tr", null, ["Budget", "In use", "Capacity", "Utilization"].map((h) => el("th", null, h))));
      budgets.forEach((b) => {
        const util = b.capacity ? Math.round((b.in_use / b.capacity) * 100) : 0;
        tbl.appendChild(el("tr", null, [
          el("td", null, b.budget), el("td", null, String(b.in_use)), el("td", null, String(b.capacity)),
          el("td", null, pill(util + "%", util >= 90 ? "red" : util >= 60 ? "yellow" : "green")),
        ]));
      });
      c.appendChild(el("div", { class: "sub-h", style: "margin-top:16px" }, "Dependency budgets"));
      c.appendChild(tbl);
    }
  } catch (e) { c.innerHTML = ""; c.appendChild(el("div", { class: "status-line err" }, String(e.message || e))); throw e; }
}

async function lookupTask() {
  const id = $("#ops-task-id").value.trim();
  const c = $("#ops-task");
  if (!id) { c.innerHTML = ""; c.appendChild(el("div", { class: "empty" }, "Enter a task id.")); return; }
  c.innerHTML = ""; c.appendChild(el("div", { class: "muted" }, "Looking up…"));
  try {
    const t = await fetchJSON("/v1/tasks/" + encodeURIComponent(id));
    const dl = el("dl", { class: "kv" });
    const rows = [
      ["Task id", t.task_id], ["Name", t.name], ["Queue", t.queue], ["State", t.state],
      ["Delivery count", t.delivery_count], ["Last delivery id", t.last_delivery_id], ["Last lease owner", t.last_lease_owner],
      ["Created", fmtTime(t.created_at)], ["Started", fmtTime(t.started_at)], ["Completed", fmtTime(t.completed_at)],
      ["Updated", fmtTime(t.updated_at)], ["Last error", t.last_error],
    ];
    rows.forEach(([k, v]) => { if (v != null && v !== "") { dl.appendChild(el("dt", null, k)); dl.appendChild(el("dd", null, k === "State" ? statePill(String(v)) : String(v))); } });
    c.innerHTML = ""; c.appendChild(dl);
    if (t.result_payload) {
      let decoded = t.result_payload;
      try { decoded = atob(t.result_payload); } catch (e) {}
      c.appendChild(el("div", { class: "sub-h", style: "margin-top:14px" }, "Result payload"));
      c.appendChild(el("pre", { style: "font-family:var(--mono);font-size:12px;background:var(--bg-input);padding:10px;border-radius:6px;overflow:auto" }, decoded));
    }
  } catch (e) {
    c.innerHTML = ""; c.appendChild(el("div", { class: "status-line err" }, String(e.message || e)));
  }
}

let autoTimer;
function setAuto(on) {
  clearInterval(autoTimer);
  if (on) autoTimer = setInterval(refreshOps, 5000);
}

// ---------- sample ----------
function loadSample() {
  const p = newPool();
  Object.assign(p, { name: "default", queue: "default", concurrency: 8, prefetch: 16, lease_ttl: "30s" });
  p.admission.enabled = true; p.admission.max_pending = 5000; p.admission.max_dead_letter_size = 1000;
  p.adaptive.enabled = true; p.adaptive.min_concurrency = 2; p.adaptive.max_concurrency = 32; p.adaptive.latency_threshold = "2s"; p.adaptive.backlog_threshold = 1000;
  p.fairness.enabled = true; p.fairness.rules.push(Object.assign(newFairnessRule(), { name: "premium", keys: "tenant:acme,tenant:globex", weight: 5, soft_quota: 50 }));
  state.pools = [p];
  state.budgets = [{ name: "smtp", capacity: 20 }, { name: "thirdparty-api", capacity: 50 }];
  state.taskBudgets = [{ task_name: "send_email", budget: "smtp", tokens: 1 }];
  state.taskLimits = [{ task_name: "transcode_video", max_concurrency: 4 }];
  state.routing = { enabled: true, defaultQueue: "default", defaultShard: "", rules: [Object.assign(newRoute(), { name: "media-to-media-queue", task_names: "transcode_video,thumbnail", queue: "media" })] };
  state.schedules = [Object.assign(newSchedule(), { id: "nightly-report", task_name: "build_report", interval: "24h", queue: "reports", payload: '{"scope":"daily"}' })];
  renderAll();
  toast("Loaded sample configuration");
}

function renderAll() {
  renderCore(); renderTiming(); renderPools(); renderBudgets(); renderTaskBudgets(); renderTaskLimits(); renderRouting(); renderSchedules(); render();
}

// ---------- wire up ----------
function init() {
  // tabs
  $$(".tab").forEach((t) => t.addEventListener("click", () => {
    $$(".tab").forEach((x) => x.classList.remove("active"));
    $$(".view").forEach((x) => x.classList.remove("active"));
    t.classList.add("active");
    $("#view-" + t.dataset.view).classList.add("active");
  }));

  $("#add-pool").addEventListener("click", () => { state.pools.push(newPool()); renderPools(); render(); });
  $("#add-budget").addEventListener("click", () => { state.budgets.push({ name: "", capacity: 1 }); renderBudgets(); render(); });
  $("#add-taskbudget").addEventListener("click", () => { state.taskBudgets.push({ task_name: "", budget: "", tokens: 1 }); renderTaskBudgets(); render(); });
  $("#add-tasklimit").addEventListener("click", () => { state.taskLimits.push({ task_name: "", max_concurrency: 1 }); renderTaskLimits(); render(); });
  $("#add-route").addEventListener("click", () => { state.routing.rules.push(newRoute()); renderRouting(); render(); });
  $("#add-schedule").addEventListener("click", () => { state.schedules.push(newSchedule()); renderSchedules(); render(); });

  $("#routing-enabled").addEventListener("change", (e) => { state.routing.enabled = e.target.checked; renderRouting(); render(); });
  $("#routing-default-queue").addEventListener("input", (e) => { state.routing.defaultQueue = e.target.value; render(); });
  $("#routing-default-shard").addEventListener("input", (e) => { state.routing.defaultShard = e.target.value; render(); });

  $$("#format-toggle button").forEach((b) => b.addEventListener("click", () => {
    outputFormat = b.dataset.fmt;
    $$("#format-toggle button").forEach((x) => x.classList.toggle("active", x === b));
    render();
  }));

  $("#copy-output").addEventListener("click", async () => {
    try { await navigator.clipboard.writeText(currentEnvText()); toast("Copied to clipboard"); }
    catch (e) { toast("Copy failed — select manually"); }
  });
  $("#download-output").addEventListener("click", () => {
    const blob = new Blob([currentEnvText()], { type: "text/plain" });
    const a = el("a", { href: URL.createObjectURL(blob), download: ".env" });
    document.body.appendChild(a); a.click(); a.remove();
  });
  $("#load-sample").addEventListener("click", loadSample);
  $("#reset-all").addEventListener("click", () => {
    if (!confirm("Reset all configuration to defaults?")) return;
    state.pools = []; state.budgets = []; state.taskBudgets = []; state.taskLimits = [];
    state.routing = { enabled: false, defaultQueue: "default", defaultShard: "", rules: [] }; state.schedules = [];
    renderAll(); toast("Reset");
  });

  // ops
  $("#ops-refresh").addEventListener("click", refreshOps);
  $("#ops-auto").addEventListener("change", (e) => setAuto(e.target.checked));
  $("#ops-task-go").addEventListener("click", lookupTask);
  $("#ops-task-id").addEventListener("keydown", (e) => { if (e.key === "Enter") lookupTask(); });

  $("#conn-hint").textContent = location.origin;
  renderAll();
}

document.addEventListener("DOMContentLoaded", init);
