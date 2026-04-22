/* ============================================================
   Nutrition Platform — Frontend
   Change API_BASE to your EC2 public IP once deployed to AWS.
   ============================================================ */
const API_BASE = "https://1y8p8hsuyg.execute-api.us-east-1.amazonaws.com";

// ── Toast ──────────────────────────────────────────────────
let toastTimer = null;

function showToast(userId) {
  const toast = document.getElementById("id-toast");
  document.getElementById("toast-user-id").textContent = userId;
  toast.classList.add("visible");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove("visible"), 7000);
}

document.getElementById("toast-close-btn").addEventListener("click", () => {
  document.getElementById("id-toast").classList.remove("visible");
  clearTimeout(toastTimer);
});

// ── Health check ───────────────────────────────────────────
async function checkHealth() {
  const badge = document.getElementById("api-status");
  try {
    const res = await fetch(`${API_BASE}/health`, { signal: AbortSignal.timeout(4000) });
    if (res.ok) {
      badge.textContent = "API Online";
      badge.className = "api-badge online";
    } else { throw new Error(); }
  } catch {
    badge.textContent = "API Offline";
    badge.className = "api-badge offline";
  }
}
checkHealth();
setInterval(checkHealth, 30000);

// ── Tab navigation ─────────────────────────────────────────
document.querySelectorAll(".nav-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".nav-btn").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById(`tab-${btn.dataset.tab}`).classList.add("active");
  });
});

// ── Helpers ────────────────────────────────────────────────
function show(el)  { el.classList.remove("hidden"); }
function hide(el)  { el.classList.add("hidden"); }
function text(id, val) { document.getElementById(id).textContent = val; }

function showError(id, msg) {
  const el = document.getElementById(id);
  el.textContent = msg;
  show(el);
}

function hideError(id) {
  hide(document.getElementById(id));
}

function deriveGoal(current, target) {
  if (target < current)  return "weight_loss";
  if (target > current)  return "weight_gain";
  return "maintenance";
}

function goalLabel(goal) {
  return { weight_loss: "Weight Loss", weight_gain: "Weight Gain", maintenance: "Maintenance" }[goal] || goal;
}

function formatDate(isoStr) {
  if (!isoStr) return "—";
  return new Date(isoStr).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
}

function formatDateTime(isoStr) {
  if (!isoStr) return "—";
  return new Date(isoStr).toLocaleString("en-GB", { day: "2-digit", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" });
}

function setLoading(btnId, loading) {
  const btn = document.getElementById(btnId);
  const txt = btn.querySelector(".btn-text");
  const spin = btn.querySelector(".btn-spinner");
  btn.disabled = loading;
  if (txt)  txt.style.display = loading ? "none" : "";
  if (spin) spin.classList.toggle("hidden", !loading);
}

// ── Render helpers ─────────────────────────────────────────
function renderGoalBadge(id, goal) {
  const el = document.getElementById(id);
  el.textContent = goalLabel(goal);
  el.className = `goal-badge ${goal}`;
}

function renderRecList(containerId, recs) {
  const container = document.getElementById(containerId);
  if (!recs || recs.length === 0) {
    container.innerHTML = `<p style="color:var(--text-muted);font-size:.85rem">No recommendations found.</p>`;
    return;
  }
  container.innerHTML = recs.map(r => `
    <div class="rec-item">
      <div class="rec-rank ${r.recommendation_rank <= 3 ? "top3" : ""}">#${r.recommendation_rank}</div>
      <div class="rec-body">
        <div class="rec-name">${r.food_name.replace(/_/g, " ")}</div>
        <div class="rec-reason">${r.recommendation_reason || ""}</div>
      </div>
      <div class="rec-score">${Number(r.recommendation_score).toFixed(1)}</div>
    </div>
  `).join("");
}

function renderStatsGrid(containerId, profile) {
  const gap = (profile.current_weight_lb - profile.target_weight_lb).toFixed(1);
  const gapSign = gap > 0 ? "+" : "";
  document.getElementById(containerId).innerHTML = `
    <div class="stat-box"><div class="stat-val">${Number(profile.current_weight_lb).toFixed(1)}</div><div class="stat-lbl">Current lb</div></div>
    <div class="stat-box"><div class="stat-val">${Number(profile.target_weight_lb).toFixed(1)}</div><div class="stat-lbl">Target lb</div></div>
    <div class="stat-box"><div class="stat-val" style="color:${gap > 0 ? "var(--orange)" : "var(--green)"}">${gapSign}${gap}</div><div class="stat-lbl">Gap lb</div></div>
    <div class="stat-box"><div class="stat-val">${profile.height_cm}</div><div class="stat-lbl">Height cm</div></div>
    <div class="stat-box"><div class="stat-val">${profile.age || "—"}</div><div class="stat-lbl">Age</div></div>
    <div class="stat-box"><div class="stat-val">${profile.gender || "—"}</div><div class="stat-lbl">Gender</div></div>
  `;
}

// ── TAB 1: Create User ─────────────────────────────────────
const cwInput = document.getElementById("c-current-wt");
const twInput = document.getElementById("c-target-wt");

function updateGoalPreview() {
  const cw = parseFloat(cwInput.value);
  const tw = parseFloat(twInput.value);
  const preview = document.getElementById("create-goal-preview");
  if (!cw || !tw) { hide(preview); return; }
  const goal = deriveGoal(cw, tw);
  const labels = {
    weight_loss:  "⬇ Weight Loss — your recommendations will prioritise high-protein, high-fibre, low-calorie foods.",
    weight_gain:  "⬆ Weight Gain — your recommendations will prioritise calorie-dense, high-protein foods.",
    maintenance:  "⟺ Maintenance — your recommendations will prioritise balanced, moderate-calorie foods.",
  };
  preview.textContent = labels[goal];
  show(preview);
}

cwInput.addEventListener("input", updateGoalPreview);
twInput.addEventListener("input", updateGoalPreview);

document.getElementById("create-form").addEventListener("submit", async e => {
  e.preventDefault();
  hideError("create-error");
  hide(document.getElementById("create-results"));
  setLoading("create-btn", true);

  const payload = {
    user_name:          document.getElementById("c-name").value.trim(),
    gender:             document.getElementById("c-gender").value,
    birth_date:         document.getElementById("c-dob").value,
    height_cm:          parseFloat(document.getElementById("c-height").value),
    current_weight_lb:  parseFloat(cwInput.value),
    target_weight_lb:   parseFloat(twInput.value),
  };

  try {
    // 1. Create user
    const createRes = await fetch(`${API_BASE}/users`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const createData = await createRes.json();
    if (!createRes.ok) {
      const detail = createData.details ? createData.details.join("; ") : (createData.detail || "Unknown error");
      throw new Error(detail);
    }

    const userId = createData.user_id;

    // 2. Poll for recommendations (consumer is async — allow up to 5s)
    let recs = [];
    for (let attempt = 0; attempt < 10; attempt++) {
      await new Promise(r => setTimeout(r, 500));
      const recRes = await fetch(`${API_BASE}/recommendations/${userId}`);
      if (recRes.ok) { const d = await recRes.json(); recs = d.recommendations; break; }
    }

    // 3. Render results
    const goal = deriveGoal(payload.current_weight_lb, payload.target_weight_lb);
    text("create-user-id", userId);
    text("create-user-name", payload.user_name);
    renderGoalBadge("create-goal-badge", goal);
    text("create-meta", `Age ${calcAge(payload.birth_date)} · ${payload.height_cm} cm`);
    renderRecList("create-recs", recs);
    show(document.getElementById("create-results"));
    showToast(userId);

  } catch (err) {
    showError("create-error", err.message);
  } finally {
    setLoading("create-btn", false);
  }
});

function calcAge(dob) {
  const bd = new Date(dob);
  const today = new Date();
  let age = today.getFullYear() - bd.getFullYear();
  if (today < new Date(today.getFullYear(), bd.getMonth(), bd.getDate())) age--;
  return age;
}

// ── TAB 2: Look Up User ────────────────────────────────────
let currentLookupId = null;
let progressChart   = null;
let allUsers        = [];    // cache from GET /users
let focusedUdIndex  = -1;   // keyboard navigation index

// Load all users whenever the lookup tab becomes active
document.querySelectorAll(".nav-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    if (btn.dataset.tab === "lookup") loadAllUsers();
  });
});

async function loadAllUsers() {
  try {
    const res = await fetch(`${API_BASE}/users`);
    if (res.ok) { const d = await res.json(); allUsers = d.users || []; }
  } catch { /* silent — dropdown just won't populate */ }
}

// Filter and render dropdown as user types
document.getElementById("lookup-id").addEventListener("input", () => {
  const q = document.getElementById("lookup-id").value.trim().toLowerCase();
  if (!q) { closeDropdown(); return; }

  const matches = allUsers.filter(u =>
    u.user_id.toLowerCase().includes(q) ||
    (u.user_name || "").toLowerCase().includes(q)
  );
  renderDropdown(matches);
});

function renderDropdown(matches) {
  const dd = document.getElementById("user-dropdown");
  focusedUdIndex = -1;

  if (matches.length === 0) {
    dd.innerHTML = `<div class="ud-empty">No users match your search.</div>`;
  } else {
    dd.innerHTML = matches.map((u, i) => `
      <div class="ud-item" data-id="${u.user_id}" data-idx="${i}" role="option">
        <span class="ud-id">${u.user_id}</span>
        <span class="ud-name">${u.user_name || "—"}</span>
        <span class="ud-goal">${goalLabel(u.goal_type || "")}</span>
      </div>
    `).join("");

    dd.querySelectorAll(".ud-item").forEach(item => {
      item.addEventListener("mousedown", e => {
        e.preventDefault(); // prevent input blur before click fires
        selectUser(item.dataset.id);
      });
    });
  }

  dd.classList.remove("hidden");
}

function closeDropdown() {
  document.getElementById("user-dropdown").classList.add("hidden");
  focusedUdIndex = -1;
}

function selectUser(userId) {
  document.getElementById("lookup-id").value = userId;
  closeDropdown();
  lookupUser();
}

// Keyboard navigation inside dropdown
document.getElementById("lookup-id").addEventListener("keydown", e => {
  const dd = document.getElementById("user-dropdown");
  const items = [...dd.querySelectorAll(".ud-item")];

  if (e.key === "ArrowDown") {
    e.preventDefault();
    if (dd.classList.contains("hidden")) return;
    focusedUdIndex = Math.min(focusedUdIndex + 1, items.length - 1);
    items.forEach((el, i) => el.classList.toggle("focused", i === focusedUdIndex));
    items[focusedUdIndex]?.scrollIntoView({ block: "nearest" });
  } else if (e.key === "ArrowUp") {
    e.preventDefault();
    focusedUdIndex = Math.max(focusedUdIndex - 1, 0);
    items.forEach((el, i) => el.classList.toggle("focused", i === focusedUdIndex));
    items[focusedUdIndex]?.scrollIntoView({ block: "nearest" });
  } else if (e.key === "Enter") {
    if (focusedUdIndex >= 0 && items[focusedUdIndex]) {
      selectUser(items[focusedUdIndex].dataset.id);
    } else {
      lookupUser();
    }
  } else if (e.key === "Escape") {
    closeDropdown();
  }
});

// Close dropdown when clicking outside
document.addEventListener("click", e => {
  if (!document.getElementById("user-combo-wrapper").contains(e.target)) closeDropdown();
});

document.getElementById("lookup-btn").addEventListener("click", () => { closeDropdown(); lookupUser(); });

async function lookupUser() {
  const userId = document.getElementById("lookup-id").value.trim().toUpperCase();
  if (!userId) return;
  hideError("lookup-error");
  hide(document.getElementById("lookup-results"));
  hideError("edit-error");
  hide(document.getElementById("edit-success"));
  hide(document.getElementById("edit-form-wrapper"));
  document.getElementById("edit-toggle-btn").textContent = "Edit Profile";

  try {
    const [profileRes, recsRes] = await Promise.all([
      fetch(`${API_BASE}/users/${userId}`),
      fetch(`${API_BASE}/recommendations/${userId}`),
    ]);

    if (!profileRes.ok) {
      const d = await profileRes.json();
      throw new Error(d.detail || "User not found");
    }

    const profile = await profileRes.json();
    const recsData = recsRes.ok ? await recsRes.json() : { recommendations: [] };

    currentLookupId = userId;

    text("lu-user-id", userId);
    text("lu-name", profile.user_name || userId);
    renderGoalBadge("lu-goal-badge", profile.goal_type);
    text("lu-meta", `${profile.gender || ""}${profile.age ? " · Age " + profile.age : ""} · v${profile.version_number}`);
    renderStatsGrid("lu-stats", profile);
    renderRecList("lu-recs", recsData.recommendations);
    show(document.getElementById("lookup-results"));

  } catch (err) {
    showError("lookup-error", err.message);
  }
}

// Edit toggle
document.getElementById("edit-toggle-btn").addEventListener("click", () => {
  const wrapper = document.getElementById("edit-form-wrapper");
  const btn = document.getElementById("edit-toggle-btn");
  if (wrapper.classList.contains("hidden")) {
    show(wrapper);
    btn.textContent = "Cancel";
  } else {
    hide(wrapper);
    btn.textContent = "Edit Profile";
  }
});

// Edit submit
document.getElementById("edit-form").addEventListener("submit", async e => {
  e.preventDefault();
  if (!currentLookupId) return;
  hideError("edit-error");
  hide(document.getElementById("edit-success"));

  const payload = {};
  const name = document.getElementById("e-name").value.trim();
  const gender = document.getElementById("e-gender").value;
  const cw = document.getElementById("e-current-wt").value;
  const tw = document.getElementById("e-target-wt").value;

  if (name)   payload.user_name         = name;
  if (gender) payload.gender            = gender;
  if (cw)     payload.current_weight_lb = parseFloat(cw);
  if (tw)     payload.target_weight_lb  = parseFloat(tw);

  if (Object.keys(payload).length === 0) {
    showError("edit-error", "No fields were changed.");
    return;
  }

  try {
    const res = await fetch(`${API_BASE}/users/${currentLookupId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok) {
      const detail = data.details ? data.details.join("; ") : (data.detail || "Update failed");
      throw new Error(detail);
    }

    const successEl = document.getElementById("edit-success");
    successEl.textContent = "Profile updated. Recommendations are being regenerated…";
    show(successEl);

    // Refresh after short delay for consumer to process
    setTimeout(() => lookupUser(), 2000);

  } catch (err) {
    showError("edit-error", err.message);
  }
});

// Delete
document.getElementById("delete-btn").addEventListener("click", async () => {
  if (!currentLookupId) return;
  if (!confirm(`Delete user ${currentLookupId}? This cannot be undone.`)) return;

  try {
    const res = await fetch(`${API_BASE}/users/${currentLookupId}`, { method: "DELETE" });
    if (!res.ok) throw new Error("Delete failed");
    hide(document.getElementById("lookup-results"));
    document.getElementById("lookup-id").value = "";
    currentLookupId = null;
    showError("lookup-error", `User deleted successfully.`);
    document.getElementById("lookup-error").style.background = "rgba(34,197,94,.1)";
    document.getElementById("lookup-error").style.borderColor = "rgba(34,197,94,.3)";
    document.getElementById("lookup-error").style.color = "var(--green)";
  } catch (err) {
    showError("lookup-error", err.message);
  }
});

// ── TAB 3: Progress History ────────────────────────────────
document.getElementById("history-btn").addEventListener("click", () => loadHistory());
document.getElementById("history-id").addEventListener("keydown", e => { if (e.key === "Enter") loadHistory(); });

async function loadHistory() {
  const userId = document.getElementById("history-id").value.trim().toUpperCase();
  if (!userId) return;
  hideError("history-error");
  hide(document.getElementById("history-results"));

  try {
    const res = await fetch(`${API_BASE}/users/${userId}/history`);
    if (!res.ok) {
      const d = await res.json();
      throw new Error(d.detail || "User not found");
    }
    const data = await res.json();
    renderHistory(userId, data);
    show(document.getElementById("history-results"));

  } catch (err) {
    showError("history-error", err.message);
  }
}

function renderHistory(userId, data) {
  const versions = data.history;

  // ── Chart ────────────────────────────────────────────────
  text("history-chart-title", `${versions[0]?.user_name || userId} — Weight Journey (${data.total_versions} versions)`);

  const labels  = versions.map(v => formatDate(v.changed_at));
  const weights = versions.map(v => Number(v.current_weight_lb));
  const targets = versions.map(v => Number(v.target_weight_lb));

  const pointColors = versions.map(v => {
    const ps = v.progress_status;
    if (ps === "improving")     return "#22c55e";
    if (ps === "not_improving") return "#ef4444";
    if (ps === "unchanged")     return "#eab308";
    return "#8892a4"; // initial
  });

  if (progressChart) progressChart.destroy();

  const ctx = document.getElementById("progress-chart").getContext("2d");
  progressChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Actual Weight (lb)",
          data: weights,
          borderColor: "#00b4d8",
          backgroundColor: "rgba(0,180,216,.08)",
          borderWidth: 2.5,
          fill: true,
          tension: 0.3,
          pointBackgroundColor: pointColors,
          pointBorderColor: "#fff",
          pointBorderWidth: 1.5,
          pointRadius: 6,
          pointHoverRadius: 8,
        },
        {
          label: "Target Weight (lb)",
          data: targets,
          borderColor: "#f97316",
          borderWidth: 2,
          borderDash: [6, 4],
          fill: false,
          tension: 0,
          pointRadius: 0,
          pointHoverRadius: 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: {
          labels: { color: "#8892a4", font: { size: 12 } },
        },
        tooltip: {
          backgroundColor: "#1a1f2e",
          borderColor: "#2d3347",
          borderWidth: 1,
          titleColor: "#e2e8f0",
          bodyColor: "#8892a4",
          callbacks: {
            afterBody: (items) => {
              const idx = items[0].dataIndex;
              const v = versions[idx];
              const gap = (v.current_weight_lb - v.target_weight_lb).toFixed(1);
              return [
                `Gap to target: ${gap > 0 ? "+" : ""}${gap} lb`,
                `Progress: ${v.progress_status}`,
                `Version: ${v.version_number}`,
              ];
            },
          },
        },
      },
      scales: {
        x: {
          ticks: { color: "#8892a4", font: { size: 11 } },
          grid:  { color: "rgba(45,51,71,.6)" },
        },
        y: {
          ticks: { color: "#8892a4", font: { size: 11 } },
          grid:  { color: "rgba(45,51,71,.6)" },
          title: { display: true, text: "Weight (lb)", color: "#8892a4", font: { size: 11 } },
        },
      },
    },
  });

  // ── Table ────────────────────────────────────────────────
  const tbody = document.getElementById("history-tbody");
  tbody.innerHTML = versions.map(v => {
    const gap = (v.current_weight_lb - v.target_weight_lb).toFixed(1);
    const gapStr = `${gap > 0 ? "+" : ""}${gap}`;
    const isCurrent = v.is_current;

    const statusDot  = `<span class="status-dot ${isCurrent ? "active" : "expired"}"></span>`;
    const statusText = isCurrent ? "Active" : "Expired";

    const ps = v.progress_status || "initial";
    const progressHtml = `<span class="progress-chip ${ps}">${ps.replace("_", " ")}</span>`;
    const versionHtml  = `<span class="version-badge ${isCurrent ? "current" : ""}">v${v.version_number}</span>`;

    return `
      <tr>
        <td>${versionHtml}</td>
        <td>${formatDateTime(v.effective_start)}</td>
        <td><strong>${Number(v.current_weight_lb).toFixed(1)}</strong></td>
        <td>${Number(v.target_weight_lb).toFixed(1)}</td>
        <td style="color:${gap > 0 ? "var(--orange)" : "var(--green)"}">${gapStr}</td>
        <td>${goalLabel(v.goal_type)}</td>
        <td>${progressHtml}</td>
        <td>${statusDot}${statusText}</td>
      </tr>
    `;
  }).join("");
}
