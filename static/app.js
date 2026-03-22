"use strict";

const page = document.body.dataset.page;
const FLASH_NOTICE_KEY = "lulynx-subhub.flash_notice";
const VIEW_META = {
  overview: {
    kicker: "Overview",
    title: "概览",
    description: "集中查看订阅健康度、自动更新状态和默认主订阅输出。"
  },
  subscriptions: {
    kicker: "Subscriptions",
    title: "订阅源",
    description: "添加、编辑、禁用或删除机场订阅，并维护它们的分组和到期时间。"
  },
  groups: {
    kicker: "Groups",
    title: "分组",
    description: "按用途、地区或价格层级整理机场订阅，让后续管理更清楚。"
  },
  profiles: {
    kicker: "Merge Profiles",
    title: "主订阅",
    description: "选择特定机场订阅来合成独立的主订阅，并为每个主订阅设置自己的过滤规则。"
  },
  nodes: {
    kicker: "Node Preview",
    title: "节点预览",
    description: "按主订阅查看最终合成出来的节点，并支持关键词和协议搜索。"
  },
  settings: {
    kicker: "Settings",
    title: "设置",
    description: "配置全局过滤规则、备份恢复和面板使用说明。"
  }
};

const NAV_ITEMS = [
  { view: "overview", label: "概览" },
  { view: "subscriptions", label: "订阅源" },
  { view: "groups", label: "分组" },
  { view: "profiles", label: "主订阅" },
  { view: "nodes", label: "节点预览" },
  { view: "settings", label: "设置" }
];

const SUPPORTED_PROTOCOLS = ["ss", "ssr", "vmess", "vless", "trojan", "hy2", "hysteria2", "anytls"];

const THEME_OPTIONS = {
  classic: {
    label: "Classic",
    title: "暖色经典",
    description: "保留当前这套偏柔和、偏内容型的控制台风格。"
  },
  "industrial-light": {
    label: "Industrial Light",
    title: "工业中控",
    description: "更冷静、更硬朗，强调状态、表格、数字和控制感。"
  }
};

const EXAMPLE_SNIPPETS = {
  bulk_import: [
    "日本主力,https://example-jp.com/sub?token=demo-jp",
    "香港低倍率,https://example-hk.com/sub?token=demo-hk",
    "自用备用,https://example-backup.com/sub?token=demo-backup"
  ].join("\n"),
  exclude_keywords: [
    "流量包",
    "到期",
    "试用",
    "官网"
  ].join("\n"),
  exclude_protocols: "ssr",
  rename_rules: [
    "香港|HK => HK",
    "日本|JP => JP",
    "新加坡|SG => SG",
    "\\s+倍率\\d+x =>"
  ].join("\n")
};

const state = {
  dashboard: null,
  view: "overview",
  theme: "classic",
  editingSubscriptionId: null,
  recentSubscriptionId: null,
  subscriptionDraft: null,
  subscriptionLogs: null,
  bulkImportResult: null,
  editingGroupId: null,
  editingProfileId: null,
  subscriptionPreview: null,
  restorePreview: null,
  nodePreview: null,
  nodeFilters: {
    profile_id: "",
    search: "",
    protocol: "",
    limit: 200
  }
};

function $(selector) {
  return document.querySelector(selector);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function showNotice(message, tone = "info") {
  const node = $("#notice");
  if (!node) {
    return;
  }
  node.hidden = false;
  node.className = `notice notice-${tone}`;
  node.textContent = message;
}

function clearNotice() {
  const node = $("#notice");
  if (!node) {
    return;
  }
  node.hidden = true;
  node.textContent = "";
  node.className = "notice";
}

function persistNotice(message, tone = "info") {
  try {
    window.sessionStorage.setItem(
      FLASH_NOTICE_KEY,
      JSON.stringify({ message: String(message || ""), tone: String(tone || "info") })
    );
  } catch (error) {
    return;
  }
}

function consumePersistedNotice() {
  try {
    const rawValue = window.sessionStorage.getItem(FLASH_NOTICE_KEY);
    if (!rawValue) {
      return;
    }
    window.sessionStorage.removeItem(FLASH_NOTICE_KEY);
    const payload = JSON.parse(rawValue);
    if (payload && payload.message) {
      showNotice(payload.message, payload.tone || "info");
    }
  } catch (error) {
    return;
  }
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    credentials: "same-origin",
    headers: {
      "Content-Type": "application/json"
    },
    ...options
  });

  let payload = {};
  try {
    payload = await response.json();
  } catch (error) {
    payload = {};
  }

  if (!response.ok) {
    const err = new Error(payload.error || `Request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }
  return payload;
}

async function requestRaw(url, options = {}) {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...options
  });
  if (!response.ok) {
    let payload = {};
    try {
      payload = await response.json();
    } catch (error) {
      payload = {};
    }
    const err = new Error(payload.error || `Request failed: ${response.status}`);
    err.status = response.status;
    throw err;
  }
  return response;
}

function formatDate(value) {
  if (!value) {
    return "从未";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "未知";
  }
  return date.toLocaleString("zh-CN", { hour12: false });
}

function formatCountdown(value, enabled, sourceType = "remote") {
  if (!enabled) {
    return "已暂停";
  }
  if (sourceType === "manual") {
    return "本地静态";
  }
  if (!value) {
    return "等待调度";
  }
  const diff = new Date(value).getTime() - Date.now();
  if (diff <= 0) {
    return "即将刷新";
  }

  const totalSeconds = Math.floor(diff / 1000);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  const parts = [];
  if (days) {
    parts.push(`${days}d`);
  }
  if (days || hours) {
    parts.push(`${hours}h`);
  }
  parts.push(`${minutes}m`);
  parts.push(`${seconds}s`);
  return parts.join(" ");
}

function formatExpiry(value, isExpired) {
  if (!value) {
    return "未填写";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "格式异常";
  }
  if (isExpired) {
    return `已到期 ${formatDate(value)}`;
  }
  return formatDate(value);
}

function toDatetimeLocal(value) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 16);
}

function fromDatetimeLocal(value) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toISOString();
}

function statusTone(status) {
  if (status === "ok") {
    return "success";
  }
  if (status === "error") {
    return "danger";
  }
  return "neutral";
}

function statusText(status) {
  if (status === "ok") {
    return "正常";
  }
  if (status === "error") {
    return "异常";
  }
  if (status === "queued") {
    return "等待首刷";
  }
  return "待机";
}

function alertTone(severity) {
  if (severity === "danger") {
    return "danger";
  }
  if (severity === "warning") {
    return "warning";
  }
  return "neutral";
}

function alertLabel(severity) {
  if (severity === "danger") {
    return "高优先级";
  }
  if (severity === "warning") {
    return "提醒";
  }
  return "信息";
}

function refreshLogTone(status) {
  if (status === "error") {
    return "danger";
  }
  if (status === "ok") {
    return "success";
  }
  return "neutral";
}

function refreshTriggerLabel(trigger) {
  if (trigger === "scheduler") {
    return "自动轮询";
  }
  if (trigger === "bulk") {
    return "批量刷新";
  }
  if (trigger === "save") {
    return "新增首刷";
  }
  if (trigger === "update") {
    return "更新首刷";
  }
  if (trigger === "enable") {
    return "启用首刷";
  }
  return "手动刷新";
}

function refreshChangeSummary(log) {
  if (!log) {
    return "";
  }
  if (log.status !== "ok") {
    return "";
  }
  return `最近刷新变化：+${log.added_count || 0} / -${log.removed_count || 0} / ${log.duration_ms || 0} ms`;
}

function readInitialView() {
  const candidate = window.location.hash.replace("#", "");
  return VIEW_META[candidate] ? candidate : "overview";
}

function readThemePreference() {
  try {
    const stored = window.localStorage.getItem("lulynx-subhub.theme");
    if (stored && THEME_OPTIONS[stored]) {
      return stored;
    }
  } catch (error) {
    return "classic";
  }
  return "classic";
}

function hasStoredThemePreference() {
  try {
    return Boolean(window.localStorage.getItem("lulynx-subhub.theme"));
  } catch (error) {
    return false;
  }
}

function applyTheme(themeName, persist = true) {
  const normalizedTheme = THEME_OPTIONS[themeName] ? themeName : "classic";
  state.theme = normalizedTheme;
  document.body.dataset.theme = normalizedTheme;
  if (!persist) {
    return;
  }
  try {
    window.localStorage.setItem("lulynx-subhub.theme", normalizedTheme);
  } catch (error) {
    return;
  }
}

function setView(view, updateHash = true) {
  state.view = VIEW_META[view] ? view : "overview";
  if (updateHash && window.location.hash !== `#${state.view}`) {
    window.location.hash = state.view;
  }
  renderShell();
}

function getDashboard() {
  return state.dashboard || {
    stats: {},
    overview: {},
    settings: {},
    alerts: [],
    subscriptions: [],
    groups: [],
    profiles: [],
    current_user: null
  };
}

function getSubscriptionById(id) {
  return getDashboard().subscriptions.find((item) => item.id === id) || null;
}

function getGroupById(id) {
  return getDashboard().groups.find((item) => item.id === id) || null;
}

function getProfileById(id) {
  return getDashboard().profiles.find((item) => item.id === id) || null;
}

function statusbarItemMarkup(label, value, tone = "neutral") {
  return `
    <div class="statusbar-item statusbar-item-${tone}">
      <span class="statusbar-label">${escapeHtml(label)}</span>
      <strong class="statusbar-value">${escapeHtml(value)}</strong>
    </div>
  `;
}

function copyFieldMarkup(id, label, value) {
  return `
    <div class="copy-block">
      <label for="${id}">${escapeHtml(label)}</label>
      <div class="copy-row">
        <input id="${id}" value="${escapeHtml(value)}" readonly>
        <button class="button button-secondary copy-button" data-copy-target="${id}" type="button">复制</button>
      </div>
    </div>
  `;
}

function profileModeMarkup(profile) {
  const isAllMode = profile.mode === "all";
  const modeLabel = isAllMode ? "全部启用订阅" : "手动选定订阅";
  const sourceCount = Number(profile.source_count || 0);
  const sourceLabel = isAllMode ? `${sourceCount} 个启用源` : `${sourceCount} 个指定源`;
  const sourceTone = sourceCount === 0 ? "danger" : (isAllMode ? "accent" : "neutral");
  const priorityCount = Number(profile.priority_source_count || 0);
    return `
    <div class="profile-meta-row">
      <span class="pill pill-${isAllMode ? "accent" : "neutral"}">${escapeHtml(modeLabel)}</span>
      <span class="pill pill-${sourceTone}">${escapeHtml(sourceLabel)}</span>
      ${
        priorityCount > 0
          ? `<span class="pill pill-warning">${escapeHtml(`${priorityCount} 个置顶源`)}</span>`
          : ""
      }
    </div>
  `;
}

function parseIdList(rawValue) {
  const values = String(rawValue || "")
    .split(",")
    .map((item) => Number(item.trim()))
    .filter((item) => Number.isInteger(item) && item > 0);
  return values.filter((item, index) => values.indexOf(item) === index);
}

function getProfileMode(form) {
  const checked = form.querySelector('input[name="mode"]:checked');
  return checked ? checked.value : "selected";
}

function getProfileSourceCheckboxes(form) {
  return [...form.querySelectorAll('input[name="subscription_ids"]')];
}

function getCheckedProfileSourceIds(form) {
  return getProfileSourceCheckboxes(form)
    .filter((field) => field.checked)
    .map((field) => Number(field.value))
    .filter((value) => Number.isInteger(value) && value > 0);
}

function getOrderedProfileSourceIds(form) {
  const hidden = form.querySelector('input[name="ordered_subscription_ids"]');
  let ordered = parseIdList(hidden ? hidden.value : "");
  const checked = getCheckedProfileSourceIds(form);
  const checkedSet = new Set(checked);
  ordered = ordered.filter((subscriptionId) => checkedSet.has(subscriptionId));
  checked.forEach((subscriptionId) => {
    if (!ordered.includes(subscriptionId)) {
      ordered.push(subscriptionId);
    }
  });
  if (hidden instanceof HTMLInputElement) {
    hidden.value = ordered.join(",");
  }
  return ordered;
}

function profileSourceModeCopy(mode) {
  if (mode === "all") {
    return {
      legend: "置顶订阅源",
      note: "勾选的源会被放到主订阅最前面，其他启用中的订阅仍会继续参与合成。",
      orderTitle: "置顶顺序",
      empty: "还没有置顶源。勾选一些订阅后，它们会优先排到主订阅最前面。"
    };
  }
  return {
    legend: "选择要参与合成的机场订阅",
    note: "勾选的源会参与合成，下面的顺序就是最终输出给客户端的顺序。",
    orderTitle: "合成顺序",
    empty: "还没有选中订阅源。勾选一些订阅后，就能在这里调整顺序。"
  };
}

function refreshProfileSourceOrderUI(form) {
  if (!(form instanceof HTMLFormElement)) {
    return;
  }

  const mode = getProfileMode(form);
  const copy = profileSourceModeCopy(mode);
  const orderedIds = getOrderedProfileSourceIds(form);
  const subscriptions = getDashboard().subscriptions || [];
  const sourceMap = new Map(subscriptions.map((item) => [Number(item.id), item]));

  const legend = form.querySelector("#profile-source-legend");
  if (legend) {
    legend.textContent = copy.legend;
  }

  const note = form.querySelector("#profile-source-mode-note");
  if (note) {
    note.textContent = copy.note;
  }

  const orderTitle = form.querySelector("#profile-source-order-title");
  if (orderTitle) {
    orderTitle.textContent = copy.orderTitle;
  }

  const summary = form.querySelector("#profile-source-summary");
  if (summary) {
    summary.textContent = mode === "all"
      ? `当前已置顶 ${orderedIds.length} 个源，未勾选的启用订阅会自动排在后面。`
      : `当前已选中 ${orderedIds.length} 个源，顺序会直接影响最终合成输出。`;
  }

  const orderMarkup = orderedIds.length
    ? orderedIds
        .map((subscriptionId, index) => {
          const item = sourceMap.get(subscriptionId);
          if (!item) {
            return "";
          }
          return `
            <div class="source-order-item">
              <span class="source-order-index">${index + 1}</span>
              <div class="source-order-copy">
                <strong>${escapeHtml(item.name)}</strong>
                <small>${escapeHtml(item.group_name || "未分组")} / ${item.is_manual ? "本地手动" : "远程订阅"} / ${item.enabled ? "启用中" : "已暂停"}</small>
              </div>
              <div class="source-order-actions">
                <button class="button button-small button-secondary" data-action="move-profile-source-up" data-id="${subscriptionId}" type="button" ${index === 0 ? "disabled" : ""}>上移</button>
                <button class="button button-small button-secondary" data-action="move-profile-source-down" data-id="${subscriptionId}" type="button" ${index === orderedIds.length - 1 ? "disabled" : ""}>下移</button>
                <button class="button button-small button-ghost" data-action="remove-profile-source" data-id="${subscriptionId}" type="button">${mode === "all" ? "取消置顶" : "移出"}</button>
              </div>
            </div>
          `;
        })
        .join("")
    : `<div class="empty-state source-order-empty">${escapeHtml(copy.empty)}</div>`;

  const orderContainer = form.querySelector("#profile-source-order");
  if (orderContainer) {
    orderContainer.innerHTML = orderMarkup;
  }

  const orderMap = new Map(orderedIds.map((subscriptionId, index) => [subscriptionId, index + 1]));
  form.querySelectorAll("[data-source-order-badge]").forEach((node) => {
    const subscriptionId = Number(node.getAttribute("data-source-order-badge") || 0);
    const order = orderMap.get(subscriptionId);
    node.hidden = !order;
    node.textContent = order ? `#${order}` : "";
  });
}

function moveProfileSourceOrder(form, subscriptionId, direction) {
  if (!(form instanceof HTMLFormElement)) {
    return;
  }
  const hidden = form.querySelector('input[name="ordered_subscription_ids"]');
  if (!(hidden instanceof HTMLInputElement)) {
    return;
  }
  const ordered = getOrderedProfileSourceIds(form);
  const index = ordered.indexOf(subscriptionId);
  if (index < 0) {
    return;
  }
  const targetIndex = direction === "up" ? index - 1 : index + 1;
  if (targetIndex < 0 || targetIndex >= ordered.length) {
    return;
  }
  const [item] = ordered.splice(index, 1);
  ordered.splice(targetIndex, 0, item);
  hidden.value = ordered.join(",");
  refreshProfileSourceOrderUI(form);
}

function exportFieldGroupMarkup(prefix, profile) {
  return `
    <div class="copy-group">
      ${[
        copyFieldMarkup(`${prefix}-base64`, "Base64 输出", profile.export_url),
        copyFieldMarkup(`${prefix}-plain`, "Plain 调试", profile.plain_export_url),
        copyFieldMarkup(`${prefix}-json`, "JSON 调试", profile.json_export_url),
        copyFieldMarkup(`${prefix}-clash`, "Clash / Mihomo", profile.clash_export_url),
        copyFieldMarkup(`${prefix}-surge`, "Surge", profile.surge_export_url),
        copyFieldMarkup(`${prefix}-singbox`, "sing-box", profile.singbox_export_url)
      ].join("")}
    </div>
  `;
}

function renderSidebar() {
  const sidebar = $("#sidebar-shell");
  if (!sidebar) {
    return;
  }
  sidebar.innerHTML = `
    <div class="sidebar-brand">
      <p class="eyebrow">Proxy Subscription Hub</p>
      <h1>Lulynx SubHub</h1>
      <p class="sidebar-copy">多个机场、统一管理、自动更新、按规则合成主订阅。</p>
    </div>

    <nav class="sidebar-nav" id="sidebar-nav">
      ${NAV_ITEMS.map(
        (item) => `
          <button class="nav-link ${state.view === item.view ? "is-active" : ""}" data-view="${item.view}" type="button">
            ${item.label}
          </button>
        `
      ).join("")}
    </nav>

    <div class="sidebar-footer">
      <div class="sidebar-theme">
        <span class="sidebar-footer-title">当前主题</span>
        <span class="pill pill-neutral">${escapeHtml(THEME_OPTIONS[state.theme].title)}</span>
      </div>
      <p class="sidebar-footer-title">已支持协议</p>
      <div class="sidebar-protocols" aria-label="supported protocols">
        ${SUPPORTED_PROTOCOLS.map((protocol) => `<span class="protocol-chip">${escapeHtml(protocol)}</span>`).join("")}
      </div>
    </div>
  `;
}

function renderTopbar() {
  const meta = VIEW_META[state.view];
  const user = getDashboard().current_user;
  const topbar = $("#topbar-shell");
  if (!topbar) {
    return;
  }
  topbar.innerHTML = `
    <div class="topbar-main">
      <p class="eyebrow">${escapeHtml(meta.kicker)}</p>
      <div class="topbar-title-row">
        <h2>${escapeHtml(meta.title)}</h2>
        <p class="topbar-copy">${escapeHtml(meta.description)}</p>
      </div>
    </div>
    <div class="topbar-actions">
      <div class="user-badge">${escapeHtml(user ? `当前账号：${user.username}` : "")}</div>
      <button class="button button-ghost" data-action="logout" type="button">退出登录</button>
    </div>
  `;
}

function renderStatusBar() {
  const statusbar = $("#statusbar-shell");
  if (!statusbar) {
    return;
  }
  const data = getDashboard();
  const defaultProfile = data.profiles.find((item) => item.id === data.overview.default_profile_id) || data.profiles[0];
  statusbar.innerHTML = `
    ${statusbarItemMarkup("主题", THEME_OPTIONS[state.theme].title, "neutral")}
    ${statusbarItemMarkup("默认主订阅", defaultProfile ? defaultProfile.name : "未配置", defaultProfile ? "accent" : "warning")}
    ${statusbarItemMarkup("启用订阅", data.stats.enabled_subscriptions || 0, "neutral")}
    ${statusbarItemMarkup("健康提醒", data.stats.alerts || 0, (data.stats.alerts || 0) > 0 ? "danger" : "success")}
  `;
}

function renderShell() {
  renderSidebar();
  renderTopbar();
  renderStatusBar();

  document.querySelectorAll(".view-panel").forEach((panel) => {
    panel.hidden = panel.id !== `${state.view}-view`;
  });
}

function renderOverview() {
  const data = getDashboard();
  const defaultProfile = data.profiles.find((item) => item.id === data.overview.default_profile_id) || data.profiles[0];
  const upcoming = [...data.subscriptions]
    .filter((item) => item.enabled && item.source_type === "remote")
    .sort((a, b) => new Date(a.next_refresh_at || 0) - new Date(b.next_refresh_at || 0))
    .slice(0, 5);
  const groupsPreview = data.groups.slice(0, 6);
  const alertsPreview = data.alerts.slice(0, 8);

  $("#overview-view").innerHTML = `
    <div class="stats-grid">
      <article class="card stat-card">
        <p class="stat-label">订阅总数</p>
        <strong class="stat-value">${escapeHtml(data.stats.subscriptions || 0)}</strong>
        <p class="stat-note">已录入到面板的机场订阅</p>
      </article>
      <article class="card stat-card">
        <p class="stat-label">启用订阅</p>
        <strong class="stat-value">${escapeHtml(data.stats.enabled_subscriptions || 0)}</strong>
        <p class="stat-note">会继续自动更新并参与主订阅合成</p>
      </article>
      <article class="card stat-card">
        <p class="stat-label">分组数量</p>
        <strong class="stat-value">${escapeHtml(data.stats.groups || 0)}</strong>
        <p class="stat-note">帮助你按用途和来源整理机场</p>
      </article>
      <article class="card stat-card">
        <p class="stat-label">主订阅数量</p>
        <strong class="stat-value">${escapeHtml(data.stats.profiles || 0)}</strong>
        <p class="stat-note">每个主订阅都可以单独选源和过滤</p>
      </article>
      <article class="card stat-card">
        <p class="stat-label">缓存节点</p>
        <strong class="stat-value">${escapeHtml(data.stats.cached_nodes || 0)}</strong>
        <p class="stat-note">所有机场当前已抓取到的节点总数</p>
      </article>
      <article class="card stat-card">
        <p class="stat-label">默认主订阅选源</p>
        <strong class="stat-value">${escapeHtml(defaultProfile ? defaultProfile.source_count || 0 : 0)}</strong>
        <p class="stat-note">默认主订阅当前纳入合成的订阅源数量</p>
      </article>
      <article class="card stat-card">
        <p class="stat-label">默认主订阅节点</p>
        <strong class="stat-value">${escapeHtml(data.stats.default_merged_nodes || 0)}</strong>
        <p class="stat-note">应用过滤后真正输出给客户端的数量</p>
      </article>
      <article class="card stat-card">
        <p class="stat-label">累计更新</p>
        <strong class="stat-value">${escapeHtml(data.stats.subscription_refreshes || 0)}</strong>
        <p class="stat-note">所有订阅源成功更新并重建节点缓存的累计次数</p>
      </article>
      <article class="card stat-card">
        <p class="stat-label">累计被订阅</p>
        <strong class="stat-value">${escapeHtml(data.stats.profile_accesses || 0)}</strong>
        <p class="stat-note">所有主订阅链接被客户端实际请求的累计次数</p>
      </article>
      <article class="card stat-card">
        <p class="stat-label">健康提醒</p>
        <strong class="stat-value">${escapeHtml(data.stats.alerts || 0)}</strong>
        <p class="stat-note">包含到期、连续失败和缓存过旧等提醒</p>
      </article>
    </div>

    <div class="page-grid">
      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Default Output</p>
            <h3>默认主订阅</h3>
          </div>
        </div>
        ${
          defaultProfile
            ? `
                <div class="hero-panel">
                  <div>
                    <div class="title-row">
                      <strong>${escapeHtml(defaultProfile.name)}</strong>
                      ${defaultProfile.is_default ? '<span class="pill pill-accent">默认</span>' : ""}
                    </div>
                  <p class="muted">${escapeHtml(defaultProfile.description || "未填写描述")}</p>
                  ${profileModeMarkup(defaultProfile)}
                  </div>
                </div>
                ${exportFieldGroupMarkup("overview-default", defaultProfile)}
            `
            : '<div class="empty-state">还没有主订阅配置，先到“主订阅”页面创建一个。</div>'
        }
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Upcoming</p>
            <h3>即将刷新</h3>
          </div>
        </div>
        ${
          upcoming.length
            ? `
              <div class="compact-list">
                ${upcoming
                  .map(
                    (item) => `
                      <div class="compact-item">
                        <div>
                          <strong>${escapeHtml(item.name)}</strong>
                          <p class="muted">${escapeHtml(item.group_name || "未分组")}</p>
                        </div>
                        <div class="compact-meta">
                          <span>${escapeHtml(formatCountdown(item.next_refresh_at, item.enabled))}</span>
                        </div>
                      </div>
                    `
                  )
                  .join("")}
              </div>
            `
            : '<div class="empty-state">当前没有启用中的订阅。</div>'
        }
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Health</p>
            <h3>订阅健康提醒</h3>
          </div>
        </div>
        ${
          alertsPreview.length
            ? `
              <div class="compact-list">
                ${alertsPreview
                  .map(
                    (alert) => `
                      <div class="compact-item compact-alert compact-alert-${alertTone(alert.severity)}">
                        <div>
                          <div class="title-row">
                            <strong>${escapeHtml(alert.title)}</strong>
                            <span class="pill pill-${alertTone(alert.severity)}">${escapeHtml(alertLabel(alert.severity))}</span>
                          </div>
                          <p class="muted">${escapeHtml(alert.detail)}</p>
                        </div>
                      </div>
                    `
                  )
                  .join("")}
              </div>
            `
            : '<div class="empty-state">当前没有需要处理的健康提醒。</div>'
        }
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Groups</p>
            <h3>分组概览</h3>
          </div>
        </div>
        ${
          groupsPreview.length
            ? `
              <div class="chip-grid">
                ${groupsPreview
                  .map(
                    (group) => `
                      <div class="group-chip">
                        <span class="swatch" style="background:${escapeHtml(group.color)}"></span>
                        <strong>${escapeHtml(group.name)}</strong>
                        <span>${group.subscription_count} 个订阅</span>
                      </div>
                    `
                  )
                  .join("")}
              </div>
            `
            : '<div class="empty-state">还没有分组。你可以按地区、价格或用途来分。</div>'
        }
      </article>
    </div>
  `;
}

function renderSubscriptions() {
  const data = getDashboard();
  const current = getSubscriptionById(state.editingSubscriptionId);
  const draft = current ? null : getSubscriptionDraft();
  const formState = current || draft;
  const formTitle = current ? `编辑订阅 #${current.id}` : "新增订阅";
  const currentSourceType = (formState && formState.source_type) || "remote";
  const preview = state.subscriptionPreview;
  const subscriptionLogs = state.subscriptionLogs;
  const bulkImportResult = state.bulkImportResult;
  const groupOptions = data.groups
    .map(
      (group) => `
        <option value="${group.id}" ${formState && String(formState.group_id || "") === String(group.id) ? "selected" : ""}>
          ${escapeHtml(group.name)}
        </option>
      `
    )
    .join("");
  const importGroupOptions = data.groups
    .map(
      (group) => `
        <option value="${group.id}">${escapeHtml(group.name)}</option>
      `
    )
    .join("");

  $("#subscriptions-view").innerHTML = `
    <div class="page-grid page-grid-subscriptions">
      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Source Library</p>
            <h3>订阅列表</h3>
          </div>
          <div class="inline-actions">
            <button class="button button-secondary" data-action="refresh-all" type="button">立即更新全部</button>
            <button class="button button-ghost" data-action="new-subscription" type="button">新增订阅</button>
          </div>
        </div>
        <div class="table-wrap">
          <table class="subscription-table">
            <thead>
              <tr>
                <th>名称</th>
                <th>分组 / 到期</th>
                <th>节点</th>
                <th>状态</th>
                <th>下次更新</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              ${
                data.subscriptions.length
                  ? data.subscriptions
                      .map(
                        (item) => `
                          <tr class="${state.recentSubscriptionId === item.id ? "subscription-row-new" : ""}">
                            <td>
                              <div class="row-title">${escapeHtml(item.name)}</div>
                              <div class="row-subtle">${escapeHtml(item.is_manual ? "本地手动节点源" : item.url)}</div>
                            </td>
                            <td>
                              <div>${escapeHtml(item.group_name || "未分组")} / ${item.is_manual ? "本地手动" : "远程"}</div>
                              <div class="row-subtle">${escapeHtml(formatExpiry(item.expires_at, item.is_expired))}</div>
                            </td>
                            <td>
                              <div>${item.node_count}</div>
                              <div class="row-subtle">${escapeHtml(item.protocol_summary || "-")}</div>
                            </td>
                            <td>
                              <span class="pill pill-${statusTone(item.last_status)}">${escapeHtml(statusText(item.last_status))}</span>
                              <div class="row-subtle">${escapeHtml(item.last_error || refreshChangeSummary(item.latest_refresh_log) || formatDate(item.last_updated_at))}</div>
                            </td>
                            <td>
                              <div data-next-refresh="${escapeHtml(item.next_refresh_at || "")}" data-enabled="${item.enabled ? "1" : "0"}" data-source-type="${escapeHtml(item.source_type)}">
                                ${escapeHtml(formatCountdown(item.next_refresh_at, item.enabled, item.source_type))}
                              </div>
                            </td>
                            <td>
                              <div class="row-actions">
                                <label class="toggle-row">
                                  <input class="enabled-toggle" data-id="${item.id}" type="checkbox" ${item.enabled ? "checked" : ""}>
                                  <span>启用</span>
                                </label>
                                <button class="button button-small button-secondary" data-action="edit-subscription" data-id="${item.id}" type="button">编辑</button>
                                <button class="button button-small button-secondary" data-action="subscription-logs" data-id="${item.id}" type="button">日志</button>
                                <button class="button button-small button-secondary" data-action="refresh-subscription" data-id="${item.id}" type="button">刷新</button>
                                <button class="button button-small button-danger" data-action="delete-subscription" data-id="${item.id}" type="button">删除</button>
                              </div>
                            </td>
                          </tr>
                        `
                      )
                      .join("")
                  : '<tr><td colspan="6" class="empty-row">还没有订阅，先在右侧添加一个机场链接。</td></tr>'
              }
            </tbody>
          </table>
        </div>
        ${
          subscriptionLogs
            ? `
              <div class="section-panel-block">
                <div class="title-row">
                  <strong>最近刷新日志</strong>
                  <span class="pill pill-neutral">${escapeHtml(subscriptionLogs.subscription_name || "订阅日志")}</span>
                </div>
                ${
                  subscriptionLogs.logs && subscriptionLogs.logs.length
                    ? `
                      <div class="compact-list">
                        ${subscriptionLogs.logs
                          .map(
                            (log) => `
                              <div class="compact-item">
                                <div>
                                  <div class="title-row">
                                    <span class="pill pill-${refreshLogTone(log.status)}">${escapeHtml(log.status === "ok" ? "成功" : "失败")}</span>
                                    <span class="pill pill-neutral">${escapeHtml(refreshTriggerLabel(log.trigger))}</span>
                                    <span class="row-subtle">${escapeHtml(formatDate(log.finished_at))}</span>
                                  </div>
                                  <p class="muted">
                                    ${log.status === "ok"
                                      ? `节点 ${log.node_count_before} -> ${log.node_count_after} / 新增 ${log.added_count} / 移除 ${log.removed_count} / ${log.duration_ms} ms`
                                      : escapeHtml(log.error_message || "刷新失败")}
                                  </p>
                                  ${
                                    log.added_sample && log.added_sample.length
                                      ? `<p class="row-subtle">新增样本：${escapeHtml(log.added_sample.join(" / "))}</p>`
                                      : ""
                                  }
                                  ${
                                    log.removed_sample && log.removed_sample.length
                                      ? `<p class="row-subtle">移除样本：${escapeHtml(log.removed_sample.join(" / "))}</p>`
                                      : ""
                                  }
                                </div>
                              </div>
                            `
                          )
                          .join("")}
                      </div>
                    `
                    : '<div class="empty-state">这个订阅还没有刷新日志。</div>'
                }
              </div>
            `
            : ""
        }
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Editor</p>
            <h3>${escapeHtml(formTitle)}</h3>
          </div>
        </div>
        <form id="subscription-form" class="stack-form">
          <input name="id" type="hidden" value="${formState ? formState.id || "" : ""}">
          <div class="mode-switch">
            <label class="radio-tile">
              <input name="source_type" type="radio" value="remote" ${currentSourceType === "remote" ? "checked" : ""}>
              <span>远程订阅链接</span>
            </label>
            <label class="radio-tile">
              <input name="source_type" type="radio" value="manual" ${currentSourceType === "manual" ? "checked" : ""}>
              <span>本地手动订阅</span>
            </label>
          </div>
          <label>
            <span>显示名称</span>
            <input name="name" type="text" value="${escapeHtml(formState ? formState.name || "" : "")}" placeholder="不填则自动使用域名">
          </label>
          <div data-source-panel="remote">
            <label>
              <span>订阅链接</span>
              <textarea name="url" rows="5" placeholder="https://example.com/subscribe?token=...">${escapeHtml(formState && currentSourceType === "remote" ? formState.url || "" : "")}</textarea>
            </label>
          </div>
          <div data-source-panel="manual">
            <label>
              <span>手动节点内容</span>
              <textarea name="manual_content" rows="8" placeholder="一行一个节点 URI，支持 ss / ssr / vmess / vless / trojan / hysteria2 / anytls">${escapeHtml(formState && currentSourceType === "manual" ? formState.manual_content || "" : "")}</textarea>
            </label>
          </div>
          <div class="field-grid">
            <label>
              <span>所属分组</span>
              <select name="group_id">
                <option value="">未分组</option>
                ${groupOptions}
              </select>
            </label>
            <label data-source-panel="remote">
              <span>刷新间隔（小时）</span>
              <input name="refresh_interval_hours" type="number" min="1" max="168" value="${escapeHtml(formState ? formState.refresh_interval_hours || 24 : 24)}">
            </label>
          </div>
          <label>
            <span>到期时间</span>
            <input name="expires_at" type="datetime-local" value="${escapeHtml(toDatetimeLocal(formState ? formState.expires_at || "" : ""))}">
          </label>
          <p class="muted subscription-help subscription-help-remote">修改订阅链接或刷新间隔后，面板会把该订阅重新排进刷新队列。</p>
          <p class="muted subscription-help subscription-help-manual">手动订阅不会自动定时刷新；保存时会立即解析，也可以随时手动点“刷新”重新载入本地节点。</p>
          ${
            preview
              ? `
                <div class="preview-box">
                  <div class="title-row">
                    <strong>预检结果</strong>
                    <span class="pill pill-neutral">${escapeHtml(preview.source_type === "manual" ? "本地手动" : "远程拉取")}</span>
                  </div>
                  <div class="compact-list">
                    <div class="compact-item"><strong>节点总数</strong><span>${preview.stats.total_nodes}</span></div>
                    <div class="compact-item"><strong>识别格式</strong><span>${escapeHtml(preview.source_format)}</span></div>
                  </div>
                  <div class="chip-grid">
                    ${Object.entries(preview.stats.protocol_counts || {})
                      .map(
                        ([protocol, count]) => `
                          <div class="group-chip">
                            <strong>${escapeHtml(protocol)}</strong>
                            <span>${count} 个</span>
                          </div>
                        `
                      )
                      .join("") || '<div class="empty-state">还没有识别到任何节点。</div>'}
                  </div>
                  ${
                    preview.warnings && preview.warnings.length
                      ? `
                        <div class="compact-list">
                          ${preview.warnings
                            .map(
                              (warning) => `
                                <div class="compact-item compact-alert compact-alert-warning">
                                  <p class="muted">${escapeHtml(warning)}</p>
                                </div>
                              `
                            )
                            .join("")}
                        </div>
                      `
                      : ""
                  }
                  ${
                    preview.sample_nodes && preview.sample_nodes.length
                      ? `
                        <div class="table-wrap">
                          <table class="subscription-table">
                            <thead>
                              <tr>
                                <th>节点名</th>
                                <th>协议</th>
                              </tr>
                            </thead>
                            <tbody>
                              ${preview.sample_nodes
                                .map(
                                  (item) => `
                                    <tr>
                                      <td><div class="row-title">${escapeHtml(item.name)}</div></td>
                                      <td><span class="pill pill-neutral">${escapeHtml(item.protocol)}</span></td>
                                    </tr>
                                  `
                                )
                                .join("")}
                            </tbody>
                          </table>
                        </div>
                      `
                      : ""
                  }
                </div>
              `
              : ""
          }
          <div class="inline-actions">
            <button class="button button-secondary" data-action="preview-subscription-input" type="button">预检订阅</button>
            <button class="button button-primary" data-action="submit-subscription-form" type="button">${current ? "保存订阅" : "添加并首刷"}</button>
            ${
              current
                ? '<button class="button button-ghost" data-action="new-subscription" type="button">取消编辑</button>'
                : ""
            }
          </div>
        </form>
        <div class="section-panel-block">
          <div class="section-head">
            <div>
              <p class="eyebrow">Bulk Import</p>
              <h3>批量导入订阅</h3>
            </div>
          </div>
          <form id="subscription-import-form" class="stack-form">
            <label>
              <span>批量订阅链接</span>
              <textarea name="raw_text" rows="7" placeholder="每行一个订阅链接，或使用 名称,链接 / 名称<TAB>链接 格式。"></textarea>
            </label>
            <div class="field-grid">
              <label>
                <span>默认分组</span>
                <select name="group_id">
                  <option value="">未分组</option>
                  ${importGroupOptions}
                </select>
              </label>
              <label>
                <span>默认刷新间隔（小时）</span>
                <input name="refresh_interval_hours" type="number" min="1" max="168" value="24">
              </label>
            </div>
            ${
              bulkImportResult
                ? `
                  <div class="preview-box">
                    <div class="title-row">
                      <strong>导入结果</strong>
                      <span class="pill pill-success">成功 ${bulkImportResult.created_count}</span>
                      <span class="pill pill-${bulkImportResult.error_count ? "danger" : "neutral"}">失败 ${bulkImportResult.error_count}</span>
                    </div>
                    ${
                      bulkImportResult.errors && bulkImportResult.errors.length
                        ? `
                          <div class="compact-list">
                            ${bulkImportResult.errors
                              .map(
                                (item) => `
                                  <div class="compact-item compact-alert compact-alert-danger">
                                    <div>
                                      <strong>第 ${item.line} 行</strong>
                                      <p class="muted">${escapeHtml(item.error)}</p>
                                    </div>
                                  </div>
                                `
                              )
                              .join("")}
                          </div>
                        `
                        : ""
                    }
                  </div>
                `
                : ""
            }
            <div class="inline-actions">
              <button class="button button-primary" type="submit">开始批量导入</button>
            </div>
          </form>
        </div>
      </article>
    </div>
  `;
}

function renderGroups() {
  const data = getDashboard();
  const current = getGroupById(state.editingGroupId);

  $("#groups-view").innerHTML = `
    <div class="page-grid page-grid-groups">
      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Collections</p>
            <h3>分组列表</h3>
          </div>
          <button class="button button-ghost" data-action="new-group" type="button">新增分组</button>
        </div>
        ${
          data.groups.length
            ? `
              <div class="card-grid">
                ${data.groups
                  .map(
                    (group) => `
                      <article class="mini-card">
                        <div class="title-row">
                          <div class="title-row">
                            <span class="swatch" style="background:${escapeHtml(group.color)}"></span>
                            <strong>${escapeHtml(group.name)}</strong>
                          </div>
                          <span class="pill pill-neutral">${group.subscription_count} 个订阅</span>
                        </div>
                        <p class="muted">${escapeHtml(group.description || "未填写描述")}</p>
                        <div class="row-actions">
                          <button class="button button-small button-secondary" data-action="edit-group" data-id="${group.id}" type="button">编辑</button>
                          <button class="button button-small button-danger" data-action="delete-group" data-id="${group.id}" type="button">删除</button>
                        </div>
                      </article>
                    `
                  )
                  .join("")}
              </div>
            `
            : '<div class="empty-state">还没有分组。用分组把多个机场按地区、流量档位或用途整理起来会更顺手。</div>'
        }
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Editor</p>
            <h3>${current ? `编辑分组 #${current.id}` : "新增分组"}</h3>
          </div>
        </div>
        <form id="group-form" class="stack-form">
          <input name="id" type="hidden" value="${current ? current.id : ""}">
          <label>
            <span>分组名称</span>
            <input name="name" type="text" value="${escapeHtml(current ? current.name : "")}" placeholder="例如：高倍率 / 日本 / 临时测试">
          </label>
          <label>
            <span>分组说明</span>
            <textarea name="description" rows="4" placeholder="给这个分组加一点备注，后面更容易区分。">${escapeHtml(current ? current.description : "")}</textarea>
          </label>
          <label>
            <span>分组颜色</span>
            <input name="color" type="color" value="${escapeHtml(current ? current.color : "#0c8d8a")}">
          </label>
          <div class="inline-actions">
            <button class="button button-primary" type="submit">${current ? "保存分组" : "创建分组"}</button>
            ${
              current
                ? '<button class="button button-ghost" data-action="new-group" type="button">取消编辑</button>'
                : ""
            }
          </div>
        </form>
      </article>
    </div>
  `;
}

function renderProfiles() {
  const data = getDashboard();
  const current = getProfileById(state.editingProfileId);
  const currentMode = current ? current.mode : "selected";
  const currentSelectedList = current ? current.selected_subscription_ids : [];
  const currentSelected = new Set(currentSelectedList);
  const modeCopy = profileSourceModeCopy(currentMode);
  const subscriptionChoices = data.subscriptions.length
    ? data.subscriptions
        .map(
          (item) => `
            <label class="checkbox-item checkbox-item-source">
              <input
                name="subscription_ids"
                type="checkbox"
                value="${item.id}"
                ${currentSelected.has(item.id) ? "checked" : ""}
              >
              <span>
                <strong>${escapeHtml(item.name)}</strong>
                <small>${escapeHtml(item.group_name || "未分组")} / ${item.is_manual ? "本地手动" : "远程订阅"} / ${item.enabled ? "启用中" : "已暂停"}</small>
              </span>
              <span class="pill pill-neutral source-order-pill" data-source-order-badge="${item.id}" ${currentSelected.has(item.id) ? "" : "hidden"}>
                ${
                  currentSelectedList.includes(item.id)
                    ? `#${currentSelectedList.indexOf(item.id) + 1}`
                    : ""
                }
              </span>
            </label>
          `
        )
        .join("")
    : '<div class="empty-state">先添加订阅源，再来配置主订阅。</div>';

  $("#profiles-view").innerHTML = `
    <div class="page-grid page-grid-profiles">
      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Output Profiles</p>
            <h3>主订阅列表</h3>
          </div>
          <button class="button button-ghost" data-action="new-profile" type="button">新增主订阅</button>
        </div>
        ${
          data.profiles.length
            ? `
              <div class="card-grid">
                ${data.profiles
                  .map(
                    (profile) => `
                      <article class="mini-card">
                        <div class="title-row">
                          <strong>${escapeHtml(profile.name)}</strong>
                          ${profile.is_default ? '<span class="pill pill-accent">默认</span>' : ""}
                        </div>
                        <p class="muted">${escapeHtml(profile.description || "未填写描述")}</p>
                        ${profileModeMarkup(profile)}
                        <p class="muted">输出节点：${profile.merged_node_count}</p>
                        <p class="muted">关联源累计更新：${escapeHtml(profile.source_refresh_count || 0)}</p>
                        <p class="muted">累计被订阅：${escapeHtml(profile.access_count || 0)}</p>
                        <p class="muted">24 小时访问：${escapeHtml(profile.access_count_24h || 0)} / 7 天访问：${escapeHtml(profile.access_count_7d || 0)}</p>
                        ${exportFieldGroupMarkup(`profile-${profile.id}`, profile)}
                        <div class="row-actions">
                          <button class="button button-small button-secondary" data-action="edit-profile" data-id="${profile.id}" type="button">编辑</button>
                          <button class="button button-small button-secondary" data-action="clone-profile" data-id="${profile.id}" type="button">复制</button>
                          <button class="button button-small button-secondary" data-action="preview-profile-nodes" data-id="${profile.id}" type="button">看节点</button>
                          <button class="button button-small button-secondary" data-action="regenerate-profile-token" data-id="${profile.id}" type="button">换 Token</button>
                          <button class="button button-small button-danger" data-action="delete-profile" data-id="${profile.id}" type="button">删除</button>
                        </div>
                      </article>
                    `
                  )
                  .join("")}
              </div>
            `
            : '<div class="empty-state">还没有主订阅。你可以按设备、地区或用途分别合成。</div>'
        }
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Editor</p>
            <h3>${current ? `编辑主订阅 #${current.id}` : "新增主订阅"}</h3>
          </div>
        </div>
        <form id="profile-form" class="stack-form">
          <input name="id" type="hidden" value="${current ? current.id : ""}">
          <label>
            <span>主订阅名称</span>
            <input name="name" type="text" value="${escapeHtml(current ? current.name : "")}" placeholder="例如：手机主订阅 / 路由器订阅">
          </label>
          <label>
            <span>说明</span>
            <textarea name="description" rows="3" placeholder="说明这个主订阅的用途。">${escapeHtml(current ? current.description : "")}</textarea>
          </label>
          <div class="mode-switch">
            <label class="radio-tile">
              <input name="mode" type="radio" value="selected" ${currentMode === "selected" ? "checked" : ""}>
              <span>只合成选中的订阅</span>
            </label>
            <label class="radio-tile">
              <input name="mode" type="radio" value="all" ${currentMode === "all" ? "checked" : ""}>
              <span>合成全部启用订阅</span>
            </label>
          </div>
          <label>
            <span>额外剔除关键词</span>
            <textarea name="exclude_keywords" rows="4" placeholder="这个主订阅专属的过滤词，每行一个或逗号分隔。">${escapeHtml(current ? current.exclude_keywords : "")}</textarea>
          </label>
          <label>
            <span>额外排除协议</span>
            <input name="exclude_protocols" type="text" value="${escapeHtml(current ? current.exclude_protocols || "" : "")}" placeholder="例如：ssr, vmess">
          </label>
          <fieldset id="profile-source-fieldset" class="checkbox-fieldset">
            <legend id="profile-source-legend">${escapeHtml(modeCopy.legend)}</legend>
            <p class="field-note" id="profile-source-mode-note">${escapeHtml(modeCopy.note)}</p>
            <input name="ordered_subscription_ids" type="hidden" value="${escapeHtml(currentSelectedList.join(","))}">
            <div class="checkbox-list">
              ${subscriptionChoices}
            </div>
          </fieldset>
          <fieldset class="checkbox-fieldset source-order-fieldset">
            <legend id="profile-source-order-title">${escapeHtml(modeCopy.orderTitle)}</legend>
            <p class="field-note" id="profile-source-summary">
              ${
                currentMode === "all"
                  ? `当前已置顶 ${currentSelectedList.length} 个源，未勾选的启用订阅会自动排在后面。`
                  : `当前已选中 ${currentSelectedList.length} 个源，顺序会直接影响最终合成输出。`
              }
            </p>
            <div id="profile-source-order"></div>
          </fieldset>
          <div class="inline-actions">
            <button class="button button-primary" type="submit">${current ? "保存主订阅" : "创建主订阅"}</button>
            ${
              current
                ? '<button class="button button-ghost" data-action="new-profile" type="button">取消编辑</button>'
                : ""
            }
          </div>
        </form>
      </article>
    </div>
  `;
}

function renderNodes() {
  const data = getDashboard();
  const filters = state.nodeFilters;
  const preview = state.nodePreview;
  const profileOptions = data.profiles
    .map(
      (profile) => `
        <option value="${profile.id}" ${String(filters.profile_id || data.overview.default_profile_id) === String(profile.id) ? "selected" : ""}>
          ${escapeHtml(profile.name)}
        </option>
      `
    )
    .join("");
  const protocolOptions = ["", "ss", "ssr", "vmess", "vless", "trojan", "hy2", "hysteria2", "anytls"]
    .map(
      (protocol) => `
        <option value="${protocol}" ${filters.protocol === protocol ? "selected" : ""}>
          ${protocol || "全部协议"}
        </option>
      `
    )
    .join("");

  $("#nodes-view").innerHTML = `
    <div class="page-grid page-grid-nodes">
      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Node Search</p>
            <h3>节点预览与搜索</h3>
          </div>
        </div>
        <form id="node-search-form" class="stack-form">
          <div class="field-grid field-grid-wide">
            <label>
              <span>主订阅</span>
              <select name="profile_id">${profileOptions}</select>
            </label>
            <label>
              <span>协议筛选</span>
              <select name="protocol">${protocolOptions}</select>
            </label>
          </div>
          <div class="field-grid field-grid-wide">
            <label>
              <span>搜索关键词</span>
              <input name="search" type="text" value="${escapeHtml(filters.search)}" placeholder="搜索节点名、协议或 URI">
            </label>
            <label>
              <span>结果上限</span>
              <input name="limit" type="number" min="1" max="500" value="${escapeHtml(filters.limit)}">
            </label>
          </div>
          <div class="inline-actions">
            <button class="button button-primary" type="submit">更新预览</button>
            <button class="button button-ghost" data-action="reset-node-search" type="button">重置筛选</button>
          </div>
        </form>
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Result</p>
            <h3>匹配结果</h3>
          </div>
        </div>
        ${
          preview
            ? `
              <div class="compact-list">
                <div class="compact-item">
                  <strong>总节点</strong>
                  <span>${preview.stats.total_nodes}</span>
                </div>
                <div class="compact-item">
                  <strong>匹配结果</strong>
                  <span>${preview.stats.matched_nodes}</span>
                </div>
                <div class="compact-item">
                  <strong>本次返回</strong>
                  <span>${preview.stats.returned_nodes}${preview.stats.truncated ? " / 已截断" : ""}</span>
                </div>
              </div>
              <div class="chip-grid">
                ${Object.keys(preview.stats.protocol_counts).length
                  ? Object.entries(preview.stats.protocol_counts)
                      .map(
                        ([protocol, count]) => `
                          <div class="group-chip">
                            <strong>${escapeHtml(protocol)}</strong>
                            <span>${count} 个</span>
                          </div>
                        `
                      )
                      .join("")
                  : '<div class="empty-state">当前没有匹配到节点。</div>'}
              </div>
            `
            : '<div class="empty-state">正在加载节点预览...</div>'
        }
      </article>
    </div>

    <article class="card section-card section-card-full">
      <div class="section-head">
        <div>
          <p class="eyebrow">Preview</p>
          <h3>${preview ? escapeHtml(preview.profile.name) : "节点列表"}</h3>
        </div>
      </div>
      ${
        preview && preview.items.length
          ? `
            <div class="table-wrap">
              <table class="subscription-table">
                <thead>
                  <tr>
                    <th>节点名</th>
                    <th>协议</th>
                    <th>URI</th>
                  </tr>
                </thead>
                <tbody>
                  ${preview.items
                    .map(
                      (item) => `
                        <tr>
                          <td><div class="row-title">${escapeHtml(item.name)}</div></td>
                          <td><span class="pill pill-neutral">${escapeHtml(item.protocol)}</span></td>
                          <td><div class="row-subtle">${escapeHtml(item.uri)}</div></td>
                        </tr>
                      `
                    )
                    .join("")}
                </tbody>
              </table>
            </div>
          `
          : '<div class="empty-state">没有匹配到任何节点，可以换个主订阅或搜索条件再试。</div>'
      }
    </article>
  `;
}

function renderSettings() {
  const data = getDashboard();
  const currentUser = data.current_user || {};
  const notifications = data.settings.notifications || {};
  const cleanup = data.settings.cleanup || {};
  const schema = data.settings.schema || {};
  const restorePreview = state.restorePreview;
  const themeOptions = Object.entries(THEME_OPTIONS)
    .map(
      ([value, theme]) => `
        <option value="${value}" ${state.theme === value ? "selected" : ""}>${escapeHtml(theme.title)}</option>
      `
    )
    .join("");

  $("#settings-view").innerHTML = `
    <div class="page-grid">
      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Appearance</p>
            <h3>界面主题</h3>
          </div>
        </div>
        <form id="appearance-form" class="stack-form">
          <label>
            <span>当前主题</span>
            <select name="theme">${themeOptions}</select>
          </label>
          <div class="theme-option-grid">
            ${Object.entries(THEME_OPTIONS)
              .map(
                ([value, theme]) => `
                  <div class="theme-preview ${state.theme === value ? "is-active" : ""}">
                    <div class="theme-preview-swatch theme-preview-${value}"></div>
                    <strong>${escapeHtml(theme.title)}</strong>
                    <p class="muted">${escapeHtml(theme.description)}</p>
                  </div>
                `
              )
              .join("")}
          </div>
          <p class="muted">主题保存在当前浏览器，切换后会立即生效，不影响订阅数据和后端配置。</p>
          <button class="button button-primary" type="submit">应用主题</button>
        </form>
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Migration</p>
            <h3>数据迁移</h3>
          </div>
        </div>
        <div class="stack-form">
          <div class="compact-list">
            <div class="compact-item"><strong>当前版本</strong><span>${escapeHtml(schema.current_version || 0)}</span></div>
            <div class="compact-item"><strong>目标版本</strong><span>${escapeHtml(schema.target_version || 0)}</span></div>
            <div class="compact-item"><strong>状态</strong><span class="pill pill-${schema.up_to_date ? "success" : "warning"}">${escapeHtml(schema.up_to_date ? "已最新" : "需要迁移")}</span></div>
          </div>
          <p class="muted">如果你是从旧版本升级，可以在服务端执行 python app.py --migrate-db --db data/subpanel.db，会自动补齐新表和新字段。</p>
        </div>
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Examples</p>
            <h3>示例配置</h3>
          </div>
        </div>
        <div class="stack-form">
          <label>
            <span>批量导入示例</span>
            <textarea readonly rows="5">${escapeHtml(EXAMPLE_SNIPPETS.bulk_import)}</textarea>
          </label>
          <label>
            <span>全局过滤关键词示例</span>
            <textarea readonly rows="4">${escapeHtml(EXAMPLE_SNIPPETS.exclude_keywords)}</textarea>
          </label>
          <label>
            <span>节点重命名规则示例</span>
            <textarea readonly rows="5">${escapeHtml(EXAMPLE_SNIPPETS.rename_rules)}</textarea>
          </label>
          <div class="inline-actions">
            <button class="button button-secondary" data-action="apply-example-import" type="button">填入批量导入示例</button>
            <button class="button button-secondary" data-action="apply-example-filters" type="button">填入过滤示例</button>
          </div>
        </div>
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Cleanup</p>
            <h3>自动清理策略</h3>
          </div>
        </div>
        <form id="cleanup-form" class="stack-form">
          <label class="checkbox-item">
            <input name="auto_disable_expired" type="checkbox" ${cleanup.auto_disable_expired ? "checked" : ""}>
            <span>
              <strong>自动禁用已到期订阅</strong>
              <small>后台轮询时遇到已到期的启用订阅，会自动暂停，避免继续参与合成。</small>
            </span>
          </label>
          <label>
            <span>连续失败自动暂停阈值</span>
            <input name="pause_failures_threshold" type="number" min="0" max="20" value="${escapeHtml(cleanup.pause_failures_threshold || 0)}">
          </label>
          <p class="muted">填 0 表示关闭。启用后，远程订阅连续失败达到阈值会自动暂停，防止反复拉取故障源。</p>
          <button class="button button-primary" type="submit">保存清理策略</button>
        </form>
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Global Rules</p>
            <h3>全局过滤规则</h3>
          </div>
        </div>
        <form id="settings-form" class="stack-form">
          <label>
            <span>全局剔除关键词</span>
            <textarea name="global_exclude_keywords" rows="8" placeholder="每行一个关键词，或者使用逗号分隔。">${escapeHtml(data.settings.global_exclude_keywords || "")}</textarea>
          </label>
          <div class="field-grid">
            <label>
              <span>全局排除协议</span>
              <input name="exclude_protocols" type="text" value="${escapeHtml(data.settings.exclude_protocols || "")}" placeholder="例如：ssr, vmess, hy2">
            </label>
            <label>
              <span>去重策略</span>
              <select name="dedup_strategy">
                <option value="uri" ${data.settings.dedup_strategy === "uri" ? "selected" : ""}>按 URI 去重</option>
                <option value="name_protocol" ${data.settings.dedup_strategy === "name_protocol" ? "selected" : ""}>按 名称+协议 去重</option>
                <option value="name" ${data.settings.dedup_strategy === "name" ? "selected" : ""}>按名称去重</option>
              </select>
            </label>
          </div>
          <label>
            <span>节点重命名规则</span>
            <textarea name="rename_rules" rows="6" placeholder="每行一条规则，格式：香港 => HK&#10;日本|JP => JP">${escapeHtml(data.settings.rename_rules || "")}</textarea>
          </label>
          <p class="muted">全局规则会作用到所有主订阅；如果某个主订阅还有额外过滤词，会在全局规则的基础上继续剔除。</p>
          <button class="button button-primary" type="submit">保存设置</button>
        </form>
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Panel</p>
            <h3>面板设置</h3>
          </div>
        </div>
        <form id="panel-settings-form" class="stack-form">
          <label>
            <span>监听端口</span>
            <input name="panel_port" type="number" min="1" max="65535" value="${escapeHtml(data.settings.panel_port || 8787)}">
          </label>
          <p class="muted">修改端口后需要重启服务才会生效。当前运行中的访问地址不会立刻变化。</p>
          <button class="button button-primary" type="submit">保存端口</button>
        </form>
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Alerts</p>
            <h3>通知渠道</h3>
          </div>
        </div>
        <form id="notifications-form" class="stack-form">
          <label>
            <span>Telegram Bot Token</span>
            <input name="telegram_bot_token" type="text" value="${escapeHtml(notifications.telegram_bot_token || "")}" placeholder="123456:ABC...">
          </label>
          <label>
            <span>Telegram Chat ID</span>
            <input name="telegram_chat_id" type="text" value="${escapeHtml(notifications.telegram_chat_id || "")}" placeholder="例如：123456789">
          </label>
          <label>
            <span>Webhook URL</span>
            <input name="webhook_url" type="url" value="${escapeHtml(notifications.webhook_url || "")}" placeholder="https://example.com/webhook">
          </label>
          <div class="field-grid">
            <label>
              <span>最小通知级别</span>
              <select name="min_severity">
                <option value="warning" ${notifications.min_severity === "warning" ? "selected" : ""}>提醒及以上</option>
                <option value="danger" ${notifications.min_severity === "danger" ? "selected" : ""}>仅高优先级</option>
              </select>
            </label>
            <label>
              <span>重复提醒冷却（分钟）</span>
              <input name="cooldown_minutes" type="number" min="5" max="10080" value="${escapeHtml(notifications.cooldown_minutes || 360)}">
            </label>
          </div>
          <label>
            <span>测试通知内容</span>
            <input name="test_message" type="text" value="这是来自 Lulynx SubHub 的测试通知。">
          </label>
          <p class="muted">当前支持 Telegram 和通用 Webhook。后台轮询刷新时也会自动检查健康提醒并尝试派发通知。</p>
          <div class="inline-actions">
            <button class="button button-primary" type="submit">保存通知设置</button>
            <button class="button button-secondary" data-action="test-notification" type="button">发送测试通知</button>
          </div>
        </form>
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Admin</p>
            <h3>管理员账号</h3>
          </div>
        </div>
        <form id="account-form" class="stack-form">
          <label>
            <span>管理员用户名</span>
            <input name="username" type="text" value="${escapeHtml(currentUser.username || "")}">
          </label>
          <label>
            <span>当前密码</span>
            <input name="current_password" type="password" autocomplete="current-password" placeholder="修改用户名或密码时都需要填写">
          </label>
          <label>
            <span>新密码</span>
            <input name="new_password" type="password" autocomplete="new-password" placeholder="留空则只修改用户名">
          </label>
          <p class="muted">如果忘记密码，可以在后端执行重置命令，不会锁死整个面板。</p>
          <button class="button button-primary" type="submit">保存账号</button>
        </form>
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Backup</p>
            <h3>备份与恢复</h3>
          </div>
        </div>
        <div class="stack-form">
          <p class="muted">备份会导出订阅、分组、主订阅、节点缓存、设置和管理员账号哈希。恢复会用备份内容覆盖当前数据。</p>
          <div class="inline-actions">
            <button class="button button-secondary" data-action="download-backup" type="button">导出备份</button>
          </div>
          <form id="restore-form" class="stack-form">
            <label>
              <span>恢复文件</span>
              <input name="backup_file" type="file" accept="application/json">
            </label>
            <label>
              <span>或直接粘贴备份 JSON</span>
              <textarea name="backup_json" rows="7" placeholder='{"version":1,...}'></textarea>
            </label>
            ${
              restorePreview
                ? `
                  <div class="preview-box">
                    <div class="title-row">
                      <strong>恢复预检</strong>
                      <span class="pill pill-neutral">版本 ${escapeHtml(restorePreview.version)}</span>
                    </div>
                    <div class="compact-list">
                      <div class="compact-item"><strong>当前订阅</strong><span>${restorePreview.current_counts.subscriptions}</span></div>
                      <div class="compact-item"><strong>备份订阅</strong><span>${restorePreview.backup_counts.subscriptions}</span></div>
                      <div class="compact-item"><strong>手动订阅</strong><span>${restorePreview.backup_counts.manual_subscriptions}</span></div>
                      <div class="compact-item"><strong>备份节点</strong><span>${restorePreview.backup_counts.nodes}</span></div>
                      <div class="compact-item"><strong>备份主订阅</strong><span>${restorePreview.backup_counts.profiles}</span></div>
                    </div>
                    <div class="compact-list">
                      ${(restorePreview.warnings || [])
                        .map(
                          (warning) => `
                            <div class="compact-item compact-alert compact-alert-warning">
                              <p class="muted">${escapeHtml(warning)}</p>
                            </div>
                          `
                        )
                        .join("")}
                    </div>
                  </div>
                `
                : ""
            }
            <div class="inline-actions">
              <button class="button button-secondary" data-action="preview-restore" type="button">先做预检</button>
              <button class="button button-danger" type="submit">恢复备份</button>
            </div>
          </form>
        </div>
      </article>

      <article class="card section-card">
        <div class="section-head">
          <div>
            <p class="eyebrow">Workflow</p>
            <h3>当前工作流</h3>
          </div>
        </div>
        <div class="compact-list">
          <div class="compact-item"><strong>1.</strong><span>添加机场订阅，面板会首刷并缓存节点。</span></div>
          <div class="compact-item"><strong>2.</strong><span>后台按倒计时自动更新，不依赖 cron。</span></div>
          <div class="compact-item"><strong>3.</strong><span>可给订阅打分组、填到期时间、禁用或删除。</span></div>
          <div class="compact-item"><strong>4.</strong><span>主订阅可以单独选择哪些机场源参与合成。</span></div>
          <div class="compact-item"><strong>5.</strong><span>也可以新增本地手动订阅，一行一个节点 URI，适合自建服务器一起合成总订阅。</span></div>
          <div class="compact-item"><strong>6.</strong><span>主订阅现在除了 Base64 和 Plain，也能导出 Clash / Mihomo、Surge 和 sing-box 格式。</span></div>
          <div class="compact-item"><strong>7.</strong><span>若忘记管理员密码，可用后端重置命令直接恢复账号。</span></div>
          <div class="compact-item"><strong>8.</strong><span>如需迁移环境，可先做恢复预检，再导出或恢复备份。</span></div>
        </div>
      </article>
    </div>
  `;
}

function renderAll() {
  renderShell();
  renderOverview();
  renderSubscriptions();
  renderGroups();
  renderProfiles();
  renderNodes();
  renderSettings();
  syncSubscriptionSourceUI();
  syncProfileModeUI();
}

async function loadNodePreview(options = {}) {
  const dashboard = getDashboard();
  const defaultProfileId = dashboard.overview.default_profile_id || (dashboard.profiles[0] && dashboard.profiles[0].id) || "";
  state.nodeFilters = {
    ...state.nodeFilters,
    ...options
  };
  if (!state.nodeFilters.profile_id) {
    state.nodeFilters.profile_id = defaultProfileId;
  }
  const params = new URLSearchParams();
  params.set("profile_id", String(state.nodeFilters.profile_id || defaultProfileId || ""));
  params.set("search", state.nodeFilters.search || "");
  params.set("protocol", state.nodeFilters.protocol || "");
  params.set("limit", String(state.nodeFilters.limit || 200));

  const response = await requestJson(`/api/nodes?${params.toString()}`);
  state.nodePreview = response.preview;
}

async function loadDashboard() {
  try {
    const payload = await requestJson("/api/dashboard");
    state.dashboard = payload;
    if (!hasStoredThemePreference() && payload.settings && payload.settings.default_theme) {
      applyTheme(String(payload.settings.default_theme), false);
    }

    if (state.editingSubscriptionId && !getSubscriptionById(state.editingSubscriptionId)) {
      state.editingSubscriptionId = null;
    }
    if (state.subscriptionLogs && !getSubscriptionById(state.subscriptionLogs.subscription_id)) {
      state.subscriptionLogs = null;
    }
    if (state.editingGroupId && !getGroupById(state.editingGroupId)) {
      state.editingGroupId = null;
    }
    if (state.editingProfileId && !getProfileById(state.editingProfileId)) {
      state.editingProfileId = null;
    }

    try {
      await loadNodePreview();
    } catch (error) {
      state.nodePreview = null;
      if (state.view === "nodes" || state.view === "overview") {
        showNotice(`主界面已更新，但节点预览加载失败：${error.message}`, "danger");
      }
    }
    renderAll();
  } catch (error) {
    if (error.status === 401) {
      window.location.href = "/login";
      return;
    }
    if (error.status === 409) {
      window.location.href = "/setup";
      return;
    }
    showNotice(error.message, "danger");
  }
}

function syncProfileModeUI() {
  const form = $("#profile-form");
  if (!form) {
    return;
  }
  refreshProfileSourceOrderUI(form);
}

function syncSubscriptionSourceUI() {
  const form = $("#subscription-form");
  if (!form) {
    return;
  }
  const checked = form.querySelector('input[name="source_type"]:checked');
  const sourceType = checked ? checked.value : "remote";
  form.querySelectorAll("[data-source-panel]").forEach((node) => {
    const matches = node.getAttribute("data-source-panel") === sourceType;
    node.hidden = !matches;
    node.querySelectorAll("input, textarea, select").forEach((field) => {
      if (field.name === "group_id" || field.name === "expires_at") {
        return;
      }
      if (field.name === "source_type" || field.type === "hidden" || field.type === "radio") {
        return;
      }
      field.disabled = !matches;
    });
  });
  form.querySelectorAll(".subscription-help").forEach((node) => {
    node.hidden = !node.classList.contains(`subscription-help-${sourceType}`);
  });
}

function startCountdownLoop() {
  window.setInterval(() => {
    document.querySelectorAll("[data-next-refresh]").forEach((node) => {
      const enabled = node.dataset.enabled === "1";
      node.textContent = formatCountdown(node.dataset.nextRefresh, enabled, node.dataset.sourceType || "remote");
    });
  }, 1000);
}

async function copyInputValue(id) {
  const input = document.getElementById(id);
  if (!input) {
    return;
  }
  try {
    await navigator.clipboard.writeText(input.value);
    showNotice("已复制到剪贴板。", "success");
  } catch (error) {
    showNotice("复制失败，请手动复制。", "danger");
  }
}

function focusFormField(formId, selectors = []) {
  const form = document.getElementById(formId);
  if (!(form instanceof HTMLFormElement)) {
    return;
  }
  form.scrollIntoView({ behavior: "smooth", block: "start" });
  const targets = selectors.length ? selectors : ["input:not([type='hidden'])", "textarea", "select"];
  for (const selector of targets) {
    const field = form.querySelector(selector);
    if (
      field instanceof HTMLInputElement
      || field instanceof HTMLTextAreaElement
      || field instanceof HTMLSelectElement
    ) {
      if (!field.disabled && !field.hidden) {
        window.setTimeout(() => field.focus(), 140);
        return;
      }
    }
  }
}

function readFileAsText(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("读取备份文件失败。"));
    reader.readAsText(file, "utf-8");
  });
}

function defaultSubscriptionDraft() {
  return {
    id: "",
    source_type: "remote",
    name: "",
    url: "",
    manual_content: "",
    group_id: null,
    expires_at: "",
    refresh_interval_hours: 24
  };
}

function getSubscriptionDraft() {
  return {
    ...defaultSubscriptionDraft(),
    ...(state.subscriptionDraft || {})
  };
}

function setFormSubmitting(form, submitting, pendingLabel = "提交中...") {
  if (!(form instanceof HTMLFormElement)) {
    return () => {};
  }
  const submitButton = form.querySelector('button[data-action="submit-subscription-form"], button[type="submit"]');
  if (!(submitButton instanceof HTMLButtonElement)) {
    return () => {};
  }
  if (!submitting) {
    submitButton.disabled = false;
    if (submitButton.dataset.originalLabel) {
      submitButton.textContent = submitButton.dataset.originalLabel;
      delete submitButton.dataset.originalLabel;
    }
    return () => {};
  }
  if (!submitButton.dataset.originalLabel) {
    submitButton.dataset.originalLabel = submitButton.textContent || "";
  }
  submitButton.disabled = true;
  submitButton.textContent = pendingLabel;
  return () => setFormSubmitting(form, false);
}

function collectSubscriptionFormPayload(form) {
  const data = new FormData(form);
  return {
    id: data.get("id"),
    source_type: String(data.get("source_type") || "remote"),
    name: String(data.get("name") || ""),
    url: String(data.get("url") || ""),
    manual_content: String(data.get("manual_content") || ""),
    group_id: data.get("group_id") ? Number(data.get("group_id")) : null,
    expires_at: fromDatetimeLocal(String(data.get("expires_at") || "")),
    refresh_interval_hours: Number(data.get("refresh_interval_hours") || 24)
  };
}

async function submitSubscriptionForm(form) {
  const payload = collectSubscriptionFormPayload(form);
  const id = payload.id;
  state.subscriptionDraft = { ...payload };
  const resetSubmitting = setFormSubmitting(form, true, id ? "保存中..." : "添加并首刷中...");
  try {
    showNotice(id ? "正在保存订阅..." : "正在添加订阅并执行首刷，请稍候...", "info");
    const response = await requestJson(
      id ? `/api/subscriptions/${id}/update` : "/api/subscriptions",
      {
        method: "POST",
        body: JSON.stringify(payload)
      }
    );
    if (id) {
      state.editingSubscriptionId = response.subscription.id;
    } else {
      state.editingSubscriptionId = null;
      state.recentSubscriptionId = response.subscription.id;
      state.subscriptionDraft = defaultSubscriptionDraft();
    }
    state.subscriptionPreview = null;
    showNotice(
      id
        ? "订阅已更新。"
        : `订阅已添加：${response.subscription.name}，已缓存 ${response.subscription.node_count || 0} 个节点。`,
      "success"
    );
    await loadDashboard();
    if (!id) {
      focusFormField("subscription-form", ["input[name='name']", "textarea[name='url']", "textarea[name='manual_content']"]);
    }
  } catch (error) {
    showNotice(error.message, "danger");
  } finally {
    resetSubmitting();
  }
}

async function readBackupPayloadFromRestoreForm(form) {
  const data = new FormData(form);
  const file = data.get("backup_file");
  const rawJson = String(data.get("backup_json") || "").trim();
  if (file && file instanceof File && file.size > 0) {
    return JSON.parse(await readFileAsText(file));
  }
  if (rawJson) {
    return JSON.parse(rawJson);
  }
  throw new Error("请选择备份文件，或者粘贴备份 JSON。");
}

async function loadSubscriptionLogs(subscriptionId, limit = 12) {
  const response = await requestJson(`/api/subscriptions/${subscriptionId}/logs?limit=${encodeURIComponent(String(limit))}`);
  const subscription = getSubscriptionById(subscriptionId);
  state.subscriptionLogs = {
    subscription_id: subscriptionId,
    subscription_name: subscription ? subscription.name : "",
    logs: response.logs || []
  };
}

async function handleDashboardSubmit(event) {
  const form = event.target;
  if (!(form instanceof HTMLFormElement)) {
    return;
  }

  if (form.id === "subscription-form") {
    event.preventDefault();
    await submitSubscriptionForm(form);
    return;
  }

  if (form.id === "group-form") {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(form).entries());
    try {
      const response = await requestJson(
        data.id ? `/api/groups/${data.id}/update` : "/api/groups",
        {
          method: "POST",
          body: JSON.stringify(data)
        }
      );
      state.editingGroupId = response.group.id;
      showNotice(data.id ? "分组已更新。" : "分组已创建。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
  }

  if (form.id === "profile-form") {
    event.preventDefault();
    const data = new FormData(form);
    const id = data.get("id");
    const payload = {
      name: String(data.get("name") || ""),
      description: String(data.get("description") || ""),
      mode: String(data.get("mode") || "selected"),
      exclude_keywords: String(data.get("exclude_keywords") || ""),
      exclude_protocols: String(data.get("exclude_protocols") || ""),
      subscription_ids: getOrderedProfileSourceIds(form)
    };
    try {
      const response = await requestJson(
        id ? `/api/profiles/${id}/update` : "/api/profiles",
        {
          method: "POST",
          body: JSON.stringify(payload)
        }
      );
      state.editingProfileId = response.profile.id;
      showNotice(id ? "主订阅已更新。" : "主订阅已创建。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
  }

  if (form.id === "node-search-form") {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(form).entries());
    try {
      await loadNodePreview({
        profile_id: String(data.profile_id || ""),
        search: String(data.search || ""),
        protocol: String(data.protocol || ""),
        limit: Number(data.limit || 200)
      });
      renderNodes();
      showNotice("节点预览已更新。", "success");
    } catch (error) {
      showNotice(error.message, "danger");
    }
  }

  if (form.id === "settings-form") {
    event.preventDefault();
    const payload = Object.fromEntries(new FormData(form).entries());
    try {
      await requestJson("/api/settings", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      showNotice("全局过滤规则已保存。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
  }

  if (form.id === "appearance-form") {
    event.preventDefault();
    const payload = Object.fromEntries(new FormData(form).entries());
    try {
      const response = await requestJson("/api/settings/theme", {
        method: "POST",
        body: JSON.stringify({ theme: String(payload.theme || "classic") })
      });
      applyTheme(String(response.theme || payload.theme || "classic"));
      renderAll();
      showNotice(`界面主题已切换为 ${THEME_OPTIONS[state.theme].title}，并保存为默认主题。`, "success");
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (form.id === "cleanup-form") {
    event.preventDefault();
    const data = new FormData(form);
    const payload = {
      auto_disable_expired: data.get("auto_disable_expired") === "on",
      pause_failures_threshold: Number(data.get("pause_failures_threshold") || 0)
    };
    try {
      await requestJson("/api/settings/cleanup", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      showNotice("自动清理策略已保存。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (form.id === "panel-settings-form") {
    event.preventDefault();
    const payload = Object.fromEntries(new FormData(form).entries());
    try {
      const response = await requestJson("/api/settings/panel", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      showNotice(`面板端口已保存为 ${response.panel_port}，重启服务后生效。`, "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
  }

  if (form.id === "notifications-form") {
    event.preventDefault();
    const payload = Object.fromEntries(new FormData(form).entries());
    try {
      await requestJson("/api/settings/notifications", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      showNotice("通知设置已保存。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
  }

  if (form.id === "account-form") {
    event.preventDefault();
    const payload = Object.fromEntries(new FormData(form).entries());
    try {
      await requestJson("/api/account", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      form.reset();
      showNotice("管理员账号信息已更新。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
  }

  if (form.id === "restore-form") {
    event.preventDefault();
    try {
      const backupPayload = await readBackupPayloadFromRestoreForm(form);

      if (!window.confirm("恢复备份会覆盖当前数据，确定继续吗？")) {
        return;
      }

      await requestJson("/api/restore", {
        method: "POST",
        body: JSON.stringify({ backup: backupPayload })
      });
      state.restorePreview = null;
      persistNotice("备份已恢复。运行时安全配置已重载，请重新登录。", "success");
      window.location.href = "/login";
      return;
    } catch (error) {
      showNotice(error.message || "恢复备份失败。", "danger");
    }
    return;
  }

  if (form.id === "subscription-import-form") {
    event.preventDefault();
    const data = new FormData(form);
    const payload = {
      raw_text: String(data.get("raw_text") || ""),
      group_id: data.get("group_id") ? Number(data.get("group_id")) : null,
      refresh_interval_hours: Number(data.get("refresh_interval_hours") || 24)
    };
    try {
      const response = await requestJson("/api/subscriptions/import", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      state.bulkImportResult = response.result;
      showNotice(`批量导入完成：成功 ${response.result.created_count} 条，失败 ${response.result.error_count} 条。`, "success");
      await loadDashboard();
      renderSubscriptions();
    } catch (error) {
      showNotice(error.message, "danger");
    }
  }
}

async function handleDashboardClick(event) {
  const nav = event.target.closest(".nav-link");
  if (nav) {
    setView(nav.dataset.view);
    return;
  }

  const copyButton = event.target.closest(".copy-button");
  if (copyButton) {
    await copyInputValue(copyButton.dataset.copyTarget);
    return;
  }

  const actionButton = event.target.closest("[data-action]");
  if (!actionButton) {
    return;
  }

  const action = actionButton.dataset.action;
  const id = Number(actionButton.dataset.id || 0);

  if (action === "move-profile-source-up" || action === "move-profile-source-down" || action === "remove-profile-source") {
    const form = document.getElementById("profile-form");
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    if (action === "remove-profile-source") {
      const checkbox = form.querySelector(`input[name="subscription_ids"][value="${id}"]`);
      if (checkbox instanceof HTMLInputElement) {
        checkbox.checked = false;
      }
      refreshProfileSourceOrderUI(form);
      return;
    }
    moveProfileSourceOrder(form, id, action === "move-profile-source-up" ? "up" : "down");
    return;
  }

  if (action === "logout") {
    await requestJson("/api/logout", { method: "POST", body: "{}" });
    window.location.href = "/login";
    return;
  }

  if (action === "refresh-all") {
    try {
      await requestJson("/api/subscriptions/refresh-all", {
        method: "POST",
        body: "{}"
      });
      showNotice("全部启用中的订阅都已进入刷新流程。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "preview-subscription-input") {
    const form = document.getElementById("subscription-form");
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    try {
      const payload = collectSubscriptionFormPayload(form);
      state.subscriptionDraft = { ...payload };
      const response = await requestJson("/api/subscriptions/preview", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      state.subscriptionPreview = response.preview;
      renderSubscriptions();
      syncSubscriptionSourceUI();
      showNotice("订阅预检完成。", "success");
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "submit-subscription-form") {
    const form = document.getElementById("subscription-form");
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    await submitSubscriptionForm(form);
    return;
  }

  if (action === "new-subscription") {
    state.editingSubscriptionId = null;
    state.subscriptionDraft = defaultSubscriptionDraft();
    state.subscriptionPreview = null;
    state.subscriptionLogs = null;
    renderSubscriptions();
    syncSubscriptionSourceUI();
    focusFormField("subscription-form", ["input[name='name']", "textarea[name='url']", "textarea[name='manual_content']"]);
    return;
  }

  if (action === "edit-subscription") {
    state.editingSubscriptionId = id;
    state.subscriptionDraft = null;
    state.subscriptionPreview = null;
    state.subscriptionLogs = null;
    renderSubscriptions();
    syncSubscriptionSourceUI();
    focusFormField("subscription-form", ["input[name='name']", "textarea[name='url']", "textarea[name='manual_content']"]);
    return;
  }

  if (action === "subscription-logs") {
    try {
      await loadSubscriptionLogs(id);
      renderSubscriptions();
      showNotice("刷新日志已加载。", "success");
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "refresh-subscription") {
    try {
      await requestJson(`/api/subscriptions/${id}/refresh`, {
        method: "POST",
        body: "{}"
      });
      if (state.subscriptionLogs && state.subscriptionLogs.subscription_id === id) {
        await loadSubscriptionLogs(id);
      }
      showNotice("订阅已刷新。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "delete-subscription") {
    if (!window.confirm("确定删除这个订阅吗？对应缓存节点会一起删除。")) {
      return;
    }
    try {
      await requestJson(`/api/subscriptions/${id}/delete`, {
        method: "POST",
        body: "{}"
      });
      if (state.editingSubscriptionId === id) {
        state.editingSubscriptionId = null;
      }
      if (state.subscriptionLogs && state.subscriptionLogs.subscription_id === id) {
        state.subscriptionLogs = null;
      }
      showNotice("订阅已删除。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "new-group") {
    state.editingGroupId = null;
    renderGroups();
    focusFormField("group-form", ["input[name='name']", "textarea[name='description']"]);
    return;
  }

  if (action === "edit-group") {
    state.editingGroupId = id;
    renderGroups();
    focusFormField("group-form", ["input[name='name']", "textarea[name='description']"]);
    return;
  }

  if (action === "delete-group") {
    if (!window.confirm("删除分组后，订阅不会被删除，但会变成未分组状态。确定继续吗？")) {
      return;
    }
    try {
      await requestJson(`/api/groups/${id}/delete`, {
        method: "POST",
        body: "{}"
      });
      if (state.editingGroupId === id) {
        state.editingGroupId = null;
      }
      showNotice("分组已删除。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "new-profile") {
    state.editingProfileId = null;
    renderProfiles();
    syncProfileModeUI();
    focusFormField("profile-form", ["input[name='name']", "textarea[name='description']"]);
    return;
  }

  if (action === "edit-profile") {
    state.editingProfileId = id;
    renderProfiles();
    syncProfileModeUI();
    focusFormField("profile-form", ["input[name='name']", "textarea[name='description']"]);
    return;
  }

  if (action === "clone-profile") {
    try {
      await requestJson(`/api/profiles/${id}/clone`, {
        method: "POST",
        body: JSON.stringify({})
      });
      showNotice("主订阅已复制。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "preview-profile-nodes") {
    try {
      await loadNodePreview({
        profile_id: String(id),
        search: "",
        protocol: "",
        limit: 200
      });
      setView("nodes");
      renderNodes();
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "delete-profile") {
    if (!window.confirm("确定删除这个主订阅吗？外部订阅这个链接的客户端会失效。")) {
      return;
    }
    try {
      await requestJson(`/api/profiles/${id}/delete`, {
        method: "POST",
        body: "{}"
      });
      if (state.editingProfileId === id) {
        state.editingProfileId = null;
      }
      showNotice("主订阅已删除。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "regenerate-profile-token") {
    try {
      await requestJson(`/api/profiles/${id}/token/regenerate`, {
        method: "POST",
        body: "{}"
      });
      showNotice("新的主订阅 Token 已生成，旧链接会立即失效。", "success");
      await loadDashboard();
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "download-backup") {
    try {
      const response = await requestRaw("/api/backup");
      const text = await response.text();
      const blob = new Blob([text], { type: "application/json;charset=utf-8" });
      const href = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = href;
      link.download = "lulynx-subhub-backup.json";
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(href);
      showNotice("备份文件已导出。", "success");
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "preview-restore") {
    const form = document.getElementById("restore-form");
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    try {
      const backupPayload = await readBackupPayloadFromRestoreForm(form);
      const response = await requestJson("/api/restore/preview", {
        method: "POST",
        body: JSON.stringify({ backup: backupPayload })
      });
      state.restorePreview = response.preview;
      renderSettings();
      showNotice("恢复预检完成。", "success");
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "apply-example-import") {
    setView("subscriptions");
    renderSubscriptions();
    const form = document.getElementById("subscription-import-form");
    if (form instanceof HTMLFormElement) {
      const field = form.querySelector('textarea[name="raw_text"]');
      if (field instanceof HTMLTextAreaElement) {
        field.value = EXAMPLE_SNIPPETS.bulk_import;
        field.focus();
      }
    }
    showNotice("批量导入示例已填入。", "success");
    return;
  }

  if (action === "apply-example-filters") {
    setView("settings");
    renderSettings();
    const form = document.getElementById("settings-form");
    if (form instanceof HTMLFormElement) {
      const keywordField = form.querySelector('textarea[name="global_exclude_keywords"]');
      const protocolField = form.querySelector('input[name="exclude_protocols"]');
      const renameField = form.querySelector('textarea[name="rename_rules"]');
      if (keywordField instanceof HTMLTextAreaElement) {
        keywordField.value = EXAMPLE_SNIPPETS.exclude_keywords;
      }
      if (protocolField instanceof HTMLInputElement) {
        protocolField.value = EXAMPLE_SNIPPETS.exclude_protocols;
      }
      if (renameField instanceof HTMLTextAreaElement) {
        renameField.value = EXAMPLE_SNIPPETS.rename_rules;
        renameField.focus();
      }
    }
    showNotice("过滤规则示例已填入。", "success");
    return;
  }

  if (action === "test-notification") {
    const form = document.getElementById("notifications-form");
    const message = form instanceof HTMLFormElement
      ? String(new FormData(form).get("test_message") || "这是来自 Lulynx SubHub 的测试通知。")
      : "这是来自 Lulynx SubHub 的测试通知。";
    try {
      if (form instanceof HTMLFormElement) {
        const payload = Object.fromEntries(new FormData(form).entries());
        await requestJson("/api/settings/notifications", {
          method: "POST",
          body: JSON.stringify(payload)
        });
      }
      await requestJson("/api/notifications/test", {
        method: "POST",
        body: JSON.stringify({ message })
      });
      showNotice("测试通知已发送。", "success");
    } catch (error) {
      showNotice(error.message, "danger");
    }
    return;
  }

  if (action === "reset-node-search") {
    state.nodeFilters = {
      profile_id: String(getDashboard().overview.default_profile_id || ""),
      search: "",
      protocol: "",
      limit: 200
    };
    try {
      await loadNodePreview();
      renderNodes();
      showNotice("节点筛选已重置。", "success");
    } catch (error) {
      showNotice(error.message, "danger");
    }
  }
}

async function handleDashboardChange(event) {
  const toggle = event.target.closest(".enabled-toggle");
  if (toggle) {
    try {
      await requestJson(`/api/subscriptions/${toggle.dataset.id}/enabled`, {
        method: "POST",
        body: JSON.stringify({ enabled: toggle.checked })
      });
      showNotice(toggle.checked ? "订阅已启用。" : "订阅已暂停。", "success");
      await loadDashboard();
    } catch (error) {
      toggle.checked = !toggle.checked;
      showNotice(error.message, "danger");
    }
    return;
  }

  if (event.target.name === "mode") {
    syncProfileModeUI();
    return;
  }

  if (event.target.name === "subscription_ids") {
    const form = $("#profile-form");
    if (form instanceof HTMLFormElement) {
      refreshProfileSourceOrderUI(form);
    }
    return;
  }

  if (event.target.name === "source_type") {
    state.subscriptionPreview = null;
    const form = $("#subscription-form");
    if (form instanceof HTMLFormElement) {
      state.subscriptionDraft = collectSubscriptionFormPayload(form);
    }
    syncSubscriptionSourceUI();
  }
}

function bindAuthForm(formId, endpoint) {
  const form = document.getElementById(formId);
  if (!form) {
    return;
  }
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    clearNotice();
    const payload = Object.fromEntries(new FormData(form).entries());
    try {
      const response = await requestJson(endpoint, {
        method: "POST",
        body: JSON.stringify(payload)
      });
      if (response.theme) {
        applyTheme(String(response.theme));
      } else if (payload.theme && THEME_OPTIONS[String(payload.theme)]) {
        applyTheme(String(payload.theme));
      }
      window.location.href = "/";
    } catch (error) {
      showNotice(error.message, "danger");
    }
  });
}

function initDashboard() {
  consumePersistedNotice();
  state.view = readInitialView();
  document.addEventListener("submit", (event) => void handleDashboardSubmit(event), true);
  document.body.addEventListener("click", (event) => void handleDashboardClick(event));
  document.body.addEventListener("change", (event) => void handleDashboardChange(event));
  window.addEventListener("hashchange", () => setView(readInitialView(), false));
  setView(state.view, false);
  loadDashboard();
  startCountdownLoop();
}

if (page) {
  applyTheme(readThemePreference(), false);
}

if (page === "dashboard") {
  initDashboard();
}

if (page === "login") {
  consumePersistedNotice();
  bindAuthForm("login-form", "/api/login");
}

if (page === "setup") {
  consumePersistedNotice();
  bindAuthForm("setup-form", "/api/setup");
  const themeField = document.querySelector('select[name="theme"]');
  if (themeField instanceof HTMLSelectElement) {
    themeField.addEventListener("change", () => applyTheme(themeField.value, false));
  }
}
