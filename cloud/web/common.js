/* D2I Cloud Web — shared runtime for all pages (loaded before each page script).
   Token plumbing + fetch/API + HTML helpers. Pages should pull from window.D2I
   instead of re-defining these helpers. */
(() => {
  const KEY = "d2i_cloud_token";

  // Capture ?token=... bootstrap, then scrub it from the address bar.
  try {
    const u = new URL(window.location.href);
    const t = (u.searchParams.get("token") || "").trim();
    if (t) {
      localStorage.setItem(KEY, t);
      u.searchParams.delete("token");
      window.history.replaceState(null, "", u.toString());
    }
  } catch {
    /* ignore */
  }

  function getToken() {
    try {
      return (localStorage.getItem(KEY) || "").trim();
    } catch {
      return "";
    }
  }

  function setToken(value) {
    try {
      if (value && String(value).trim()) localStorage.setItem(KEY, String(value).trim());
      else localStorage.removeItem(KEY);
    } catch {
      /* ignore */
    }
  }

  function authHeaders() {
    const token = getToken();
    return token ? { Authorization: `Bearer ${token}` } : {};
  }

  let asking = false;
  let lastPromptAt = 0;
  function promptToken(message) {
    // A burst of parallel 401s must not stack prompts; but unlike the old
    // permanent one-shot, we DO re-prompt later so an expired/wrong token
    // can be corrected instead of dead-locking the whole console.
    let now = 0;
    try {
      now = Date.now();
    } catch {
      now = 0;
    }
    if (asking) return "";
    if (now && now - lastPromptAt < 1500) return "";
    asking = true;
    try {
      const t = window.prompt(message || "接口返回 401：请输入 D2I_WEB_TOKEN（留空取消）", getToken());
      lastPromptAt = now || lastPromptAt;
      if (t && t.trim()) {
        setToken(t.trim());
        return t.trim();
      }
      return "";
    } finally {
      asking = false;
    }
  }

  function jsonHeaders() {
    return { "Content-Type": "application/json", "Accept": "application/json" };
  }

  function looksLikeAccessGate(text, contentType) {
    const t = String(text || "");
    const ct = String(contentType || "").toLowerCase();
    if (ct.includes("text/html") && /cloudflare\s*access|cloudflareaccess\.com|Sign in/i.test(t)) {
      return true;
    }
    if (/<title[^>]*>\s*Sign in\s*[·•]\s*Cloudflare Access/i.test(t)) return true;
    if (/cloudflareaccess\.com/i.test(t) && /<html/i.test(t)) return true;
    return false;
  }

  async function api(path, options = {}) {
    const timeoutMs = Number(options.timeoutMs || 20000);
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    const { timeoutMs: _t, headers: extraHeaders, ...fetchOpts } = options;
    try {
      const res = await fetch(path, {
        ...fetchOpts,
        credentials: fetchOpts.credentials || "same-origin",
        signal: ctrl.signal,
        headers: { ...jsonHeaders(), ...authHeaders(), ...(extraHeaders || {}) },
      });
      const text = await res.text();
      const contentType = res.headers.get("content-type") || "";
      if (looksLikeAccessGate(text, contentType)) {
        const err = new Error("需要先完成 Cloudflare Access 登录（域名门禁）");
        err.status = 401;
        err.code = "cf_access";
        err.loginUrl = window.location.origin + "/";
        throw err;
      }
      let body = null;
      try {
        body = text ? JSON.parse(text) : null;
      } catch {
        const err = new Error(
          contentType.includes("text/html")
            ? "接口返回了网页而不是 JSON（可能未过域名门禁）"
            : `接口返回非 JSON：${path}`
        );
        err.status = res.status || 0;
        err.code = "non_json";
        err.raw = text.slice(0, 200);
        throw err;
      }
      if (res.status === 401) {
        const hadToken = Boolean(getToken());
        if (hadToken) setToken(""); // stale/wrong token — drop before re-prompting
        const t = promptToken(
          hadToken ? "Token 无效或已过期：请重新输入 D2I_WEB_TOKEN（留空取消）" : undefined
        );
        if (t) return api(path, options);
      }
      if (!res.ok) {
        const detail = body?.detail || body?.error || res.statusText || "请求失败";
        const err = new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
        err.status = res.status;
        throw err;
      }
      return body;
    } catch (err) {
      if (err && err.name === "AbortError") {
        const e = new Error(`请求超时（${timeoutMs}ms）：${path}`);
        e.status = 0;
        throw e;
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  // Only http(s) is safe as an href target; javascript:/data:/file: are dropped.
  function safeHttpUrl(value) {
    const s = String(value || "").trim();
    return /^https?:\/\//i.test(s) ? s : "";
  }

  // Accepts unix seconds or milliseconds; non-numeric strings pass through.
  function fmtTime(ts) {
    if (ts == null || ts === "") return "—";
    if (typeof ts === "string" && Number.isNaN(Number(ts))) return ts;
    const n = Number(ts);
    if (!Number.isFinite(n) || n <= 0) return String(ts);
    const ms = n > 1e12 ? n : n * 1000;
    try {
      return new Date(ms).toLocaleString();
    } catch {
      return String(ts);
    }
  }

  // Unified toast: isError=true paints red (coverage was the only page that did this).
  function toast(message, isError = false) {
    const el = document.getElementById("toast");
    if (!el) return;
    el.hidden = false;
    el.textContent = message == null ? "" : String(message);
    el.style.background = isError ? "#b42318" : "#101828";
    clearTimeout(toast._t);
    toast._t = setTimeout(() => {
      el.hidden = true;
    }, 3200);
  }

  // Binary fetch with Authorization (img src cannot set headers).
  async function fetchAuthorizedBlob(url) {
    const res = await fetch(url, {
      credentials: "same-origin",
      headers: authHeaders(),
    });
    if (!res.ok) {
      const text = await res.text();
      let detail = res.statusText;
      try {
        detail = JSON.parse(text)?.detail || detail;
      } catch {
        detail = text || detail;
      }
      const err = new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
      err.status = res.status;
      throw err;
    }
    return res.blob();
  }

  // Load a preview image into an <img>. stillValid() may abort after the fetch
  // (selection moved on). urlBag is an array (push) or function (callback) so
  // the page can track object URLs for later revoke — keeps state local.
  async function loadPreviewInto(imgEl, previewUrl, fallbackEl, stillValid, urlBag) {
    if (!imgEl || !previewUrl) {
      if (imgEl) {
        imgEl.hidden = true;
        imgEl.removeAttribute("src");
      }
      if (fallbackEl) fallbackEl.hidden = false;
      return null;
    }
    try {
      const blob = await fetchAuthorizedBlob(previewUrl);
      if (typeof stillValid === "function" && !stillValid()) return null;
      const objectUrl = URL.createObjectURL(blob);
      if (Array.isArray(urlBag)) urlBag.push(objectUrl);
      else if (typeof urlBag === "function") urlBag(objectUrl);
      imgEl.src = objectUrl;
      imgEl.hidden = false;
      if (fallbackEl) fallbackEl.hidden = true;
      return objectUrl;
    } catch (err) {
      if (typeof stillValid === "function" && !stillValid()) return null;
      imgEl.hidden = true;
      imgEl.removeAttribute("src");
      if (fallbackEl) {
        fallbackEl.hidden = false;
        fallbackEl.textContent = `预览失败：${err.message || "unknown"}`;
      }
      return null;
    }
  }

  // Human-readable failure catalog for vision + scraper item errors.
  // Keep codes stable: backend writes these strings into job items / reason fields.
  const ERROR_CATALOG = {
    image_not_found: {
      label: "缺图",
      hint: "本地找不到图片文件，检查落盘路径或重新下载",
      action: "重抓图片",
      severity: "must_recrawl",
    },
    ambiguous_name_match: {
      label: "同名歧义",
      hint: "同名多人且缺单位/城市约束，已跳过写入避免写错人",
      action: "人工核对单位后强制重跑",
      severity: "review",
    },
    vision_runtime_unavailable: {
      label: "视觉服务不可用",
      hint: "Grok 运行时未配置或密钥缺失，整批无法识别",
      action: "检查密钥与网络后重试",
      severity: "transient",
    },
    classify_failed: {
      label: "识别失败",
      hint: "模型调用或解析失败（瞬时故障也会落此码）",
      action: "可重跑；若反复失败再查图",
      severity: "transient",
    },
    no_person: {
      label: "图中无人",
      hint: "模型判定图中无可辨认人物",
      action: "换图或重抓",
      severity: "must_recrawl",
    },
    multi_person: {
      label: "多人图",
      hint: "图中多人，不适合单人角色肖像",
      action: "换单人图或裁剪后重抓",
      severity: "must_recrawl",
    },
    uncertain_visual: {
      label: "视觉不确定",
      hint: "性别等视觉字段不确定，需人工复核",
      action: "打开原图人工确认",
      severity: "review",
    },
    gender_conflict: {
      label: "性别冲突",
      hint: "来源页性别与视觉识别不一致",
      action: "人工裁定后写回",
      severity: "review",
    },
    already_classified: {
      label: "已识别",
      hint: "已有 visual_gender，默认跳过",
      action: "如需重跑请勾选强制",
      severity: "ok",
    },
    ok: {
      label: "通过",
      hint: "视觉检查通过",
      action: "",
      severity: "ok",
    },
    // Scraper / queue item reasons (free text often; these are common tokens).
    "无图": { label: "无图", hint: "条目没有下载到图片", action: "重试下载", severity: "must_recrawl" },
    "元数据失败": { label: "元数据失败", hint: "元数据写入或审计未通过", action: "查看复核字段", severity: "review" },
    "详情失败": { label: "详情失败", hint: "详情页抓取失败", action: "检查详情 URL / 反爬", severity: "must_recrawl" },
    "名单失败": { label: "名单失败", hint: "列表页解析失败", action: "检查列表选择器", severity: "must_recrawl" },
  };

  function explainError(codeOrText) {
    const raw = String(codeOrText || "").trim();
    if (!raw) {
      return { code: "", label: "—", hint: "", action: "", severity: "ok", raw: "" };
    }
    // Prefer exact catalog hit; else scan known codes as substring (backend sometimes
    // writes "TypeError:..." or "image_not_found: path").
    let hit = ERROR_CATALOG[raw];
    let code = raw;
    if (!hit) {
      for (const key of Object.keys(ERROR_CATALOG)) {
        if (raw === key || raw.includes(key)) {
          hit = ERROR_CATALOG[key];
          code = key;
          break;
        }
      }
    }
    if (!hit) {
      // Free-text scraper reason: show as-is, mild severity.
      return {
        code: raw,
        label: raw.length > 24 ? `${raw.slice(0, 24)}…` : raw,
        hint: raw,
        action: "查看日志与原页",
        severity: "review",
        raw,
      };
    }
    return { code, label: hit.label, hint: hit.hint, action: hit.action, severity: hit.severity, raw };
  }

  function countByReason(rows, field = "error") {
    const counts = {};
    for (const row of rows || []) {
      const raw = row?.[field] || row?.reason || row?.error || "";
      const exp = explainError(raw);
      const key = exp.code || exp.label || "unknown";
      if (!counts[key]) {
        counts[key] = { code: key, label: exp.label, severity: exp.severity, count: 0, action: exp.action, hint: exp.hint };
      }
      counts[key].count += 1;
    }
    return Object.values(counts).sort((a, b) => b.count - a.count || a.label.localeCompare(b.label, "zh"));
  }

  function renderReasonChips(host, rows, field = "error") {
    if (!host) return [];
    const groups = countByReason(rows, field);
    if (!groups.length) {
      host.innerHTML = "";
      host.hidden = true;
      return [];
    }
    host.hidden = false;
    host.innerHTML = groups
      .map((g) => {
        const sev = g.severity === "must_recrawl" ? "failed" : g.severity === "transient" ? "running" : g.severity === "ok" ? "completed" : "soft";
        return `<button type="button" class="reason-chip tag ${sev}" data-reason="${escapeHtml(g.code)}" title="${escapeHtml(g.hint || g.label)}">${escapeHtml(g.label)} · ${g.count}</button>`;
      })
      .join("");
    return groups;
  }

  window.D2I = {
    getToken,
    setToken,
    authHeaders,
    promptToken,
    jsonHeaders,
    looksLikeAccessGate,
    api,
    escapeHtml,
    safeHttpUrl,
    fmtTime,
    toast,
    fetchAuthorizedBlob,
    loadPreviewInto,
    ERROR_CATALOG,
    explainError,
    countByReason,
    renderReasonChips,
  };
})();
