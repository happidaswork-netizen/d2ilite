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
  };
})();
