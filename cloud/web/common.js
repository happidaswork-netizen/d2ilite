/* D2I Cloud Web — shared token plumbing for all pages (loaded before each page script). */
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

  window.D2I = { getToken, setToken, authHeaders, promptToken };
})();
