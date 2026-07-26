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

  let asked = false;
  function promptToken(message) {
    if (asked) return "";
    asked = true;
    const t = window.prompt(message || "接口返回 401：请输入 D2I_WEB_TOKEN（留空取消）", "");
    if (t && t.trim()) {
      setToken(t.trim());
      return t.trim();
    }
    return "";
  }

  window.D2I = { getToken, setToken, authHeaders, promptToken };
})();
