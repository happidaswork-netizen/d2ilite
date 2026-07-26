/* Theme switcher + Lucide chrome. Classic is default; Observatory is opt-in. */
(() => {
  const KEY = "d2i_cloud_theme";
  const THEMES = {
    classic: { id: "classic", label: "经典", href: "/static/styles.css", scheme: "light" },
    observatory: {
      id: "observatory",
      label: "观测台",
      href: "/static/styles.observatory.css",
      scheme: "dark",
    },
  };

  const ICONS = {
    queues: "list-ordered",
    vision: "scan-eye",
    coverage: "map",
    library: "images",
    templates: "file-code-2",
    settings: "settings-2",
    emptyQueue: "radar",
    emptyNode: "map-pinned",
    emptyItem: "image",
    emptyVision: "scan-search",
  };

  let signalRaf = 0;
  let signalCanvas = null;

  function currentThemeId() {
    try {
      const raw = (localStorage.getItem(KEY) || "").trim();
      if (raw && THEMES[raw]) return raw;
    } catch {
      /* ignore */
    }
    // ?theme=observatory one-shot bootstrap
    try {
      const u = new URL(window.location.href);
      const q = (u.searchParams.get("theme") || "").trim();
      if (q && THEMES[q]) {
        localStorage.setItem(KEY, q);
        u.searchParams.delete("theme");
        window.history.replaceState(null, "", u.toString());
        return q;
      }
    } catch {
      /* ignore */
    }
    return "classic";
  }

  function ensureStylesheetLink() {
    let link = document.getElementById("d2iThemeStyles");
    if (link) return link;
    link =
      document.querySelector('link[href*="styles.css"]') ||
      document.querySelector('link[href*="styles.observatory.css"]');
    if (link) {
      link.id = "d2iThemeStyles";
      return link;
    }
    link = document.createElement("link");
    link.id = "d2iThemeStyles";
    link.rel = "stylesheet";
    document.head.appendChild(link);
    return link;
  }

  function applyTheme(id, { persist = true } = {}) {
    const theme = THEMES[id] || THEMES.classic;
    const link = ensureStylesheetLink();
    if (link.getAttribute("href") !== theme.href) {
      link.setAttribute("href", theme.href);
    }
    document.documentElement.dataset.theme = theme.id;
    document.documentElement.style.colorScheme = theme.scheme;
    let meta = document.querySelector('meta[name="color-scheme"]');
    if (!meta) {
      meta = document.createElement("meta");
      meta.name = "color-scheme";
      document.head.appendChild(meta);
    }
    meta.content = theme.scheme;
    if (persist) {
      try {
        localStorage.setItem(KEY, theme.id);
      } catch {
        /* ignore */
      }
    }
    const btn = document.getElementById("btnThemeToggle");
    if (btn) {
      btn.dataset.theme = theme.id;
      btn.title = theme.id === "observatory" ? "切换到经典主题" : "切换到观测台主题（实验）";
      btn.setAttribute("aria-label", btn.title);
      const label = btn.querySelector("[data-theme-label]");
      if (label) label.textContent = theme.id === "observatory" ? "观测台" : "经典";
    }
    // Observatory-only atmosphere
    if (theme.id === "observatory") startSignalField();
    else stopSignalField();
    // Re-apply lucide after stylesheet swap (icons stay)
    lucideCreate();
    return theme.id;
  }

  function toggleTheme() {
    const next = currentThemeId() === "observatory" ? "classic" : "observatory";
    return applyTheme(next);
  }

  function stopSignalField() {
    if (signalRaf) {
      cancelAnimationFrame(signalRaf);
      signalRaf = 0;
    }
    if (signalCanvas) {
      signalCanvas.remove();
      signalCanvas = null;
    }
  }

  function startSignalField() {
    if (signalCanvas || document.getElementById("signalField")) return;
    const reduce = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (reduce) return; // never run canvas when user asks for less motion

    const canvas = document.createElement("canvas");
    canvas.id = "signalField";
    canvas.setAttribute("aria-hidden", "true");
    document.body.prepend(canvas);
    signalCanvas = canvas;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let w = 0;
    let h = 0;
    let t0 = performance.now();
    let last = 0;
    const dprCap = 1.25;
    const nodes = Array.from({ length: 10 }, () => ({
      x: Math.random(),
      y: Math.random(),
      r: 0.7 + Math.random() * 1.1,
      vx: (Math.random() - 0.5) * 0.00006,
      vy: (Math.random() - 0.5) * 0.00006,
      phase: Math.random() * Math.PI * 2,
    }));

    function resize() {
      const dpr = Math.min(window.devicePixelRatio || 1, dprCap);
      w = canvas.width = Math.floor(window.innerWidth * dpr);
      h = canvas.height = Math.floor(window.innerHeight * dpr);
      canvas.style.width = window.innerWidth + "px";
      canvas.style.height = window.innerHeight + "px";
    }
    resize();
    window.addEventListener("resize", resize, { passive: true });

    function frame(now) {
      if (!signalCanvas) return;
      // ~18fps — atmosphere only
      if (now - last < 55) {
        signalRaf = requestAnimationFrame(frame);
        return;
      }
      last = now;
      const t = (now - t0) / 1000;
      const dpr = Math.min(window.devicePixelRatio || 1, dprCap);
      ctx.clearRect(0, 0, w, h);
      ctx.strokeStyle = "rgba(232,224,208,0.02)";
      ctx.lineWidth = 1;
      const step = 110 * dpr;
      for (let x = 0; x < w; x += step) {
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, h);
        ctx.stroke();
      }
      for (let y = 0; y < h; y += step) {
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(w, y);
        ctx.stroke();
      }
      const pts = nodes.map((n) => {
        n.x = (n.x + n.vx + 1) % 1;
        n.y = (n.y + n.vy + 1) % 1;
        return {
          x: n.x * w,
          y: n.y * h,
          r: n.r * dpr,
          a: 0.18 + 0.25 * Math.sin(t + n.phase),
        };
      });
      for (let i = 0; i < pts.length; i++) {
        for (let j = i + 1; j < pts.length; j++) {
          const dx = pts[i].x - pts[j].x;
          const dy = pts[i].y - pts[j].y;
          const d2 = dx * dx + dy * dy;
          const max = 100 * dpr;
          if (d2 < max * max) {
            const alpha = (1 - Math.sqrt(d2) / max) * 0.08;
            ctx.strokeStyle = `rgba(196,92,38,${alpha})`;
            ctx.beginPath();
            ctx.moveTo(pts[i].x, pts[i].y);
            ctx.lineTo(pts[j].x, pts[j].y);
            ctx.stroke();
          }
        }
      }
      for (const p of pts) {
        ctx.fillStyle = `rgba(61,214,198,${p.a})`;
        ctx.beginPath();
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        ctx.fill();
      }
      signalRaf = requestAnimationFrame(frame);
    }
    signalRaf = requestAnimationFrame(frame);
    window.addEventListener(
      "pagehide",
      () => {
        stopSignalField();
      },
      { once: true }
    );
  }

  function lucideCreate() {
    try {
      if (window.lucide && typeof window.lucide.createIcons === "function") {
        window.lucide.createIcons({
          attrs: { "stroke-width": 1.7, "aria-hidden": "true" },
        });
      }
    } catch {
      /* ignore */
    }
  }

  function enhanceNav() {
    const map = [
      { href: "/", icon: ICONS.queues },
      { href: "/vision", icon: ICONS.vision },
      { href: "/coverage", icon: ICONS.coverage },
      { href: "/library", icon: ICONS.library },
    ];
    document.querySelectorAll(".side-nav .nav-item").forEach((a) => {
      const href = a.getAttribute("href") || "";
      const hit = map.find((m) => m.href === href);
      const ico = a.querySelector(".nav-ico");
      if (!ico) return;
      if (a.classList.contains("disabled")) {
        const label = (a.textContent || "").trim();
        const icon = /模板/.test(label) ? ICONS.templates : ICONS.settings;
        ico.innerHTML = `<i data-lucide="${icon}"></i>`;
        return;
      }
      if (hit) ico.innerHTML = `<i data-lucide="${hit.icon}"></i>`;
    });
  }

  function enhanceEmptyStates() {
    document.querySelectorAll(".empty-illustration").forEach((el) => {
      if (el.querySelector("[data-lucide]")) return;
      let icon = ICONS.emptyQueue;
      if (el.classList.contains("sm")) icon = ICONS.emptyItem;
      const page = location.pathname || "/";
      if (page.startsWith("/coverage")) icon = ICONS.emptyNode;
      if (page.startsWith("/vision")) icon = ICONS.emptyVision;
      if (page.startsWith("/library")) icon = ICONS.emptyItem;
      el.innerHTML = `<i data-lucide="${icon}"></i>`;
    });
  }

  function enhanceButtons() {
    const map = [
      { sel: "#btnRefresh", icon: "refresh-cw" },
      { sel: "#btnOpenCreate, #btnOpenCreate2", icon: "plus" },
      { sel: "#btnInventory, #btnInventory2", icon: "clipboard-list" },
      { sel: "#btnPlan", icon: "waypoints" },
      { sel: "#btnEnqueue", icon: "list-plus" },
      { sel: "#btnPump", icon: "play" },
      { sel: "#btnPumpOne", icon: "skip-forward" },
      { sel: "#btnRequeueAll", icon: "rotate-ccw" },
      { sel: "#btnApplyFilters", icon: "filter" },
      { sel: "#btnResetFilters", icon: "eraser" },
      { sel: "#btnPrevPage", icon: "chevron-left" },
      { sel: "#btnNextPage", icon: "chevron-right" },
      { sel: "#btnOpenLightbox", icon: "expand" },
      { sel: '[data-action="start"]', icon: "play" },
      { sel: '[data-action="pause"]', icon: "pause" },
      { sel: '[data-action="resume"]', icon: "play" },
      { sel: '[data-action="retry"]', icon: "rotate-ccw" },
      { sel: '[data-action="finalize"]', icon: "hard-drive-download" },
      { sel: '[data-action="cancel"]', icon: "ban" },
      { sel: '[data-action="run"]', icon: "play" },
      { sel: '[data-action="requeue"]', icon: "rotate-ccw" },
    ];
    for (const { sel, icon } of map) {
      document.querySelectorAll(sel).forEach((btn) => {
        if (btn.querySelector("[data-lucide]")) return;
        const i = document.createElement("i");
        i.setAttribute("data-lucide", icon);
        btn.prepend(i);
      });
    }
  }

  function ensureThemeToggle() {
    if (document.getElementById("btnThemeToggle")) return;
    const foot = document.querySelector(".side-foot");
    if (!foot) return;
    const row = document.createElement("button");
    row.type = "button";
    row.id = "btnThemeToggle";
    row.className = "theme-toggle";
    row.innerHTML = `<i data-lucide="palette"></i><span data-theme-label>经典</span>`;
    row.addEventListener("click", () => toggleTheme());
    foot.appendChild(row);
  }

  // Minimal toggle styles that work on both themes
  function ensureToggleCss() {
    if (document.getElementById("d2iThemeToggleCss")) return;
    const s = document.createElement("style");
    s.id = "d2iThemeToggleCss";
    s.textContent = `
      .theme-toggle{
        display:flex;align-items:center;gap:8px;width:100%;
        margin-top:4px;padding:8px 10px;border-radius:10px;
        border:1px solid var(--border, #e4e7ec);
        background:transparent;color:var(--text-3, #475467);
        font:inherit;font-size:0.8rem;font-weight:600;cursor:pointer;
      }
      .theme-toggle:hover{color:var(--text, #101828);background:var(--bg-muted, #f8fafc)}
      .theme-toggle svg{width:16px;height:16px;flex-shrink:0}
      html[data-theme="observatory"] .theme-toggle{
        border-color:rgba(232,224,208,0.12);color:#9a9286
      }
      html[data-theme="observatory"] .theme-toggle:hover{
        color:#e8e0d0;background:rgba(255,255,255,0.04)
      }
      #signalField{position:fixed;inset:0;z-index:0;pointer-events:none;opacity:.45}
      html[data-theme="observatory"] .app-shell{position:relative;z-index:1}
    `;
    document.head.appendChild(s);
  }

  function bootShell() {
    ensureToggleCss();
    ensureThemeToggle();
    applyTheme(currentThemeId(), { persist: true });
    enhanceNav();
    enhanceEmptyStates();
    enhanceButtons();
    lucideCreate();
    const obs = new MutationObserver(() => {
      enhanceEmptyStates();
      enhanceButtons();
      lucideCreate();
    });
    obs.observe(document.body, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootShell);
  } else {
    bootShell();
  }

  window.D2I = window.D2I || {};
  window.D2I.refreshIcons = lucideCreate;
  window.D2I.bootShell = bootShell;
  window.D2I.getTheme = currentThemeId;
  window.D2I.setTheme = applyTheme;
  window.D2I.toggleTheme = toggleTheme;
})();
