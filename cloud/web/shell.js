/* Shell chrome: Lucide icons + signal-field canvas. Loaded via common.js boot. */
(() => {
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

  function ensureSignalField() {
    if (document.getElementById("signalField")) return;
    const canvas = document.createElement("canvas");
    canvas.id = "signalField";
    canvas.setAttribute("aria-hidden", "true");
    document.body.prepend(canvas);
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    let w = 0;
    let h = 0;
    let raf = 0;
    let t0 = performance.now();
    const reduce = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    function resize() {
      const dpr = Math.min(window.devicePixelRatio || 1, 1.5);
      w = canvas.width = Math.floor(window.innerWidth * dpr);
      h = canvas.height = Math.floor(window.innerHeight * dpr);
      canvas.style.width = window.innerWidth + "px";
      canvas.style.height = window.innerHeight + "px";
    }
    resize();
    window.addEventListener("resize", resize, { passive: true });

    const nodes = Array.from({ length: 14 }, () => ({
      x: Math.random(),
      y: Math.random(),
      r: 0.7 + Math.random() * 1.2,
      vx: (Math.random() - 0.5) * 0.00008,
      vy: (Math.random() - 0.5) * 0.00008,
      phase: Math.random() * Math.PI * 2,
    }));
    let last = 0;

    function frame(now) {
      // Cap ~24fps — atmospheric, not a game loop.
      if (now - last < 40) {
        if (!reduce) raf = requestAnimationFrame(frame);
        return;
      }
      last = now;
      const t = (now - t0) / 1000;
      const dpr = Math.min(window.devicePixelRatio || 1, 1.5);
      ctx.clearRect(0, 0, w, h);
      ctx.strokeStyle = "rgba(232,224,208,0.025)";
      ctx.lineWidth = 1;
      const step = 96 * dpr;
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
          a: 0.2 + 0.3 * Math.sin(t * 1.1 + n.phase),
        };
      });
      for (let i = 0; i < pts.length; i++) {
        for (let j = i + 1; j < pts.length; j++) {
          const dx = pts[i].x - pts[j].x;
          const dy = pts[i].y - pts[j].y;
          const d2 = dx * dx + dy * dy;
          const max = 120 * dpr;
          if (d2 < max * max) {
            const alpha = (1 - Math.sqrt(d2) / max) * 0.1;
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

      if (!reduce) raf = requestAnimationFrame(frame);
    }
    if (reduce) {
      frame(performance.now());
    } else {
      raf = requestAnimationFrame(frame);
    }
    window.addEventListener(
      "pagehide",
      () => {
        if (raf) cancelAnimationFrame(raf);
      },
      { once: true }
    );
  }

  function lucideCreate() {
    try {
      if (window.lucide && typeof window.lucide.createIcons === "function") {
        window.lucide.createIcons({
          attrs: {
            "stroke-width": 1.7,
            "aria-hidden": "true",
          },
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
      // disabled items
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
  function bootShell() {
    ensureSignalField();
    enhanceNav();
    enhanceEmptyStates();
    enhanceButtons();
    lucideCreate();
    // re-run after late DOM paints (detail panels toggle)
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
})();
