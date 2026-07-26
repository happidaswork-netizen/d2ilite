(() => {
  const TOKEN_KEY = "d2i_cloud_token";
  const PAGE_SIZE = 60;

  const state = {
    items: [],
    queues: [],
    total: 0,
    previewable: 0,
    offset: 0,
    status: "all",
    queueId: "",
    q: "",
    selectedId: "",
    objectUrls: [],
    loading: false,
  };

  const $ = (id) => document.getElementById(id);

  function token() {
    return (localStorage.getItem(TOKEN_KEY) || "").trim();
  }

  function setToken(value) {
    const next = String(value || "").trim();
    if (next) localStorage.setItem(TOKEN_KEY, next);
    else localStorage.removeItem(TOKEN_KEY);
    return next;
  }

  function bootstrapTokenFromUrl() {
    try {
      const url = new URL(window.location.href);
      const fromQuery =
        url.searchParams.get("token") ||
        url.searchParams.get("access_token") ||
        url.searchParams.get("d2i_token") ||
        "";
      let fromHash = "";
      if (url.hash && url.hash.length > 1) {
        const hp = new URLSearchParams(url.hash.replace(/^#/, ""));
        fromHash = hp.get("token") || hp.get("access_token") || hp.get("d2i_token") || "";
      }
      const boot = String(fromQuery || fromHash || "").trim();
      if (!boot) return false;
      setToken(boot);
      url.searchParams.delete("token");
      url.searchParams.delete("access_token");
      url.searchParams.delete("d2i_token");
      const cleanHash = new URLSearchParams(url.hash.replace(/^#/, ""));
      cleanHash.delete("token");
      cleanHash.delete("access_token");
      cleanHash.delete("d2i_token");
      const hashText = cleanHash.toString();
      url.hash = hashText ? `#${hashText}` : "";
      window.history.replaceState({}, document.title, url.pathname + url.search + url.hash);
      return true;
    } catch {
      return false;
    }
  }

  function authHeaders() {
    const headers = { "Content-Type": "application/json" };
    const t = token();
    if (t) headers.Authorization = `Bearer ${t}`;
    return headers;
  }

  async function api(path, options = {}) {
    const res = await fetch(path, {
      ...options,
      headers: { ...authHeaders(), ...(options.headers || {}) },
    });
    const text = await res.text();
    let body = null;
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      body = { raw: text };
    }
    if (!res.ok) {
      const detail = body?.detail || body?.error || res.statusText || "request failed";
      const err = new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
      err.status = res.status;
      throw err;
    }
    return body;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function toast(message) {
    const el = $("toast");
    el.hidden = false;
    el.textContent = message;
    clearTimeout(toast._t);
    toast._t = setTimeout(() => {
      el.hidden = true;
    }, 2600);
  }

  function setConn(ok, text) {
    const dot = $("connDot");
    const label = $("connText");
    if (dot) dot.className = `dot ${ok ? "ok" : "bad"}`;
    if (label) label.textContent = text;
  }

  function revokeObjectUrls() {
    for (const url of state.objectUrls) {
      try {
        URL.revokeObjectURL(url);
      } catch {
        /* ignore */
      }
    }
    state.objectUrls = [];
  }

  async function fetchAuthorizedBlob(url) {
    const res = await fetch(url, { headers: authHeaders() });
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

  async function loadPreviewInto(imgEl, previewUrl, fallbackEl) {
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
      const objectUrl = URL.createObjectURL(blob);
      state.objectUrls.push(objectUrl);
      imgEl.src = objectUrl;
      imgEl.hidden = false;
      if (fallbackEl) fallbackEl.hidden = true;
      return objectUrl;
    } catch (err) {
      imgEl.hidden = true;
      imgEl.removeAttribute("src");
      if (fallbackEl) {
        fallbackEl.hidden = false;
        fallbackEl.textContent = `预览失败：${err.message || "unknown"}`;
      }
      return null;
    }
  }

  function flagText(ok, label) {
    return `${label}${ok ? "√" : "·"}`;
  }

  function selectedItem() {
    return state.items.find((row) => row.library_id === state.selectedId) || null;
  }

  function renderStats() {
    $("statTotal").textContent = String(state.total || 0);
    $("statPreviewable").textContent = String(state.previewable || 0);
    $("statQueues").textContent = String((state.queues || []).length);
    if (!state.total) {
      $("statPage").textContent = "—";
    } else {
      const start = state.offset + 1;
      const end = Math.min(state.offset + state.items.length, state.total);
      $("statPage").textContent = `${start}-${end}`;
    }
    $("btnPrevPage").disabled = state.offset <= 0 || state.loading;
    $("btnNextPage").disabled = state.offset + state.items.length >= state.total || state.loading;
  }

  function renderQueueFilter() {
    const select = $("queueFilter");
    const current = state.queueId;
    const options = ['<option value="">全部队列</option>'].concat(
      (state.queues || []).map((row) => {
        const label = `${row.name || row.id} · ${row.previewable || 0}/${row.item_count || 0}`;
        const selected = row.id === current ? " selected" : "";
        return `<option value="${escapeHtml(row.id)}"${selected}>${escapeHtml(label)}</option>`;
      })
    );
    select.innerHTML = options.join("");

    const summary = $("queueSummary");
    if (!(state.queues || []).length) {
      summary.innerHTML = `<div class="muted-hint">暂无队列摘要</div>`;
      return;
    }
    summary.innerHTML = (state.queues || [])
      .slice(0, 12)
      .map((row) => {
        const active = row.id === state.queueId ? "active" : "";
        return `<button type="button" class="library-queue-chip ${active}" data-queue-id="${escapeHtml(row.id)}">
          <strong>${escapeHtml(row.name || row.id)}</strong>
          <span>${row.previewable || 0} 可预览 / ${row.item_count || 0}</span>
        </button>`;
      })
      .join("");
  }

  function renderItemSide(item) {
    const empty = $("itemSideEmpty");
    const body = $("itemSideBody");
    if (!item) {
      empty.hidden = false;
      body.hidden = true;
      return;
    }
    empty.hidden = true;
    body.hidden = false;
    $("itemSideName").textContent = item.name || "未命名";
    $("itemSideTags").innerHTML = [
      `<span class="tag soft">${escapeHtml(item.queue_name || item.queue_id || "queue")}</span>`,
      `<span class="tag soft">${escapeHtml(item.status || item.bucket || "item")}</span>`,
      item.has_preview ? `<span class="tag running">可预览</span>` : `<span class="tag">无图</span>`,
      `<span class="tag soft">${escapeHtml(flagText(item.flags?.detail_ok, "详"))}</span>`,
      `<span class="tag soft">${escapeHtml(flagText(item.flags?.image_ok, "图"))}</span>`,
      `<span class="tag soft">${escapeHtml(flagText(item.flags?.meta_ok, "元"))}</span>`,
    ].join("");

    const meta = [
      ["库 ID", item.library_id || "—"],
      ["队列", item.queue_name || item.queue_id || "—"],
      ["条目 ID", item.id || "—"],
      ["状态", item.status || "—"],
      ["落盘路径", item.image_path || "—"],
      ["原因", item.reason || "—"],
      ["详情 URL", item.detail_url || "—"],
    ];
    $("itemSideMeta").innerHTML = meta
      .map(([k, v]) => `<dt>${escapeHtml(k)}</dt><dd title="${escapeHtml(v)}">${escapeHtml(v)}</dd>`)
      .join("");

    const source = $("itemSideSource");
    if (item.detail_url) {
      source.hidden = false;
      source.href = item.detail_url;
    } else {
      source.hidden = true;
      source.removeAttribute("href");
    }

    const queueLink = $("itemSideQueue");
    if (item.queue_id) {
      queueLink.hidden = false;
      queueLink.href = `/?queue=${encodeURIComponent(item.queue_id)}`;
    } else {
      queueLink.hidden = true;
      queueLink.removeAttribute("href");
    }

    $("btnOpenLightbox").disabled = !item.has_preview;

    const img = $("itemSidePreview");
    const fallback = $("itemSideFallback");
    fallback.textContent = item.has_preview ? "加载预览…" : "无本地预览";
    fallback.hidden = false;
    img.hidden = true;
    img.removeAttribute("src");
    if (item.has_preview && item.preview_url) {
      loadPreviewInto(img, item.preview_url, fallback);
    }
  }

  function renderItemGrid() {
    const grid = $("itemGrid");
    const rows = state.items || [];
    if (!rows.length) {
      grid.innerHTML = `<div class="empty-state" style="grid-column:1/-1">当前筛选下没有结果。</div>`;
      $("itemsHint").textContent = state.total ? `0 / ${state.total}` : "暂无结果";
      return;
    }
    $("itemsHint").textContent = `本页 ${rows.length} · 可预览 ${state.previewable} · 共 ${state.total}`;
    grid.innerHTML = rows
      .map((row) => {
        const active = row.library_id === state.selectedId ? "active" : "";
        const flags = [
          flagText(row.flags?.detail_ok, "详"),
          flagText(row.flags?.image_ok, "图"),
          flagText(row.flags?.meta_ok, "元"),
        ].join(" ");
        const thumbInner = row.has_preview
          ? `<div class="item-thumb-loading">加载中</div>`
          : `<div class="item-thumb-placeholder">${escapeHtml(flags || "无图")}</div>`;
        return `<article class="item-card ${active}" data-library-id="${escapeHtml(row.library_id)}" title="${escapeHtml(
          row.queue_name || row.reason || row.image_path || ""
        )}">
          <div class="item-thumb ${row.has_preview ? "has-image" : "has-flags"}" data-preview-host="1">${thumbInner}</div>
          <div class="item-body">
            <div class="item-name">${escapeHtml(row.name || "未命名")}</div>
            <div class="item-meta">
              <span>${escapeHtml(row.queue_name || row.queue_id || "")}</span>
              <span>${escapeHtml(row.status || row.bucket || "")}</span>
            </div>
          </div>
        </article>`;
      })
      .join("");

    const cards = [...grid.querySelectorAll(".item-card[data-library-id]")];
    let cursor = 0;
    const pump = () => {
      const slice = cards.slice(cursor, cursor + 4);
      cursor += 4;
      if (!slice.length) return;
      Promise.all(
        slice.map(async (card) => {
          const id = card.dataset.libraryId;
          const row = state.items.find((item) => item.library_id === id);
          if (!row?.has_preview || !row.preview_url) return;
          const host = card.querySelector("[data-preview-host]");
          if (!host) return;
          try {
            const blob = await fetchAuthorizedBlob(row.preview_url);
            const objectUrl = URL.createObjectURL(blob);
            state.objectUrls.push(objectUrl);
            host.innerHTML = `<img src="${objectUrl}" alt="${escapeHtml(row.name || "")}" loading="lazy" />`;
          } catch {
            host.innerHTML = `<div class="item-thumb-placeholder">预览失败</div>`;
          }
        })
      ).finally(() => {
        if (cursor < cards.length) pump();
      });
    };
    pump();
  }

  function selectItem(libraryId) {
    state.selectedId = libraryId;
    renderItemGrid();
    renderItemSide(selectedItem());
  }

  function openLightbox(item) {
    const dialog = $("lightboxDialog");
    if (!dialog || !item) return;
    $("lightboxTitle").textContent = item.name || item.library_id || "预览";
    $("lightboxMeta").textContent =
      [item.queue_name, item.image_path || item.detail_url || item.reason].filter(Boolean).join(" · ") || "—";
    const img = $("lightboxImage");
    const empty = $("lightboxEmpty");
    img.hidden = true;
    img.removeAttribute("src");
    empty.hidden = false;
    empty.textContent = item.has_preview ? "加载中…" : "当前项没有可预览的本地图片。";
    dialog.showModal();
    if (item.has_preview && item.preview_url) {
      loadPreviewInto(img, item.preview_url, empty).then((url) => {
        if (url) empty.hidden = true;
      });
    }
  }

  function shiftLightbox(delta) {
    if (!state.items.length || !state.selectedId) return;
    const idx = state.items.findIndex((row) => row.library_id === state.selectedId);
    if (idx < 0) return;
    let next = idx;
    for (let step = 0; step < state.items.length; step += 1) {
      next = (next + delta + state.items.length) % state.items.length;
      if (state.items[next]?.has_preview || step === state.items.length - 1) break;
    }
    selectItem(state.items[next].library_id);
    if ($("lightboxDialog")?.open) openLightbox(state.items[next]);
  }

  async function loadLibrary({ keepSelection = true, resetOffset = false } = {}) {
    if (state.loading) return;
    if (resetOffset) state.offset = 0;
    state.loading = true;
    renderStats();
    const prevId = keepSelection ? state.selectedId : "";
    revokeObjectUrls();
    const params = new URLSearchParams();
    params.set("limit", String(PAGE_SIZE));
    params.set("offset", String(state.offset));
    if (state.status && state.status !== "all") params.set("status", state.status);
    if (state.q) params.set("q", state.q);
    if (state.queueId) params.set("queue_id", state.queueId);
    try {
      const payload = await api(`/api/v1/library?${params.toString()}`);
      state.items = payload.items || [];
      state.queues = payload.queues || [];
      state.total = Number(payload.total || 0);
      state.previewable = Number(payload.previewable || 0);
      state.offset = Number(payload.offset || 0);
      if (prevId && state.items.some((row) => row.library_id === prevId)) {
        state.selectedId = prevId;
      } else {
        const firstPreview = state.items.find((row) => row.has_preview);
        state.selectedId = firstPreview?.library_id || state.items[0]?.library_id || "";
      }
      renderQueueFilter();
      renderItemGrid();
      renderItemSide(selectedItem());
      renderStats();
      const errCount = (payload.errors || []).length;
      $("filterHint").textContent = errCount
        ? `已加载；${errCount} 个队列读取异常已跳过。`
        : "默认优先展示有图结果。";
      setConn(true, "已连接");
    } catch (err) {
      state.items = [];
      state.total = 0;
      state.previewable = 0;
      $("itemGrid").innerHTML = `<div class="empty-state" style="grid-column:1/-1">结果库读取失败：${escapeHtml(
        err.message
      )}</div>`;
      $("itemsHint").textContent = "读取失败";
      renderItemSide(null);
      renderStats();
      setConn(false, err.status === 401 ? "需要 Token" : "连接失败");
      if (err.status === 401) toast("需要 API Token");
    } finally {
      state.loading = false;
      renderStats();
    }
  }

  function readFiltersFromUi() {
    state.q = String($("searchInput").value || "").trim();
    state.queueId = String($("queueFilter").value || "").trim();
  }

  function bindEvents() {
    $("btnRefresh")?.addEventListener("click", () => {
      loadLibrary({ keepSelection: true }).catch((err) => toast(err.message));
    });
    $("btnApplyFilters")?.addEventListener("click", () => {
      readFiltersFromUi();
      loadLibrary({ keepSelection: false, resetOffset: true }).catch((err) => toast(err.message));
    });
    $("btnResetFilters")?.addEventListener("click", () => {
      state.status = "all";
      state.queueId = "";
      state.q = "";
      $("searchInput").value = "";
      $("queueFilter").value = "";
      $("statusPills")
        .querySelectorAll(".pill")
        .forEach((btn) => btn.classList.toggle("active", btn.dataset.status === "all"));
      loadLibrary({ keepSelection: false, resetOffset: true }).catch((err) => toast(err.message));
    });
    $("statusPills")?.addEventListener("click", (event) => {
      const btn = event.target.closest("[data-status]");
      if (!btn) return;
      state.status = btn.dataset.status || "all";
      $("statusPills")
        .querySelectorAll(".pill")
        .forEach((el) => el.classList.toggle("active", el === btn));
      loadLibrary({ keepSelection: false, resetOffset: true }).catch((err) => toast(err.message));
    });
    $("queueSummary")?.addEventListener("click", (event) => {
      const btn = event.target.closest("[data-queue-id]");
      if (!btn) return;
      state.queueId = btn.dataset.queueId || "";
      $("queueFilter").value = state.queueId;
      loadLibrary({ keepSelection: false, resetOffset: true }).catch((err) => toast(err.message));
    });
    $("searchInput")?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        readFiltersFromUi();
        loadLibrary({ keepSelection: false, resetOffset: true }).catch((err) => toast(err.message));
      }
    });
    $("btnPrevPage")?.addEventListener("click", () => {
      state.offset = Math.max(0, state.offset - PAGE_SIZE);
      loadLibrary({ keepSelection: false }).catch((err) => toast(err.message));
    });
    $("btnNextPage")?.addEventListener("click", () => {
      state.offset = state.offset + PAGE_SIZE;
      loadLibrary({ keepSelection: false }).catch((err) => toast(err.message));
    });
    $("itemGrid")?.addEventListener("click", (event) => {
      const card = event.target.closest("[data-library-id]");
      if (!card) return;
      selectItem(card.dataset.libraryId);
    });
    $("itemGrid")?.addEventListener("dblclick", (event) => {
      const card = event.target.closest("[data-library-id]");
      if (!card) return;
      const item = state.items.find((row) => row.library_id === card.dataset.libraryId);
      if (item) openLightbox(item);
    });
    $("btnOpenLightbox")?.addEventListener("click", () => {
      const item = selectedItem();
      if (item) openLightbox(item);
    });
    $("btnLightboxPrev")?.addEventListener("click", () => shiftLightbox(-1));
    $("btnLightboxNext")?.addEventListener("click", () => shiftLightbox(1));
    $("btnLightboxClose")?.addEventListener("click", () => $("lightboxDialog")?.close());
    $("lightboxDialog")?.addEventListener("click", (event) => {
      if (event.target === $("lightboxDialog")) $("lightboxDialog").close();
    });
    document.addEventListener("keydown", (event) => {
      if (!$("lightboxDialog")?.open) return;
      if (event.key === "ArrowLeft") shiftLightbox(-1);
      if (event.key === "ArrowRight") shiftLightbox(1);
      if (event.key === "Escape") $("lightboxDialog").close();
    });

    $("btnToken")?.addEventListener("click", () => {
      $("tokenInput").value = token();
      $("tokenMsg").textContent = "";
      $("tokenDialog").showModal();
    });
    $("tokenDialog")?.addEventListener("close", () => {
      if ($("tokenDialog").returnValue === "save") {
        setToken($("tokenInput").value);
        toast("Token 已保存");
        loadLibrary({ keepSelection: true }).catch((err) => toast(err.message));
      }
    });
  }

  async function boot() {
    bootstrapTokenFromUrl();
    bindEvents();
    try {
      await api("/api/v1/health");
      setConn(true, "已连接");
    } catch {
      setConn(false, "服务不可达");
    }
    await loadLibrary({ keepSelection: false, resetOffset: true });
  }

  boot().catch((err) => {
    setConn(false, "启动失败");
    toast(err.message || String(err));
  });
})();
