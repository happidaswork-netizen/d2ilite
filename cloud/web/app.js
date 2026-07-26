(() => {
  const state = {
    queues: [],
    templates: [],
    selectedId: localStorage.getItem("d2i_cloud_selected_queue") || "",
    filter: "all",
    itemFilter: "all",
    items: [],
    itemsMeta: null,
    selectedItemId: "",
    objectUrls: [],
    timer: null,
    lastAutoUrl: "",
    detailLoadedFor: "",
    polling: false,
    sideSeq: 0,
  };

  const $ = (id) => document.getElementById(id);
  const {
    api,
    escapeHtml,
    safeHttpUrl,
    fmtTime,
    toast,
    fetchAuthorizedBlob,
    loadPreviewInto: loadPreviewIntoCore,
    looksLikeAccessGate,
    explainError,
    renderReasonChips,
  } = window.D2I;

  function statusLabel(status) {
    const map = {
      running: "运行中",
      paused: "已暂停",
      completed: "已完成",
      cooldown: "冷却中",
      created: "已创建",
      idle: "空闲",
      stopped: "已停止",
      error: "异常",
      cancelled: "已取消",
      downloaded: "已下载",
      pending: "未完成",
      done: "已完成",
      image: "有图",
    };
    return map[status] || status || "未知";
  }

  function tierLabel(tier) {
    const map = { safe: "安全", standard: "标准", turbo: "极速" };
    const key = String(tier || "safe");
    return `${map[key] || key} · ${key}`;
  }

  function setConn(ok, text) {
    const dot = $("connDot");
    const label = $("connText");
    if (dot) dot.className = `dot ${ok ? "ok" : "bad"}`;
    if (label) label.textContent = text;
  }

  function filteredQueues() {
    if (state.filter === "all") return state.queues;
    return state.queues.filter((q) => String(q.runtime?.status || "") === state.filter);
  }

  function progressRatio(q) {
    const kpi = q.kpi || {};
    const done = Number(kpi.completed || 0);
    const total = Math.max(done, Number(kpi.discovered || 0), Number(kpi.profiles || 0), 1);
    return Math.max(0, Math.min(100, Math.round((done / total) * 100)));
  }

  function renderQueues() {
    const list = $("queueList");
    const rows = filteredQueues();
    // Skip DOM rewrite when nothing visible changed (kills 5s poll flicker).
    const key = JSON.stringify([
      state.filter,
      state.selectedId,
      rows.map((q) => [q.id, q.name, q.updated_at, q.speed_tier, q.runtime?.status, q.kpi]),
    ]);
    if (renderQueues._key === key) return;
    renderQueues._key = key;
    if (!state.queues.length) {
      list.innerHTML = `<div class="empty-state">还没有队列。点击右上角「新建队列」开始。</div>`;
      return;
    }
    if (!rows.length) {
      list.innerHTML = `<div class="empty-state">当前筛选下没有队列。</div>`;
      return;
    }
    list.innerHTML = rows
      .map((q) => {
        const rt = q.runtime || {};
        const kpi = q.kpi || {};
        const active = q.id === state.selectedId ? "active" : "";
        return `<article class="queue-card ${active}" data-id="${escapeHtml(q.id)}">
          <div class="queue-card-top">
            <div>
              <div class="queue-name">${escapeHtml(q.name || q.id)}</div>
              <div class="queue-id">${escapeHtml(q.id)}</div>
            </div>
            <span class="tag ${escapeHtml(rt.status || "idle")}">${escapeHtml(statusLabel(rt.status))}</span>
          </div>
          <div class="queue-metrics">
            <div class="metric"><div class="n">${kpi.completed ?? 0}</div><div class="l">完成</div></div>
            <div class="metric"><div class="n">${kpi.downloaded ?? 0}</div><div class="l">下载</div></div>
            <div class="metric"><div class="n">${kpi.failures ?? 0}</div><div class="l">失败</div></div>
            <div class="metric"><div class="n">${kpi.review ?? 0}</div><div class="l">复核</div></div>
          </div>
          <div class="queue-foot">
            <span>${escapeHtml(tierLabel(q.speed_tier))}</span>
            <span>${escapeHtml(fmtTime(q.updated_at))}</span>
          </div>
        </article>`;
      })
      .join("");
  }

  function renderOverview(status) {
    const running = state.queues.filter((q) => q.runtime?.session_running || q.runtime?.status === "running").length;
    const paused = state.queues.filter((q) => q.runtime?.status === "paused").length;
    $("statQueues").textContent = String(state.queues.length);
    // Prefer live session/runtime status over stale desired_state counts.
    $("statRunning").textContent = String(status?.running ?? running);
    $("statPaused").textContent = String(status?.paused ?? paused);
    // Only reflect service health when we actually have a /status payload.
    // loadQueues() calls this with no arg every 5s poll; without this guard
    // "服务状态" flips to 异常 on every tick even on a healthy server.
    if (status) $("statService").textContent = status.ok ? "正常" : "异常";
  }

  async function loadStatus() {
    try {
      const data = await api("/api/v1/status", { timeoutMs: 25000 });
      setConn(true, `已连接 · v${data.version || "0.1"}`);
      renderOverview(data);
      return data;
    } catch (err) {
      setConn(false, formatConnError(err));
      throw err;
    }
  }

  async function loadTemplates() {
    const data = await api("/api/v1/templates");
    state.templates = data.templates || [];
    const sel = $("templateId");
    const prev = sel.value;
    sel.innerHTML = "";
    if (!state.templates.length) {
      sel.innerHTML = `<option value="">（无可用模板）</option>`;
      return;
    }
    for (const t of state.templates) {
      const opt = document.createElement("option");
      opt.value = t.id;
      opt.dataset.path = t.path || "";
      const urls = (t.start_urls || []).filter(Boolean);
      opt.dataset.url = urls[0] || "";
      opt.textContent = `${t.name || t.id}`;
      sel.appendChild(opt);
    }
    if (prev && [...sel.options].some((o) => o.value === prev)) sel.value = prev;
    onTemplateChange();
  }

  function onTemplateChange() {
    const opt = $("templateId").selectedOptions[0];
    if (!opt) return;
    const input = $("startUrl");
    const current = input.value.trim();
    const autoUrl = opt.dataset.url || "";
    // Fill when empty, and keep following template switches until the user hand-edits the URL.
    if (!current || current === state.lastAutoUrl) {
      input.value = autoUrl;
      state.lastAutoUrl = autoUrl;
    }
  }

  function onTierChange() {
    const turbo = $("speedTier").value === "turbo";
    $("allowTurboWrap").hidden = !turbo;
    if (!turbo) $("allowTurbo").checked = false;
  }

  async function loadQueues() {
    const data = await api("/api/v1/queues?limit=200");
    state.queues = data.queues || [];
    renderQueues();
    renderOverview();
    if (state.selectedId) {
      const row = state.queues.find((q) => q.id === state.selectedId);
      if (!row) {
        state.selectedId = "";
        localStorage.removeItem("d2i_cloud_selected_queue");
        clearDetail();
      } else if (state.detailLoadedFor !== state.selectedId) {
        // First restore after reload: full detail incl. items/logs.
        await showDetail(state.selectedId).catch(() => renderDetailData(row));
      } else {
        // Poll refresh: reuse the list payload instead of a second GET per tick.
        renderDetailData(row);
      }
    }
  }

  function clearDetail() {
    $("detailEmpty").hidden = false;
    $("detailBody").hidden = true;
    state.detailLoadedFor = "";
    const chips = $("reasonChips");
    if (chips) {
      chips.hidden = true;
      chips.innerHTML = "";
    }
    clearItems();
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

  function clearItems() {
    revokeObjectUrls();
    state.items = [];
    state.itemsMeta = null;
    state.selectedItemId = "";
    if ($("itemGrid")) $("itemGrid").innerHTML = "";
    if ($("itemsHint")) $("itemsHint").textContent = "队列成功项与可预览图片";
    renderItemSide(null);
  }

function loadPreviewInto(imgEl, previewUrl, fallbackEl, stillValid) {
    return loadPreviewIntoCore(imgEl, previewUrl, fallbackEl, stillValid, state.objectUrls);
  }

  function selectedItem() {
    return state.items.find((row) => row.id === state.selectedItemId) || null;
  }

  function flagText(ok, label) {
    return `${label}${ok ? "+" : "-"}`;
  }

  function renderItemSide(item) {
    const empty = $("itemSideEmpty");
    const body = $("itemSideBody");
    if (!empty || !body) return;
    if (!item) {
      empty.hidden = false;
      body.hidden = true;
      return;
    }
    empty.hidden = true;
    body.hidden = false;
    $("itemSideName").textContent = item.name || "未命名";
    $("itemSideTags").innerHTML = [
      `<span class="tag soft">${escapeHtml(item.status || item.bucket || "item")}</span>`,
      item.has_preview ? `<span class="tag running">可预览</span>` : `<span class="tag">无图</span>`,
      `<span class="tag soft">${escapeHtml(flagText(item.flags?.detail_ok, "详"))}</span>`,
      `<span class="tag soft">${escapeHtml(flagText(item.flags?.image_ok, "图"))}</span>`,
      `<span class="tag soft">${escapeHtml(flagText(item.flags?.meta_ok, "元"))}</span>`,
    ].join("");

    const exp = explainError(item.reason || "");
    const meta = [
      ["条目 ID", item.id || "—"],
      ["状态", statusLabel(item.status || item.bucket || "") || "—"],
      ["落盘路径", item.image_path || "—"],
      ["失败原因", exp.label || item.reason || "—"],
      ["详情 URL", item.detail_url || "—"],
    ];
    $("itemSideMeta").innerHTML = meta
      .map(([k, v]) => `<dt>${escapeHtml(k)}</dt><dd title="${escapeHtml(v)}">${escapeHtml(v)}</dd>`)
      .join("");

    const ev = $("itemSideEvidence");
    if (ev) {
      const reasonText = item.reason
        ? exp.label || item.reason
        : item.has_preview
          ? "正常 / 可预览"
          : "无图/未完成";
      const hintText =
        exp.hint ||
        (item.reason
          ? item.reason
          : item.has_preview
            ? "该条目当前无失败原因；可在下方查看路径与标志位。"
            : "条目没有可预览的本地图片");
      ev.hidden = false;
      ev.innerHTML = `
        <h4>判定说明</h4>
        <div><strong>原因：</strong>${escapeHtml(reasonText)}</div>
        <div><strong>说明：</strong>${escapeHtml(hintText)}</div>
        <div class="ev-actions"><strong>建议：</strong>${escapeHtml(exp.action || "查看运行日志与详情页")}</div>
        <pre>${escapeHtml(
          JSON.stringify(
            {
              id: item.id,
              status: item.status,
              bucket: item.bucket,
              reason: item.reason || null,
              flags: item.flags || null,
              image_path: item.image_path || null,
              detail_url: item.detail_url || null,
            },
            null,
            2
          )
        )}</pre>
      `;
    }

    const source = $("itemSideSource");
    const safeUrl = safeHttpUrl(item.detail_url);
    if (safeUrl) {
      source.hidden = false;
      source.href = safeUrl;
    } else {
      source.hidden = true;
      source.removeAttribute("href");
    }
    $("btnOpenLightbox").disabled = !item.has_preview;

    const img = $("itemSidePreview");
    const fallback = $("itemSideFallback");
    fallback.textContent = item.has_preview ? "加载预览…" : "无本地预览";
    fallback.hidden = false;
    img.hidden = true;
    img.removeAttribute("src");
    if (item.has_preview && item.preview_url) {
      // Guard against a stale async blob overwriting a newer selection.
      const seq = ++state.sideSeq;
      loadPreviewInto(img, item.preview_url, fallback, () => seq === state.sideSeq);
    } else {
      state.sideSeq += 1;
    }
  }

  function renderItemGrid() {
    const grid = $("itemGrid");
    if (!grid) return;
    const rows = state.items || [];
    const meta = state.itemsMeta || {};
    if (!rows.length) {
      grid.innerHTML = `<div class="empty-state" style="grid-column:1/-1">当前筛选下没有条目。</div>`;
      $("itemsHint").textContent = meta.total != null ? `0 / ${meta.total}` : "暂无条目";
      return;
    }
    $("itemsHint").textContent = `${rows.length} 条展示 · 可预览 ${meta.previewable ?? 0} · 共 ${meta.total ?? rows.length}`;
    grid.innerHTML = rows
      .map((row) => {
        const active = row.id === state.selectedItemId ? "active" : "";
        const flags = [
          flagText(row.flags?.detail_ok, "详"),
          flagText(row.flags?.image_ok, "图"),
          flagText(row.flags?.meta_ok, "元"),
        ].join(" ");
        const thumbInner = row.has_preview
          ? `<div class="item-thumb-loading">加载中</div>`
          : `<div class="item-thumb-placeholder">${escapeHtml(flags || "无图")}</div>`;
        return `<article class="item-card ${active}" data-item-id="${escapeHtml(row.id)}" title="${escapeHtml(
          row.reason || row.detail_url || row.image_path || ""
        )}">
          <div class="item-thumb ${row.has_preview ? "has-image" : "has-flags"}" data-preview-host="1">${thumbInner}</div>
          <div class="item-body">
            <div class="item-name">${escapeHtml(row.name || "未命名")}</div>
            <div class="item-meta">
              <span>${escapeHtml(statusLabel(row.status || row.bucket || ""))}</span>
              <span>${escapeHtml(flags)}</span>
            </div>
          </div>
        </article>`;
      })
      .join("");

    // Lazy-fill thumbs with authorized blobs (sequential small batch).
    const cards = [...grid.querySelectorAll(".item-card[data-item-id]")];
    let cursor = 0;
    const pump = () => {
      const slice = cards.slice(cursor, cursor + 4);
      cursor += 4;
      if (!slice.length) return;
      Promise.all(
        slice.map(async (card) => {
          const id = card.dataset.itemId;
          const row = state.items.find((item) => item.id === id);
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

  async function loadItems(queueId, { keepSelection = true } = {}) {
    if (!queueId) return;
    const prevId = keepSelection ? state.selectedItemId : "";
    revokeObjectUrls();
    const statusParam =
      state.itemFilter === "all" ? "" : `status=${encodeURIComponent(state.itemFilter)}&`;
    try {
      const payload = await api(
        `/api/v1/queues/${encodeURIComponent(queueId)}/items?${statusParam}limit=60&offset=0`
      );
      if (state.selectedId !== queueId) return; // user switched queues mid-flight
      state.items = payload.items || [];
      state.itemsMeta = {
        total: payload.total,
        previewable: payload.previewable,
        counts: payload.counts || {},
        reason_counts: payload.reason_counts || [],
      };
      const chipHost = $("reasonChips");
      if (chipHost) {
        // Prefer live item reasons; fall back to server aggregates.
        const fromItems = (payload.items || []).filter((row) => row.reason || (!row.has_preview && row.bucket === "pending"));
        if (fromItems.length) {
          renderReasonChips(chipHost, fromItems, "reason");
        } else if ((payload.reason_counts || []).length) {
          chipHost.hidden = false;
          chipHost.innerHTML = (payload.reason_counts || [])
            .map((g) => {
              const exp = explainError(g.reason);
              return `<span class="reason-chip tag soft" title="${escapeHtml(exp.hint || g.reason)}">${escapeHtml(exp.label)} · ${g.count}</span>`;
            })
            .join("");
        } else {
          chipHost.hidden = true;
          chipHost.innerHTML = "";
        }
      }
      if (prevId && state.items.some((row) => row.id === prevId)) {
        state.selectedItemId = prevId;
      } else {
        const firstPreview = state.items.find((row) => row.has_preview);
        state.selectedItemId = firstPreview?.id || state.items[0]?.id || "";
      }
      renderItemGrid();
      renderItemSide(selectedItem());
    } catch (err) {
      if (state.selectedId !== queueId) return;
      state.items = [];
      state.itemsMeta = null;
      $("itemGrid").innerHTML = `<div class="empty-state" style="grid-column:1/-1">条目读取失败：${escapeHtml(
        err.message
      )}</div>`;
      $("itemsHint").textContent = "读取失败";
      renderItemSide(null);
    }
  }

  function selectItem(itemId) {
    state.selectedItemId = itemId;
    renderItemGrid();
    renderItemSide(selectedItem());
  }

  function openLightbox(item) {
    const dialog = $("lightboxDialog");
    if (!dialog || !item) return;
    $("lightboxTitle").textContent = item.name || item.id || "预览";
    $("lightboxMeta").textContent = item.image_path || item.detail_url || item.reason || "—";
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
    if (!state.items.length || !state.selectedItemId) return;
    const idx = state.items.findIndex((row) => row.id === state.selectedItemId);
    if (idx < 0) return;
    let next = idx;
    for (let step = 0; step < state.items.length; step += 1) {
      next = (next + delta + state.items.length) % state.items.length;
      if (state.items[next]?.has_preview || step === state.items.length - 1) break;
    }
    selectItem(state.items[next].id);
    if ($("lightboxDialog")?.open) openLightbox(state.items[next]);
  }

  function renderDetailData(q) {
    const kpi = q.kpi || {};
    const rt = q.runtime || {};

    $("detailEmpty").hidden = true;
    $("detailBody").hidden = false;
    $("detailKicker").textContent = q.template_id || "QUEUE";
    $("detailTitle").textContent = q.name || q.id;
    $("detailStatus").className = `tag ${rt.status || "idle"}`;
    $("detailStatus").textContent = statusLabel(rt.status);
    $("detailTier").textContent = tierLabel(q.speed_tier);

    const running = Boolean(rt.session_running) || rt.status === "running";
    const paused = Boolean(rt.manual_paused) || rt.status === "paused";
    const actionState = {
      start: !running,
      pause: Boolean(rt.can_pause) || running,
      resume: Boolean(rt.can_continue) || paused,
      retry: Boolean(rt.can_retry) || Number(kpi.failures || 0) > 0,
      finalize: Boolean(rt.can_finalize),
      cancel: running || paused,
    };
    $("detailActions").querySelectorAll("button[data-action]").forEach((btn) => {
      const key = btn.dataset.action;
      btn.disabled = actionState[key] === false;
      if (key === "finalize") {
        btn.textContent = rt.promoted ? "已写入终落点" : "写入终落点";
        btn.title = rt.final_base ? `终落点：${rt.final_base}` : "写入 角色肖像 并回写 people";
      }
    });

    const cards = [
      ["发现", kpi.discovered],
      ["下载", kpi.downloaded],
      ["完成", kpi.completed],
      ["失败", kpi.failures],
      ["档案", kpi.profiles],
      ["图片", kpi.images],
      ["待处理", kpi.pending],
      ["复核", kpi.review],
    ];
    $("kpiGrid").innerHTML = cards
      .map(
        ([label, n]) =>
          `<div class="kpi"><div class="n">${n ?? 0}</div><div class="l">${label}</div></div>`
      )
      .join("");

    const lastPromote = (q.meta && q.meta.last_promote) || {};
    const promoteLabel = rt.promoted
      ? `已写入${lastPromote.auto ? "（自动）" : ""} · ${lastPromote.at || ""}`
      : lastPromote.error
        ? `失败：${lastPromote.error}`
        : "未写入";
    const meta = [
      ["ID", q.id],
      ["PID", rt.pid || "—"],
      ["期望状态", q.desired_state || "—"],
      ["模板", q.template_id || q.template_path || "—"],
      ["URL", q.start_url || "—"],
      ["输出目录", q.output_root || "—"],
      ["落盘", rt.output_path || "—"],
      ["终落点", rt.final_base || promoteLabel],
      ["写入状态", promoteLabel],
      ["日志", rt.log_path || "—"],
      ["档位理由", q.speed_tier_reason || "—"],
      ["创建时间", fmtTime(q.created_at)],
      ["更新时间", fmtTime(q.updated_at)],
      ["最近错误", q.last_error || "—"],
    ];
    $("metaGrid").innerHTML = meta
      .map(([k, v]) => `<dt>${escapeHtml(k)}</dt><dd>${escapeHtml(v)}</dd>`)
      .join("");

    const ratio = progressRatio(q);
    $("progressText").textContent = rt.progress_text || `完成 ${kpi.completed ?? 0} / 发现 ${kpi.discovered ?? 0}`;
    $("progressFill").style.width = `${ratio}%`;
    $("progressLegend").innerHTML = `
      <span>进度 ${ratio}%</span>
      <span>失败 ${kpi.failures ?? 0}</span>
      <span>复核 ${kpi.review ?? 0}</span>
    `;
  }

  async function showDetail(id) {
    state.selectedId = id;
    localStorage.setItem("d2i_cloud_selected_queue", id);
    renderQueues();

    const data = await api(`/api/v1/queues/${encodeURIComponent(id)}`);
    if (state.selectedId !== id) return; // another queue was selected while awaiting
    renderDetailData(data.queue || {});
    state.detailLoadedFor = id;

    try {
      const logs = await api(`/api/v1/queues/${encodeURIComponent(id)}/logs?lines=100`);
      if (state.selectedId !== id) return;
      $("logTail").textContent = logs.tail || "（暂无日志）";
    } catch (err) {
      if (state.selectedId !== id) return;
      $("logTail").textContent = `日志读取失败：${err.message}`;
    }
    await loadItems(id, { keepSelection: false });
  }

  function openCreate() {
    $("createMsg").textContent = "";
    $("createMsg").className = "form-msg";
    $("createDialog").showModal();
  }

  function closeCreate() {
    if ($("createDialog").open) $("createDialog").close();
  }

  async function createQueue(ev) {
    ev.preventDefault();
    const msg = $("createMsg");
    msg.textContent = "创建中…";
    msg.className = "form-msg";
    const submitBtn = $("btnSubmitCreate");
    if (submitBtn) submitBtn.disabled = true;
    const opt = $("templateId").selectedOptions[0];
    const payload = {
      template_id: $("templateId").value,
      template_path: opt?.dataset.path || "",
      start_url: $("startUrl").value.trim(),
      name: $("queueName").value.trim(),
      speed_tier: $("speedTier").value,
      speed_tier_reason: $("speedReason").value.trim(),
      start: $("startNow").checked,
      allow_turbo: $("allowTurbo").checked,
    };
    try {
      const data = await api("/api/v1/queues", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      msg.textContent = `已创建 ${data.queue?.id || ""}`;
      msg.className = "form-msg ok";
      toast("队列已创建");
      closeCreate();
      await loadStatus();
      await loadQueues();
      if (data.queue?.id) await showDetail(data.queue.id);
    } catch (err) {
      msg.textContent = err.message;
      msg.className = "form-msg err";
    } finally {
      if (submitBtn) submitBtn.disabled = false;
    }
  }

  async function control(action) {
    if (!state.selectedId) return;
    const queue = state.queues.find((q) => q.id === state.selectedId) || {};
    const queueName = queue.name || queue.id || state.selectedId;
    if (action === "finalize") {
      const ok = window.confirm(
        `确认对「${queueName}」写入终落点？\n\n将把已完成的结果搬入 角色肖像 并回写 people 档案，操作不可撤销。`
      );
      if (!ok) return;
    } else if (action === "cancel") {
      const ok = window.confirm(`确认取消「${queueName}」？\n\n正在运行的抓取会被终止。`);
      if (!ok) return;
    }
    const btn = document.querySelector(`#detailActions [data-action="${action}"]`);
    if (btn) btn.disabled = true;
    const labels = {
      start: "启动",
      pause: "暂停",
      resume: "继续",
      retry: "重试",
      cancel: "取消",
      finalize: "写入终落点",
    };
    try {
      if (action === "finalize") {
        const data = await api(`/api/v1/queues/${encodeURIComponent(state.selectedId)}/finalize`, {
          method: "POST",
          body: JSON.stringify({ dry_run: false, write_people: true }),
        });
        const counts = data?.promote?.counts || {};
        toast(
          `写入终落点完成：promoted ${counts.promoted ?? 0} · people ${counts.people_update ?? counts.people_updated ?? 0}`
        );
      } else {
        await api(`/api/v1/queues/${encodeURIComponent(state.selectedId)}/${action}`, {
          method: "POST",
          body: JSON.stringify({ options: {} }),
        });
        toast(`${labels[action] || action} 成功`);
      }
      await loadStatus();
      await loadQueues();
      await showDetail(state.selectedId);
    } catch (err) {
      toast(`${labels[action] || action} 失败：${err.message}`);
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  function bind() {
    $("btnRefresh")?.addEventListener("click", () => refreshAll(true));
    $("btnOpenCreate")?.addEventListener("click", openCreate);
    $("btnOpenCreate2")?.addEventListener("click", openCreate);
    $("btnCloseCreate")?.addEventListener("click", closeCreate);
    $("btnCancelCreate")?.addEventListener("click", closeCreate);
    $("templateId")?.addEventListener("change", onTemplateChange);
    $("speedTier")?.addEventListener("change", onTierChange);
    $("createForm")?.addEventListener("submit", createQueue);

    $("filterPills")?.addEventListener("click", (ev) => {
      const btn = ev.target.closest("button[data-filter]");
      if (!btn) return;
      state.filter = btn.dataset.filter;
      [...$("filterPills").children].forEach((el) => el.classList.toggle("active", el === btn));
      renderQueues();
    });

    $("queueList")?.addEventListener("click", (ev) => {
      const card = ev.target.closest(".queue-card[data-id]");
      if (!card) return;
      showDetail(card.dataset.id).catch((err) => toast(err.message));
    });

    $("detailActions")?.addEventListener("click", (ev) => {
      const btn = ev.target.closest("button[data-action]");
      if (!btn) return;
      control(btn.dataset.action);
    });

    $("btnReloadLogs")?.addEventListener("click", async () => {
      if (!state.selectedId) return;
      try {
        const logs = await api(`/api/v1/queues/${encodeURIComponent(state.selectedId)}/logs?lines=100`);
        $("logTail").textContent = logs.tail || "（暂无日志）";
      } catch (err) {
        toast(err.message);
      }
    });

    $("btnReloadItems")?.addEventListener("click", () => {
      if (!state.selectedId) return;
      loadItems(state.selectedId).catch((err) => toast(err.message));
    });

    $("itemFilterPills")?.addEventListener("click", (ev) => {
      const btn = ev.target.closest("button[data-item-filter]");
      if (!btn) return;
      state.itemFilter = btn.dataset.itemFilter || "all";
      [...$("itemFilterPills").children].forEach((el) => el.classList.toggle("active", el === btn));
      if (state.selectedId) loadItems(state.selectedId, { keepSelection: false }).catch((err) => toast(err.message));
    });

    $("itemGrid")?.addEventListener("click", (ev) => {
      const card = ev.target.closest(".item-card[data-item-id]");
      if (!card) return;
      selectItem(card.dataset.itemId);
    });

    $("itemGrid")?.addEventListener("dblclick", (ev) => {
      const card = ev.target.closest(".item-card[data-item-id]");
      if (!card) return;
      const item = state.items.find((row) => row.id === card.dataset.itemId);
      if (item) openLightbox(item);
    });

    $("btnOpenLightbox")?.addEventListener("click", () => {
      const item = selectedItem();
      if (item) openLightbox(item);
    });

    $("btnLightboxClose")?.addEventListener("click", () => $("lightboxDialog")?.close());
    $("btnLightboxPrev")?.addEventListener("click", () => shiftLightbox(-1));
    $("btnLightboxNext")?.addEventListener("click", () => shiftLightbox(1));
    $("lightboxDialog")?.addEventListener("click", (ev) => {
      if (ev.target === $("lightboxDialog")) $("lightboxDialog").close();
    });
    document.addEventListener("keydown", (ev) => {
      if (!$("lightboxDialog")?.open) return;
      if (ev.key === "ArrowLeft") shiftLightbox(-1);
      if (ev.key === "ArrowRight") shiftLightbox(1);
      if (ev.key === "Escape") $("lightboxDialog").close();
    });
  }

  function formatConnError(err) {
    if (!err) return "连接失败";
    if (err.code === "cf_access") return "未过 Cloudflare Access，请先登录域名门禁";
    return `连接失败：${err.message || err}`;
  }

  async function refreshAll(showToast = false) {
    try {
      try {
        const health = await api("/api/v1/health", { timeoutMs: 8000 });
        if (!health || health.ok !== true) {
          throw new Error("健康检查响应异常");
        }
        setConn(true, "已连接");
      } catch (probeErr) {
        setConn(false, formatConnError(probeErr));
        throw probeErr;
      }
      await Promise.all([
        loadStatus().catch((err) => {
          console.error(err);
          setConn(false, formatConnError(err));
        }),
        loadTemplates().catch((err) => console.error(err)),
        loadQueues().catch((err) => console.error(err)),
      ]);
      if (showToast) toast("已刷新");
    } catch (err) {
      console.error(err);
      const cur = String($("connText")?.textContent || "");
      if (!cur.includes("失败") && !cur.includes("Access")) {
        setConn(false, formatConnError(err));
      }
    }
  }

  try {
    const bootQueue = new URL(window.location.href).searchParams.get("queue") || "";
    if (bootQueue.trim()) {
      state.selectedId = bootQueue.trim();
      localStorage.setItem("d2i_cloud_selected_queue", state.selectedId);
    }
  } catch {
    /* ignore */
  }
  bind();
  onTierChange();
  refreshAll(false);
  state.timer = setInterval(() => {
    if (state.polling) return; // don't stack ticks behind a slow NAS response
    state.polling = true;
    Promise.allSettled([loadStatus().catch(() => {}), loadQueues().catch(() => {})]).finally(() => {
      state.polling = false;
    });
  }, 5000);
})();
