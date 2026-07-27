(() => {
  const state = {
    jobs: [],
    counts: {},
    status: null,
    filter: "all",
    selectedId: localStorage.getItem("d2i_cloud_selected_vision") || "",
    timer: null,
    busy: false,
    polling: false,
    inventory: null,
    plan: null,
    detailItems: [],
    reasonFilter: "",
    evidenceItemId: "",
  };

  const $ = (id) => document.getElementById(id);
  const { api, escapeHtml, fmtTime, toast, looksLikeAccessGate, explainError, renderReasonChips } = window.D2I;

  function statusLabel(status) {
    const map = {
      queued: "排队中",
      running: "运行中",
      completed: "已完成",
      completed_with_errors: "部分失败",
      failed: "失败",
      cancelled: "已取消",
    };
    return map[status] || status || "未知";
  }

  function setConn(ok, text) {
    const dot = $("connDot");
    const label = $("connText");
    if (dot) dot.className = `dot ${ok ? "ok" : "bad"}`;
    if (label) label.textContent = text;
  }

  function formatConnError(err) {
    if (!err) return "连接失败";
    if (err.code === "cf_access") return "未过 Cloudflare Access，请先登录域名门禁";
    return err.message || String(err);
  }

  function filteredJobs() {
    if (state.filter === "all") return state.jobs;
    return state.jobs.filter((j) => String(j.status || "") === state.filter);
  }

  function progressRatio(job) {
    const total = Math.max(1, Number(job.total || job.item_count || 0));
    const done = Number(job.done_count || job.ok_count || 0);
    return Math.max(0, Math.min(100, Math.round((done / total) * 100)));
  }

  function renderOverview() {
    const c = state.counts || {};
    $("statTotal").textContent = String(c.total || state.jobs.length || 0);
    $("statQueued").textContent = String(c.queued || 0);
    $("statRunning").textContent = String(c.running || 0);
    $("statDone").textContent = `${c.completed || 0} · 部分 ${c.completed_with_errors || 0} · 失败 ${c.failed || 0}`;

    const pump = (state.status && state.status.pump) || {};
    const model = (state.status && state.status.model) || "—";
    const avail = state.status && state.status.available;
    const pumpText = pump.running
      ? `泵运行中 · 已跑 ${pump.ran_count || 0} 批 · 当前 ${pump.last_job_id || "—"}`
      : pump.finished_at
        ? `泵空闲 · 上次结束 ${pump.finished_at} · 共跑 ${pump.ran_count || 0}`
        : `泵空闲 · 模型 ${model}${avail ? "" : "（不可用）"}`;
    if ($("pumpHint")) $("pumpHint").textContent = pumpText;
    // Reflect actual pump state on the trigger buttons so a running drain isn't re-launched.
    const pumpRunning = Boolean(pump.running);
    if ($("btnPump")) $("btnPump").disabled = state.busy || pumpRunning;
    if ($("btnPumpOne")) $("btnPumpOne").disabled = state.busy || pumpRunning;
  }

  function renderJobs() {
    const list = $("jobList");
    const rows = filteredJobs();
    // Skip DOM rewrite when nothing visible changed (kills poll flicker).
    const key = JSON.stringify([
      state.filter,
      state.selectedId,
      rows.map((j) => [j.id, j.status, j.done_count, j.ok_count, j.failed_count, j.total, j.updated_at]),
    ]);
    if (renderJobs._key === key) return;
    renderJobs._key = key;
    if (!state.jobs.length) {
      list.innerHTML = `<div class="empty-state">还没有视觉任务。上方「盘点 → 规划 → 入队」可按省/市拆批创建。</div>`;
      return;
    }
    if (!rows.length) {
      list.innerHTML = `<div class="empty-state">当前筛选下没有任务。</div>`;
      return;
    }
    list.innerHTML = rows
      .map((j) => {
        const active = j.id === state.selectedId ? "active" : "";
        const ratio = progressRatio(j);
        const place = [j.province, j.city].filter(Boolean).join(" / ") || j.batch_key || "—";
        return `<article class="queue-card ${active}" data-id="${escapeHtml(j.id)}">
          <div class="queue-card-top">
            <div>
              <div class="queue-name">${escapeHtml(j.name || j.id)}</div>
              <div class="queue-id">${escapeHtml(j.id)} · ${escapeHtml(place)}</div>
            </div>
            <span class="tag ${escapeHtml(j.status || "idle")}">${escapeHtml(statusLabel(j.status))}</span>
          </div>
          <div class="queue-metrics">
            <div class="metric"><div class="n">${j.ok_count ?? 0}</div><div class="l">成功</div></div>
            <div class="metric"><div class="n ${Number(j.failed_count || 0) > 0 ? "bad" : ""}">${j.failed_count ?? 0}</div><div class="l">失败</div></div>
            <div class="metric"><div class="n">${j.total ?? j.item_count ?? 0}</div><div class="l">总数</div></div>
            <div class="metric"><div class="n">${ratio}%</div><div class="l">进度</div></div>
          </div>
          <div class="queue-foot">
            <span>P${j.priority ?? "—"}</span>
            <span>${escapeHtml(fmtTime(j.updated_at || j.created_at))}</span>
          </div>
        </article>`;
      })
      .join("");
  }

  function clearDetail() {
    $("detailEmpty").hidden = false;
    $("detailBody").hidden = true;
  }

  function itemStatusLabel(st) {
    const map = {
      ok: "成功",
      skipped: "跳过",
      failed: "失败",
      missing: "缺图",
      pending: "待跑",
      unknown: "—",
    };
    return map[st] || st || "—";
  }

  function renderItemTable(items, { filterReason = "" } = {}) {
    const host = $("itemTable");
    const rows = items || [];
    state.detailItems = rows;
    const okN = rows.filter((x) => x.status === "ok" || (x.visual_gender && x.status !== "failed")).length;
    const failN = rows.filter((x) => x.status === "failed" || x.status === "missing").length;
    $("itemsHint").textContent = `${rows.length} 人 · 成功/已识别 ${okN} · 失败 ${failN}`;

    const chipHost = $("reasonChips");
    if (chipHost) {
      renderReasonChips(
        chipHost,
        rows.filter((x) => x.error || x.status === "failed" || x.status === "missing"),
        "error"
      );
      chipHost.querySelectorAll(".reason-chip").forEach((btn) => {
        btn.classList.toggle("active", Boolean(filterReason) && btn.dataset.reason === filterReason);
        btn.onclick = () => {
          const next = state.reasonFilter === btn.dataset.reason ? "" : btn.dataset.reason;
          state.reasonFilter = next;
          renderItemTable(state.detailItems || [], { filterReason: next });
        };
      });
    }

    const visible = filterReason
      ? rows.filter((it) => {
          const exp = explainError(it.error || "");
          return exp.code === filterReason || String(it.error || "").includes(filterReason);
        })
      : rows;

    if (!rows.length) {
      host.innerHTML = `<div class="empty-state">本批无条目快照。</div>`;
      const ev = $("itemEvidence");
      if (ev) {
        ev.hidden = true;
        ev.innerHTML = "";
      }
      return;
    }
    if (!visible.length) {
      host.innerHTML = `<div class="empty-state">当前原因筛选下没有条目。</div>`;
      return;
    }
    host.innerHTML = `
      <table class="vision-table">
        <thead>
          <tr>
            <th>#</th>
            <th>姓名</th>
            <th>状态</th>
            <th>视觉性别</th>
            <th>体型</th>
            <th>发型</th>
            <th>姿态</th>
            <th>人数</th>
            <th>失败原因</th>
            <th>路径</th>
          </tr>
        </thead>
        <tbody>
          ${visible
            .slice(0, 120)
            .map((it, idx) => {
              const path = it.path || it.primary_image_path || it.resolved_path || "—";
              const st = it.status || (it.visual_gender ? "ok" : it.error ? "failed" : "unknown");
              const exp = explainError(it.error || "");
              const errLabel = exp.label || "—";
              const errTitle = [exp.hint, exp.action, exp.raw].filter(Boolean).join(" · ");
              const selected =
                state.evidenceItemId &&
                (it.person_id === state.evidenceItemId || it.name === state.evidenceItemId);
              return `<tr class="vision-row-${escapeHtml(st)}${selected ? " is-selected" : ""}${
                filterReason ? " reason-hit" : ""
              }" data-item-key="${escapeHtml(it.person_id || it.name || String(idx))}">
                <td>${idx + 1}</td>
                <td>${escapeHtml(it.name || "—")}</td>
                <td><span class="tag ${escapeHtml(
                  st === "ok"
                    ? "completed"
                    : st === "failed" || st === "missing"
                      ? "failed"
                      : st === "skipped"
                        ? "queued"
                        : "running"
                )}">${escapeHtml(itemStatusLabel(st))}</span></td>
                <td>${escapeHtml(it.visual_gender || "—")}</td>
                <td>${escapeHtml(it.visual_body_type || "—")}</td>
                <td>${escapeHtml(it.visual_hairstyle || "—")}</td>
                <td>${escapeHtml(it.visual_pose || "—")}</td>
                <td>${it.person_count ?? "—"}</td>
                <td class="path-cell" title="${escapeHtml(errTitle || errLabel)}">${escapeHtml(errLabel)}</td>
                <td class="path-cell" title="${escapeHtml(path)}">${escapeHtml(path)}</td>
              </tr>`;
            })
            .join("")}
        </tbody>
      </table>
      ${visible.length > 120 ? `<div class="muted-hint">仅显示前 120 / ${visible.length}</div>` : ""}
    `;
    host.querySelectorAll("tbody tr[data-item-key]").forEach((tr) => {
      tr.addEventListener("click", () => {
        const key = tr.dataset.itemKey;
        const item = (state.detailItems || []).find(
          (it, i) => String(it.person_id || it.name || i) === key
        );
        state.evidenceItemId = key;
        showItemEvidence(item);
        host.querySelectorAll("tr.is-selected").forEach((el) => el.classList.remove("is-selected"));
        tr.classList.add("is-selected");
      });
    });
  }

  function showItemEvidence(item) {
    const host = $("itemEvidence");
    if (!host) return;
    if (!item) {
      host.hidden = true;
      host.innerHTML = "";
      return;
    }
    const exp = explainError(item.error || "");
    const st = item.status || (item.visual_gender ? "ok" : item.error ? "failed" : "unknown");
    const path = item.path || item.primary_image_path || item.resolved_path || "—";
    const raw = {
      status: st,
      error: item.error || null,
      visual_gender: item.visual_gender || null,
      person_count: item.person_count ?? null,
      path,
      person_id: item.person_id || null,
    };
    host.hidden = false;
    const rawErr = String(item.error || "").trim();
    const showCode =
      exp.code && exp.code !== exp.label && exp.code.length <= 40 && !exp.code.includes("{");
    host.innerHTML = `
      <h4>${escapeHtml(item.name || "未命名")} · ${escapeHtml(itemStatusLabel(st))}</h4>
      <div><strong>判定：</strong>${escapeHtml(exp.label)}${
        showCode ? ` <span class="tag soft">${escapeHtml(exp.code)}</span>` : ""
      }</div>
      <div><strong>说明：</strong>${escapeHtml(exp.hint || "—")}</div>
      <div class="ev-actions"><strong>建议：</strong>${escapeHtml(exp.action || "—")}</div>
      <div class="ev-actions"><strong>路径：</strong>${escapeHtml(path)}</div>
      <div class="ev-actions workflow-actions" id="itemWorkflowActions">
        <button type="button" class="btn sm" data-wf="no_photo" ${item.person_id ? "" : "disabled"}>确认无图</button>
        <button type="button" class="btn sm ghost" data-wf="hold" ${item.person_id ? "" : "disabled"}>暂挂</button>
        <button type="button" class="btn sm ghost" data-wf="unusable" ${item.person_id ? "" : "disabled"}>图不可用</button>
        <button type="button" class="btn sm ghost" data-wf="resume" ${item.person_id ? "" : "disabled"}>恢复</button>
      </div>
      ${
        rawErr
          ? `<details class="ev-raw"><summary>原始错误</summary><pre>${escapeHtml(rawErr)}</pre></details>`
          : ""
      }
      <details class="ev-raw"><summary>条目快照</summary><pre>${escapeHtml(
        JSON.stringify(raw, null, 2)
      )}</pre></details>
    `;
    host.querySelectorAll("[data-wf]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const action = btn.getAttribute("data-wf");
        if (!action || !item.person_id) return;
        markPersonWorkflow(item.person_id, action, item.name || "");
      });
    });
  }

  async function markPersonWorkflow(personId, action, name) {
    if (!personId || state.busy) return;
    const labels = {
      no_photo: "确认无图（停下载+视觉，直到恢复）",
      hold: "暂挂（暂不入 vision/补采）",
      unusable: "图不可用（不入 vision，可再抓）",
      resume: "恢复（重新进入开放工作流）",
      dismiss: "忽略本条收件箱",
    };
    if (action === "dismiss") {
      if (!window.confirm(`忽略收件箱条目\n\n${name || personId}`)) return;
      state.busy = true;
      try {
        await api("/api/v1/ai/vision/recrawl-inbox", {
          method: "PATCH",
          body: JSON.stringify({
            person_id: personId,
            status: "dismissed",
            action: "dismiss",
            reason: "vision-ui:dismiss",
          }),
          timeoutMs: 15000,
        });
        toast(`已忽略 ${name || personId}`);
        await loadRecrawlInbox({ soft: true });
      } catch (err) {
        toast(`忽略失败：${err.message || err}`);
      } finally {
        state.busy = false;
      }
      return;
    }
    if (!window.confirm(`${labels[action] || action}\n\n${name || personId}`)) return;
    state.busy = true;
    try {
      const data = await api("/api/v1/people/mark", {
        method: "POST",
        body: JSON.stringify({
          action,
          person_id: personId,
          reason: `vision-ui:${action}`,
          clear_primary_path: action === "no_photo",
        }),
        timeoutMs: 30000,
      });
      const wf = (data.workflow && data.workflow.label) || action;
      toast(`已标记 ${name || personId} → ${wf}`);
      // P0-1: mark auto-resolves inbox; refresh open list so row disappears.
      if (action === "no_photo" || action === "unusable" || action === "hold") {
        await loadRecrawlInbox({ soft: true });
      }
    } catch (err) {
      toast(`标记失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  async function runSelectedPersons(personIds, { force = false } = {}) {
    const ids = (personIds || []).map((x) => String(x || "").trim()).filter(Boolean);
    if (!ids.length || state.busy) return;
    if (
      !window.confirm(
        `对选中 ${ids.length} 人跑视觉（只跑这些人，不卷全市）\n\n调用 Grok 产生真实计费`
      )
    ) {
      return;
    }
    state.busy = true;
    try {
      const data = await api("/api/v1/ai/vision/run-persons", {
        method: "POST",
        body: JSON.stringify({
          person_ids: ids,
          force: !!force,
          write_people: true,
        }),
        timeoutMs: 600000,
      });
      toast(
        `按人视觉完成：ok ${data.ok_count || 0} / 失败 ${data.failed_count || 0}（请求 ${data.requested || ids.length}）`
      );
      await loadRecrawlInbox({ soft: true });
    } catch (err) {
      toast(`按人视觉失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  async function enqueueSelectedPersons(personIds) {
    const ids = (personIds || []).map((x) => String(x || "").trim()).filter(Boolean);
    if (!ids.length || state.busy) return;
    if (!window.confirm(`入队视觉作业：仅 ${ids.length} 人（不卷全市）`)) return;
    state.busy = true;
    try {
      const data = await api("/api/v1/ai/vision/enqueue", {
        method: "POST",
        body: JSON.stringify({ person_ids: ids, start: false, force: false }),
        timeoutMs: 60000,
      });
      toast(
        `已入队 ${data.created || 0} 批 · total_items=${data.total_items || 0} · 可解析 ${data.resolvable || 0}`
      );
      await refreshAll({ softDetail: true });
    } catch (err) {
      toast(`入队失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  async function batchMarkPersons(personIds, action) {
    const ids = (personIds || []).map((x) => String(x || "").trim()).filter(Boolean);
    if (!ids.length || state.busy) return;
    const label =
      action === "no_photo"
        ? "确认无图"
        : action === "unusable"
          ? "图不可用"
          : action === "hold"
            ? "暂挂"
            : action;
    if (!window.confirm(`批量 ${label}：${ids.length} 人？`)) return;
    state.busy = true;
    try {
      const data = await api("/api/v1/people/mark", {
        method: "POST",
        body: JSON.stringify({
          action,
          person_ids: ids,
          reason: `vision-ui:batch:${action}`,
          clear_primary_path: action === "no_photo",
        }),
        timeoutMs: 60000,
      });
      toast(`批量完成 ok=${data.count_ok || 0} err=${data.count_error || 0}`);
      await loadRecrawlInbox({ soft: true });
    } catch (err) {
      toast(`批量标记失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  async function probePersonSource(personId, name) {
    if (!personId || state.busy) return;
    state.busy = true;
    try {
      const data = await api("/api/v1/people/probe-source", {
        method: "POST",
        body: JSON.stringify({ person_id: personId, timeout: 12 }),
        timeoutMs: 20000,
      });
      const act = data.suggest_action || "?";
      const concl = data.conclusion || "?";
      const bucket = data.source_bucket ? ` · ${data.source_bucket}` : "";
      toast(
        `探测 ${name || personId}：${concl} → 建议 ${act}${bucket}\n${data.reason || ""}`.slice(0, 240)
      );
      if (act === "no_photo" && window.confirm(`探测建议确认无图\n\n${name || personId}\n${data.reason || ""}\n\n现在标记？`)) {
        state.busy = false;
        await markPersonWorkflow(personId, "no_photo", name);
        return;
      }
    } catch (err) {
      toast(`探测失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  async function showDetail(id, { soft = false } = {}) {
    if (!id) return;
    state.selectedId = id;
    localStorage.setItem("d2i_cloud_selected_vision", id);
    renderJobs();
    try {
      const data = await api(`/api/v1/ai/vision/jobs/${encodeURIComponent(id)}`, {
        timeoutMs: soft ? 12000 : 20000,
      });
      if (state.selectedId !== id) return; // selection changed while awaiting
      const j = data.job || {};
      $("detailEmpty").hidden = true;
      $("detailBody").hidden = false;
      $("detailKicker").textContent = j.batch_key || "VISION JOB";
      $("detailTitle").textContent = j.name || j.id;
      $("detailStatus").className = `tag ${j.status || "idle"}`;
      $("detailStatus").textContent = statusLabel(j.status);
      $("detailBatch").textContent = [j.province, j.city].filter(Boolean).join(" · ") || j.batch_key || "—";

      const st = String(j.status || "");
      // Backend run_vision_job only accepts a queued job (or one already running);
      // failed/cancelled must go through 重跑 (requeue), not Run — so gate accordingly.
      const canRun = st === "queued";
      const canCancel = st === "queued" || st === "running";
      const canRequeue =
        (Number(j.failed_count || 0) > 0 || Number(j.missing_count || 0) > 0) &&
        (st === "completed" || st === "completed_with_errors" || st === "failed");
      $("detailActions").querySelectorAll("button[data-action]").forEach((btn) => {
        if (btn.dataset.action === "run") btn.disabled = state.busy || !canRun;
        if (btn.dataset.action === "cancel") btn.disabled = state.busy || !canCancel;
        if (
          btn.dataset.action === "requeue" ||
          btn.dataset.action === "requeue-all" ||
          btn.dataset.action === "requeue-recrawl"
        ) {
          btn.disabled = state.busy || !canRequeue;
        }
      });

      const cards = [
        ["总数", j.total],
        ["成功", j.ok_count],
        ["失败", j.failed_count],
        ["跳过", j.skipped_count],
        ["缺失", j.missing_count],
        ["完成计数", j.done_count],
        ["优先级", j.priority],
        ["写 people", j.write_people ? "是" : "否"],
      ];
      $("kpiGrid").innerHTML = cards
        .map(
          ([label, n]) =>
            `<div class="kpi"><div class="n">${n ?? 0}</div><div class="l">${label}</div></div>`
        )
        .join("");

      // Auto-route follow-up summary (written when the job finished).
      const fu = (j.result && j.result.followup) || null;
      let fuHost = $("followupBanner");
      if (!fuHost) {
        fuHost = document.createElement("div");
        fuHost.id = "followupBanner";
        fuHost.className = "followup-banner";
        const kpi = $("kpiGrid");
        if (kpi && kpi.parentNode) kpi.parentNode.insertBefore(fuHost, kpi.nextSibling);
      }
      if (fu && (fu.retry_created || fu.held_items || fu.skipped)) {
        fuHost.hidden = false;
        const bits = [];
        if (fu.retry_created) bits.push(`已自动建重跑 ${fu.retry_created} 批 / ${fu.retry_items || 0} 人`);
        if (fu.held_items) bits.push(`建议重抓 ${fu.held_items} 人`);
        if (fu.skipped) bits.push(`跳过：${fu.skipped}`);
        const heldTop = Object.entries(fu.held_reason_counts || {})
          .sort((a, b) => b[1] - a[1])
          .slice(0, 3)
          .map(([k, n]) => `${k}×${n}`)
          .join(" · ");
        fuHost.innerHTML = `<strong>跑完分流</strong> · ${escapeHtml(bits.join(" · "))}${
          heldTop ? ` <span class="muted-hint">（${escapeHtml(heldTop)}）</span>` : ""
        }${
          fu.inbox_path
            ? ` <button type="button" class="btn ghost sm" id="btnOpenInboxFromDetail">打开收件箱</button>`
            : ""
        }`;
        $("btnOpenInboxFromDetail")?.addEventListener("click", () => loadRecrawlInbox());
      } else {
        fuHost.hidden = true;
        fuHost.innerHTML = "";
      }

      const meta = [
        ["ID", j.id],
        ["状态", statusLabel(j.status)],
        ["批次键", j.batch_key || "—"],
        ["省 / 市", [j.province, j.city].filter(Boolean).join(" / ") || "—"],
        ["强制重跑", j.force ? "是" : "否"],
        ["演练 dry_run", j.dry_run ? "是（仍真实调用 Grok 计费，仅不写库）" : "否"],
        ["创建", fmtTime(j.created_at)],
        ["开始", fmtTime(j.started_at)],
        ["结束", fmtTime(j.finished_at)],
        ["更新", fmtTime(j.updated_at)],
        ["错误", j.error || "—"],
      ];
      $("metaGrid").innerHTML = meta
        .map(([k, v]) => `<dt>${escapeHtml(k)}</dt><dd title="${escapeHtml(v)}">${escapeHtml(v)}</dd>`)
        .join("");

      const ratio = progressRatio(j);
      $("progressText").textContent = `完成 ${j.done_count ?? 0} / 总数 ${j.total ?? 0} · 成功 ${j.ok_count ?? 0} · 失败 ${j.failed_count ?? 0}`;
      $("progressFill").style.width = `${ratio}%`;
      $("progressLegend").innerHTML = `
        <span>进度 ${ratio}%</span>
        <span>跳过 ${j.skipped_count ?? 0}</span>
        <span>缺失 ${j.missing_count ?? 0}</span>
      `;

      const result = j.result || {};
      const compact = {
        at: result.at,
        model: result.model,
        prompt_version: result.prompt_version,
        counts: result.counts,
        errors: result.errors,
      };
      $("resultBox").textContent = Object.keys(result).length
        ? JSON.stringify(compact, null, 2)
        : st === "running"
          ? "运行中，完成后写入 result…"
          : "（尚无结果）";

      state.reasonFilter = "";
      state.evidenceItemId = "";
      const ev = $("itemEvidence");
      if (ev) {
        ev.hidden = true;
        ev.innerHTML = "";
      }
      renderItemTable(j.items || []);
      if (!(j.items || []).length && j.error) {
        const chipHost = $("reasonChips");
        if (chipHost) renderReasonChips(chipHost, [{ error: j.error }], "error");
      }
    } catch (err) {
      if (!soft) toast(err.message || String(err));
    }
  }

  async function loadStatus() {
    const data = await api("/api/v1/ai/vision/status", { timeoutMs: 15000 });
    state.status = data;
    if (data.jobs) state.counts = data.jobs;
    setConn(true, data.available ? `已连接 · ${data.model || "vision"}` : "已连接 · 视觉不可用");
    renderOverview();
    return data;
  }

  async function loadJobs() {
    const data = await api("/api/v1/ai/vision/jobs?limit=200", { timeoutMs: 20000 });
    // Prefer active first then recent: API returns updated_at DESC already.
    state.jobs = data.jobs || [];
    if (data.counts) state.counts = data.counts;
    // stable-ish sort: running, queued, then others by priority/created
    const rank = { running: 0, queued: 1, failed: 2, completed_with_errors: 3, completed: 4, cancelled: 5 };
    state.jobs.sort((a, b) => {
      const ra = rank[a.status] ?? 9;
      const rb = rank[b.status] ?? 9;
      if (ra !== rb) return ra - rb;
      if (ra <= 1) return Number(a.priority || 0) - Number(b.priority || 0);
      return Number(b.updated_at || 0) - Number(a.updated_at || 0);
    });
    renderJobs();
    renderOverview();
    if (state.selectedId) {
      const still = state.jobs.some((j) => j.id === state.selectedId);
      if (still) {
        // list row highlight only; detail refreshed on click / soft poll
      } else {
        state.selectedId = "";
        localStorage.removeItem("d2i_cloud_selected_vision");
        clearDetail();
      }
    }
  }

  async function refreshAll({ showToast = false, softDetail = true } = {}) {
    try {
      try {
        const health = await api("/api/v1/health", { timeoutMs: 8000 });
        if (!health || health.ok !== true) throw new Error("健康检查响应异常");
      } catch (probeErr) {
        setConn(false, formatConnError(probeErr));
        throw probeErr;
      }
      await Promise.all([loadStatus().catch((e) => { throw e; }), loadJobs()]);
      if (state.selectedId && softDetail) {
        await showDetail(state.selectedId, { soft: true });
      }
      if (showToast) toast("已刷新");
    } catch (err) {
      console.error(err);
      setConn(false, formatConnError(err));
    }
  }

  async function startPump({ maxClaim = 0, background = true } = {}) {
    if (state.busy) return;
    state.busy = true;
    try {
      const body = {
        max_running: 1,
        max_claim: maxClaim,
        background,
      };
      const data = await api("/api/v1/ai/vision/pump", {
        method: "POST",
        body: JSON.stringify(body),
        timeoutMs: background ? 30000 : 600000,
      });
      if (data.already_running) toast("泵已在运行");
      else if (background) toast(`后台泵已启动${data.requeued_stale ? `（复位 ${data.requeued_stale}）` : ""}`);
      else toast(`已跑 ${data.ran_count || 0} 批`);
      await refreshAll({ softDetail: true });
    } catch (err) {
      toast(`泵失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  async function runJob(id) {
    if (!id || state.busy) return;
    const job = state.jobs.find((j) => j.id === id) || {};
    const name = job.name || id;
    const n = job.total ?? job.item_count ?? "";
    if (
      !window.confirm(
        `确认运行「${name}」？\n\n将调用 Grok 视觉模型识别${n ? ` ${n} 张` : ""}图片，产生真实计费。`
      )
    )
      return;
    state.busy = true;
    try {
      toast("本批运行中…");
      const data = await api(`/api/v1/ai/vision/jobs/${encodeURIComponent(id)}/run`, {
        method: "POST",
        body: "{}",
        timeoutMs: 600000,
      });
      const counts = data?.report?.counts || data?.job || {};
      toast(
        data.ok
          ? `本批完成 · ok ${counts.classified ?? data.job?.ok_count ?? "—"}`
          : `本批结束：${data.error || "见详情"}`
      );
      await refreshAll({ softDetail: false });
      await showDetail(id);
    } catch (err) {
      toast(`运行失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  async function cancelJob(id) {
    if (!id || state.busy) return;
    state.busy = true;
    try {
      await api(`/api/v1/ai/vision/jobs/${encodeURIComponent(id)}/cancel`, {
        method: "POST",
        body: "{}",
      });
      toast("已取消");
      await refreshAll({ softDetail: false });
      await showDetail(id);
    } catch (err) {
      toast(`取消失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  function opsFilters() {
    return {
      province: ($("fltProvince")?.value || "").trim(),
      city: ($("fltCity")?.value || "").trim(),
      unit_like: ($("fltUnit")?.value || "").trim(),
      batch_size: Math.max(5, Math.min(200, Number($("fltBatchSize")?.value || 40))),
      max_batches: Math.max(0, Math.min(500, Number($("fltMaxBatches")?.value || 0))),
      limit: Math.max(100, Math.min(20000, Number($("fltLimit")?.value || 20000))),
      write_people: Boolean($("fltWritePeople")?.checked),
      force: Boolean($("fltForce")?.checked),
      start: Boolean($("fltStart")?.checked),
    };
  }

  function setOpsMsg(text, isErr = false) {
    const el = $("opsMsg");
    if (!el) return;
    el.textContent = text || "";
    el.style.color = isErr ? "var(--danger)" : "";
  }

  function renderInventorySummary(inv, plan) {
    const total = inv?.total ?? "—";
    const resolvable = inv?.resolvable ?? plan?.resolvable ?? "—";
    const missing = inv?.missing_on_disk ?? plan?.missing_on_disk ?? "—";
    const batches = plan?.batch_count ?? "—";
    if ($("invTotal")) $("invTotal").textContent = String(total);
    if ($("invResolvable")) $("invResolvable").textContent = String(resolvable);
    if ($("invMissing")) $("invMissing").textContent = String(missing);
    if ($("invBatches")) $("invBatches").textContent = String(batches);

    const byCity = (plan && plan.by_city) || (inv && inv.by_city) || {};
    const host = $("invByCity");
    if (host) {
      const entries = Object.entries(byCity);
      if (!entries.length) {
        host.innerHTML = `<div class="muted-hint">当前筛选下没有未视觉条目。</div>`;
      } else {
        host.innerHTML = entries
          .slice(0, 20)
          .map(([key, n]) => {
            const parts = String(key).split("/");
            const prov = parts[0] || "";
            const city = parts.slice(1).join("/") || "";
            return `<div class="vision-city-row">
              <button type="button" data-prov="${escapeHtml(prov)}" data-city="${escapeHtml(city)}" title="填入筛选">
                ${escapeHtml(key)}
              </button>
              <span class="n">${n}</span>
            </div>`;
          })
          .join("");
      }
    }

    const box = $("planBox");
    if (box) {
      if (plan && plan.batches) {
        const slim = (plan.batches || []).slice(0, 12).map((b) => ({
          batch_id: b.batch_id,
          batch_key: b.batch_key,
          count: b.count,
          offset: b.offset,
        }));
        box.textContent = JSON.stringify(
          {
            batch_size: plan.batch_size,
            batch_count: plan.batch_count,
            total_items: plan.total_items,
            resolvable: plan.resolvable,
            missing_on_disk: plan.missing_on_disk,
            preview: slim,
            more: Math.max(0, (plan.batch_count || 0) - slim.length),
          },
          null,
          2
        );
      } else if (inv) {
        box.textContent = JSON.stringify(
          {
            total: inv.total,
            resolvable: inv.resolvable,
            missing_on_disk: inv.missing_on_disk,
            filters: inv.filters,
            sample: (inv.items || []).slice(0, 5).map((x) => ({
              name: x.name,
              person_id: x.person_id,
              on_disk: x.on_disk,
              path: x.resolved_path || x.primary_image_path,
            })),
          },
          null,
          2
        );
      }
    }
  }

  async function runInventory() {
    if (state.busy) return;
    state.busy = true;
    setOpsMsg("盘点中…");
    try {
      const f = opsFilters();
      const qs = new URLSearchParams({
        province: f.province,
        city: f.city,
        unit_like: f.unit_like,
        limit: String(f.limit),
        only_resolvable: "false",
      });
      const data = await api(`/api/v1/ai/vision/inventory?${qs}`, { timeoutMs: 120000 });
      state.inventory = data;
      renderInventorySummary(data, state.plan);
      setOpsMsg(`盘点完成：候选 ${data.total || 0} · 可解析 ${data.resolvable || 0} · 缺盘 ${data.missing_on_disk || 0}`);
      toast("盘点完成");
    } catch (err) {
      setOpsMsg(err.message || String(err), true);
      toast(`盘点失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  async function runPlan() {
    if (state.busy) return;
    state.busy = true;
    setOpsMsg("规划中…");
    try {
      const f = opsFilters();
      const qs = new URLSearchParams({
        batch_size: String(f.batch_size),
        province: f.province,
        city: f.city,
        unit_like: f.unit_like,
        limit: String(f.limit),
      });
      const data = await api(`/api/v1/ai/vision/plan?${qs}`, { timeoutMs: 120000 });
      state.plan = data;
      if (!state.inventory) {
        state.inventory = {
          total: data.total_items,
          resolvable: data.resolvable,
          missing_on_disk: data.missing_on_disk,
          by_city: data.by_city,
        };
      }
      renderInventorySummary(state.inventory, data);
      setOpsMsg(
        `规划完成：${data.batch_count || 0} 批 · 每批 ${data.batch_size || f.batch_size} · 可解析 ${data.resolvable || 0}`
      );
      toast(`规划 ${data.batch_count || 0} 批`);
    } catch (err) {
      setOpsMsg(err.message || String(err), true);
      toast(`规划失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  async function runEnqueue() {
    if (state.busy) return;
    const f = opsFilters();
    const planned = state.plan?.batch_count;
    const hint =
      planned != null
        ? `将入队约 ${f.max_batches > 0 ? Math.min(planned, f.max_batches) : planned} 批（每批 ${f.batch_size} 人）。确认？`
        : `将按省/市拆批入队（每批 ${f.batch_size}，上限 ${f.max_batches || "不限"}）。确认？`;
    if (!window.confirm(hint)) return;
    state.busy = true;
    setOpsMsg("入队中…");
    try {
      const body = {
        batch_size: f.batch_size,
        province: f.province,
        city: f.city,
        unit_like: f.unit_like,
        limit: f.limit,
        max_batches: f.max_batches,
        force: f.force,
        write_people: f.write_people,
        dry_run: false,
        start: f.start,
        max_running: 1,
      };
      const data = await api("/api/v1/ai/vision/enqueue", {
        method: "POST",
        body: JSON.stringify(body),
        timeoutMs: 180000,
      });
      if (data.counts) state.counts = data.counts;
      const created = data.created || 0;
      setOpsMsg(
        `已入队 ${created} 批 · 规划 ${data.planned_batches || "—"} · 可解析 ${data.inventory?.resolvable ?? "—"}` +
          (data.pump?.started || data.pump?.already_running ? " · 泵已启动" : "")
      );
      toast(`已入队 ${created} 个视觉批次`);
      await refreshAll({ softDetail: true });
      if (created && data.enqueued?.[0]?.id) {
        await showDetail(data.enqueued[0].id);
      }
    } catch (err) {
      setOpsMsg(err.message || String(err), true);
      toast(`入队失败：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  async function loadRecrawlInbox(opts = {}) {
    try {
      const data = await api(
        "/api/v1/ai/vision/recrawl-inbox?limit=100&status=open",
        { timeoutMs: 20000 }
      );
      state.recrawlInbox = data;
      const total = Number(data.total || 0);
      const statusCounts = data.status_counts || {};
      const top = Object.entries(data.reason_counts || {})
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([k, n]) => `${k}×${n}`)
        .join(" · ");
      if (!opts.soft) {
        toast(
          total
            ? `建议重抓收件箱 ${data.day || ""}：open ${total}（resolved ${statusCounts.resolved || 0} / dismissed ${statusCounts.dismissed || 0}）${top ? ` · ${top}` : ""}`
            : `建议重抓收件箱 ${data.day || ""} open 为空（resolved ${statusCounts.resolved || 0}）`
        );
      }
      let host = $("recrawlInboxPanel");
      if (!host) {
        host = document.createElement("section");
        host.id = "recrawlInboxPanel";
        host.className = "card recrawl-inbox-panel";
        const main = document.querySelector(".main-column") || document.body;
        main.appendChild(host);
      }
      if (!total) {
        host.innerHTML = `<div class="section-bar tight"><h2>建议重抓收件箱</h2><span class="muted-hint">${escapeHtml(
          data.day || ""
        )} · open 空 · resolved ${statusCounts.resolved || 0}</span></div><div class="empty-state">暂无 open 条目（已处理的不会再出现在默认视图）。</div>`;
        return;
      }
      const rows = (data.items || []).slice(0, 80);
      host.innerHTML = `
        <div class="section-bar tight">
          <h2>建议重抓收件箱</h2>
          <span class="muted-hint">${escapeHtml(data.day || "")} · open ${total} · 默认只看 open</span>
          <button type="button" class="btn sm" id="btnInboxRunSelected">跑选中视觉</button>
          <button type="button" class="btn sm ghost" id="btnInboxEnqueueSelected">入队选中</button>
          <button type="button" class="btn sm ghost" id="btnInboxBatchNoPhoto">批量确认无图</button>
          <button type="button" class="btn ghost sm" id="btnCloseInbox">收起</button>
        </div>
        <div class="muted-hint" style="margin:0 12px 8px">${escapeHtml(top)}</div>
        <div class="table-wrap">
          <table class="vision-table">
            <thead><tr><th></th><th>姓名</th><th>原因</th><th>路径</th><th>来源批</th><th>时间</th><th>工作流</th></tr></thead>
            <tbody>
              ${rows
                .map((it) => {
                  const exp = explainError(it.error_code || it.last_error || "");
                  const pid = it.person_id || "";
                  return `<tr data-person-id="${escapeHtml(pid)}">
                    <td><input type="checkbox" class="inbox-pick" data-pid="${escapeHtml(pid)}" ${pid ? "" : "disabled"} /></td>
                    <td>${escapeHtml(it.name || "—")}</td>
                    <td title="${escapeHtml(exp.hint || it.last_error || "")}">${escapeHtml(
                      exp.label || it.error_code || "—"
                    )}</td>
                    <td class="path-cell" title="${escapeHtml(it.path || "")}">${escapeHtml(
                      it.path || "—"
                    )}</td>
                    <td class="path-cell">${escapeHtml(it.source_job || "—")}</td>
                    <td>${escapeHtml(it.at || "—")}</td>
                    <td class="wf-cell">
                      <button type="button" class="btn sm" data-wf="no_photo" data-pid="${escapeHtml(
                        pid
                      )}" data-name="${escapeHtml(it.name || "")}" ${pid ? "" : "disabled"}>确认无图</button>
                      <button type="button" class="btn sm ghost" data-wf="unusable" data-pid="${escapeHtml(
                        pid
                      )}" data-name="${escapeHtml(it.name || "")}" ${pid ? "" : "disabled"}>图不可用</button>
                      <button type="button" class="btn sm ghost" data-wf="hold" data-pid="${escapeHtml(
                        pid
                      )}" data-name="${escapeHtml(it.name || "")}" ${pid ? "" : "disabled"}>暂挂</button>
                      <button type="button" class="btn sm ghost" data-wf="dismiss" data-pid="${escapeHtml(
                        pid
                      )}" data-name="${escapeHtml(it.name || "")}" ${pid ? "" : "disabled"}>忽略</button>
                      <button type="button" class="btn sm" data-run-one="${escapeHtml(pid)}" data-name="${escapeHtml(
                        it.name || ""
                      )}" ${pid ? "" : "disabled"}>跑视觉</button>
                      <button type="button" class="btn sm ghost" data-probe="${escapeHtml(pid)}" data-name="${escapeHtml(
                        it.name || ""
                      )}" ${pid ? "" : "disabled"}>探测源站</button>
                    </td>
                  </tr>`;
                })
                .join("")}
            </tbody>
          </table>
        </div>`;
      $("btnCloseInbox")?.addEventListener("click", () => {
        host.innerHTML = "";
      });
      const pickedIds = () =>
        Array.from(host.querySelectorAll(".inbox-pick:checked"))
          .map((el) => el.getAttribute("data-pid") || "")
          .filter(Boolean);
      $("btnInboxRunSelected")?.addEventListener("click", () => {
        runSelectedPersons(pickedIds());
      });
      $("btnInboxEnqueueSelected")?.addEventListener("click", () => {
        enqueueSelectedPersons(pickedIds());
      });
      $("btnInboxBatchNoPhoto")?.addEventListener("click", () => {
        batchMarkPersons(pickedIds(), "no_photo");
      });
      host.querySelectorAll("[data-wf]").forEach((btn) => {
        btn.addEventListener("click", () => {
          markPersonWorkflow(
            btn.getAttribute("data-pid"),
            btn.getAttribute("data-wf"),
            btn.getAttribute("data-name") || ""
          );
        });
      });
      host.querySelectorAll("[data-run-one]").forEach((btn) => {
        btn.addEventListener("click", () => {
          const pid = btn.getAttribute("data-run-one");
          if (pid) runSelectedPersons([pid]);
        });
      });
      host.querySelectorAll("[data-probe]").forEach((btn) => {
        btn.addEventListener("click", () => {
          const pid = btn.getAttribute("data-probe");
          if (pid) probePersonSource(pid, btn.getAttribute("data-name") || "");
        });
      });
      if (!opts.soft) host.scrollIntoView({ behavior: "smooth", block: "start" });
    } catch (err) {
      toast(`读取建议重抓收件箱失败：${err.message || err}`);
    }
  }

  async function loadMarkedPeople() {
    try {
      const data = await api("/api/v1/people/workflow/marked?limit=100", { timeoutMs: 20000 });
      const total = Number(data.total || 0);
      toast(total ? `工作流标记 ${total} 人` : "当前没有确认无图/暂挂/图不可用标记");
      let host = $("recrawlInboxPanel");
      if (!host) {
        host = document.createElement("section");
        host.id = "recrawlInboxPanel";
        host.className = "card recrawl-inbox-panel";
        const main = document.querySelector(".main-column") || document.body;
        main.appendChild(host);
      }
      const rows = data.items || [];
      host.innerHTML = `
        <div class="section-bar tight">
          <h2>工作流标记</h2>
          <span class="muted-hint">${escapeHtml(data.workflow_filter || "any")} · ${total} 人</span>
          <button type="button" class="btn ghost sm" id="btnCloseMarked">收起</button>
        </div>
        <div class="table-wrap">
          <table class="vision-table">
            <thead><tr><th>姓名</th><th>状态</th><th>单位</th><th>省/市</th><th>操作</th></tr></thead>
            <tbody>
              ${
                rows.length
                  ? rows
                      .map((it) => {
                        const wf = (it.workflow && it.workflow.label) || "—";
                        const pid = it.person_id || "";
                        return `<tr>
                          <td>${escapeHtml(it.name || "—")}</td>
                          <td>${escapeHtml(wf)}</td>
                          <td>${escapeHtml(it.unit_name || "—")}</td>
                          <td>${escapeHtml([it.province, it.city].filter(Boolean).join(" / ") || "—")}</td>
                          <td><button type="button" class="btn sm" data-wf="resume" data-pid="${escapeHtml(
                            pid
                          )}" data-name="${escapeHtml(it.name || "")}">恢复</button></td>
                        </tr>`;
                      })
                      .join("")
                  : `<tr><td colspan="5">无标记</td></tr>`
              }
            </tbody>
          </table>
        </div>`;
      $("btnCloseMarked")?.addEventListener("click", () => {
        host.innerHTML = "";
      });
      host.querySelectorAll("[data-wf]").forEach((btn) => {
        btn.addEventListener("click", () => {
          markPersonWorkflow(
            btn.getAttribute("data-pid"),
            btn.getAttribute("data-wf"),
            btn.getAttribute("data-name") || ""
          ).then(() => loadMarkedPeople());
        });
      });
      host.scrollIntoView({ behavior: "smooth", block: "start" });
    } catch (err) {
      toast(`读取工作流标记失败：${err.message || err}`);
    }
  }

  async function requeueFailed(jobId, { policy = "retryable", includeReview = false } = {}) {
    if (state.busy) return;
    state.busy = true;
    try {
      const body = {
        job_id: jobId || "",
        batch_size: 20,
        start: policy !== "must_recrawl",
        max_running: 1,
        policy,
        include_review: Boolean(includeReview),
      };
      const data = await api("/api/v1/ai/vision/requeue-failed", {
        method: "POST",
        body: JSON.stringify(body),
        timeoutMs: 120000,
      });
      const held = Number(data.held_items || 0);
      const requeued = Number(data.requeued_items || 0);
      const created = Number(data.created_jobs || 0);
      const already = Number(data.skipped_already_visioned || 0);
      const heldTop = Object.entries(data.held_reason_counts || {})
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([k, n]) => `${k}×${n}`)
        .join(" · ");
      const retryTop = Object.entries(data.retry_reason_counts || {})
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([k, n]) => `${k}×${n}`)
        .join(" · ");
      let msg = data.hint || "";
      if (!msg) {
        if (policy === "must_recrawl") {
          msg = `需重抓 ${held} 人（未建视觉队列）`;
        } else {
          msg = `可恢复重跑：新建 ${created} 批 / ${requeued} 人`;
          if (held) msg += ` · 扣下需重抓 ${held}`;
        }
      } else {
        msg = `${msg}（入队 ${created} 批/${requeued} 人` + (held ? `，扣下 ${held}` : "") + "）";
      }
      if (already) msg += ` · 已识别跳过 ${already}`;
      if (retryTop) msg += ` · 重跑:${retryTop}`;
      if (heldTop) msg += ` · 扣下:${heldTop}`;
      toast(msg);
      state.lastRequeue = data;
      await refreshAll({ softDetail: true });
    } catch (err) {
      toast(`分流重跑出错：${err.message || err}`);
    } finally {
      state.busy = false;
    }
  }

  function bind() {
    $("btnRefresh")?.addEventListener("click", () => refreshAll({ showToast: true }));
    $("btnPump")?.addEventListener("click", () => {
      if (
        window.confirm(
          "启动后台泵？\n\n将依次跑完所有排队中的视觉批次，逐批调用 Grok，产生真实计费。"
        )
      ) {
        startPump({ maxClaim: 0, background: true });
      }
    });
    $("btnPumpOne")?.addEventListener("click", () => {
      if (
        window.confirm(
          "前台跑一个批次？\n\n将调用 Grok 识别一批图片，产生真实计费；期间请勿关闭页面。"
        )
      ) {
        startPump({ maxClaim: 1, background: false });
      }
    });
    $("btnInventory")?.addEventListener("click", () => runInventory());
    $("btnInventory2")?.addEventListener("click", () => runInventory());
    $("btnPlan")?.addEventListener("click", () => runPlan());
    $("btnEnqueue")?.addEventListener("click", () => runEnqueue());
    $("btnRequeueAll")?.addEventListener("click", () => {
      if (
        window.confirm(
          "只重跑「可恢复」失败（限流/超时/上游 5xx 等），并启动泵。\n\n" +
            "图太小 / 图片损坏 / 缺图 会扣下，不进视觉队列（避免白烧 Grok）。\n" +
            "需要强制全量重跑请用详情里的「强制全量」。"
        )
      ) {
        requeueFailed("", { policy: "retryable" });
      }
    });
    $("btnRequeueRecrawl")?.addEventListener("click", () => {
      if (
        window.confirm(
          "只汇总「需重抓」条目（图太小/损坏/缺图），不建视觉队列、不计费。\n\n" +
            "结果会 toast 数量；详细 sample 在控制台 state.lastRequeue.held_sample。"
        )
      ) {
        requeueFailed("", { policy: "must_recrawl" });
      }
    });
    $("btnRecrawlInbox")?.addEventListener("click", () => loadRecrawlInbox());
    $("btnMarkedPeople")?.addEventListener("click", () => loadMarkedPeople());

    $("invByCity")?.addEventListener("click", (ev) => {
      const btn = ev.target.closest("button[data-prov]");
      if (!btn) return;
      if ($("fltProvince")) $("fltProvince").value = btn.dataset.prov || "";
      if ($("fltCity")) $("fltCity").value = btn.dataset.city || "";
      toast(`已填入 ${btn.dataset.prov || ""} / ${btn.dataset.city || ""}`);
    });

    $("filterPills")?.addEventListener("click", (ev) => {
      const btn = ev.target.closest("button[data-filter]");
      if (!btn) return;
      state.filter = btn.dataset.filter || "all";
      [...$("filterPills").children].forEach((el) => el.classList.toggle("active", el === btn));
      renderJobs();
    });

    $("jobList")?.addEventListener("click", (ev) => {
      const card = ev.target.closest(".queue-card[data-id]");
      if (!card) return;
      showDetail(card.dataset.id).catch((err) => toast(err.message || String(err)));
    });

    $("detailActions")?.addEventListener("click", (ev) => {
      const btn = ev.target.closest("button[data-action]");
      if (!btn || !state.selectedId) return;
      if (btn.dataset.action === "run") runJob(state.selectedId);
      if (btn.dataset.action === "cancel") cancelJob(state.selectedId);
      if (btn.dataset.action === "requeue") {
        if (
          window.confirm(
            "本批：只把「可恢复」失败（限流/超时等）重新入队并启动泵。\n\n" +
              "图太小/损坏/缺图会扣下，不进视觉（避免白烧 Grok）。"
          )
        ) {
          requeueFailed(state.selectedId, { policy: "retryable" });
        }
      }
      if (btn.dataset.action === "requeue-all") {
        if (
          window.confirm(
            "本批：强制把全部失败/缺图重新入视觉队并启动泵？\n\n" +
              "图太小/损坏的也会再跑一遍（大概率再失败，仍会产生调用）。"
          )
        ) {
          requeueFailed(state.selectedId, { policy: "all" });
        }
      }
      if (btn.dataset.action === "requeue-recrawl") {
        if (
          window.confirm(
            "本批：只汇总「需重抓」条目，不建视觉队列、不计费。"
          )
        ) {
          requeueFailed(state.selectedId, { policy: "must_recrawl" });
        }
      }
    });
  }

  try {
    const boot = new URL(window.location.href).searchParams.get("job") || "";
    if (boot.trim()) {
      state.selectedId = boot.trim();
      localStorage.setItem("d2i_cloud_selected_vision", state.selectedId);
    }
  } catch {
    /* ignore */
  }

  bind();
  refreshAll({ softDetail: true }).then(() => {
    if (state.selectedId) showDetail(state.selectedId).catch(() => {});
  });
  state.timer = setInterval(() => {
    if (state.polling) return; // don't stack ticks behind a slow NAS response
    state.polling = true;
    const jobsDone = [loadStatus().catch(() => {}), loadJobs().catch(() => {})];
    if (state.selectedId) jobsDone.push(showDetail(state.selectedId, { soft: true }).catch(() => {}));
    Promise.allSettled(jobsDone).finally(() => {
      state.polling = false;
    });
  }, 8000);
})();
