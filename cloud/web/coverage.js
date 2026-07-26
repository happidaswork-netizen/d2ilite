(() => {
  const GEO_CHINA = "/static/geo/china_100000_full.json";
  const GEO_SD = "/static/geo/shandong_370000_full.json";

  const state = {
    tree: null,
    geo: null,
    view: "tree",
    selectedId: "",
    selectedNode: null,
    mapLevel: "china",
    mapFocusProvince: "",
    mapMetric: "strength",
    chart: null,
    chinaGeo: null,
    sdGeo: null,
    pendingAction: "",
  };

  const $ = (id) => document.getElementById(id);

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
        headers: { ...jsonHeaders(), ...(window.D2I ? D2I.authHeaders() : {}), ...(extraHeaders || {}) },
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
      if (res.status === 401 && window.D2I && !D2I.getToken()) {
        if (D2I.promptToken()) return api(path, options);
      }
      if (!res.ok) {
        const detail = body?.detail || body?.error || res.statusText || "request failed";
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

  function toast(msg, isError = false) {
    const el = $("toast");
    el.hidden = false;
    el.textContent = msg;
    el.style.background = isError ? "#b42318" : "#101828";
    clearTimeout(el._t);
    el._t = setTimeout(() => {
      el.hidden = true;
    }, 3600);
  }

  function heatClass(heat) {
    const h = String(heat || "").toLowerCase();
    if (h === "green") return "heat-green";
    if (h === "yellow") return "heat-yellow";
    if (h === "red") return "heat-red";
    return "heat-gray";
  }

  function actionLabel(code) {
    const map = {
      scout: "先侦察入口",
      g_photo: "补缺图",
      g_crawl: "新开抓取",
      g_index: "只入库索引",
      reconcile: "先对账",
      maintain: "维持观察",
      ban: "禁止再下载",
      unban: "允许再下载",
      ok: "已较完整",
      empty: "尚无人物",
      g_reconcile: "需对账",
    };
    const key = String(code || "").trim();
    return map[key] || key || "—";
  }

  function heatLabel(heat) {
    const map = {
      green: "够厚且较完整",
      yellow: "偏薄或未收口",
      red: "缺图压力大",
      gray: "暂无数据",
    };
    return map[String(heat || "").toLowerCase()] || String(heat || "—");
  }

  function metricMeta(key) {
    const defs = (state.geo && state.geo.metrics) || {};
    const fallback = {
      strength: { label: "战役厚度", unit: "分", higher: "更好", desc: "有主图规模×完成度" },
      missing: { label: "缺图压力", unit: "分", higher: "更糟", desc: "缺主图人数+比例" },
      rate: { label: "有主图率", unit: "%", higher: "更好", desc: "仅完成度" },
      people: { label: "登记规模", unit: "人", higher: "更多", desc: "登记人数" },
    };
    return defs[key] || fallback[key] || { label: key, unit: "", higher: "", desc: "" };
  }

  function metricValue(item, key = state.mapMetric) {
    const m = item?.metrics || {};
    if (key === "missing") return Number(m.missing ?? item.missing_pressure ?? 0);
    if (key === "rate") return Number(m.rate ?? item.rate_pct ?? ((item.path_rate || 0) * 100));
    if (key === "people") return Number(m.people ?? item.people_n ?? 0);
    return Number(m.strength ?? item.strength ?? item.value ?? 0);
  }

  function metricColors(key = state.mapMetric) {
    // missing pressure: high = bad → green→red reversed visually
    if (key === "missing") return ["#12b76a", "#f79009", "#f04438"];
    if (key === "people") return ["#e0f2fe", "#53b1fd", "#155eef"];
    return ["#f04438", "#f79009", "#12b76a"];
  }

  function metricAxisText(key = state.mapMetric) {
    const meta = metricMeta(key);
    if (key === "missing") return ["压力高", "压力低"];
    if (key === "people") return ["人多", "人少"];
    if (key === "rate") return ["完成度高", "完成度低"];
    return ["更厚", "更薄"];
  }

  function formatConnError(err) {
    if (!err) return "连接失败";
    if (err.code === "cf_access") return "未过 Cloudflare Access，请先登录域名门禁";
    return err.message || String(err);
  }

  function setConn(ok, text) {
    $("connDot").className = `dot ${ok ? "ok" : "bad"}`;
    $("connText").textContent = text;
  }

  function renderTreeNode(node, depth = 0) {
    const kids = node.children || [];
    const hasKids = kids.length > 0;
    const id = node.node_id || "";
    const open = depth < 1 || (state.selectedId && id && state.selectedId.startsWith(id.slice(0, 4)));
    return `
      <div class="ctree-node" data-id="${escapeHtml(id)}" style="--d:${depth}">
        <div class="ctree-row ${state.selectedId === id ? "selected" : ""}" data-select="${escapeHtml(id)}">
          ${
            hasKids
              ? `<button type="button" class="ctree-toggle" data-toggle="${escapeHtml(id)}">${open ? "▾" : "▸"}</button>`
              : `<span class="ctree-spacer"></span>`
          }
          <span class="heat-dot ${heatClass(node.heat)}"></span>
          <span class="ctree-name">${escapeHtml(node.name || "—")}</span>
          <span class="ctree-meta">${Number(node.people_n || 0)}人 · 缺主图 ${Number(node.no_path_n || 0)}</span>
          <span class="tag soft ctree-action">${escapeHtml(actionLabel(node.next_action || node.gap_type || ""))}</span>
        </div>
        ${
          hasKids
            ? `<div class="ctree-children" data-children-of="${escapeHtml(id)}" ${open ? "" : "hidden"}>
                ${kids.map((c) => renderTreeNode(c, depth + 1)).join("")}
              </div>`
            : ""
        }
      </div>
    `;
  }

  function renderTree() {
    const panel = $("treePanel");
    const roots = state.tree?.tree || [];
    if (!roots.length) {
      panel.innerHTML = `<div class="empty-state">无覆盖数据。请设置 D2I_PEOPLE_DB 或放入 data/coverage 快照。</div>`;
      return;
    }
    panel.innerHTML = `<div class="ctree">${roots.map((n) => renderTreeNode(n)).join("")}</div>`;
    panel.querySelectorAll("[data-select]").forEach((el) => {
      el.addEventListener("click", (e) => {
        if (e.target.closest("[data-toggle]")) return;
        selectNode(el.getAttribute("data-select"));
      });
    });
    panel.querySelectorAll("[data-toggle]").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const id = btn.getAttribute("data-toggle");
        const box = panel.querySelector(`[data-children-of="${CSS.escape(id)}"]`);
        if (!box) return;
        const open = box.hasAttribute("hidden");
        if (open) box.removeAttribute("hidden");
        else box.setAttribute("hidden", "");
        btn.textContent = open ? "▾" : "▸";
      });
    });
  }

  function findProvinceNode(province) {
    const roots = state.tree?.tree || [];
    const target = normalizeRegionName(province);
    for (const d of roots) {
      for (const p of d.children || []) {
        const name = p.name || p.province || "";
        if (name === province || normalizeRegionName(name) === target) return p;
      }
    }
    return null;
  }

  function findCityNode(province, city) {
    const roots = state.tree?.tree || [];
    const pTarget = normalizeRegionName(province);
    const cTarget = normalizeRegionName(city);
    for (const d of roots) {
      for (const p of d.children || []) {
        const pn = p.name || p.province || "";
        if (pn !== province && normalizeRegionName(pn) !== pTarget) continue;
        for (const c of p.children || []) {
          const cn = c.name || c.city || "";
          if (cn === city || normalizeRegionName(cn) === cTarget) return c;
        }
      }
    }
    return null;
  }

  function normalizeRegionName(name) {
    return String(name || "")
      .replace(/(特别行政区|维吾尔自治区|壮族自治区|回族自治区|自治区|省|市)$/g, "")
      .trim();
  }

  function mapSeriesNames(items, geoNames) {
    const byNorm = new Map();
    for (const it of items || []) {
      const raw = it.name || "";
      byNorm.set(normalizeRegionName(raw), it);
      byNorm.set(raw, it);
    }
    return (geoNames || []).map((gName) => {
      const hit =
        byNorm.get(gName) ||
        byNorm.get(normalizeRegionName(gName)) ||
        null;
      if (!hit || Number(hit.people_n || 0) <= 0) {
        // null value → ECharts keeps default gray areaColor (not deep red)
        return {
          name: gName,
          value: null,
          people_n: 0,
          with_path_n: 0,
          no_path_n: 0,
          strength: 0,
          missing_pressure: 0,
          rate_pct: 0,
          heat: "gray",
          metrics: { strength: 0, missing: 0, rate: 0, people: 0 },
        };
      }
      const metrics = hit.metrics || {
        strength: hit.strength ?? hit.value ?? 0,
        missing: hit.missing_pressure ?? 0,
        rate: hit.rate_pct ?? ((hit.path_rate || 0) * 100),
        people: hit.people_n ?? 0,
      };
      return {
        name: gName,
        value: metricValue({ ...hit, metrics }),
        people_n: Number(hit.people_n || 0),
        with_path_n: Number(hit.with_path_n || 0),
        no_path_n: Number(hit.no_path_n || 0),
        strength: Number(metrics.strength || 0),
        missing_pressure: Number(metrics.missing || 0),
        rate_pct: Number(metrics.rate || 0),
        heat: hit.heat || "gray",
        metrics,
      };
    });
  }

  function heatListHtml(cities) {
    if (!cities.length) return `<div class="empty-state">无区域聚合</div>`;
    const key = state.mapMetric || "strength";
    const sorted = [...cities].sort((a, b) => {
      if (key === "missing") return Number(b.no_path_n || 0) - Number(a.no_path_n || 0);
      if (key === "rate") return Number(b.path_rate || 0) - Number(a.path_rate || 0);
      if (key === "people") return Number(b.people_n || 0) - Number(a.people_n || 0);
      return Number(b.strength || 0) - Number(a.strength || 0);
    });
    const maxBar = Math.max(
      ...sorted.map((c) => {
        if (key === "missing") return Number(c.no_path_n || 0);
        if (key === "rate") return Number(c.path_rate || 0) * 100;
        if (key === "people") return Number(c.people_n || 0);
        return Number(c.strength || 0);
      }),
      1
    );
    return `
      <div class="map-legend">
        <span class="heat-dot heat-green"></span>够厚且较完整
        <span class="heat-dot heat-yellow"></span>偏薄或未收口
        <span class="heat-dot heat-red"></span>缺图压力大
        <span class="heat-dot heat-gray"></span>暂无数据
        <span class="muted-hint">列表按当前指标排序 · 点色点=综合状态，不是纯完成率</span>
      </div>
      <div class="heat-list">
        ${sorted
          .slice(0, 80)
          .map((c) => {
            let barRaw = Number(c.strength || 0);
            let num = `厚度 ${Number(c.strength || 0).toFixed(0)} · 有主图${Number(c.with_path_n || 0)} · 缺${Number(c.no_path_n || 0)}`;
            if (key === "missing") {
              barRaw = Number(c.no_path_n || 0);
              num = `缺主图 ${barRaw} · 登记${Number(c.people_n || 0)}`;
            } else if (key === "rate") {
              barRaw = Number(c.path_rate || 0) * 100;
              num = `有主图率 ${barRaw.toFixed(0)}% · 有主图${Number(c.with_path_n || 0)}`;
            } else if (key === "people") {
              barRaw = Number(c.people_n || 0);
              num = `登记 ${barRaw} · 有主图${Number(c.with_path_n || 0)}`;
            }
            const w = Math.max(6, Math.round((barRaw / maxBar) * 100));
            const label = `${c.province} · ${c.city}`;
            return `
              <button type="button" class="heat-row" data-city="${escapeHtml(c.city)}" data-province="${escapeHtml(c.province)}">
                <span class="heat-dot ${heatClass(c.heat)}"></span>
                <span class="heat-label">${escapeHtml(label)}</span>
                <span class="heat-bar"><i style="width:${w}%"></i></span>
                <span class="heat-num">${escapeHtml(num)}</span>
              </button>
            `;
          })
          .join("")}
      </div>
    `;
  }

  function ensureMapShell() {
    const panel = $("mapPanel");
    if (panel.querySelector("#mapChart")) {
      syncMetricPills();
      return;
    }
    panel.innerHTML = `
      <div class="map-toolbar">
        <button type="button" class="btn" id="btnMapChina">全国</button>
        <button type="button" class="btn" id="btnMapShandong">山东省</button>
        <div class="metric-pills" id="metricPills">
          <button type="button" class="pill" data-metric="strength">战役厚度</button>
          <button type="button" class="pill" data-metric="missing">缺图压力</button>
          <button type="button" class="pill" data-metric="rate">有主图率</button>
          <button type="button" class="pill" data-metric="people">登记规模</button>
        </div>
        <span class="muted-hint" id="mapFocusHint">默认看厚度，不是纯完成率</span>
      </div>
      <p class="muted-hint map-metric-desc" id="mapMetricDesc"></p>
      <div id="mapChart" class="map-chart"></div>
      <div id="mapFallback" class="map-fallback"></div>
    `;
    $("btnMapChina").addEventListener("click", () => {
      state.mapLevel = "china";
      state.mapFocusProvince = "";
      paintMap();
    });
    $("btnMapShandong").addEventListener("click", () => {
      state.mapLevel = "shandong";
      state.mapFocusProvince = "山东省";
      paintMap();
    });
    panel.querySelectorAll("#metricPills [data-metric]").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.mapMetric = btn.getAttribute("data-metric") || "strength";
        paintMap();
      });
    });
    syncMetricPills();
  }

  function syncMetricPills() {
    const pills = document.querySelectorAll("#metricPills [data-metric]");
    pills.forEach((p) => {
      p.classList.toggle("active", p.getAttribute("data-metric") === state.mapMetric);
    });
    const desc = $("mapMetricDesc");
    if (desc) {
      const meta = metricMeta(state.mapMetric);
      desc.textContent = `${meta.label}：${meta.desc}${meta.higher ? `（${meta.higher}更深/更靠谱）` : ""}`;
    }
  }

  async function loadGeoAssets() {
    if (!state.chinaGeo) {
      const res = await fetch(GEO_CHINA);
      if (!res.ok) throw new Error("china geojson load failed");
      state.chinaGeo = await res.json();
    }
    if (!state.sdGeo) {
      const res = await fetch(GEO_SD);
      if (!res.ok) throw new Error("shandong geojson load failed");
      state.sdGeo = await res.json();
    }
  }

  function disposeChart() {
    if (state.chart) {
      try {
        state.chart.dispose();
      } catch {
        /* ignore */
      }
      state.chart = null;
    }
  }

  function bindHeatFallback(root) {
    root.querySelectorAll(".heat-row").forEach((btn) => {
      btn.addEventListener("click", () => {
        const province = btn.getAttribute("data-province");
        const city = btn.getAttribute("data-city");
        const hit = findCityNode(province, city);
        if (hit) selectNode(hit.node_id);
        else toast(`未在树中定位：${province} ${city}`, true);
      });
    });
  }

  function setMapStatus(msg) {
    const el = $("mapChart");
    if (!el) return;
    disposeChart();
    el.innerHTML = `<div class="empty-state">${escapeHtml(msg)}</div>`;
  }

  function paintMap() {
    ensureMapShell();
    const panel = $("mapPanel");
    if (panel.hasAttribute("hidden")) return;
    syncMetricPills();

    const cities = state.geo?.cities || [];
    $("mapFallback").innerHTML = heatListHtml(cities);
    bindHeatFallback($("mapFallback"));

    if (typeof echarts === "undefined") {
      setMapStatus("ECharts 本地库未加载，下方是列表热力。请确认 /static/vendor/echarts.min.js");
      return;
    }

    const useSd = state.mapLevel === "shandong" || normalizeRegionName(state.mapFocusProvince) === "山东";
    const geo = useSd ? state.sdGeo : state.chinaGeo;
    if (!geo) {
      setMapStatus("地图 GeoJSON 加载中…");
      return;
    }

    const mapName = useSd ? "coverage_shandong" : "coverage_china";
    try {
      echarts.registerMap(mapName, geo);
    } catch (err) {
      setMapStatus(`registerMap 失败：${err.message || err}`);
      return;
    }
    const geoNames = (geo.features || []).map((f) => (f.properties || {}).name).filter(Boolean);

    let seriesData = [];
    if (useSd) {
      const citySeries = (state.geo?.city_series_by_province || {})["山东省"] || [];
      const alt = (state.geo?.city_series_by_province || {})["山东"] || [];
      seriesData = mapSeriesNames(citySeries.length ? citySeries : alt, geoNames);
      $("mapFocusHint").textContent = "山东省 · 点击市定位覆盖树节点";
    } else {
      seriesData = mapSeriesNames(state.geo?.province_series || [], geoNames);
      $("mapFocusHint").textContent = "全国 · 点击省进入市热力；山东省有完整市级底图";
    }

    const metricKey = state.mapMetric || "strength";
    const meta = metricMeta(metricKey);
    const values = seriesData
      .map((d) => d.value)
      .filter((v) => v != null && !Number.isNaN(Number(v)))
      .map(Number);
    const vmax = values.length ? Math.max(...values) : metricKey === "people" ? 10 : 1;
    // people uses data max; others stay 0–100 so thin campaigns don't look full-green
    const visualMax = metricKey === "people" ? Math.max(vmax, 10) : 100;

    const host = $("mapChart");
    disposeChart();
    host.innerHTML = "";
    const w = Math.max(host.clientWidth || host.offsetWidth || 0, panel.clientWidth || 0, 320);
    host.style.width = "100%";
    host.style.height = "480px";
    if (w < 40) {
      setMapStatus("地图容器宽度为 0，请切换到「地图热力」标签后刷新");
      return;
    }

    try {
      state.chart = echarts.init(host, null, { renderer: "canvas", width: w, height: 480 });
      state.chart.setOption(
        {
          backgroundColor: "#f8fafc",
          tooltip: {
            trigger: "item",
            formatter: (p) => {
              const d = p.data || {};
              const title = meta.label || metricKey;
              return (
                `${p.name}<br/>` +
                `${title}：${Number(d.value || 0).toFixed(metricKey === "people" ? 0 : 1)}${meta.unit || ""}<br/>` +
                `有主图 ${d.with_path_n || 0} · 缺主图 ${d.no_path_n || 0} · 登记 ${d.people_n || 0}<br/>` +
                `完成率 ${Number(d.rate_pct || 0).toFixed(0)}% · 厚度 ${Number(d.strength || 0).toFixed(0)}`
              );
            },
          },
          visualMap: {
            min: 0,
            max: visualMax,
            left: 12,
            bottom: 12,
            text: metricAxisText(metricKey),
            inRange: {
              color: metricColors(metricKey),
            },
            calculable: true,
          },
          series: [
            {
              name: meta.label || metricKey,
              type: "map",
              map: mapName,
              roam: true,
              scaleLimit: { min: 0.8, max: 8 },
              itemStyle: {
                areaColor: "#e4e7ec",
                borderColor: "#98a2b3",
              },
              emphasis: {
                label: { show: true },
                itemStyle: { areaColor: "#c7d7fe" },
              },
              data: seriesData,
            },
          ],
        },
        true
      );
    } catch (err) {
      setMapStatus(`地图渲染失败：${err.message || err}`);
      return;
    }

    state.chart.off("click");
    state.chart.on("click", (params) => {
      const name = params.name || "";
      if (!name) return;
      if (!useSd) {
        if (normalizeRegionName(name) === "山东") {
          state.mapLevel = "shandong";
          state.mapFocusProvince = "山东省";
          paintMap();
          return;
        }
        const hit = findProvinceNode(name);
        if (hit) selectNode(hit.node_id);
        else toast(`树中暂无省级节点：${name}`, true);
        return;
      }
      const hit = findCityNode("山东省", name) || findCityNode("山东", name);
      if (hit) selectNode(hit.node_id);
      else toast(`树中暂无市级节点：${name}`, true);
    });

    requestAnimationFrame(() => {
      if (state.chart) state.chart.resize();
    });
  }

  async function renderMap() {
    ensureMapShell();
    try {
      await loadGeoAssets();
      // wait until #mapPanel is visible and laid out
      await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
      paintMap();
    } catch (err) {
      ensureMapShell();
      setMapStatus(`地图加载失败：${err.message || err}，使用下方列表热力`);
      const cities = state.geo?.cities || [];
      $("mapFallback").innerHTML = heatListHtml(cities);
      bindHeatFallback($("mapFallback"));
    }
  }

  function hideEnqueueForm() {
    state.pendingAction = "";
    $("enqueueForm").hidden = true;
  }

  function showEnqueueForm(act) {
    state.pendingAction = act;
    $("enqueueForm").hidden = false;
    $("enqAction").value = `${actionLabel(act)}（${act}）`;
    $("enqUrl").value = "";
    $("enqTemplate").value = "";
    $("enqTier").value = "safe";
    $("enqStart").checked = false;
    $("enqNotes").value = "";
    const needsUrl = act === "g_photo" || act === "g_crawl";
    $("enqUrl").parentElement.style.opacity = needsUrl ? "1" : "0.55";
    $("enqTemplate").parentElement.style.opacity = needsUrl ? "1" : "0.55";
  }

  async function selectNode(nodeId) {
    if (!nodeId) return;
    state.selectedId = nodeId;
    hideEnqueueForm();
    try {
      const detail = await api(`/api/v1/coverage/nodes/${encodeURIComponent(nodeId)}`);
      state.selectedNode = detail;
      $("detailEmpty").hidden = true;
      $("detailBody").hidden = false;
      const n = detail.node || {};
      $("detailKicker").textContent = {
        unit: "单位",
        city: "市",
        province: "省",
        domain: "领域",
      }[n.kind] || (n.kind || "节点");
      $("detailTitle").textContent = n.name || n.unit_name || nodeId;
      $("detailHeat").textContent = heatLabel(n.heat) || n.gap_type_label || "—";
      $("detailHeat").className = `tag ${heatClass(n.heat)}`;
      $("detailAction").textContent = n.next_action_label || actionLabel(n.next_action) || "—";
      $("detailKpis").innerHTML = [
        ["登记人数", n.people_n],
        ["有主图", n.with_image_n ?? n.with_path_n],
        ["缺主图", n.missing_image_n ?? n.no_path_n],
        ["战役厚度", n.strength != null ? Number(n.strength).toFixed(0) : "—"],
        ["有主图率", n.image_rate != null || n.path_rate != null ? `${Math.round(Number(n.image_rate ?? n.path_rate) * 100)}%` : "—"],
        ["禁止再下载", n.ban_redownload ? "是" : "否"],
      ]
        .map(
          ([k, v]) => `
        <article class="stat-card compact">
          <div class="stat-label">${escapeHtml(k)}</div>
          <div class="stat-value sm">${escapeHtml(v)}</div>
        </article>`
        )
        .join("");

      const glossary = detail.glossary || {};
      const statusLine = n.status_summary
        ? `<p class="status-summary">${escapeHtml(n.status_summary)}</p>`
        : "";
      const glossLine = glossary.image_path
        ? `<p class="muted-hint glossary-hint">${escapeHtml(glossary.image_path)}</p>`
        : `<p class="muted-hint glossary-hint">「主图」= 人物库里的图片路径字段。有主图表示库已挂上图；缺主图表示库里还没路径（盘上未必没有）。</p>`;

      const actions = detail.actions || [];
      $("detailActions").innerHTML =
        statusLine +
        glossLine +
        actions
          .map((a) => {
            const disabled = a.enabled === false ? "disabled" : "";
            const note = a.note ? `<span class="action-note">${escapeHtml(a.note)}</span>` : "";
            return `<button type="button" class="btn action-btn ${a.id === "scout" ? "primary" : ""}" data-act="${escapeHtml(a.id)}" ${disabled} title="${escapeHtml(a.note || "")}">
              <span class="action-label">${escapeHtml(a.label)}</span>
              ${note}
            </button>`;
          })
          .join("");
      $("actionNote").textContent =
        "用法：先点「先侦察入口」看方案 → 再选一个动作 → 确认后才会建任务。补缺图/新开抓取需要填写网页入口；只入库索引、先对账、禁止再下载不会直接开下载。";
      $("detailActions").querySelectorAll("[data-act]").forEach((btn) => {
        btn.addEventListener("click", () => runAction(btn.getAttribute("data-act"), nodeId));
      });
      $("nodeJson").textContent = JSON.stringify(n, null, 2);
      $("proposalBox").textContent = "尚未侦察";
      if (state.view === "tree") renderTree();
    } catch (err) {
      toast(err.message || String(err), true);
    }
  }

  async function runAction(act, nodeId) {
    if (act === "scout") {
      try {
        const prop = await api(`/api/v1/coverage/nodes/${encodeURIComponent(nodeId)}/scout`, {
          method: "POST",
          body: "{}",
        });
        $("proposalBox").textContent = JSON.stringify(prop.proposal || prop, null, 2);
        toast("已生成侦察提案（未入队）");
      } catch (err) {
        toast(err.message || String(err), true);
      }
      return;
    }
    showEnqueueForm(act);
  }

  async function confirmEnqueue() {
    const nodeId = state.selectedId;
    const act = state.pendingAction || $("enqAction").value;
    if (!nodeId || !act) {
      toast("未选择节点或动作", true);
      return;
    }
    if (!window.confirm(`确认对节点执行「${act}」？\n不会跳过人确认；默认不自动 start。`)) {
      return;
    }
    const body = {
      action: act,
      confirm: true,
      start_url: ($("enqUrl").value || "").trim(),
      template_id: ($("enqTemplate").value || "").trim(),
      speed_tier: $("enqTier").value || "safe",
      start: Boolean($("enqStart").checked),
      notes: ($("enqNotes").value || "").trim(),
    };
    try {
      const res = await api(`/api/v1/coverage/nodes/${encodeURIComponent(nodeId)}/enqueue`, {
        method: "POST",
        body: JSON.stringify(body),
      });
      $("proposalBox").textContent = JSON.stringify(res, null, 2);
      toast(res.message || "已处理");
      hideEnqueueForm();
      if (act === "ban" || act === "unban") await refresh();
      else if (state.selectedId) await selectNode(state.selectedId);
    } catch (err) {
      toast(err.message || String(err), true);
    }
  }

  function setView(view) {
    state.view = view;
    document.querySelectorAll("#viewTabs .pill").forEach((p) => {
      p.classList.toggle("active", p.getAttribute("data-view") === view);
    });
    $("treePanel").hidden = view !== "tree";
    $("mapPanel").hidden = view !== "map";
    $("leftTitle").textContent = view === "tree" ? "覆盖树" : "地图热力";
    $("leftHint").textContent =
      view === "tree" ? "领域 → 省 → 市 → 单位" : "ECharts 省/市热力 · 列表备援";
    if (view === "map") {
      // unhide first, then paint after layout so echarts gets non-zero width
      renderMap()
        .then(() => {
          if (state.chart) state.chart.resize();
        })
        .catch((err) => toast(err.message || String(err), true));
    } else {
      disposeChart();
    }
  }

  function onWindowResize() {
    if (state.view === "map" && state.chart) state.chart.resize();
  }

  async function refresh() {
    try {
      const [tree, geo] = await Promise.all([
        api("/api/v1/coverage/tree"),
        api("/api/v1/coverage/geo-summary"),
      ]);
      state.tree = tree;
      state.geo = geo;
      $("statPeople").textContent = String(tree.people_total ?? 0);
      $("statUnits").textContent = String(tree.unit_count ?? 0);
      $("statSource").textContent = String(tree.source || "—").slice(0, 42);
      setConn(true, "已连接");
      renderTree();
      if (state.view === "map") await renderMap();
      if (state.selectedId) selectNode(state.selectedId);
    } catch (err) {
      setConn(false, formatConnError(err));
      toast(formatConnError(err), true);
    }
  }

  function bind() {
    $("btnRefresh").addEventListener("click", refresh);
    document.querySelectorAll("#viewTabs .pill").forEach((p) => {
      p.addEventListener("click", () => setView(p.getAttribute("data-view")));
    });
    $("btnConfirmEnqueue").addEventListener("click", confirmEnqueue);
    $("btnCancelEnqueue").addEventListener("click", hideEnqueueForm);
    window.addEventListener("resize", onWindowResize);
  }

  bind();
  setView("tree");
  refresh();
})();
