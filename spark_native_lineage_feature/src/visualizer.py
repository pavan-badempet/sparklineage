"""
Interactive HTML/SVG Execution Plan & Lineage Visualizer.
Embeds cleanly in Jupyter, Databricks, or exports to standalone HTML files.
Professional, minimal, zero-dependency design.
"""

import json
from typing import Dict, Any, List, Optional
from .models import PlanGraph, NodeType, ColumnLineage, LintReport, Severity


class Visualizer:
    """
    Renders an interactive, standalone HTML/SVG directed graph of the plan.
    Zero external dependencies, zero CDN reliance.
    """

    NODE_COLORS = {
        NodeType.SOURCE: "#10b981",     # Emerald
        NodeType.PROJECT: "#06b6d4",    # Cyan
        NodeType.FILTER: "#f59e0b",     # Amber
        NodeType.JOIN: "#3b82f6",       # Blue
        NodeType.AGGREGATE: "#8b5cf6",  # Purple
        NodeType.WINDOW: "#ec4899",     # Pink
        NodeType.SORT: "#64748b",       # Slate
        NodeType.EXCHANGE: "#ef4444",   # Rose
        NodeType.UNION: "#14b8a6",      # Teal
        NodeType.LIMIT: "#6b7280",      # Gray
        NodeType.OTHER: "#475569",      # Dark Slate
    }

    RISK_COLORS = {
        "CRITICAL": "#ef4444",
        "WARNING": "#f59e0b",
        "INFO": "#3b82f6",
        "SAFE": "#10b981",
    }

    @classmethod
    def generate_html(
        cls,
        graph: PlanGraph,
        column_lineage: Optional[Dict[str, ColumnLineage]] = None,
        lint_report: Optional[LintReport] = None,
        title: str = "Spark Execution & Lineage DAG"
    ) -> str:
        """
        Generates a clean, minimal, self-contained HTML document with an interactive SVG graph.
        """
        node_issues: Dict[str, List[Dict[str, str]]] = {}
        if lint_report:
            for issue in lint_report.issues:
                if issue.node_id:
                    if issue.node_id not in node_issues:
                        node_issues[issue.node_id] = []
                    node_issues[issue.node_id].append({
                        "rule_id": issue.rule_id,
                        "title": issue.title,
                        "severity": issue.severity.value,
                        "problem": issue.message,
                        "recommendation": issue.recommendation
                    })

        nodes_data = []
        edges_data = []

        for nid, node in graph.nodes.items():
            color = cls.NODE_COLORS.get(node.node_type, "#475569")
            issues = node_issues.get(nid, [])

            highest_sev = "SAFE"
            if any(i["severity"] == "CRITICAL" for i in issues):
                highest_sev = "CRITICAL"
            elif any(i["severity"] == "WARNING" for i in issues):
                highest_sev = "WARNING"
            elif any(i["severity"] == "INFO" for i in issues):
                highest_sev = "INFO"
            elif node.node_type == NodeType.EXCHANGE:
                highest_sev = "WARNING"

            nodes_data.append({
                "id": node.node_id,
                "label": node.name,
                "type": node.node_type.value,
                "color": color,
                "riskSeverity": highest_sev,
                "details": node.details[:350],
                "columns": node.output_columns,
                "expressions": node.expressions[:12],
                "issues": issues,
            })
            for cid in node.children_ids:
                edges_data.append({
                    "from": cid,
                    "to": node.node_id,
                })

        col_lineage_data = {}
        if column_lineage:
            for cname, cl in column_lineage.items():
                col_lineage_data[cname] = {
                    "sources": cl.sources,
                    "transformations": cl.transformations,
                    "upstream_columns": list(cl.upstream_columns),
                }

        payload_json = json.dumps({
            "nodes": nodes_data,
            "edges": edges_data,
            "columnLineage": col_lineage_data,
            "title": title
        })

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
  :root {{
    --bg: #090d16;
    --card-bg: #111827;
    --border: #1f2937;
    --border-subtle: #162032;
    --text: #f9fafb;
    --muted: #9ca3af;
    --primary: #38bdf8;
    --danger: #ef4444;
    --warning: #f59e0b;
    --success: #10b981;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: var(--bg);
    color: var(--text);
    font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    -webkit-font-smoothing: antialiased;
  }}
  header {{
    padding: 10px 20px;
    background: var(--card-bg);
    border-bottom: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    z-index: 20;
  }}
  .logo-group {{
    display: flex;
    align-items: center;
    gap: 10px;
  }}
  .logo-title {{
    font-size: 14px;
    font-weight: 600;
    color: var(--text);
    display: flex;
    align-items: center;
    gap: 8px;
    letter-spacing: -0.2px;
  }}
  .badge {{
    font-size: 10px;
    padding: 2px 7px;
    border-radius: 4px;
    background: #1f2937;
    color: var(--muted);
    font-weight: 500;
    border: 1px solid #374151;
  }}
  .header-actions {{
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .search-input {{
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    padding: 5px 10px;
    font-size: 12px;
    width: 210px;
    outline: none;
    transition: border-color 0.15s ease;
  }}
  .search-input:focus {{
    border-color: var(--primary);
  }}
  .main-container {{
    display: flex;
    flex: 1;
    overflow: hidden;
    position: relative;
  }}
  #graph-canvas {{
    flex: 1;
    height: 100%;
    cursor: grab;
    background-image: radial-gradient(#1f2937 1px, transparent 1px);
    background-size: 20px 20px;
  }}
  #graph-canvas:active {{ cursor: grabbing; }}
  .sidebar {{
    width: 360px;
    background: var(--card-bg);
    border-left: 1px solid var(--border);
    padding: 16px;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 14px;
    z-index: 10;
  }}
  .card {{
    background: #0d121f;
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 12px 14px;
  }}
  .card h3 {{
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    color: var(--muted);
    margin-bottom: 8px;
    font-weight: 600;
  }}
  .col-pill {{
    display: inline-block;
    padding: 2px 7px;
    border-radius: 4px;
    font-size: 11px;
    background: #1f2937;
    border: 1px solid #374151;
    margin: 2px;
    cursor: pointer;
    transition: all 0.15s ease;
  }}
  .col-pill:hover {{
    border-color: var(--primary);
    color: var(--primary);
  }}
  .col-pill.active {{
    background: #0369a1;
    color: #ffffff;
    border-color: var(--primary);
    font-weight: 600;
  }}
  .node-tag {{
    display: inline-block;
    font-size: 9px;
    padding: 2px 6px;
    border-radius: 4px;
    font-weight: 600;
    text-transform: uppercase;
    color: #fff;
  }}
  .detail-line {{ font-size: 11px; color: var(--muted); word-break: break-all; margin-top: 6px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
  .hazard-banner {{
    background: rgba(239, 68, 68, 0.08);
    border: 1px solid rgba(239, 68, 68, 0.3);
    border-radius: 6px;
    padding: 10px;
    margin-top: 8px;
    font-size: 11px;
  }}
  .hazard-title {{ color: #f87171; font-weight: 600; margin-bottom: 3px; }}
  .hazard-fix {{ color: #94a3b8; margin-top: 4px; }}
  .legend {{ display: flex; flex-wrap: wrap; gap: 8px; font-size: 11px; }}
  .legend-item {{ display: flex; align-items: center; gap: 5px; color: var(--muted); }}
  .legend-dot {{ width: 7px; height: 7px; border-radius: 50%; }}
  .controls {{
    position: absolute;
    bottom: 16px;
    left: 16px;
    display: flex;
    gap: 4px;
    z-index: 10;
    background: var(--card-bg);
    padding: 3px;
    border-radius: 6px;
    border: 1px solid var(--border);
  }}
  .btn {{
    background: transparent;
    color: var(--muted);
    border: 1px solid transparent;
    padding: 5px 10px;
    border-radius: 4px;
    font-size: 11px;
    cursor: pointer;
    font-weight: 500;
    transition: all 0.15s ease;
  }}
  .btn:hover {{
    background: #1f2937;
    color: var(--text);
  }}
  .btn.active {{
    background: #1f2937;
    color: var(--primary);
    border-color: #374151;
  }}
  .edge-highlight {{
    stroke: var(--primary) !important;
    stroke-width: 2.5 !important;
    stroke-dasharray: 5 3;
  }}
  .node-highlight rect.node-box {{
    stroke: var(--primary) !important;
    stroke-width: 2 !important;
  }}
  .node-dimmed {{
    opacity: 0.2;
    transition: opacity 0.15s ease;
  }}
  .edge-dimmed {{
    opacity: 0.12;
    transition: opacity 0.15s ease;
  }}
</style>
</head>
<body>
<header>
  <div class="logo-group">
    <div class="logo-title">sparklineage <span class="badge">Execution DAG</span></div>
  </div>
  <div class="header-actions">
    <input type="text" id="node-search" class="search-input" placeholder="Search nodes or columns..." oninput="searchGraph(this.value)">
    <button class="btn" id="btn-heatmap" onclick="toggleHeatmap()">Risk View</button>
    <button class="btn" onclick="exportSVG()">Export SVG</button>
  </div>
</header>

<div class="main-container">
  <svg id="graph-canvas"></svg>

  <div class="controls">
    <button class="btn" onclick="resetView()">Reset</button>
    <button class="btn" onclick="zoom(1.25)">+</button>
    <button class="btn" onclick="zoom(0.8)">−</button>
    <button class="btn" id="btn-layout" onclick="toggleLayout()">Layout: Horizontal</button>
  </div>

  <div class="sidebar">
    <div class="card">
      <h3>Node Details</h3>
      <div id="node-info">
        <p style="font-size: 12px; color: var(--muted);">Click any node on the canvas to inspect its Catalyst expressions, schema, and diagnostics.</p>
      </div>
    </div>

    <div class="card">
      <h3>Column Lineage</h3>
      <p style="font-size: 11px; color: var(--muted); margin-bottom: 8px;">Click a column to trace its upstream path:</p>
      <div id="col-list"></div>
      <div id="col-trace" style="margin-top: 10px;"></div>
    </div>

    <div class="card">
      <h3>Legend</h3>
      <div class="legend" id="legend"></div>
    </div>
  </div>
</div>

<script>
const DATA = {payload_json};

const nodes = DATA.nodes;
const edges = DATA.edges;
const nodeMap = new Map();
nodes.forEach(n => nodeMap.set(n.id, n));

let isHeatmap = false;
let isVertical = false;

// Adjacency and Level Assignment
const inDegree = new Map();
const adj = new Map();
const revAdj = new Map();
nodes.forEach(n => {{
  inDegree.set(n.id, 0);
  adj.set(n.id, []);
  revAdj.set(n.id, []);
}});
edges.forEach(e => {{
  adj.get(e.from).push(e.to);
  revAdj.get(e.to).push(e.from);
  inDegree.set(e.to, (inDegree.get(e.to) || 0) + 1);
}});

// Topological Levels
const levels = new Map();
let queue = [];
nodes.forEach(n => {{
  if (inDegree.get(n.id) === 0) {{
    queue.push(n.id);
    levels.set(n.id, 0);
  }}
}});

while (queue.length > 0) {{
  const curr = queue.shift();
  const currLvl = levels.get(curr) || 0;
  for (const nxt of adj.get(curr) || []) {{
    const nxtLvl = Math.max(levels.get(nxt) || 0, currLvl + 1);
    levels.set(nxt, nxtLvl);
    queue.push(nxt);
  }}
}}
nodes.forEach(n => {{ if (!levels.has(n.id)) levels.set(n.id, 0); }});

// Dimensions
const NODE_WIDTH = 180;
const NODE_HEIGHT = 62;
const GAP_X = 110;
const GAP_Y = 40;

function computeLayout() {{
  const levelGroups = new Map();
  nodes.forEach(n => {{
    const lvl = levels.get(n.id);
    if (!levelGroups.has(lvl)) levelGroups.set(lvl, []);
    levelGroups.get(lvl).push(n);
  }});

  levelGroups.forEach((group, lvl) => {{
    group.forEach((node, idx) => {{
      if (!isVertical) {{
        node.x = 60 + lvl * (NODE_WIDTH + GAP_X);
        node.y = 60 + idx * (NODE_HEIGHT + GAP_Y);
      }} else {{
        node.x = 60 + idx * (NODE_WIDTH + 30);
        node.y = 60 + lvl * (NODE_HEIGHT + 50);
      }}
    }});
  }});
}}
computeLayout();

const svg = document.getElementById("graph-canvas");
let scale = 1;
let panX = 0, panY = 0;
let isPanning = false, startX, startY;

const container = document.createElementNS("http://www.w3.org/2000/svg", "g");
container.setAttribute("id", "main-graph-group");
svg.appendChild(container);

function updateTransform() {{
  container.setAttribute("transform", `translate(${{panX}}, ${{panY}}) scale(${{scale}})`);
}}

svg.addEventListener("mousedown", e => {{
  if (e.target === svg || e.target.id === "main-graph-group") {{
    isPanning = true;
    startX = e.clientX - panX;
    startY = e.clientY - panY;
  }}
}});

window.addEventListener("mousemove", e => {{
  if (isPanning) {{
    panX = e.clientX - startX;
    panY = e.clientY - startY;
    updateTransform();
  }}
}});

window.addEventListener("mouseup", () => {{ isPanning = false; }});

svg.addEventListener("wheel", e => {{
  e.preventDefault();
  const zoomFactor = e.deltaY < 0 ? 1.1 : 0.9;
  scale = Math.min(Math.max(0.25, scale * zoomFactor), 3.0);
  updateTransform();
}});

function zoom(factor) {{
  scale = Math.min(Math.max(0.25, scale * factor), 3.0);
  updateTransform();
}}

function resetView() {{
  scale = 1; panX = 0; panY = 0;
  updateTransform();
  clearHighlights();
}}

function toggleLayout() {{
  isVertical = !isVertical;
  document.getElementById("btn-layout").textContent = isVertical ? "Layout: Vertical" : "Layout: Horizontal";
  computeLayout();
  renderGraph();
}}

// Arrow Defs
const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
defs.innerHTML = `
  <marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="#374151"/>
  </marker>
  <marker id="arrow-active" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="#38bdf8"/>
  </marker>
`;
svg.appendChild(defs);

let edgeElements = [];
let nodeElements = [];

function renderGraph() {{
  container.innerHTML = "";
  edgeElements = [];
  nodeElements = [];

  edges.forEach(e => {{
    const source = nodeMap.get(e.from);
    const target = nodeMap.get(e.to);
    if (!source || !target) return;

    let sx, sy, tx, ty, pathStr;
    if (!isVertical) {{
      sx = source.x + NODE_WIDTH;
      sy = source.y + NODE_HEIGHT / 2;
      tx = target.x;
      ty = target.y + NODE_HEIGHT / 2;
      const dx = (tx - sx) / 2;
      pathStr = `M ${{sx}} ${{sy}} C ${{sx + dx}} ${{sy}}, ${{tx - dx}} ${{ty}}, ${{tx}} ${{ty}}`;
    }} else {{
      sx = source.x + NODE_WIDTH / 2;
      sy = source.y + NODE_HEIGHT;
      tx = target.x + NODE_WIDTH / 2;
      ty = target.y;
      const dy = (ty - sy) / 2;
      pathStr = `M ${{sx}} ${{sy}} C ${{sx}} ${{sy + dy}}, ${{tx}} ${{ty - dy}}, ${{tx}} ${{ty}}`;
    }}

    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", pathStr);
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", "#1f2937");
    path.setAttribute("stroke-width", "1.5");
    path.setAttribute("marker-end", "url(#arrow)");
    path.dataset.from = e.from;
    path.dataset.to = e.to;
    container.appendChild(path);
    edgeElements.push(path);
  }});

  nodes.forEach(n => {{
    const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("transform", `translate(${{n.x}}, ${{n.y}})`);
    g.dataset.nodeId = n.id;
    g.dataset.label = n.label.toLowerCase();
    g.dataset.columns = n.columns.join(" ").toLowerCase();
    g.style.cursor = "pointer";

    const hasCritical = n.issues && n.issues.some(i => i.severity === "CRITICAL");
    const hasWarning = n.issues && n.issues.some(i => i.severity === "WARNING");

    let strokeColor = "#374151";
    if (isHeatmap) {{
      if (n.riskSeverity === "CRITICAL") strokeColor = "#ef4444";
      else if (n.riskSeverity === "WARNING") strokeColor = "#f59e0b";
      else if (n.riskSeverity === "INFO") strokeColor = "#3b82f6";
      else strokeColor = "#10b981";
    }} else if (hasCritical) {{
      strokeColor = "#ef4444";
    }} else if (hasWarning) {{
      strokeColor = "#f59e0b";
    }}

    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("class", "node-box");
    rect.setAttribute("width", NODE_WIDTH);
    rect.setAttribute("height", NODE_HEIGHT);
    rect.setAttribute("rx", "6");
    rect.setAttribute("fill", "#111827");
    rect.setAttribute("stroke", strokeColor);
    rect.setAttribute("stroke-width", hasCritical || hasWarning ? "1.5" : "1");

    const badge = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    badge.setAttribute("x", "8");
    badge.setAttribute("y", "8");
    badge.setAttribute("width", "54");
    badge.setAttribute("height", "14");
    badge.setAttribute("rx", "3");
    badge.setAttribute("fill", n.color);

    const typeText = document.createElementNS("http://www.w3.org/2000/svg", "text");
    typeText.setAttribute("x", "35");
    typeText.setAttribute("y", "18");
    typeText.setAttribute("text-anchor", "middle");
    typeText.setAttribute("fill", "#090d16");
    typeText.setAttribute("font-size", "8.5px");
    typeText.setAttribute("font-weight", "600");
    typeText.textContent = n.type.toUpperCase();

    const title = document.createElementNS("http://www.w3.org/2000/svg", "text");
    title.setAttribute("x", "10");
    title.setAttribute("y", "40");
    title.setAttribute("fill", "#f3f4f6");
    title.setAttribute("font-size", "11.5px");
    title.setAttribute("font-weight", "500");
    title.textContent = n.label.length > 20 ? n.label.substring(0, 18) + "…" : n.label;

    g.appendChild(rect);
    g.appendChild(badge);
    g.appendChild(typeText);
    g.appendChild(title);

    // Clean inline hazard indicator
    if (hasCritical || hasWarning) {{
      const tag = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      tag.setAttribute("x", NODE_WIDTH - 20);
      tag.setAttribute("y", "8");
      tag.setAttribute("width", "12");
      tag.setAttribute("height", "12");
      tag.setAttribute("rx", "2");
      tag.setAttribute("fill", hasCritical ? "#ef4444" : "#f59e0b");

      const tagText = document.createElementNS("http://www.w3.org/2000/svg", "text");
      tagText.setAttribute("x", NODE_WIDTH - 14);
      tagText.setAttribute("y", "17");
      tagText.setAttribute("text-anchor", "middle");
      tagText.setAttribute("fill", "#ffffff");
      tagText.setAttribute("font-size", "9px");
      tagText.setAttribute("font-weight", "700");
      tagText.textContent = "!";

      g.appendChild(tag);
      g.appendChild(tagText);
    }}

    g.addEventListener("click", () => selectNode(n));
    container.appendChild(g);
    nodeElements.push(g);
  }});
}}
renderGraph();

function selectNode(n) {{
  const info = document.getElementById("node-info");
  let issuesHtml = "";
  if (n.issues && n.issues.length > 0) {{
    issuesHtml = n.issues.map(iss => `
      <div class="hazard-banner">
        <div class="hazard-title">[${{iss.severity}}] ${{iss.title}}</div>
        <div>${{iss.problem}}</div>
        <div class="hazard-fix"><strong>Fix:</strong> ${{iss.recommendation}}</div>
      </div>
    `).join("");
  }}

  info.innerHTML = `
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
      <h4 style="font-size:13px; font-weight:600; color:#f9fafb;">${{n.label}}</h4>
      <span class="node-tag" style="background:${{n.color}}">${{n.type}}</span>
    </div>
    <div class="detail-line"><strong>Details:</strong> ${{n.details}}</div>
    ${{issuesHtml}}
    <div style="margin-top:10px;">
      <span style="font-size:10px; color:#9ca3af; font-weight:600; text-transform:uppercase;">Output Columns (${{n.columns.length}})</span>
      <div style="margin-top:4px;">
        ${{n.columns.map(c => `<span class="col-pill" onclick="highlightColumnLineage('${{c}}')">${{c}}</span>`).join("") || "<span style='font-size:11px; color:#6b7280;'>None</span>"}}
      </div>
    </div>
  `;
}}

function highlightColumnLineage(col) {{
  const lin = DATA.columnLineage[col];
  if (!lin) return;

  clearHighlights();

  const relevantNodes = new Set();
  nodes.forEach(n => {{
    if (n.columns.includes(col)) relevantNodes.add(n.id);
    for (const src of lin.sources) {{
      if (src.includes(n.label) || (n.details && n.details.includes(src))) {{
        relevantNodes.add(n.id);
      }}
    }}
  }});

  const queue = Array.from(relevantNodes);
  const visited = new Set(queue);
  while (queue.length > 0) {{
    const curr = queue.shift();
    for (const parent of revAdj.get(curr) || []) {{
      if (!visited.has(parent)) {{
        visited.add(parent);
        queue.push(parent);
      }}
    }}
  }}

  nodeElements.forEach(el => {{
    const nid = el.dataset.nodeId;
    if (visited.has(nid)) {{
      el.classList.add("node-highlight");
    }} else {{
      el.classList.add("node-dimmed");
    }}
  }});

  edgeElements.forEach(el => {{
    if (visited.has(el.dataset.from) && visited.has(el.dataset.to)) {{
      el.classList.add("edge-highlight");
      el.setAttribute("marker-end", "url(#arrow-active)");
    }} else {{
      el.classList.add("edge-dimmed");
    }}
  }});
}}

function clearHighlights() {{
  nodeElements.forEach(el => {{
    el.classList.remove("node-highlight", "node-dimmed");
  }});
  edgeElements.forEach(el => {{
    el.classList.remove("edge-highlight", "edge-dimmed");
    el.setAttribute("marker-end", "url(#arrow)");
  }});
}}

function searchGraph(query) {{
  const q = query.trim().toLowerCase();
  if (!q) {{
    clearHighlights();
    return;
  }}

  let firstMatch = null;
  nodeElements.forEach(el => {{
    const label = el.dataset.label || "";
    const cols = el.dataset.columns || "";
    if (label.includes(q) || cols.includes(q)) {{
      el.classList.add("node-highlight");
      el.classList.remove("node-dimmed");
      if (!firstMatch) firstMatch = nodeMap.get(el.dataset.nodeId);
    }} else {{
      el.classList.remove("node-highlight");
      el.classList.add("node-dimmed");
    }}
  }});

  if (firstMatch) {{
    panX = -(firstMatch.x - 200);
    panY = -(firstMatch.y - 200);
    scale = 1.1;
    updateTransform();
  }}
}}

function toggleHeatmap() {{
  isHeatmap = !isHeatmap;
  const btn = document.getElementById("btn-heatmap");
  btn.classList.toggle("active", isHeatmap);
  btn.textContent = isHeatmap ? "Default View" : "Risk View";
  renderGraph();
}}

function exportSVG() {{
  const svgData = new XMLSerializer().serializeToString(svg);
  const blob = new Blob([svgData], {{ type: "image/svg+xml;charset=utf-8" }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "spark_lineage_graph.svg";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}}

const colList = document.getElementById("col-list");
const colTrace = document.getElementById("col-trace");
const cols = Object.keys(DATA.columnLineage);
if (cols.length === 0) {{
  colList.innerHTML = "<span style='font-size:11px; color:#6b7280;'>No column lineage derived.</span>";
}} else {{
  cols.forEach(col => {{
    const pill = document.createElement("span");
    pill.className = "col-pill";
    pill.textContent = col;
    pill.onclick = () => {{
      document.querySelectorAll(".col-pill").forEach(p => p.classList.remove("active"));
      pill.classList.add("active");
      highlightColumnLineage(col);
      const lin = DATA.columnLineage[col];
      colTrace.innerHTML = `
        <div style="font-size:11px; margin-bottom:6px; color:#9ca3af;">Trace for <strong style="color:#f9fafb;">${{col}}</strong>:</div>
        <div class="card" style="font-size:11px;">
          <div style="color:#10b981; font-weight:600; margin-bottom:3px;">Upstream Sources:</div>
          <div>${{lin.sources.join("<br>") || "None found"}}</div>
          <div style="color:#f59e0b; font-weight:600; margin-top:8px; margin-bottom:3px;">Transformations:</div>
          <div>${{lin.transformations.join("<br>") || "Identity / Passthrough"}}</div>
        </div>
      `;
    }};
    colList.appendChild(pill);
  }});
}}

const legend = document.getElementById("legend");
const types = [
  {{ label: "Source", color: "#10b981" }},
  {{ label: "Join", color: "#3b82f6" }},
  {{ label: "Project", color: "#06b6d4" }},
  {{ label: "Aggregate", color: "#8b5cf6" }},
  {{ label: "Filter", color: "#f59e0b" }},
  {{ label: "Exchange", color: "#ef4444" }},
];
types.forEach(t => {{
  const item = document.createElement("div");
  item.className = "legend-item";
  item.innerHTML = `<span class="legend-dot" style="background:${{t.color}}"></span>${{t.label}}`;
  legend.appendChild(item);
}});
</script>
</body>
</html>
"""
        return html
