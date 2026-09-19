"""
Zero-dependency Interactive HTML/SVG DAG Visualizer for Spark Lineage.
Embeds cleanly in Jupyter, Databricks, or exports to standalone HTML files.
Features interactive column path highlighting, pre-flight compiler hazard badges,
live node search, risk heatmap mode, and SVG export.
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
        Generates a self-contained HTML document with interactive SVG graph,
        column path highlighting, hazard badges, and live search.
        """
        # Map node_id -> list of issues
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

        # Build clean JSON serializable representation
        for nid, node in graph.nodes.items():
            color = cls.NODE_COLORS.get(node.node_type, "#475569")
            issues = node_issues.get(nid, [])

            # Compute risk level
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
                # Catalyst data flow: child (source) -> parent (sink)
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
    --bg: #0b0f19;
    --card-bg: #151d30;
    --border: #22314e;
    --text: #f8fafc;
    --muted: #94a3b8;
    --primary: #38bdf8;
    --accent: #818cf8;
    --danger: #ef4444;
    --warning: #f59e0b;
    --success: #10b981;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", sans-serif;
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
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
    font-size: 15px;
    font-weight: 700;
    color: var(--primary);
    display: flex;
    align-items: center;
    gap: 6px;
    letter-spacing: -0.3px;
  }}
  .badge {{
    font-size: 10px;
    padding: 2px 8px;
    border-radius: 999px;
    background: rgba(56,189,248,0.15);
    color: var(--primary);
    font-weight: 600;
    border: 1px solid rgba(56,189,248,0.3);
  }}
  .header-actions {{
    display: flex;
    align-items: center;
    gap: 10px;
  }}
  .search-input {{
    background: #0b0f19;
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    padding: 6px 12px;
    font-size: 12px;
    width: 220px;
    outline: none;
    transition: all 0.2s ease;
  }}
  .search-input:focus {{
    border-color: var(--primary);
    box-shadow: 0 0 0 2px rgba(56,189,248,0.2);
    width: 260px;
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
    background-image: radial-gradient(#1e293b 1px, transparent 1px);
    background-size: 24px 24px;
  }}
  #graph-canvas:active {{ cursor: grabbing; }}
  .sidebar {{
    width: 380px;
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
    background: rgba(11, 15, 25, 0.7);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 14px;
  }}
  .card h3 {{
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--muted);
    margin-bottom: 10px;
    font-weight: 700;
  }}
  .col-pill {{
    display: inline-block;
    padding: 3px 8px;
    border-radius: 4px;
    font-size: 11px;
    background: #1e293b;
    border: 1px solid #334155;
    margin: 2px;
    cursor: pointer;
    transition: all 0.15s ease;
  }}
  .col-pill:hover {{
    background: #334155;
    border-color: var(--primary);
    color: var(--primary);
  }}
  .col-pill.active {{
    background: var(--primary);
    color: #0b0f19;
    border-color: var(--primary);
    font-weight: 700;
  }}
  .node-tag {{
    display: inline-block;
    font-size: 10px;
    padding: 2px 6px;
    border-radius: 4px;
    font-weight: bold;
    color: #fff;
  }}
  .detail-line {{ font-size: 12px; color: var(--muted); word-break: break-all; margin-top: 6px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
  .hazard-banner {{
    background: rgba(239, 68, 68, 0.12);
    border: 1px solid rgba(239, 68, 68, 0.4);
    border-radius: 6px;
    padding: 10px;
    margin-top: 10px;
    font-size: 11px;
  }}
  .hazard-title {{ color: #f87171; font-weight: 700; display: flex; align-items: center; gap: 4px; }}
  .hazard-fix {{ color: #cbd5e1; margin-top: 4px; }}
  .legend {{ display: flex; flex-wrap: wrap; gap: 8px; font-size: 11px; }}
  .legend-item {{ display: flex; align-items: center; gap: 5px; }}
  .legend-dot {{ width: 8px; height: 8px; border-radius: 50%; }}
  .controls {{
    position: absolute;
    bottom: 20px;
    left: 20px;
    display: flex;
    gap: 6px;
    z-index: 10;
    background: var(--card-bg);
    padding: 4px;
    border-radius: 8px;
    border: 1px solid var(--border);
    box-shadow: 0 4px 12px rgba(0,0,0,0.4);
  }}
  .btn {{
    background: transparent;
    color: var(--text);
    border: 1px solid transparent;
    padding: 6px 12px;
    border-radius: 6px;
    font-size: 12px;
    cursor: pointer;
    font-weight: 500;
    transition: all 0.15s ease;
  }}
  .btn:hover {{
    background: #1e293b;
    border-color: var(--border);
    color: var(--primary);
  }}
  .btn.active {{
    background: rgba(56, 189, 248, 0.15);
    color: var(--primary);
    border-color: rgba(56, 189, 248, 0.3);
    font-weight: 600;
  }}
  /* Animated Pulse for Critical Hazard Nodes */
  @keyframes pulseGlow {{
    0% {{ filter: drop-shadow(0 0 2px rgba(239, 68, 68, 0.5)); }}
    50% {{ filter: drop-shadow(0 0 10px rgba(239, 68, 68, 0.9)); }}
    100% {{ filter: drop-shadow(0 0 2px rgba(239, 68, 68, 0.5)); }}
  }}
  .hazard-pulse {{
    animation: pulseGlow 1.8s infinite ease-in-out;
  }}
  /* Animated Edge Flow for Lineage Tracing */
  @keyframes dashFlow {{
    from {{ stroke-dashoffset: 24; }}
    to {{ stroke-dashoffset: 0; }}
  }}
  .edge-highlight {{
    stroke: var(--primary) !important;
    stroke-width: 3.5 !important;
    stroke-dasharray: 6 3;
    animation: dashFlow 0.8s linear infinite;
    filter: drop-shadow(0 0 4px var(--primary));
  }}
  .node-highlight rect.node-box {{
    stroke: var(--primary) !important;
    stroke-width: 3 !important;
    filter: drop-shadow(0 0 8px rgba(56, 189, 248, 0.6));
  }}
  .node-dimmed {{
    opacity: 0.25;
    transition: opacity 0.2s ease;
  }}
  .edge-dimmed {{
    opacity: 0.15;
    transition: opacity 0.2s ease;
  }}
</style>
</head>
<body>
<header>
  <div class="logo-group">
    <div class="logo-title">⚡ sparklineage <span class="badge">Next-Gen Plan DAG</span></div>
  </div>
  <div class="header-actions">
    <input type="text" id="node-search" class="search-input" placeholder="🔍 Search nodes, tables, columns..." oninput="searchGraph(this.value)">
    <button class="btn" id="btn-heatmap" onclick="toggleHeatmap()">🔥 Risk Heatmap</button>
    <button class="btn" onclick="exportSVG()">📥 Export SVG</button>
  </div>
</header>

<div class="main-container">
  <svg id="graph-canvas"></svg>

  <div class="controls">
    <button class="btn" onclick="resetView()">⟲ Reset</button>
    <button class="btn" onclick="zoom(1.25)">＋</button>
    <button class="btn" onclick="zoom(0.8)">−</button>
    <button class="btn" id="btn-layout" onclick="toggleLayout()">↔ Horizontal</button>
  </div>

  <div class="sidebar">
    <div class="card">
      <h3>Selected Node Details</h3>
      <div id="node-info">
        <p style="font-size: 12px; color: var(--muted);">Click any node on the canvas to inspect its Catalyst expressions, output schema, and pre-flight compiler diagnostics.</p>
      </div>
    </div>

    <div class="card">
      <h3>Column Lineage Explorer</h3>
      <p style="font-size: 11px; color: var(--muted); margin-bottom: 8px;">Click a column to animate its exact upstream path across the pipeline:</p>
      <div id="col-list"></div>
      <div id="col-trace" style="margin-top: 10px;"></div>
    </div>

    <div class="card">
      <h3>Execution Plan Legend</h3>
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

// Compute Topological Levels from Sources
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

// Layout Dimensions
const NODE_WIDTH = 190;
const NODE_HEIGHT = 68;
const GAP_X = 130;
const GAP_Y = 46;

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
        node.x = 80 + lvl * (NODE_WIDTH + GAP_X);
        node.y = 80 + idx * (NODE_HEIGHT + GAP_Y);
      }} else {{
        node.x = 80 + idx * (NODE_WIDTH + 40);
        node.y = 80 + lvl * (NODE_HEIGHT + 60);
      }}
    }});
  }});
}}
computeLayout();

// Render Canvas
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
  const zoomFactor = e.deltaY < 0 ? 1.12 : 0.89;
  scale = Math.min(Math.max(0.25, scale * zoomFactor), 3.5);
  updateTransform();
}});

function zoom(factor) {{
  scale = Math.min(Math.max(0.25, scale * factor), 3.5);
  updateTransform();
}}

function resetView() {{
  scale = 1; panX = 0; panY = 0;
  updateTransform();
  clearHighlights();
}}

function toggleLayout() {{
  isVertical = !isVertical;
  document.getElementById("btn-layout").textContent = isVertical ? "↕ Vertical" : "↔ Horizontal";
  computeLayout();
  renderGraph();
}}

// Arrow Defs
const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
defs.innerHTML = `
  <marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="#475569"/>
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

  // Draw Edges
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
    path.setAttribute("stroke", "#334155");
    path.setAttribute("stroke-width", "2");
    path.setAttribute("marker-end", "url(#arrow)");
    path.dataset.from = e.from;
    path.dataset.to = e.to;
    container.appendChild(path);
    edgeElements.push(path);
  }});

  // Draw Nodes
  nodes.forEach(n => {{
    const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("transform", `translate(${{n.x}}, ${{n.y}})`);
    g.dataset.nodeId = n.id;
    g.dataset.label = n.label.toLowerCase();
    g.dataset.columns = n.columns.join(" ").toLowerCase();
    g.style.cursor = "pointer";

    const hasCritical = n.issues && n.issues.some(i => i.severity === "CRITICAL");
    const hasWarning = n.issues && n.issues.some(i => i.severity === "WARNING");
    if (hasCritical) g.classList.add("hazard-pulse");

    let strokeColor = n.color;
    if (isHeatmap) {{
      if (n.riskSeverity === "CRITICAL") strokeColor = "#ef4444";
      else if (n.riskSeverity === "WARNING") strokeColor = "#f59e0b";
      else if (n.riskSeverity === "INFO") strokeColor = "#3b82f6";
      else strokeColor = "#10b981";
    }}

    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("class", "node-box");
    rect.setAttribute("width", NODE_WIDTH);
    rect.setAttribute("height", NODE_HEIGHT);
    rect.setAttribute("rx", "8");
    rect.setAttribute("fill", "#151d30");
    rect.setAttribute("stroke", strokeColor);
    rect.setAttribute("stroke-width", "2");

    const badge = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    badge.setAttribute("x", "8");
    badge.setAttribute("y", "8");
    badge.setAttribute("width", "62");
    badge.setAttribute("height", "15");
    badge.setAttribute("rx", "4");
    badge.setAttribute("fill", strokeColor);

    const typeText = document.createElementNS("http://www.w3.org/2000/svg", "text");
    typeText.setAttribute("x", "39");
    typeText.setAttribute("y", "19");
    typeText.setAttribute("text-anchor", "middle");
    typeText.setAttribute("fill", "#0b0f19");
    typeText.setAttribute("font-size", "9px");
    typeText.setAttribute("font-weight", "bold");
    typeText.textContent = n.type.toUpperCase();

    const title = document.createElementNS("http://www.w3.org/2000/svg", "text");
    title.setAttribute("x", "12");
    title.setAttribute("y", "44");
    title.setAttribute("fill", "#f8fafc");
    title.setAttribute("font-size", "12px");
    title.setAttribute("font-weight", "600");
    title.textContent = n.label.length > 20 ? n.label.substring(0, 18) + "…" : n.label;

    g.appendChild(rect);
    g.appendChild(badge);
    g.appendChild(typeText);
    g.appendChild(title);

    // If node has hazard issues, add inline indicator badge
    if (hasCritical || hasWarning) {{
      const warnCircle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      warnCircle.setAttribute("cx", NODE_WIDTH - 14);
      warnCircle.setAttribute("cy", "14");
      warnCircle.setAttribute("r", "8");
      warnCircle.setAttribute("fill", hasCritical ? "#ef4444" : "#f59e0b");

      const warnExcl = document.createElementNS("http://www.w3.org/2000/svg", "text");
      warnExcl.setAttribute("x", NODE_WIDTH - 14);
      warnExcl.setAttribute("y", "18");
      warnExcl.setAttribute("text-anchor", "middle");
      warnExcl.setAttribute("fill", "#ffffff");
      warnExcl.setAttribute("font-size", "11px");
      warnExcl.setAttribute("font-weight", "bold");
      warnExcl.textContent = "!";

      g.appendChild(warnCircle);
      g.appendChild(warnExcl);
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
        <div class="hazard-title">🚨 [${{iss.severity}}] ${{iss.title}}</div>
        <div style="margin-top:3px;">${{iss.problem}}</div>
        <div class="hazard-fix"><strong>Fix:</strong> ${{iss.recommendation}}</div>
      </div>
    `).join("");
  }}

  info.innerHTML = `
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
      <h4 style="font-size:14px; font-weight:700; color:#38bdf8;">${{n.label}}</h4>
      <span class="node-tag" style="background:${{n.color}}">${{n.type}}</span>
    </div>
    <div class="detail-line"><strong>Details:</strong> ${{n.details}}</div>
    ${{issuesHtml}}
    <div style="margin-top:10px;">
      <span style="font-size:10px; color:#94a3b8; font-weight:700; text-transform:uppercase;">Output Columns (${{n.columns.length}})</span>
      <div style="margin-top:4px;">
        ${{n.columns.map(c => `<span class="col-pill" onclick="highlightColumnLineage('${{c}}')">${{c}}</span>`).join("") || "<span style='font-size:12px; color:#64748b;'>None</span>"}}
      </div>
    </div>
  `;
}}

function highlightColumnLineage(col) {{
  const lin = DATA.columnLineage[col];
  if (!lin) return;

  clearHighlights();

  // Find upstream sources
  const relevantNodes = new Set();
  nodes.forEach(n => {{
    if (n.columns.includes(col)) relevantNodes.add(n.id);
    for (const src of lin.sources) {{
      if (src.includes(n.label) || (n.details && n.details.includes(src))) {{
        relevantNodes.add(n.id);
      }}
    }}
  }});

  // Back-trace BFS to highlight full path
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

  // Apply Dim & Highlight classes
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
  btn.textContent = isHeatmap ? "✨ Default Colors" : "🔥 Risk Heatmap";
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

// Populate Column Explorer
const colList = document.getElementById("col-list");
const colTrace = document.getElementById("col-trace");
const cols = Object.keys(DATA.columnLineage);
if (cols.length === 0) {{
  colList.innerHTML = "<span style='font-size:12px; color:#64748b;'>No column lineage derived.</span>";
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
        <div style="font-size:12px; margin-bottom:6px;">Lineage trace for <strong>${{col}}</strong>:</div>
        <div class="card" style="font-size:11px;">
          <div style="color:#10b981; font-weight:700; margin-bottom:4px;">🌱 Upstream Sources:</div>
          <div>${{lin.sources.join("<br>") || "None found"}}</div>
          <div style="color:#f59e0b; font-weight:700; margin-top:8px; margin-bottom:4px;">⚙️ Applied Transformations:</div>
          <div>${{lin.transformations.join("<br>") || "Identity / Passthrough"}}</div>
        </div>
      `;
    }};
    colList.appendChild(pill);
  }});
}}

// Populate Legend
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
