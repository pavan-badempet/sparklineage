"""
Zero-dependency Interactive HTML/SVG DAG Visualizer for Spark Lineage.
Embeds cleanly in Jupyter, Databricks, or exports to standalone HTML files.
"""

import json
from typing import Dict, Any
from .models import PlanGraph, NodeType, ColumnLineage


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

    @classmethod
    def generate_html(
        cls,
        graph: PlanGraph,
        column_lineage: Dict[str, ColumnLineage] = None,
        title: str = "Spark Execution & Lineage DAG"
    ) -> str:
        """
        Generates a self-contained HTML document with interactive SVG graph.
        """
        nodes_data = []
        edges_data = []

        # Build clean JSON serializable representation
        for nid, node in graph.nodes.items():
            color = cls.NODE_COLORS.get(node.node_type, "#475569")
            nodes_data.append({
                "id": node.node_id,
                "label": node.name,
                "type": node.node_type.value,
                "color": color,
                "details": node.details[:250],
                "columns": node.output_columns,
                "expressions": node.expressions[:8],
            })
            for cid in node.children_ids:
                # In Catalyst, parent points to child; data flows child (source) -> parent (sink)
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
    --bg: #0f172a;
    --card-bg: #1e293b;
    --border: #334155;
    --text: #f8fafc;
    --muted: #94a3b8;
    --primary: #38bdf8;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }}
  header {{
    padding: 12px 20px;
    background: var(--card-bg);
    border-bottom: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
    align-items: center;
  }}
  header h1 {{ font-size: 16px; font-weight: 600; color: var(--primary); display: flex; align-items: center; gap: 8px; }}
  header .badge {{ font-size: 11px; padding: 2px 8px; border-radius: 999px; background: rgba(56,189,248,0.15); color: var(--primary); }}
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
    background-image: radial-gradient(#334155 1px, transparent 1px);
    background-size: 24px 24px;
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
    gap: 16px;
  }}
  .card {{
    background: rgba(15, 23, 42, 0.6);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 12px;
  }}
  .card h3 {{ font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--muted); margin-bottom: 8px; }}
  .col-pill {{
    display: inline-block;
    padding: 3px 8px;
    border-radius: 4px;
    font-size: 12px;
    background: #334155;
    margin: 2px;
    cursor: pointer;
    transition: all 0.15s ease;
  }}
  .col-pill:hover, .col-pill.active {{
    background: var(--primary);
    color: #0f172a;
    font-weight: 600;
  }}
  .node-tag {{
    display: inline-block;
    font-size: 10px;
    padding: 2px 6px;
    border-radius: 4px;
    font-weight: bold;
    color: #fff;
  }}
  .detail-line {{ font-size: 12px; color: var(--muted); word-break: break-all; margin-top: 4px; font-family: monospace; }}
  .legend {{ display: flex; flex-wrap: wrap; gap: 8px; font-size: 11px; }}
  .legend-item {{ display: flex; align-items: center; gap: 4px; }}
  .legend-dot {{ width: 8px; height: 8px; border-radius: 50%; }}
  .controls {{ position: absolute; bottom: 16px; left: 16px; display: flex; gap: 8px; z-index: 10; }}
  .btn {{
    background: var(--card-bg);
    color: var(--text);
    border: 1px solid var(--border);
    padding: 6px 12px;
    border-radius: 6px;
    font-size: 12px;
    cursor: pointer;
  }}
  .btn:hover {{ border-color: var(--primary); color: var(--primary); }}
</style>
</head>
<body>
<header>
  <h1>⚡ sparklineage <span class="badge">v0.2.0 • Interactive Plan DAG</span></h1>
  <div style="font-size: 12px; color: var(--muted);">Click any node or column to inspect field origins</div>
</header>

<div class="main-container">
  <svg id="graph-canvas"></svg>

  <div class="controls">
    <button class="btn" onclick="resetView()">Reset View</button>
    <button class="btn" onclick="zoom(1.2)">+</button>
    <button class="btn" onclick="zoom(0.8)">-</button>
  </div>

  <div class="sidebar">
    <div class="card">
      <h3>Selected Node Details</h3>
      <div id="node-info">
        <p style="font-size: 13px; color: var(--muted);">Select a node on the canvas to inspect its operations, schema, and Catalyst expressions.</p>
      </div>
    </div>

    <div class="card">
      <h3>Column Lineage Explorer</h3>
      <div id="col-list"></div>
      <div id="col-trace" style="margin-top: 12px;"></div>
    </div>

    <div class="card">
      <h3>Plan Legend</h3>
      <div class="legend" id="legend"></div>
    </div>
  </div>
</div>

<script>
const DATA = {payload_json};

// Simple Sugiyama-inspired topological level assignment for layout
const nodes = DATA.nodes;
const edges = DATA.edges;
const nodeMap = new Map();
nodes.forEach(n => nodeMap.set(n.id, n));

// In-degree & levels
const inDegree = new Map();
const adj = new Map();
nodes.forEach(n => {{ inDegree.set(n.id, 0); adj.set(n.id, []); }});
edges.forEach(e => {{
  adj.get(e.from).push(e.to);
  inDegree.set(e.to, (inDegree.get(e.to) || 0) + 1);
}});

// Assign levels from sources (in-degree == 0)
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

// Fallback for unconnected nodes
nodes.forEach(n => {{ if (!levels.has(n.id)) levels.set(n.id, 0); }});

// Group by level
const levelGroups = new Map();
nodes.forEach(n => {{
  const lvl = levels.get(n.id);
  if (!levelGroups.has(lvl)) levelGroups.set(lvl, []);
  levelGroups.get(lvl).push(n);
}});

// Layout coordinates
const NODE_WIDTH = 180;
const NODE_HEIGHT = 65;
const GAP_X = 140;
const GAP_Y = 50;

levelGroups.forEach((group, lvl) => {{
  group.forEach((node, idx) => {{
    node.x = 80 + lvl * (NODE_WIDTH + GAP_X);
    node.y = 80 + idx * (NODE_HEIGHT + GAP_Y);
  }});
}});

// Render SVG
const svg = document.getElementById("graph-canvas");
let scale = 1;
let panX = 0, panY = 0;
let isPanning = false, startX, startY;

const container = document.createElementNS("http://www.w3.org/2000/svg", "g");
svg.appendChild(container);

function updateTransform() {{
  container.setAttribute("transform", `translate(${{panX}}, ${{panY}}) scale(${{scale}})`);
}}

svg.addEventListener("mousedown", e => {{
  if (e.target === svg) {{
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
  scale = Math.min(Math.max(0.3, scale * zoomFactor), 3);
  updateTransform();
}});

function zoom(factor) {{
  scale = Math.min(Math.max(0.3, scale * factor), 3);
  updateTransform();
}}

function resetView() {{
  scale = 1; panX = 0; panY = 0;
  updateTransform();
}}

// Draw Edges with smooth beziers
edges.forEach(e => {{
  const source = nodeMap.get(e.from);
  const target = nodeMap.get(e.to);
  if (!source || !target) return;

  const sx = source.x + NODE_WIDTH;
  const sy = source.y + NODE_HEIGHT / 2;
  const tx = target.x;
  const ty = target.y + NODE_HEIGHT / 2;
  const dx = (tx - sx) / 2;

  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", `M ${{sx}} ${{sy}} C ${{sx + dx}} ${{sy}}, ${{tx - dx}} ${{ty}}, ${{tx}} ${{ty}}`);
  path.setAttribute("fill", "none");
  path.setAttribute("stroke", "#475569");
  path.setAttribute("stroke-width", "2");
  path.setAttribute("marker-end", "url(#arrow)");
  container.appendChild(path);
}});

// Defs for arrows
const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
defs.innerHTML = `
  <marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="#64748b"/>
  </marker>
`;
svg.appendChild(defs);

// Draw Nodes
nodes.forEach(n => {{
  const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
  g.setAttribute("transform", `translate(${{n.x}}, ${{n.y}})`);
  g.style.cursor = "pointer";

  const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  rect.setAttribute("width", NODE_WIDTH);
  rect.setAttribute("height", NODE_HEIGHT);
  rect.setAttribute("rx", "8");
  rect.setAttribute("fill", "#1e293b");
  rect.setAttribute("stroke", n.color);
  rect.setAttribute("stroke-width", "2");

  const badge = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  badge.setAttribute("x", "8");
  badge.setAttribute("y", "8");
  badge.setAttribute("width", "60");
  badge.setAttribute("height", "14");
  badge.setAttribute("rx", "4");
  badge.setAttribute("fill", n.color);

  const typeText = document.createElementNS("http://www.w3.org/2000/svg", "text");
  typeText.setAttribute("x", "38");
  typeText.setAttribute("y", "19");
  typeText.setAttribute("text-anchor", "middle");
  typeText.setAttribute("fill", "#0f172a");
  typeText.setAttribute("font-size", "9px");
  typeText.setAttribute("font-weight", "bold");
  typeText.textContent = n.type.toUpperCase();

  const title = document.createElementNS("http://www.w3.org/2000/svg", "text");
  title.setAttribute("x", "12");
  title.setAttribute("y", "42");
  title.setAttribute("fill", "#f8fafc");
  title.setAttribute("font-size", "12px");
  title.setAttribute("font-weight", "600");
  title.textContent = n.label.length > 20 ? n.label.substring(0, 18) + "…" : n.label;

  g.appendChild(rect);
  g.appendChild(badge);
  g.appendChild(typeText);
  g.appendChild(title);

  g.addEventListener("click", () => selectNode(n));
  container.appendChild(g);
}});

function selectNode(n) {{
  const info = document.getElementById("node-info");
  info.innerHTML = `
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
      <h4 style="font-size:14px; font-weight:600; color:#38bdf8;">${{n.label}}</h4>
      <span class="node-tag" style="background:${{n.color}}">${{n.type}}</span>
    </div>
    <div class="detail-line"><strong>Details:</strong> ${{n.details}}</div>
    <div style="margin-top:8px;">
      <span style="font-size:11px; color:#94a3b8; font-weight:600;">OUTPUT COLUMNS (${{n.columns.length}})</span>
      <div style="margin-top:4px;">
        ${{n.columns.map(c => `<span class="col-pill">${{c}}</span>`).join("") || "<span style='font-size:12px; color:#64748b;'>None</span>"}}
      </div>
    </div>
  `;
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
    pill.onclick = () => selectColumn(col, pill);
    colList.appendChild(pill);
  }});
}}

function selectColumn(col, pill) {{
  document.querySelectorAll(".col-pill").forEach(p => p.classList.remove("active"));
  pill.classList.add("active");
  const lin = DATA.columnLineage[col];
  if (!lin) return;

  colTrace.innerHTML = `
    <div style="font-size:12px; margin-bottom:6px;">Lineage trace for <strong>${{col}}</strong>:</div>
    <div class="card" style="font-size:11px;">
      <div style="color:#10b981; font-weight:600; margin-bottom:4px;">🌱 Upstream Sources:</div>
      <div>${{lin.sources.join("<br>") || "None found"}}</div>
      <div style="color:#f59e0b; font-weight:600; margin-top:8px; margin-bottom:4px;">⚙️ Applied Transformations:</div>
      <div>${{lin.transformations.join("<br>") || "Identity / Passthrough"}}</div>
    </div>
  `;
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
