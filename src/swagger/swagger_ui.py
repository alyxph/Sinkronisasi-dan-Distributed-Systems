"""
Swagger UI handler — serves an embedded Swagger UI page and the OpenAPI spec.

Usage in BaseNode:
    from src.swagger.swagger_ui import setup_swagger
    setup_swagger(self.app, role="lock_manager")
"""
from __future__ import annotations

import copy
import os
from pathlib import Path

import yaml
from aiohttp import web

_SPEC_PATH = Path(__file__).parent / "openapi.yaml"

# Swagger UI 5.x CDN assets
_SWAGGER_CDN = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5"

# Mapping: NODE_ROLE -> allowed Swagger tags (Common is always included)
_ROLE_TAGS: dict[str, list[str]] = {
    "lock_manager": ["Common", "Lock Manager"],
    "queue_node":   ["Common", "Queue"],
    "cache_node":   ["Common", "Cache"],
}

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Distributed Sync System — API Docs</title>
  <link rel="stylesheet" href="{cdn}/swagger-ui.css" />
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    html {{ scrollbar-color: #4a4a6a #1a1a2e; }}
    body {{
      margin: 0;
      background: linear-gradient(135deg, #0f0c29 0%, #1a1a2e 40%, #16213e 100%);
      font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
      min-height: 100vh;
    }}
    /* ── Top Bar ────────────────────────────────────────────── */
    .topbar-wrapper {{
      display: flex; align-items: center; gap: 12px;
    }}
    .swagger-ui .topbar {{
      background: rgba(15,12,41,.85);
      backdrop-filter: blur(12px);
      border-bottom: 1px solid rgba(255,255,255,.08);
      padding: 10px 24px;
    }}
    .swagger-ui .topbar a span {{ display: none; }}
    .swagger-ui .topbar::after {{
      content: 'Distributed Synchronization System';
      color: #e2e8f0; font-size: 1.15rem; font-weight: 600;
      letter-spacing: .3px;
    }}
    /* ── Scheme container ──────────────────────────────────── */
    .swagger-ui .scheme-container {{
      background: rgba(26,26,46,.7);
      backdrop-filter: blur(8px);
      border-bottom: 1px solid rgba(255,255,255,.06);
      box-shadow: none;
    }}
    .swagger-ui .scheme-container select {{
      background: #1e1e3a; color: #c7d2fe;
      border: 1px solid rgba(255,255,255,.12); border-radius: 6px;
    }}
    /* ── General text colours ──────────────────────────────── */
    .swagger-ui,
    .swagger-ui .opblock-tag,
    .swagger-ui .opblock .opblock-summary-description,
    .swagger-ui table thead tr td,
    .swagger-ui table thead tr th,
    .swagger-ui .parameter__name,
    .swagger-ui .parameter__type,
    .swagger-ui .response-col_status,
    .swagger-ui .response-col_description,
    .swagger-ui .tab li,
    .swagger-ui label,
    .swagger-ui .btn {{ color: #c7d2fe; }}
    .swagger-ui .opblock-tag small {{ color: #94a3b8; }}
    .swagger-ui a.nostyle {{ color: #818cf8; }}
    /* ── Info section ──────────────────────────────────────── */
    .swagger-ui .info {{ margin: 30px 0 20px; }}
    .swagger-ui .info .title {{ color: #e2e8f0; }}
    .swagger-ui .info .description p {{ color: #94a3b8; }}
    .swagger-ui .info .description strong {{ color: #c7d2fe; }}
    /* ── Operation blocks ──────────────────────────────────── */
    .swagger-ui .opblock {{
      border-radius: 10px;
      border: 1px solid rgba(255,255,255,.06);
      margin-bottom: 8px;
      box-shadow: 0 2px 12px rgba(0,0,0,.25);
    }}
    .swagger-ui .opblock .opblock-summary {{
      border-bottom: 1px solid rgba(255,255,255,.06);
      border-radius: 10px 10px 0 0;
    }}
    .swagger-ui .opblock.opblock-get {{
      background: rgba(56,189,248,.08);
      border-color: rgba(56,189,248,.18);
    }}
    .swagger-ui .opblock.opblock-get .opblock-summary {{
      border-color: rgba(56,189,248,.15);
    }}
    .swagger-ui .opblock.opblock-post {{
      background: rgba(52,211,153,.08);
      border-color: rgba(52,211,153,.18);
    }}
    .swagger-ui .opblock.opblock-post .opblock-summary {{
      border-color: rgba(52,211,153,.15);
    }}
    .swagger-ui .opblock-body {{ background: rgba(15,12,41,.45); }}
    .swagger-ui .opblock-description-wrapper p {{ color: #94a3b8; }}
    /* ── Tag groups ────────────────────────────────────────── */
    .swagger-ui .opblock-tag-section {{
      border-radius: 12px;
      overflow: hidden;
      margin-bottom: 16px;
    }}
    .swagger-ui .opblock-tag {{
      border-bottom: 1px solid rgba(255,255,255,.06);
      padding: 12px 20px;
    }}
    /* ── Models ─────────────────────────────────────────────── */
    .swagger-ui section.models {{
      border: 1px solid rgba(255,255,255,.06);
      border-radius: 10px;
      background: rgba(26,26,46,.55);
    }}
    .swagger-ui section.models .model-container {{
      background: rgba(15,12,41,.5);
      border-radius: 8px;
      margin: 4px 8px;
    }}
    .swagger-ui .model {{ color: #c7d2fe; }}
    /* ── Tables ─────────────────────────────────────────────── */
    .swagger-ui table tbody tr td {{ color: #a5b4fc; border-color: rgba(255,255,255,.06); }}
    .swagger-ui .parameters-col_description input,
    .swagger-ui .parameters-col_description textarea,
    .swagger-ui .body-param__text {{
      background: #1e1e3a; color: #c7d2fe;
      border: 1px solid rgba(255,255,255,.12);
      border-radius: 6px;
    }}
    /* ── Response section ───────────────────────────────────── */
    .swagger-ui .responses-inner {{ background: transparent; }}
    .swagger-ui .response-col_description__inner span {{ color: #94a3b8; }}
    .swagger-ui .microlight {{ background: #12122a !important; color: #c7d2fe !important; border-radius: 8px; }}
    /* ── Buttons ────────────────────────────────────────────── */
    .swagger-ui .btn {{
      border-radius: 6px;
      border: 1px solid rgba(255,255,255,.12);
      background: rgba(255,255,255,.04);
      transition: all .2s ease;
    }}
    .swagger-ui .btn:hover {{ background: rgba(255,255,255,.1); }}
    .swagger-ui .btn.execute {{
      background: linear-gradient(135deg, #6366f1, #818cf8);
      border: none; color: #fff; font-weight: 600;
    }}
    .swagger-ui .btn.execute:hover {{ filter: brightness(1.15); }}
    /* ── Highlight / JSON ──────────────────────────────────── */
    .swagger-ui .highlight-code {{ background: #12122a; border-radius: 8px; }}
    .swagger-ui .highlight-code .microlight {{ background: transparent !important; }}
    /* ── Scrollbar ──────────────────────────────────────────── */
    .swagger-ui ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
    .swagger-ui ::-webkit-scrollbar-track {{ background: transparent; }}
    .swagger-ui ::-webkit-scrollbar-thumb {{ background: #4a4a6a; border-radius: 3px; }}
    /* ── Wrapper padding ───────────────────────────────────── */
    .swagger-ui .wrapper {{ padding: 0 24px; max-width: 1320px; }}
    /* ── Custom footer ─────────────────────────────────────── */
    #custom-footer {{
      text-align: center; padding: 24px;
      color: #475569; font-size: .8rem;
      border-top: 1px solid rgba(255,255,255,.05);
      margin-top: 40px;
    }}
  </style>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
</head>
<body>
  <div id="swagger-ui"></div>
  <div id="custom-footer">
    Sistem Parallel dan Terdistribusi &mdash; Distributed Synchronization System
  </div>
  <script src="{cdn}/swagger-ui-bundle.js"></script>
  <script src="{cdn}/swagger-ui-standalone-preset.js"></script>
  <script>
    SwaggerUIBundle({{
      url: window.location.origin + '/docs/openapi.json',
      dom_id: '#swagger-ui',
      deepLinking: true,
      presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIStandalonePreset
      ],
      plugins: [
        SwaggerUIBundle.plugins.DownloadUrl
      ],
      layout: 'StandaloneLayout',
      defaultModelsExpandDepth: 1,
      defaultModelExpandDepth: 2,
      docExpansion: 'list',
      filter: true,
      tryItOutEnabled: true,
    }});
  </script>
</body>
</html>
"""


def _filter_spec_by_role(spec: dict, role: str) -> dict:
    """Return a copy of the OpenAPI spec containing only tags relevant to *role*."""
    allowed_tags = set(_ROLE_TAGS.get(role, []))
    if not allowed_tags:
        return spec  # unknown role → return full spec

    filtered = copy.deepcopy(spec)

    # Filter tags list
    if "tags" in filtered:
        filtered["tags"] = [t for t in filtered["tags"] if t.get("name") in allowed_tags]

    # Filter paths: keep only operations whose tags intersect with allowed_tags
    new_paths: dict = {}
    for path, methods in filtered.get("paths", {}).items():
        new_methods: dict = {}
        for method, operation in methods.items():
            if isinstance(operation, dict):
                op_tags = set(operation.get("tags", []))
                if op_tags & allowed_tags:
                    new_methods[method] = operation
        if new_methods:
            new_paths[path] = new_methods
    filtered["paths"] = new_paths

    # Update servers to only show the current node's URL
    filtered["servers"] = [{"url": "/", "description": "Current node"}]

    return filtered


async def _handle_swagger_ui(request: web.Request) -> web.Response:
    """Serve the Swagger UI HTML page."""
    html = _HTML_TEMPLATE.format(cdn=_SWAGGER_CDN)
    return web.Response(text=html, content_type="text/html")


async def _handle_openapi_spec(request: web.Request) -> web.Response:
    """Serve the OpenAPI spec as JSON, filtered by node role."""
    role = request.app.get("_node_role", "")
    with open(_SPEC_PATH, "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)
    filtered = _filter_spec_by_role(spec, role)
    return web.json_response(filtered)


async def _handle_openapi_yaml(request: web.Request) -> web.Response:
    """Serve the OpenAPI spec as YAML, filtered by node role."""
    role = request.app.get("_node_role", "")
    with open(_SPEC_PATH, "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)
    filtered = _filter_spec_by_role(spec, role)
    content = yaml.dump(filtered, default_flow_style=False, allow_unicode=True)
    return web.Response(text=content, content_type="text/yaml")


def setup_swagger(app: web.Application, role: str = "") -> None:
    """Register Swagger UI routes on the given aiohttp application.

    Args:
        app: The aiohttp application.
        role: The node role (e.g. 'lock_manager', 'queue_node', 'cache_node').
              Used to filter the spec so only relevant endpoints are shown.
    """
    app["_node_role"] = role
    app.add_routes(
        [
            web.get("/docs", _handle_swagger_ui),
            web.get("/docs/", _handle_swagger_ui),
            web.get("/docs/openapi.json", _handle_openapi_spec),
            web.get("/docs/openapi.yaml", _handle_openapi_yaml),
        ]
    )
