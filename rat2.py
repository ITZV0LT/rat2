# -*- coding: utf-8 -*-
"""
RAT2 - Warehouse Remote Management
  Manager : python rat2.py
  Agent   : python rat2.py agent
"""

import sys
_MODE = "agent" if len(sys.argv) > 1 and sys.argv[1] == "agent" else "manager"

import asyncio, base64, hashlib, io, json, os, platform, socket
import subprocess, threading, time, urllib.request, uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# ── Configuration ──────────────────────────────────────────────────────────────
# Edit these three lines (or set environment variables).
#   RAT2_KEY      must match on both manager and every agent
#   RAT2_URL      agents use this to connect (ws:// or wss://)
#   RAT2_LOCATION label shown in the dashboard for this agent machine
SECRET_KEY  = os.environ.get("RAT2_KEY",      "Zu6_4hEGklhBBQzHYjj1-0n2hbvr-6cuu4huzkufhZQ")
MANAGER_URL = os.environ.get("RAT2_URL",      "ws://YOUR-MANAGER-IP:8080/ws/agent")
LOCATION    = os.environ.get("RAT2_LOCATION", "Warehouse A")
PORT        = int(os.environ.get("PORT", 8080))
HOST        = "0.0.0.0"
HEARTBEAT_INTERVAL = 5
# ───────────────────────────────────────────────────────────────────────────────

THIS_FILE = Path(__file__).resolve()


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ==============================================================================
#  MANAGER
# ==============================================================================
if _MODE == "manager":
    import uvicorn
    from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
    from fastapi.responses import FileResponse, HTMLResponse

    app = FastAPI(title="RAT2 Manager")

    agents:     Dict[str, dict] = {}
    pending:    Dict[str, asyncio.Future] = {}
    ui_clients: List[WebSocket] = []

    def current_agent_hash() -> str:
        return file_hash(THIS_FILE)

    def agent_summary(aid: str) -> dict:
        a = agents[aid]
        ah = a["info"].get("agent_hash", "")
        return {
            "id":           aid,
            "hostname":     a["info"].get("hostname", "Unknown"),
            "os":           a["info"].get("os", "Unknown"),
            "ip":           a["info"].get("ip", "Unknown"),
            "location":     a["info"].get("location", ""),
            "cpu_count":    a["info"].get("cpu_count", 0),
            "total_ram_gb": round(a["info"].get("total_ram", 0) / 1_073_741_824, 1),
            "cpu":          a.get("cpu", 0),
            "ram":          a.get("ram", 0),
            "disk":         a.get("disk", 0),
            "connected_at": a.get("connected_at", ""),
            "last_seen":    a.get("last_seen", 0),
            "outdated":     bool(ah and ah != current_agent_hash()),
        }

    async def broadcast_ui(data: dict):
        msg  = json.dumps(data)
        dead = []
        for ws in ui_clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            try: ui_clients.remove(ws)
            except ValueError: pass

    async def send_command(agent_id: str, command: dict, timeout: float = 60.0) -> dict:
        if agent_id not in agents:
            raise HTTPException(404, "Agent is offline")
        cmd_id = str(uuid.uuid4())
        command["cmd_id"] = cmd_id
        loop   = asyncio.get_running_loop()
        future = loop.create_future()
        pending[cmd_id] = future
        try:
            await agents[agent_id]["ws"].send_json(command)
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            raise HTTPException(504, "Agent did not respond in time")
        finally:
            pending.pop(cmd_id, None)

    @app.websocket("/ws/ui")
    async def ui_ws(ws: WebSocket):
        await ws.accept()
        ui_clients.append(ws)
        await ws.send_text(json.dumps({
            "type":   "init",
            "agents": [agent_summary(aid) for aid in agents],
        }))
        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            try: ui_clients.remove(ws)
            except ValueError: pass

    @app.websocket("/ws/agent")
    async def agent_ws(ws: WebSocket):
        await ws.accept()
        agent_id = None
        try:
            reg = await asyncio.wait_for(ws.receive_json(), timeout=15)
            if reg.get("secret") != SECRET_KEY:
                await ws.close(code=4001, reason="Unauthorized")
                return

            agent_id = reg.get("agent_id") or str(uuid.uuid4())
            agents[agent_id] = {
                "ws":           ws,
                "info":         reg,
                "cpu": 0, "ram": 0, "disk": 0,
                "connected_at": datetime.now().isoformat(timespec="seconds"),
                "last_seen":    time.time(),
            }
            print(f"[+] Agent connected: {reg.get('hostname','?')} ({agent_id[:8]})")
            await broadcast_ui({"type": "agent_up", "agent": agent_summary(agent_id)})

            if reg.get("agent_hash") and reg["agent_hash"] != current_agent_hash():
                print(f"[*] Agent {agent_id[:8]} is outdated - pushing update")
                asyncio.create_task(send_command(agent_id, {"type": "update"}, timeout=45))

            while True:
                try:
                    raw   = await ws.receive_text()
                    msg   = json.loads(raw)
                    mtype = msg.get("type")
                    if mtype == "heartbeat":
                        agents[agent_id]["last_seen"] = time.time()
                        agents[agent_id]["cpu"]  = msg.get("cpu",  0)
                        agents[agent_id]["ram"]  = msg.get("ram",  0)
                        agents[agent_id]["disk"] = msg.get("disk", 0)
                        await broadcast_ui({
                            "type":     "stats",
                            "agent_id": agent_id,
                            "cpu":  msg.get("cpu"),
                            "ram":  msg.get("ram"),
                            "disk": msg.get("disk"),
                        })
                    elif mtype == "result":
                        cmd_id = msg.get("cmd_id")
                        if cmd_id and cmd_id in pending:
                            fut = pending[cmd_id]
                            if not fut.done():
                                fut.set_result(msg)
                except WebSocketDisconnect:
                    break
                except Exception as e:
                    print(f"[!] Agent message error: {e}")
                    break
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        except Exception as e:
            print(f"[!] Agent WS error: {e}")
        finally:
            if agent_id and agent_id in agents:
                del agents[agent_id]
                print(f"[-] Agent disconnected: {agent_id[:8]}")
                await broadcast_ui({"type": "agent_down", "agent_id": agent_id})

    @app.get("/rat2.py")
    def serve_rat2():
        return FileResponse(THIS_FILE, media_type="text/plain")

    @app.get("/api/agent-hash")
    def agent_hash_endpoint():
        return {"hash": current_agent_hash()}

    @app.get("/api/agents")
    def list_agents():
        return [agent_summary(aid) for aid in agents]

    @app.post("/api/agents/{agent_id}/update")
    async def update_agent(agent_id: str):
        return await send_command(agent_id, {"type": "update"}, timeout=45)

    @app.post("/api/agents/update-all")
    async def update_all_agents():
        results = {}
        for aid in list(agents):
            try:
                results[aid] = await send_command(aid, {"type": "update"}, timeout=45)
            except Exception as e:
                results[aid] = {"error": str(e)}
        return results

    @app.post("/api/agents/{agent_id}/exec")
    async def exec_cmd(agent_id: str, req: Request):
        body = await req.json()
        cmd  = body.get("cmd", "")
        if not cmd:
            raise HTTPException(400, "cmd is required")
        return await send_command(agent_id, {"type": "exec", "cmd": cmd}, timeout=60)

    @app.post("/api/agents/{agent_id}/screenshot")
    async def screenshot(agent_id: str):
        return await send_command(agent_id, {"type": "screenshot"}, timeout=20)

    @app.get("/api/agents/{agent_id}/processes")
    async def processes(agent_id: str):
        return await send_command(agent_id, {"type": "processes"}, timeout=15)

    @app.post("/api/agents/{agent_id}/kill")
    async def kill_proc(agent_id: str, req: Request):
        body = await req.json()
        return await send_command(agent_id, {"type": "kill", "pid": body["pid"]})

    @app.get("/api/agents/{agent_id}/drives")
    async def list_drives(agent_id: str):
        return await send_command(agent_id, {"type": "drives"}, timeout=15)

    @app.get("/api/agents/{agent_id}/files")
    async def list_files(agent_id: str, path: str = "C:\\"):
        return await send_command(agent_id, {"type": "ls", "path": path}, timeout=15)

    @app.get("/api/agents/{agent_id}/search")
    async def search_files(agent_id: str, path: str, query: str):
        return await send_command(agent_id, {"type": "search", "path": path, "query": query}, timeout=90)

    @app.post("/api/agents/{agent_id}/download")
    async def download_file(agent_id: str, req: Request):
        body = await req.json()
        return await send_command(agent_id, {"type": "download", "path": body["path"]}, timeout=30)

    @app.post("/api/agents/{agent_id}/upload")
    async def upload_file(agent_id: str, req: Request):
        body = await req.json()
        return await send_command(
            agent_id,
            {"type": "upload", "path": body["path"], "data": body["data"]},
            timeout=30,
        )

    DASHBOARD = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RAT2</title>
<style>
  :root {
    --bg:        #0d1117;
    --panel:     #161b22;
    --border:    #30363d;
    --accent:    #58a6ff;
    --green:     #3fb950;
    --red:       #f85149;
    --yellow:    #d29922;
    --text:      #e6edf3;
    --muted:     #8b949e;
    --hover:     #21262d;
    --selected:  #1f3a5f;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font: 14px/1.4 'Segoe UI', system-ui, sans-serif; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }
  #header { background: var(--panel); border-bottom: 1px solid var(--border); padding: 10px 18px; display: flex; align-items: center; gap: 14px; flex-shrink: 0; }
  #header h1 { font-size: 18px; font-weight: 700; letter-spacing: .04em; color: var(--accent); }
  #conn-status { font-size: 12px; color: var(--muted); margin-left: auto; display: flex; align-items: center; gap: 6px; }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); }
  .dot.off { background: var(--red); }
  #layout { display: grid; grid-template-columns: 240px 1fr; flex: 1; overflow: hidden; }
  #sidebar { background: var(--panel); border-right: 1px solid var(--border); display: flex; flex-direction: column; overflow: hidden; }
  #sidebar-header { padding: 12px 14px; border-bottom: 1px solid var(--border); font-size: 11px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; display: flex; justify-content: space-between; align-items: center; }
  #agent-count { background: var(--border); color: var(--text); border-radius: 10px; padding: 1px 7px; font-size: 11px; }
  #agent-list { flex: 1; overflow-y: auto; padding: 6px; }
  .agent-card { padding: 10px 12px; border-radius: 8px; cursor: pointer; margin-bottom: 4px; border: 1px solid transparent; transition: background .15s; }
  .agent-card:hover { background: var(--hover); }
  .agent-card.selected { background: var(--selected); border-color: var(--accent); }
  .agent-top { display: flex; align-items: center; gap: 8px; }
  .status-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .status-dot.on  { background: var(--green); box-shadow: 0 0 6px var(--green); }
  .agent-name { font-weight: 600; font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1; }
  .update-btn { background: var(--yellow); color: #000; border: none; border-radius: 4px; padding: 2px 7px; font-size: 10px; font-weight: 700; cursor: pointer; flex-shrink: 0; }
  .update-btn:hover { opacity: .85; }
  .update-btn:disabled { opacity: .5; cursor: default; }
  .agent-loc  { font-size: 11px; color: var(--muted); margin-left: 16px; margin-top: 1px; }
  .agent-bars { margin-top: 6px; display: flex; flex-direction: column; gap: 3px; }
  .bar-row { display: flex; align-items: center; gap: 6px; font-size: 11px; color: var(--muted); }
  .bar-label { width: 30px; }
  .bar-track { flex: 1; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; }
  .bar-fill  { height: 100%; border-radius: 2px; transition: width .5s; }
  .bar-fill.cpu  { background: var(--accent); }
  .bar-fill.ram  { background: var(--green); }
  .bar-fill.disk { background: var(--yellow); }
  .bar-pct { width: 32px; text-align: right; }
  #main { display: flex; flex-direction: column; overflow: hidden; }
  #no-selection { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; color: var(--muted); gap: 10px; }
  #no-selection svg { opacity: .3; }
  #computer-panel { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
  #comp-header { padding: 14px 20px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 14px; flex-shrink: 0; }
  #comp-name { font-size: 18px; font-weight: 700; }
  #comp-meta { font-size: 12px; color: var(--muted); }
  #tabs { display: flex; border-bottom: 1px solid var(--border); padding: 0 20px; flex-shrink: 0; background: var(--panel); }
  .tab { padding: 10px 18px; cursor: pointer; font-size: 13px; font-weight: 500; color: var(--muted); border-bottom: 2px solid transparent; transition: color .15s; user-select: none; }
  .tab:hover { color: var(--text); }
  .tab.active { color: var(--accent); border-bottom-color: var(--accent); }
  #tab-content { flex: 1; overflow: hidden; display: flex; flex-direction: column; }
  #term-output { flex: 1; overflow-y: auto; padding: 14px 18px; font-family: 'Cascadia Code', 'Consolas', monospace; font-size: 13px; white-space: pre-wrap; word-break: break-all; }
  .term-entry { margin-bottom: 10px; }
  .term-cmd  { color: var(--accent); }
  .term-out  { color: var(--text); }
  .term-err  { color: var(--red); }
  #term-input-row { display: flex; align-items: center; padding: 10px 18px; border-top: 1px solid var(--border); gap: 8px; background: var(--panel); flex-shrink: 0; }
  #term-prompt { font-family: monospace; color: var(--accent); white-space: nowrap; font-size: 13px; }
  #term-input { flex: 1; background: transparent; border: none; outline: none; color: var(--text); font-family: 'Cascadia Code', 'Consolas', monospace; font-size: 13px; }
  #info-wrap { padding: 20px; overflow-y: auto; flex: 1; }
  .info-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 14px; }
  .info-card { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 16px; }
  .info-card-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .07em; margin-bottom: 6px; }
  .info-card-value { font-size: 22px; font-weight: 700; }
  .info-card-sub   { font-size: 12px; color: var(--muted); margin-top: 2px; }
  #files-wrap { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
  #files-path-bar { display: flex; align-items: center; gap: 8px; padding: 10px 18px; border-bottom: 1px solid var(--border); background: var(--panel); flex-shrink: 0; }
  #files-path { flex: 1; background: var(--bg); border: 1px solid var(--border); border-radius: 6px; padding: 6px 10px; color: var(--text); font-size: 13px; font-family: monospace; outline: none; }
  #drives-bar { display: flex; align-items: center; gap: 8px; padding: 8px 18px; border-bottom: 1px solid var(--border); background: var(--panel); flex-shrink: 0; flex-wrap: wrap; }
  .drive-chip { display: flex; align-items: center; gap: 6px; background: var(--hover); border: 1px solid var(--border); border-radius: 8px; padding: 5px 10px; cursor: pointer; font-size: 12px; transition: background .15s; min-width: 120px; }
  .drive-chip:hover { background: var(--border); }
  .drive-chip-label { font-weight: 600; font-family: monospace; }
  .drive-chip-bar { flex: 1; height: 3px; background: var(--border); border-radius: 2px; overflow: hidden; min-width: 40px; }
  .drive-chip-fill { height: 100%; border-radius: 2px; background: var(--accent); }
  .drive-chip-pct { font-size: 11px; color: var(--muted); }
  #files-search-bar { display: flex; align-items: center; gap: 8px; padding: 8px 18px; border-bottom: 1px solid var(--border); background: var(--bg); flex-shrink: 0; }
  #files-search-input { flex: 1; background: var(--panel); border: 1px solid var(--border); border-radius: 6px; padding: 6px 10px; color: var(--text); font-size: 13px; outline: none; }
  #files-search-input:focus { border-color: var(--accent); }
  .search-result-path { font-size: 11px; color: var(--muted); flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  #files-list { flex: 1; overflow-y: auto; padding: 6px 12px; }
  .file-row { display: flex; align-items: center; gap: 10px; padding: 6px 8px; border-radius: 6px; cursor: pointer; }
  .file-row:hover { background: var(--hover); }
  .file-row.hidden-file { opacity: .5; }
  .file-icon { font-size: 16px; width: 20px; text-align: center; flex-shrink: 0; }
  .file-name { flex: 1; font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .file-name.readonly { color: var(--muted); }
  .file-size { font-size: 12px; color: var(--muted); min-width: 70px; text-align: right; flex-shrink: 0; }
  .file-date { font-size: 12px; color: var(--muted); min-width: 130px; text-align: right; flex-shrink: 0; }
  .file-ext  { font-size: 11px; color: var(--muted); min-width: 50px; text-align: right; flex-shrink: 0; font-family: monospace; }
  .file-created { font-size: 12px; color: var(--muted); min-width: 130px; text-align: right; flex-shrink: 0; }
  .file-badges { display: flex; gap: 4px; flex-shrink: 0; }
  .badge { font-size: 10px; padding: 1px 5px; border-radius: 3px; font-weight: 600; }
  .badge-ro { background: #3a1a1a; color: var(--red); }
  .badge-hid { background: #1a2a3a; color: var(--muted); }
  #files-list.compact .file-date,
  #files-list.compact .file-ext,
  #files-list.compact .file-created,
  #files-list.compact .file-badges { display: none; }
  #proc-bar  { display: flex; align-items: center; gap: 10px; padding: 10px 18px; border-bottom: 1px solid var(--border); background: var(--panel); flex-shrink: 0; }
  #proc-search { flex: 1; background: var(--bg); border: 1px solid var(--border); border-radius: 6px; padding: 6px 10px; color: var(--text); font-size: 13px; outline: none; }
  #proc-table-wrap { flex: 1; overflow-y: auto; }
  #proc-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  #proc-table th { position: sticky; top: 0; background: var(--panel); border-bottom: 1px solid var(--border); padding: 8px 14px; text-align: left; font-size: 11px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: .06em; cursor: pointer; user-select: none; }
  #proc-table th:hover { color: var(--text); }
  #proc-table td { padding: 6px 14px; border-bottom: 1px solid #1c2128; }
  #proc-table tr:hover td { background: var(--hover); }
  .kill-btn { background: none; border: 1px solid var(--red); color: var(--red); border-radius: 4px; padding: 2px 8px; cursor: pointer; font-size: 11px; }
  .kill-btn:hover { background: var(--red); color: #fff; }
  #snap-wrap { flex: 1; display: flex; flex-direction: column; overflow: hidden; align-items: center; }
  #snap-bar  { display: flex; align-items: center; gap: 10px; padding: 10px 18px; border-bottom: 1px solid var(--border); background: var(--panel); width: 100%; flex-shrink: 0; }
  #snap-area { flex: 1; overflow: auto; padding: 20px; display: flex; align-items: flex-start; justify-content: center; }
  #snap-img  { max-width: 100%; border: 1px solid var(--border); border-radius: 8px; }
  .btn { background: var(--accent); color: #000; border: none; border-radius: 6px; padding: 7px 14px; font-size: 13px; font-weight: 600; cursor: pointer; transition: opacity .15s; }
  .btn:hover { opacity: .85; }
  .btn.sec { background: var(--hover); color: var(--text); border: 1px solid var(--border); }
  .btn.sec:hover { background: var(--border); }
  .btn:disabled { opacity: .4; cursor: default; }
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
  .empty { text-align: center; padding: 40px; color: var(--muted); }
</style>
</head>
<body>

<div id="header">
  <h1>RAT2</h1>
  <span style="color:var(--muted);font-size:13px;">Warehouse Remote Management</span>
  <button class="btn sec" id="update-all-btn" onclick="updateAll()" style="font-size:12px;padding:5px 12px;">Update All Agents</button>
  <div id="conn-status">
    <div class="dot off" id="ws-dot"></div>
    <span id="ws-label">Connecting...</span>
  </div>
</div>

<div id="layout">
  <div id="sidebar">
    <div id="sidebar-header">
      Computers
      <span id="agent-count">0</span>
    </div>
    <div id="agent-list"></div>
  </div>

  <div id="main">
    <div id="no-selection">
      <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
        <rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>
      </svg>
      <div style="font-size:16px;color:var(--text);font-weight:500;">Select a computer</div>
      <div>Choose a machine from the sidebar to manage it</div>
    </div>
    <div id="computer-panel" style="display:none;">
      <div id="comp-header">
        <div>
          <div id="comp-name"></div>
          <div id="comp-meta"></div>
        </div>
      </div>
      <div id="tabs">
        <div class="tab active" data-tab="terminal">Terminal</div>
        <div class="tab" data-tab="info">System Info</div>
        <div class="tab" data-tab="files">Files</div>
        <div class="tab" data-tab="processes">Processes</div>
        <div class="tab" data-tab="screenshot">Screenshot</div>
      </div>
      <div id="tab-content">

        <div id="tab-terminal" class="tab-pane" style="flex:1;display:flex;flex-direction:column;overflow:hidden;">
          <div id="term-output"></div>
          <div id="term-input-row">
            <span id="term-prompt">$</span>
            <input id="term-input" type="text" placeholder="Enter command..." autocomplete="off" spellcheck="false">
          </div>
        </div>

        <div id="tab-info" class="tab-pane" style="display:none;flex:1;overflow-y:auto;">
          <div id="info-wrap"><div class="info-grid" id="info-grid"></div></div>
        </div>

        <div id="tab-files" class="tab-pane" style="display:none;flex:1;display:none;flex-direction:column;overflow:hidden;">
          <div id="files-path-bar">
            <button class="btn sec" id="files-back" onclick="navBack()" title="Back" disabled style="padding:7px 10px;font-size:15px;">&#8592;</button>
            <button class="btn sec" onclick="navUp()" title="Up one folder" style="padding:7px 10px;font-size:15px;">&#8593;</button>
            <input id="files-path" type="text" value="C:\" onkeydown="if(event.key==='Enter')browseFiles()">
            <button class="btn sec" onclick="browseFiles()">Go</button>
            <button class="btn sec" id="detail-toggle" onclick="toggleDetail()" title="Toggle detail view">&#9776; Details</button>
            <button class="btn" onclick="document.getElementById('upload-input').click()">Upload</button>
            <input id="upload-input" type="file" multiple style="display:none" onchange="uploadFiles(this)">
          </div>
          <div id="drives-bar"></div>
          <div id="files-search-bar">
            <input id="files-search-input" type="text" placeholder="Search files and folders from current path..." onkeydown="if(event.key==='Enter')runSearch()">
            <button class="btn" onclick="runSearch()">Search</button>
            <button class="btn sec" id="search-clear" onclick="clearSearch()" style="display:none">Clear</button>
            <span id="search-status" style="font-size:12px;color:var(--muted);white-space:nowrap;"></span>
          </div>
          <div id="files-list" class="compact"><div class="empty">Enter a path above and click Go</div></div>
        </div>

        <div id="tab-processes" class="tab-pane" style="display:none;flex:1;flex-direction:column;overflow:hidden;">
          <div id="proc-bar">
            <input id="proc-search" type="text" placeholder="Filter processes..." oninput="filterProcs()">
            <button class="btn sec" onclick="loadProcesses()">Refresh</button>
          </div>
          <div id="proc-table-wrap">
            <table id="proc-table">
              <thead><tr>
                <th onclick="sortProcs('pid')">PID</th>
                <th onclick="sortProcs('name')">Name</th>
                <th onclick="sortProcs('cpu_percent')">CPU%</th>
                <th onclick="sortProcs('memory_percent')">RAM%</th>
                <th onclick="sortProcs('status')">Status</th>
                <th></th>
              </tr></thead>
              <tbody id="proc-body"></tbody>
            </table>
          </div>
        </div>

        <div id="tab-screenshot" class="tab-pane" style="display:none;flex:1;flex-direction:column;overflow:hidden;">
          <div id="snap-bar">
            <button class="btn" onclick="takeScreenshot()">Capture Screenshot</button>
            <span id="snap-time" style="color:var(--muted);font-size:12px;"></span>
            <a id="snap-dl" style="display:none;" class="btn sec" download="screenshot.jpg">Save Image</a>
          </div>
          <div id="snap-area"><div class="empty">Click "Capture Screenshot" to take a screenshot</div></div>
        </div>

      </div>
    </div>
  </div>
</div>

<script>
const state = {
  agents: {}, selected: null, ws: null,
  cmdHistory: [], histIdx: -1,
  allProcs: [], sortKey: 'cpu_percent', sortAsc: false,
  filesPath: '', fileHistory: [],
};

function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  state.ws = new WebSocket(`${proto}://${location.host}/ws/ui`);
  state.ws.onopen  = () => setWsStatus(true);
  state.ws.onclose = () => { setWsStatus(false); setTimeout(connectWS, 3000); };
  state.ws.onmessage = e => handleMsg(JSON.parse(e.data));
}

function setWsStatus(ok) {
  document.getElementById('ws-dot').className   = `dot${ok ? '' : ' off'}`;
  document.getElementById('ws-label').textContent = ok ? 'Connected' : 'Reconnecting...';
}

function handleMsg(msg) {
  if (msg.type === 'init') {
    state.agents = {};
    msg.agents.forEach(a => { state.agents[a.id] = a; });
    renderSidebar();
  } else if (msg.type === 'agent_up') {
    state.agents[msg.agent.id] = msg.agent;
    renderSidebar();
  } else if (msg.type === 'agent_down') {
    delete state.agents[msg.agent_id];
    if (state.selected === msg.agent_id) clearSelection();
    renderSidebar();
  } else if (msg.type === 'stats') {
    if (state.agents[msg.agent_id]) {
      state.agents[msg.agent_id].cpu  = msg.cpu;
      state.agents[msg.agent_id].ram  = msg.ram;
      state.agents[msg.agent_id].disk = msg.disk;
      updateSidebarCard(msg.agent_id);
      if (state.selected === msg.agent_id) updateInfoCards();
    }
  }
}

function renderSidebar() {
  const list  = document.getElementById('agent-list');
  const count = Object.keys(state.agents).length;
  document.getElementById('agent-count').textContent = count;
  list.innerHTML = '';
  if (count === 0) {
    list.innerHTML = '<div class="empty" style="font-size:12px;padding:20px 10px;">No agents connected.<br>Run: python rat2.py agent</div>';
    return;
  }
  Object.values(state.agents).sort((a,b)=>a.hostname.localeCompare(b.hostname)).forEach(a => list.appendChild(buildCard(a)));
}

function buildCard(a) {
  const card = document.createElement('div');
  card.className = `agent-card${state.selected===a.id ? ' selected' : ''}`;
  card.id = `card-${a.id}`;
  card.onclick = () => selectAgent(a.id);
  card.innerHTML = `
    <div class="agent-top">
      <div class="status-dot on"></div>
      <div class="agent-name">${esc(a.hostname)}</div>
      ${a.outdated ? `<button class="update-btn" id="upd-${a.id}" onclick="event.stopPropagation();updateAgent('${a.id}')" title="Update available">&#8593; Update</button>` : ''}
    </div>
    ${a.location ? `<div class="agent-loc">${esc(a.location)}</div>` : ''}
    <div class="agent-bars">
      <div class="bar-row"><span class="bar-label">CPU</span><div class="bar-track"><div class="bar-fill cpu" id="bar-cpu-${a.id}" style="width:${a.cpu}%"></div></div><span class="bar-pct" id="pct-cpu-${a.id}">${a.cpu.toFixed(0)}%</span></div>
      <div class="bar-row"><span class="bar-label">RAM</span><div class="bar-track"><div class="bar-fill ram" id="bar-ram-${a.id}" style="width:${a.ram}%"></div></div><span class="bar-pct" id="pct-ram-${a.id}">${a.ram.toFixed(0)}%</span></div>
      <div class="bar-row"><span class="bar-label">Disk</span><div class="bar-track"><div class="bar-fill disk" id="bar-disk-${a.id}" style="width:${a.disk}%"></div></div><span class="bar-pct" id="pct-disk-${a.id}">${a.disk.toFixed(0)}%</span></div>
    </div>`;
  return card;
}

function updateSidebarCard(id) {
  const a = state.agents[id];
  if (!a) return;
  for (const [k,v] of [['cpu',a.cpu],['ram',a.ram],['disk',a.disk]]) {
    const bar = document.getElementById(`bar-${k}-${id}`);
    const pct = document.getElementById(`pct-${k}-${id}`);
    if (bar) bar.style.width = v + '%';
    if (pct) pct.textContent = v.toFixed(0) + '%';
  }
}

function selectAgent(id) {
  state.selected = id;
  document.getElementById('no-selection').style.display = 'none';
  document.getElementById('computer-panel').style.display = 'flex';
  document.querySelectorAll('.agent-card').forEach(c => c.classList.remove('selected'));
  const card = document.getElementById(`card-${id}`);
  if (card) card.classList.add('selected');
  const a = state.agents[id];
  document.getElementById('comp-name').textContent = a.hostname;
  document.getElementById('comp-meta').textContent = `${a.os}  |  ${a.ip}  |  ${a.location || 'No location'}  |  Connected ${new Date(a.connected_at).toLocaleString()}`;
  document.getElementById('term-prompt').textContent = `${a.hostname}>`;
  document.getElementById('term-output').innerHTML = '';
  state.fileHistory = [];
  state.filesPath   = '';
  updateInfoCards();
  showTab('terminal');
}

function clearSelection() {
  state.selected = null;
  document.getElementById('no-selection').style.display = '';
  document.getElementById('computer-panel').style.display = 'none';
}

document.querySelectorAll('.tab').forEach(t => { t.onclick = () => showTab(t.dataset.tab); });

function showTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab===name));
  document.querySelectorAll('.tab-pane').forEach(p => {
    const show = p.id === `tab-${name}`;
    p.style.display = show ? 'flex' : 'none';
    p.style.flexDirection = 'column';
  });
  if (name === 'processes') loadProcesses();
  if (name === 'files') {
    const path = state.agents[state.selected]?.os?.startsWith('Windows') ? 'C:\\' : '/';
    document.getElementById('files-path').value = path;
    loadDrives();
    browseFiles();
  }
}

const termInput  = document.getElementById('term-input');
const termOutput = document.getElementById('term-output');

termInput.addEventListener('keydown', async e => {
  if (e.key === 'Enter') {
    const cmd = termInput.value.trim();
    if (!cmd) return;
    state.cmdHistory.unshift(cmd);
    state.histIdx = -1;
    termInput.value = '';
    termInput.disabled = true;
    appendTerm('cmd', `${state.agents[state.selected]?.hostname || '$'}> ${cmd}`);
    try {
      const r = await api('POST', `/api/agents/${state.selected}/exec`, { cmd });
      if (r.stdout) appendTerm('out', r.stdout);
      if (r.stderr) appendTerm('err', r.stderr);
      if (!r.stdout && !r.stderr) appendTerm('out', '(no output)');
    } catch(err) { appendTerm('err', `Error: ${err.message}`); }
    termInput.disabled = false;
    termInput.focus();
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    state.histIdx = Math.min(state.histIdx+1, state.cmdHistory.length-1);
    termInput.value = state.cmdHistory[state.histIdx] || '';
  } else if (e.key === 'ArrowDown') {
    e.preventDefault();
    state.histIdx = Math.max(state.histIdx-1, -1);
    termInput.value = state.histIdx >= 0 ? state.cmdHistory[state.histIdx] : '';
  }
});

function appendTerm(type, text) {
  const div = document.createElement('div');
  div.className = `term-entry term-${type}`;
  div.textContent = text;
  termOutput.appendChild(div);
  termOutput.scrollTop = termOutput.scrollHeight;
}

function updateInfoCards() {
  const a = state.agents[state.selected];
  if (!a) return;
  document.getElementById('info-grid').innerHTML = [
    ['Hostname',   a.hostname,                                ''],
    ['OS',         a.os,                                      ''],
    ['IP Address', a.ip,                                      ''],
    ['Location',   a.location || '-',                         ''],
    ['CPU Cores',  a.cpu_count,                               'logical cores'],
    ['Total RAM',  a.total_ram_gb + ' GB',                    ''],
    ['CPU Usage',  a.cpu.toFixed(1) + '%',                    'live'],
    ['RAM Usage',  a.ram.toFixed(1) + '%',                    'live'],
    ['Disk Usage', a.disk.toFixed(1) + '%',                   'live'],
    ['Connected',  new Date(a.connected_at).toLocaleString(), ''],
  ].map(([label, val, sub]) => `
    <div class="info-card">
      <div class="info-card-label">${label}</div>
      <div class="info-card-value">${esc(String(val))}</div>
      ${sub ? `<div class="info-card-sub">${esc(sub)}</div>` : ''}
    </div>`).join('');
}

function toggleDetail() {
  const list = document.getElementById('files-list');
  const btn  = document.getElementById('detail-toggle');
  const detailed = list.classList.toggle('compact') === false;
  btn.style.color = detailed ? 'var(--accent)' : '';
}

async function loadDrives() {
  try {
    const r = await api('GET', `/api/agents/${state.selected}/drives`);
    renderDrives(r.drives || []);
  } catch(e) {}
}

function renderDrives(drives) {
  const bar = document.getElementById('drives-bar');
  if (!drives.length) { bar.innerHTML = ''; return; }
  bar.innerHTML = '<span style="font-size:11px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.06em;">Drives</span>' +
    drives.map(d => {
      const label = d.mountpoint.replace(/\\$/, '') || d.device;
      const free  = fmtSize(d.free), total = fmtSize(d.total);
      return `<div class="drive-chip" data-path="${esc(d.mountpoint)}" title="${esc(d.fstype)} - ${free} free of ${total}">
        <span class="drive-chip-label">${esc(label)}</span>
        <div class="drive-chip-bar"><div class="drive-chip-fill" style="width:${d.percent}%"></div></div>
        <span class="drive-chip-pct">${d.percent.toFixed(0)}%</span>
      </div>`;
    }).join('');
  bar.querySelectorAll('.drive-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      document.getElementById('files-path').value = chip.dataset.path;
      browseFiles(true);
    });
  });
}

async function browseFiles(pushHistory = false) {
  const path = document.getElementById('files-path').value.trim();
  if (pushHistory && state.filesPath && state.filesPath !== path)
    state.fileHistory.push(state.filesPath);
  state.filesPath = path;
  document.getElementById('files-path').value = path;
  document.getElementById('files-back').disabled = state.fileHistory.length === 0;
  document.getElementById('files-list').innerHTML = '<div class="empty">Loading...</div>';
  try {
    const r = await api('GET', `/api/agents/${state.selected}/files?path=${encodeURIComponent(path)}`);
    if (r.error) { document.getElementById('files-list').innerHTML = `<div class="empty">${esc(r.error)}</div>`; return; }
    renderFiles(r.entries || []);
  } catch(e) {
    document.getElementById('files-list').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderFiles(entries) {
  const list = document.getElementById('files-list');
  if (!entries.length) { list.innerHTML = '<div class="empty">Empty folder</div>'; return; }
  list.innerHTML = entries.map(f => {
    const badges = [
      f.readonly ? '<span class="badge badge-ro">RO</span>' : '',
      f.hidden   ? '<span class="badge badge-hid">HID</span>' : '',
    ].join('');
    return `<div class="file-row${f.hidden ? ' hidden-file' : ''}" data-name="${esc(f.name)}" data-isdir="${f.is_dir}">
      <span class="file-icon">${f.is_dir ? '&#128193;' : fileIcon(f.ext)}</span>
      <span class="file-name${f.readonly ? ' readonly' : ''}">${esc(f.name)}</span>
      <span class="file-size">${f.is_dir ? '' : fmtSize(f.size)}</span>
      <span class="file-ext">${esc(f.ext || (f.is_dir ? 'folder' : ''))}</span>
      <span class="file-date">${new Date(f.modified*1000).toLocaleString()}</span>
      <span class="file-created">${new Date(f.created*1000).toLocaleString()}</span>
      <span class="file-badges">${badges}</span>
    </div>`;
  }).join('');
}

function fileIcon(ext) {
  const map = {
    '.pdf':'&#128196;','.doc':'&#128221;','.docx':'&#128221;','.xls':'&#128202;','.xlsx':'&#128202;',
    '.zip':'&#128476;','.rar':'&#128476;','.7z':'&#128476;',
    '.jpg':'&#128444;','.jpeg':'&#128444;','.png':'&#128444;','.gif':'&#128444;','.bmp':'&#128444;',
    '.mp4':'&#127916;','.mov':'&#127916;','.avi':'&#127916;','.mkv':'&#127916;',
    '.mp3':'&#127925;','.wav':'&#127925;','.flac':'&#127925;',
    '.py':'&#128013;','.js':'&#128221;','.html':'&#127760;','.css':'&#127912;',
    '.exe':'&#9881;','.bat':'&#9881;','.ps1':'&#9881;',
    '.txt':'&#128195;','.log':'&#128195;','.csv':'&#128195;','.json':'&#128195;',
  };
  return map[ext] || '&#128196;';
}

document.getElementById('files-list').addEventListener('click', e => {
  const row = e.target.closest('.file-row');
  if (!row) return;
  if (row.dataset.searchResult) {
    if (e.target.closest('.sr-download')) downloadFileByPath(row.dataset.path);
    else { document.getElementById('files-path').value = row.dataset.parent; clearSearch(); browseFiles(true); }
  } else {
    if (row.dataset.isdir === 'true') navTo(row.dataset.name);
    else downloadFile(state.filesPath, row.dataset.name);
  }
});

async function runSearch() {
  const q = document.getElementById('files-search-input').value.trim();
  if (!q) return;
  const path = state.filesPath || document.getElementById('files-path').value.trim();
  const list  = document.getElementById('files-list');
  const status = document.getElementById('search-status');
  document.getElementById('search-clear').style.display = '';
  status.textContent = 'Searching...';
  list.innerHTML = '<div class="empty">Searching...</div>';
  try {
    const r = await api('GET', `/api/agents/${state.selected}/search?path=${encodeURIComponent(path)}&query=${encodeURIComponent(q)}`);
    if (r.error) { list.innerHTML = `<div class="empty">${esc(r.error)}</div>`; status.textContent = ''; return; }
    const results = r.results || [];
    status.textContent = `${results.length} result${results.length!==1?'s':''}${r.truncated?' (first 500)':''}`;
    if (!results.length) { list.innerHTML = '<div class="empty">No results found</div>'; return; }
    list.innerHTML = results.map(f => `
      <div class="file-row" data-search-result="1" data-path="${esc(f.path)}" data-parent="${esc(f.parent)}" data-isdir="${f.is_dir}">
        <span class="file-icon">${f.is_dir ? '&#128193;' : fileIcon(f.ext)}</span>
        <span class="file-name" style="flex:0 0 auto;max-width:220px;">${esc(f.name)}</span>
        <span class="search-result-path" title="${esc(f.path)}">${esc(f.path)}</span>
        <span class="file-size">${f.is_dir ? '' : fmtSize(f.size)}</span>
        <span class="file-date">${new Date(f.modified*1000).toLocaleDateString()}</span>
        ${!f.is_dir ? '<button class="btn sec sr-download" style="padding:3px 8px;font-size:11px;flex-shrink:0">Save</button>' : ''}
      </div>`).join('');
  } catch(e) { list.innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`; status.textContent = ''; }
}

function clearSearch() {
  document.getElementById('files-search-input').value = '';
  document.getElementById('search-clear').style.display = 'none';
  document.getElementById('search-status').textContent = '';
  browseFiles(false);
}

async function downloadFileByPath(path) {
  try {
    const r = await api('POST', `/api/agents/${state.selected}/download`, { path });
    if (r.error) { alert('Download error: ' + r.error); return; }
    const bytes = Uint8Array.from(atob(r.data), c => c.charCodeAt(0));
    const url = URL.createObjectURL(new Blob([bytes]));
    const a = document.createElement('a');
    a.href = url; a.download = r.filename; a.click();
    URL.revokeObjectURL(url);
  } catch(e) { alert('Download failed: ' + e.message); }
}

function navTo(name) {
  const cur = document.getElementById('files-path').value;
  const sep = cur.includes('\\') ? '\\' : '/';
  document.getElementById('files-path').value = cur.replace(/[/\\]$/, '') + sep + name;
  browseFiles(true);
}

function navUp() {
  const cur = document.getElementById('files-path').value;
  const sep = cur.includes('\\') ? '\\' : '/';
  const parts = cur.replace(/[/\\]+$/, '').split(/[/\\]/);
  parts.pop();
  document.getElementById('files-path').value = parts.join(sep) || sep;
  browseFiles(true);
}

function navBack() {
  if (!state.fileHistory.length) return;
  const prev = state.fileHistory.pop();
  state.filesPath = prev;
  document.getElementById('files-path').value = prev;
  document.getElementById('files-back').disabled = state.fileHistory.length === 0;
  browseFiles(false);
}

async function downloadFile(dir, name) {
  const sep  = dir.includes('\\') ? '\\' : '/';
  const path = dir.replace(/[/\\]$/, '') + sep + name;
  try {
    const r = await api('POST', `/api/agents/${state.selected}/download`, { path });
    if (r.error) { alert('Download error: ' + r.error); return; }
    const bytes = Uint8Array.from(atob(r.data), c => c.charCodeAt(0));
    const url = URL.createObjectURL(new Blob([bytes]));
    const a = document.createElement('a');
    a.href = url; a.download = r.filename || name; a.click();
    URL.revokeObjectURL(url);
  } catch(e) { alert('Download failed: ' + e.message); }
}

async function uploadFiles(input) {
  const files = Array.from(input.files);
  if (!files.length) return;
  const dir = state.filesPath;
  const sep = dir.includes('\\') ? '\\' : '/';
  let done = 0, failed = 0;
  for (const file of files) {
    const path = dir.replace(/[/\\]$/, '') + sep + file.name;
    try {
      const data = await new Promise((res, rej) => {
        const reader = new FileReader();
        reader.onload  = e => res(e.target.result.split(',')[1]);
        reader.onerror = rej;
        reader.readAsDataURL(file);
      });
      const r = await api('POST', `/api/agents/${state.selected}/upload`, { path, data });
      if (r.error) { failed++; } else done++;
    } catch(e) { failed++; }
  }
  input.value = '';
  if (failed) alert(`${done} uploaded, ${failed} failed.`);
  browseFiles(false);
}

async function loadProcesses() {
  document.getElementById('proc-body').innerHTML = '<tr><td colspan="6" style="text-align:center;padding:20px;color:var(--muted)">Loading...</td></tr>';
  try {
    const r = await api('GET', `/api/agents/${state.selected}/processes`);
    state.allProcs = r.processes || [];
    renderProcs();
  } catch(e) {
    document.getElementById('proc-body').innerHTML = `<tr><td colspan="6" style="color:var(--red);padding:20px">${esc(e.message)}</td></tr>`;
  }
}

function filterProcs() { renderProcs(); }

function sortProcs(key) {
  if (state.sortKey === key) state.sortAsc = !state.sortAsc;
  else { state.sortKey = key; state.sortAsc = false; }
  renderProcs();
}

function renderProcs() {
  const q = document.getElementById('proc-search').value.toLowerCase();
  let procs = state.allProcs.filter(p => !q || p.name?.toLowerCase().includes(q));
  const key = state.sortKey;
  procs.sort((a,b) => {
    const av = a[key]??0, bv = b[key]??0;
    return state.sortAsc ? (av>bv?1:-1) : (av<bv?1:-1);
  });
  document.getElementById('proc-body').innerHTML = procs.slice(0,200).map(p => `
    <tr>
      <td>${p.pid}</td>
      <td>${esc(p.name||'')}</td>
      <td>${(p.cpu_percent||0).toFixed(1)}%</td>
      <td>${(p.memory_percent||0).toFixed(1)}%</td>
      <td>${esc(p.status||'')}</td>
      <td><button class="kill-btn" onclick="killProc(${p.pid},this)">Kill</button></td>
    </tr>`).join('');
}

async function killProc(pid, btn) {
  if (!confirm(`Kill process ${pid}?`)) return;
  btn.disabled = true;
  try {
    const r = await api('POST', `/api/agents/${state.selected}/kill`, { pid });
    if (r.error) alert('Error: ' + r.error);
    else loadProcesses();
  } catch(e) { alert('Error: ' + e.message); btn.disabled = false; }
}

async function takeScreenshot() {
  const area = document.getElementById('snap-area');
  area.innerHTML = '<div class="empty">Capturing...</div>';
  try {
    const r = await api('POST', `/api/agents/${state.selected}/screenshot`);
    if (r.error) { area.innerHTML = `<div class="empty">${esc(r.error)}</div>`; return; }
    const src = `data:image/jpeg;base64,${r.image}`;
    area.innerHTML = `<img id="snap-img" src="${src}">`;
    document.getElementById('snap-time').textContent = 'Captured at ' + new Date().toLocaleTimeString();
    const dl = document.getElementById('snap-dl');
    dl.href = src; dl.style.display = '';
  } catch(e) { area.innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`; }
}

async function api(method, url, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(url, opts);
  if (!r.ok) { const t = await r.text(); throw new Error(t || r.statusText); }
  return r.json();
}

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function fmtSize(bytes) {
  if (bytes < 1024)       return bytes + ' B';
  if (bytes < 1048576)    return (bytes/1024).toFixed(1) + ' KB';
  if (bytes < 1073741824) return (bytes/1048576).toFixed(1) + ' MB';
  return (bytes/1073741824).toFixed(1) + ' GB';
}

async function updateAgent(id) {
  const btn = document.getElementById(`upd-${id}`);
  if (btn) { btn.disabled = true; btn.textContent = 'Updating...'; }
  try {
    const r = await api('POST', `/api/agents/${id}/update`);
    if (r.error) { alert(`Update failed: ${r.error}`); if (btn) { btn.disabled=false; btn.textContent='Update'; } }
    else if (r.status === 'up_to_date') { alert('Already up to date.'); if (btn) btn.disabled=false; }
  } catch(e) { alert(`Update error: ${e.message}`); if (btn) { btn.disabled=false; btn.textContent='Update'; } }
}

async function updateAll() {
  const btn = document.getElementById('update-all-btn');
  btn.disabled = true; btn.textContent = 'Updating...';
  try {
    const r = await api('POST', '/api/agents/update-all');
    const updated  = Object.values(r).filter(v => v.status==='updated').length;
    const uptodate = Object.values(r).filter(v => v.status==='up_to_date').length;
    const failed   = Object.values(r).filter(v => v.error).length;
    alert(`Update complete.\nUpdated: ${updated}  Already current: ${uptodate}  Failed: ${failed}`);
  } catch(e) { alert(`Error: ${e.message}`); }
  btn.disabled = false; btn.textContent = 'Update All Agents';
}

connectWS();
document.getElementById('term-input').focus();
</script>
</body>
</html>"""

    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        return DASHBOARD

    if __name__ == "__main__":
        print("=" * 60)
        print("  RAT2 Manager")
        print(f"  Dashboard : http://localhost:{PORT}")
        print(f"  Agent URL : ws://<this-machine-ip>:{PORT}/ws/agent")
        print(f"  Secret    : {SECRET_KEY}")
        print("=" * 60)
        uvicorn.run(app, host=HOST, port=PORT, log_level="info")


# ==============================================================================
#  AGENT
# ==============================================================================
elif _MODE == "agent":
    import psutil
    import websockets

    FROZEN   = getattr(sys, "frozen", False)
    BASE_DIR = Path(sys.executable).parent if FROZEN else THIS_FILE.parent
    ID_FILE  = BASE_DIR / ".rat2_agent_id"

    def load_or_create_id() -> str:
        if ID_FILE.exists():
            aid = ID_FILE.read_text().strip()
            if aid:
                return aid
        aid = str(uuid.uuid4())
        ID_FILE.write_text(aid)
        return aid

    AGENT_ID = load_or_create_id()

    def local_ip() -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return socket.gethostbyname(socket.gethostname())

    def self_hash() -> str:
        path = sys.executable if FROZEN else THIS_FILE
        return file_hash(Path(path))

    def registration_msg() -> dict:
        return {
            "type":       "register",
            "secret":     SECRET_KEY,
            "agent_id":   AGENT_ID,
            "hostname":   socket.gethostname(),
            "os":         f"{platform.system()} {platform.release()} {platform.version()[:30]}",
            "ip":         local_ip(),
            "location":   LOCATION,
            "cpu_count":  psutil.cpu_count(logical=True),
            "total_ram":  psutil.virtual_memory().total,
            "agent_hash": self_hash(),
        }

    def manager_http_base() -> str:
        url = MANAGER_URL.replace("wss://", "https://").replace("ws://", "http://")
        return url.split("/ws/agent")[0]

    def _exec(cmd: str) -> dict:
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                                    timeout=55, cwd=os.path.expanduser("~"))
            return {"stdout": result.stdout, "stderr": result.stderr, "returncode": result.returncode}
        except subprocess.TimeoutExpired:
            return {"stdout": "", "stderr": "Command timed out (55s limit)", "returncode": -1}
        except Exception as e:
            return {"stdout": "", "stderr": str(e), "returncode": -1}

    def _screenshot() -> dict:
        try:
            from PIL import ImageGrab
            img = ImageGrab.grab()
            img.thumbnail((1920, 1080))
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=55)
            return {"image": base64.b64encode(buf.getvalue()).decode()}
        except ImportError:
            return {"error": "Pillow not installed - run: pip install Pillow"}
        except Exception as e:
            return {"error": str(e)}

    def _processes() -> dict:
        procs = []
        for p in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent", "status"]):
            try:
                info = p.info
                procs.append({
                    "pid":            info["pid"],
                    "name":           info["name"] or "",
                    "cpu_percent":    round(info.get("cpu_percent") or 0.0, 2),
                    "memory_percent": round(info.get("memory_percent") or 0.0, 2),
                    "status":         info.get("status") or "",
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        procs.sort(key=lambda p: p["cpu_percent"], reverse=True)
        return {"processes": procs[:300]}

    def _kill(pid: int) -> dict:
        try:
            psutil.Process(pid).terminate()
            return {"success": True, "pid": pid}
        except psutil.NoSuchProcess:
            return {"error": f"No process with PID {pid}"}
        except psutil.AccessDenied:
            return {"error": f"Access denied to kill PID {pid}"}
        except Exception as e:
            return {"error": str(e)}

    def _ls(path: str) -> dict:
        try:
            entries = []
            for entry in os.scandir(path):
                try:
                    stat   = entry.stat()
                    is_dir = entry.is_dir()
                    name   = entry.name
                    ext    = "" if is_dir else os.path.splitext(name)[1].lower()
                    hidden = (name.startswith(".") if platform.system() != "Windows"
                              else bool(getattr(stat, "st_file_attributes", 0) & 2))
                    entries.append({
                        "name":     name,
                        "is_dir":   is_dir,
                        "size":     0 if is_dir else stat.st_size,
                        "modified": stat.st_mtime,
                        "created":  stat.st_ctime,
                        "ext":      ext,
                        "hidden":   hidden,
                        "readonly": not os.access(entry.path, os.W_OK),
                    })
                except Exception:
                    pass
            entries.sort(key=lambda e: (not e["is_dir"], e["name"].lower()))
            return {"entries": entries}
        except PermissionError:
            return {"error": f"Permission denied: {path}"}
        except FileNotFoundError:
            return {"error": f"Path not found: {path}"}
        except Exception as e:
            return {"error": str(e)}

    def _search(path: str, query: str, max_results: int = 500) -> dict:
        results = []
        q = query.lower()
        try:
            for root, dirs, files in os.walk(path, onerror=lambda e: None):
                for is_dir, names in ((True, dirs), (False, files)):
                    for name in names:
                        if q in name.lower():
                            full = os.path.join(root, name)
                            try:
                                stat = os.stat(full)
                                results.append({
                                    "name":     name,
                                    "path":     full,
                                    "parent":   root,
                                    "is_dir":   is_dir,
                                    "size":     0 if is_dir else stat.st_size,
                                    "modified": stat.st_mtime,
                                    "ext":      "" if is_dir else os.path.splitext(name)[1].lower(),
                                })
                            except Exception:
                                pass
                if len(results) >= max_results:
                    return {"results": results, "truncated": True}
        except Exception as e:
            return {"error": str(e)}
        return {"results": results, "truncated": False}

    def _drives() -> dict:
        drives = []
        for part in psutil.disk_partitions(all=False):
            try:
                usage = psutil.disk_usage(part.mountpoint)
                drives.append({
                    "device": part.device, "mountpoint": part.mountpoint,
                    "fstype": part.fstype, "total": usage.total,
                    "used": usage.used, "free": usage.free, "percent": usage.percent,
                })
            except Exception:
                drives.append({
                    "device": part.device, "mountpoint": part.mountpoint,
                    "fstype": part.fstype, "total": 0, "used": 0, "free": 0, "percent": 0,
                })
        return {"drives": drives}

    def _download(path: str) -> dict:
        try:
            with open(path, "rb") as f:
                data = f.read()
            return {"data": base64.b64encode(data).decode(), "filename": os.path.basename(path)}
        except PermissionError:
            return {"error": f"Permission denied: {path}"}
        except FileNotFoundError:
            return {"error": f"File not found: {path}"}
        except Exception as e:
            return {"error": str(e)}

    def _upload(path: str, data: str) -> dict:
        try:
            with open(path, "wb") as f:
                f.write(base64.b64decode(data))
            return {"success": True, "path": path}
        except Exception as e:
            return {"error": str(e)}

    def _update() -> dict:
        try:
            update_url = manager_http_base() + "/rat2.py"
            with urllib.request.urlopen(update_url, timeout=30) as resp:
                new_code = resp.read()
            new_hash = hashlib.sha256(new_code).hexdigest()
            if new_hash == self_hash():
                return {"status": "up_to_date"}
            compile(new_code, "rat2.py", "exec")  # reject if syntax error
            with open(THIS_FILE, "wb") as f:
                f.write(new_code)
            def restart():
                time.sleep(1)
                subprocess.Popen([sys.executable, str(THIS_FILE), "agent"])
                os._exit(0)
            threading.Thread(target=restart, daemon=True).start()
            return {"status": "updated", "hash": new_hash[:12]}
        except Exception as e:
            return {"error": str(e)}

    def _heartbeat_stats() -> dict:
        disk_path = "C:\\" if platform.system() == "Windows" else "/"
        try:
            disk_pct = psutil.disk_usage(disk_path).percent
        except Exception:
            disk_pct = 0.0
        return {
            "type": "heartbeat",
            "cpu":  psutil.cpu_percent(interval=1),
            "ram":  psutil.virtual_memory().percent,
            "disk": disk_pct,
        }

    async def heartbeat_loop(ws):
        loop = asyncio.get_running_loop()
        while True:
            try:
                stats = await loop.run_in_executor(None, _heartbeat_stats)
                await ws.send(json.dumps(stats))
            except Exception:
                break
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def handle_command(ws, msg: dict):
        cmd_id = msg.get("cmd_id")
        mtype  = msg.get("type")
        loop   = asyncio.get_running_loop()
        dispatch = {
            "exec":       lambda: _exec(msg["cmd"]),
            "screenshot": lambda: _screenshot(),
            "processes":  lambda: _processes(),
            "kill":       lambda: _kill(msg["pid"]),
            "ls":         lambda: _ls(msg["path"]),
            "drives":     lambda: _drives(),
            "search":     lambda: _search(msg["path"], msg["query"]),
            "update":     lambda: _update(),
            "download":   lambda: _download(msg["path"]),
            "upload":     lambda: _upload(msg["path"], msg["data"]),
        }
        handler = dispatch.get(mtype)
        result  = await loop.run_in_executor(None, handler) if handler else {"error": f"Unknown command: {mtype}"}
        result["type"]   = "result"
        result["cmd_id"] = cmd_id
        await ws.send(json.dumps(result))

    async def message_loop(ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
                asyncio.create_task(handle_command(ws, msg))
            except Exception as e:
                print(f"[!] Error: {e}")

    async def run_agent():
        backoff = 5
        while True:
            try:
                print(f"[*] Connecting to {MANAGER_URL}...")
                async with websockets.connect(
                    MANAGER_URL, ping_interval=20, ping_timeout=10,
                    max_size=50 * 1024 * 1024,
                ) as ws:
                    print("[+] Connected. Registering...")
                    await ws.send(json.dumps(registration_msg()))
                    print(f"[+] Registered as {socket.gethostname()} (ID: {AGENT_ID[:8]}...)")
                    backoff = 5
                    hb_task  = asyncio.create_task(heartbeat_loop(ws))
                    msg_task = asyncio.create_task(message_loop(ws))
                    done, pending_tasks = await asyncio.wait(
                        [hb_task, msg_task], return_when=asyncio.FIRST_COMPLETED
                    )
                    for t in pending_tasks:
                        t.cancel()
                        try: await t
                        except asyncio.CancelledError: pass
            except websockets.exceptions.InvalidStatus as e:
                code = getattr(getattr(e, "response", None), "status_code", None)
                if code == 4001:
                    print("[!] AUTHORIZATION FAILED - check SECRET_KEY")
                    sys.exit(1)
                print(f"[!] Connection refused (status {code})")
            except (ConnectionRefusedError, OSError) as e:
                print(f"[!] Cannot reach manager: {e}")
            except Exception as e:
                print(f"[!] Error: {type(e).__name__}: {e}")
            print(f"[*] Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    if __name__ == "__main__":
        if "YOUR-MANAGER-IP" in MANAGER_URL:
            print("\n[!] MANAGER_URL is not configured.")
            print("\n    Edit rat2.py and set MANAGER_URL, or use an environment variable:")
            print("      set RAT2_URL=ws://192.168.1.10:8080/ws/agent")
            input("\nPress Enter to close...")
            sys.exit(1)
        print("=" * 55)
        print("  RAT2 Agent")
        print(f"  Manager : {MANAGER_URL}")
        print(f"  Location: {LOCATION}")
        print(f"  ID      : {AGENT_ID[:8]}...")
        print("=" * 55)
        try:
            asyncio.run(run_agent())
        except KeyboardInterrupt:
            print("\n[*] Agent stopped.")
