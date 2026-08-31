#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股即時熱力圖 — 中繼伺服器（給要分享給朋友看的公開版用）

這支程式部署在雲端主機上（例如 Render、Railway），完全不知道你的永豐帳號、
API 金鑰、密碼是什麼——它只做一件事：接收你自己電腦上 live_quote_server.py
每 5 秒主動「推」過來的、已經算好的族群報價數字（族群名稱、平均漲跌、成交金額…），
存在記憶體裡，然後轉發給所有正在看這個公開網址的人。

安全設計重點：
  - 你的電腦（跑 live_quote_server.py 那台）永遠只是「主動送資料出去」的一方，
    不接受任何人從外面連進來，也不會對外開放任何連接你永豐帳號的通道。
  - 這支中繼伺服器完全沒有、也不需要你的 API 金鑰／密碼；就算這台雲端主機被入侵，
    最多也只洩漏「族群漲跌幅」這種本來就公開的行情數字，碰不到你的交易帳號。
  - 只有知道 RELAY_TOKEN（一組你自己設定的密碼）的人才能把資料推進來，
    避免有人亂送假資料進來洗版；但「看」這個公開網址不需要任何密碼——
    這是設計上刻意的，因為這是要給朋友看的公開分享網址，不是你自己的帳號系統。

部署方式：見同一個資料夾的 README.md。
"""

import os
import hmac
import json
import asyncio
import threading
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# RELAY_TOKEN 一定要用環境變數設定（部署平台的 Environment Variables 那一頁），
# 不要寫死在程式碼裡——這樣就算這份程式碼被公開看到（例如放在 GitHub 上），
# 密碼本身也不會外流。沒設定的話，這支伺服器會拒絕所有推送，等於「鎖住」，
# 不會有「忘記設定結果變成沒有密碼保護」這種事。
RELAY_TOKEN = os.environ.get("RELAY_TOKEN", "").strip()

app = FastAPI()

state_lock = threading.Lock()
latest_payload = None       # 最後一次收到、完整的 {"type":"heatmap",...} payload
last_ingest_at = None       # datetime，最後一次成功收到推送的時間（給前端判斷「資料是不是太舊了」用）

watchers = set()            # 正在看這個公開網址的瀏覽器連線，每個一個 asyncio.Queue
watchers_lock = threading.Lock()
MAIN_LOOP = None


def check_token(req_token):
    if not RELAY_TOKEN:
        return False
    if not req_token:
        return False
    # hmac.compare_digest：常數時間比較，避免用「字串一個字一個字比對、比對時間長短」
    # 這種側channel去猜出正確token的理論風險，雖然這裡風險不高，但養成習慣沒有壞處。
    return hmac.compare_digest(req_token, RELAY_TOKEN)


@app.on_event("startup")
async def on_startup():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_event_loop()
    if not RELAY_TOKEN:
        print("警告：沒有設定 RELAY_TOKEN 這個環境變數，/ingest 會拒絕所有推送。"
              "請到部署平台的 Environment Variables 設定一個你自己的密碼字串。")
    else:
        print("中繼伺服器已啟動，RELAY_TOKEN 已設定，等待 live_quote_server.py 推送資料...")


@app.post("/ingest")
async def ingest(request: Request):
    """live_quote_server.py 每一輪算完之後，會呼叫這個網址把結果推過來。"""
    token = request.headers.get("X-Relay-Token", "")
    if not check_token(token):
        return JSONResponse({"ok": False, "error": "token 不對，拒絕接收"}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "收到的不是合法的 JSON"}, status_code=400)
    if not isinstance(payload, dict) or "sectors" not in payload:
        return JSONResponse({"ok": False, "error": "payload 格式不對，缺少 sectors"}, status_code=400)

    global latest_payload, last_ingest_at
    with state_lock:
        latest_payload = payload
        last_ingest_at = datetime.now(timezone.utc)

    with watchers_lock:
        queues = list(watchers)
    for q in queues:
        q.put_nowait(payload)

    return {"ok": True}


@app.get("/status")
async def status():
    with state_lock:
        has_data = latest_payload is not None
        ts = last_ingest_at
    age_seconds = (datetime.now(timezone.utc) - ts).total_seconds() if ts else None
    return {"hasData": has_data, "lastIngestAgeSeconds": age_seconds, "tokenConfigured": bool(RELAY_TOKEN)}


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = os.path.join(SCRIPT_DIR, "relay_heatmap.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


@app.websocket("/ws/heatmap")
async def ws_heatmap(websocket: WebSocket):
    await websocket.accept()

    with state_lock:
        payload = latest_payload
    if payload is not None:
        await websocket.send_json(payload)

    queue = asyncio.Queue()
    with watchers_lock:
        watchers.add(queue)

    try:
        while True:
            msg = await queue.get()
            await websocket.send_json(msg)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("中繼伺服器 WebSocket 傳送發生錯誤：", repr(e))
    finally:
        with watchers_lock:
            watchers.discard(queue)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))  # Render/Railway 這類平台會用 PORT 這個環境變數指定要監聽哪個port
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
