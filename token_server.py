from fastapi import WebSocket, WebSocketDisconnect
import websockets
import asyncio

@app.websocket("/_stcore/stream")
async def websocket_proxy(websocket: WebSocket):
    await websocket.accept()
    uri = "ws://localhost:8501/_stcore/stream"
    try:
        async with websockets.connect(uri) as ws:
            async def forward():
                async for msg in ws:
                    if isinstance(msg, bytes):
                        await websocket.send_bytes(msg)
                    else:
                        await websocket.send_text(msg)
            async def backward():
                async for msg in websocket.iter_bytes():
                    await ws.send(msg)
            await asyncio.gather(forward(), backward())
    except (WebSocketDisconnect, Exception):
        pass
