import os
import time
import secrets
import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI()

MASTER_SECRET = os.environ.get("MASTER_SECRET", "")
TOKEN_TTL = 60  # seconds

token_store: dict[str, float] = {}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://www.tbmdigital.com"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/generate-token")
def generate_token():
    now = time.time()
    for t in [t for t, exp in token_store.items() if exp < now]:
        del token_store[t]
    token = secrets.token_urlsafe(32)
    token_store[token] = now + TOKEN_TTL
    return {"token": token}

@app.get("/health")
def health():
    return {"ok": True}

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def proxy(request: Request, path: str):
    async with httpx.AsyncClient() as client:
        url = f"http://localhost:8501/{path}"
        params = dict(request.query_params)
        headers = dict(request.headers)
        response = await client.request(
            method=request.method,
            url=url,
            params=params,
            headers=headers,
            content=await request.body()
        )
        return StreamingResponse(
            response.aiter_bytes(),
            status_code=response.status_code,
            headers=dict(response.headers)
        )