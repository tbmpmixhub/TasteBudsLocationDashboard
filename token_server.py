import os
import time
import secrets
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

MASTER_SECRET = os.environ.get("MASTER_SECRET", "")
TOKEN_TTL = 60  # seconds — long enough for the iframe to load

token_store: dict[str, float] = {}  # token -> expiry timestamp

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://www.tbmdigital.com"],  # your Wix domain — update if different
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/generate-token")
def generate_token():
    # Clean up expired tokens
    now = time.time()
    for t in [t for t, exp in token_store.items() if exp < now]:
        del token_store[t]

    token = secrets.token_urlsafe(32)
    token_store[token] = now + TOKEN_TTL
    return {"token": token}

@app.get("/health")
def health():
    return {"ok": True}