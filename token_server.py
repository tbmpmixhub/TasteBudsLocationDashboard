import httpx
from fastapi import Request
from fastapi.responses import StreamingResponse

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