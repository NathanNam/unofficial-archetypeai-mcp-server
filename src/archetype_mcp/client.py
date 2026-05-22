from __future__ import annotations

import base64
import json
import mimetypes
import os
import time
from pathlib import Path
from typing import Any

import httpx

DEFAULT_BASE = "https://api.u1.archetypeai.app/v0.5"
DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)


class ArchetypeError(RuntimeError):
    def __init__(self, status: int, body: Any):
        self.status = status
        self.body = body
        super().__init__(f"Archetype API error {status}: {body}")


class ArchetypeClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: httpx.Timeout = DEFAULT_TIMEOUT,
    ) -> None:
        key = api_key or os.environ.get("ATAI_API_KEY")
        if not key:
            raise RuntimeError(
                "ATAI_API_KEY is not set. Provide it via env var or pass api_key explicitly."
            )
        self._api_key = key
        self._base = (base_url or os.environ.get("ATAI_API_ENDPOINT") or DEFAULT_BASE).rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=timeout,
            headers={"Authorization": f"Bearer {key}"},
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        files: Any | None = None,
        data: Any | None = None,
    ) -> Any:
        clean_params = {k: v for k, v in (params or {}).items() if v is not None}
        resp = await self._client.request(
            method,
            path,
            params=clean_params or None,
            json=json,
            files=files,
            data=data,
        )
        if resp.status_code >= 400:
            try:
                body = resp.json()
            except Exception:
                body = resp.text
            raise ArchetypeError(resp.status_code, body)
        if resp.headers.get("content-type", "").startswith("application/json"):
            return resp.json()
        return resp.text

    async def upload_file(self, file_path: str) -> Any:
        p = Path(file_path).expanduser().resolve()
        if not p.is_file():
            raise FileNotFoundError(f"File not found: {p}")
        with p.open("rb") as fh:
            files = {"file": (p.name, fh)}
            resp = await self._client.post("/files", files=files)
        if resp.status_code >= 400:
            try:
                body = resp.json()
            except Exception:
                body = resp.text
            raise ArchetypeError(resp.status_code, body)
        return resp.json()

    async def upload_file_base64(self, file_path: str) -> Any:
        p = Path(file_path).expanduser().resolve()
        if not p.is_file():
            raise FileNotFoundError(f"File not found: {p}")
        encoded = base64.b64encode(p.read_bytes())
        mime = mimetypes.guess_type(p.name)[0] or "application/octet-stream"
        files = {"file": (p.name, encoded, mime)}
        resp = await self._client.post("/files/base64", files=files)
        if resp.status_code >= 400:
            try:
                body = resp.json()
            except Exception:
                body = resp.text
            raise ArchetypeError(resp.status_code, body)
        return resp.json()

    def _open_session_websocket(self, session_endpoint: str) -> Any:
        import websockets

        subprotocols = [
            f"authenticationauthorization.bearer.{self._api_key}",
            "event-protocol-v1",
        ]
        return websockets.connect(
            session_endpoint, subprotocols=subprotocols, open_timeout=10.0
        )

    async def send_session_websocket_event(
        self,
        session_endpoint: str,
        event: dict[str, Any],
        timeout_sec: float = 30.0,
    ) -> Any:
        import asyncio

        async with self._open_session_websocket(session_endpoint) as ws:
            await ws.send(json.dumps(event))
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout_sec)
            except asyncio.TimeoutError:
                return {"error": "timeout", "timeout_sec": timeout_sec}
            try:
                return json.loads(raw)
            except Exception:
                return {"raw": raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")}

    async def consume_session_websocket(
        self,
        session_endpoint: str,
        client_id: str | None = None,
        max_messages: int = 50,
        max_wait_sec: float = 30.0,
        poll_interval_sec: float = 2.0,
    ) -> dict[str, Any]:
        import asyncio
        import uuid

        import websockets

        if client_id is None:
            client_id = str(uuid.uuid4())[:8]
        messages: list[Any] = []
        deadline = time.monotonic() + max_wait_sec
        async with self._open_session_websocket(session_endpoint) as ws:
            while len(messages) < max_messages:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                read_event = {
                    "type": "session.read",
                    "event_data": {"client_id": client_id},
                }
                await ws.send(json.dumps(read_event))
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except (asyncio.TimeoutError, websockets.ConnectionClosed):
                    break
                try:
                    response: Any = json.loads(raw)
                except Exception:
                    continue
                event_data = response.get("event_data") if isinstance(response, dict) else None
                batch = (event_data or {}).get("messages") or []
                messages.extend(batch)
                if not batch:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    await asyncio.sleep(min(poll_interval_sec, remaining))
        truncated = messages[:max_messages]
        return {
            "messages": truncated,
            "count": len(truncated),
            "client_id": client_id,
            "max_messages_reached": len(messages) >= max_messages,
        }

    async def download_file(self, file_id: str, dest_path: str) -> dict[str, Any]:
        dest = Path(dest_path).expanduser().resolve()
        dest.parent.mkdir(parents=True, exist_ok=True)
        bytes_written = 0
        async with self._client.stream("GET", f"/files/download/{file_id}") as resp:
            if resp.status_code >= 400:
                body_bytes = await resp.aread()
                try:
                    body: Any = httpx.Response(resp.status_code, content=body_bytes).json()
                except Exception:
                    body = body_bytes.decode("utf-8", errors="replace")
                raise ArchetypeError(resp.status_code, body)
            with dest.open("wb") as fh:
                async for chunk in resp.aiter_bytes(chunk_size=1 << 20):
                    fh.write(chunk)
                    bytes_written += len(chunk)
        return {"file_id": file_id, "path": str(dest), "bytes_written": bytes_written}
