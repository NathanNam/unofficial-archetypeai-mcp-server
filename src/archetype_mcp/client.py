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

    async def consume_session_events(
        self,
        session_id: str,
        max_events: int = 50,
        max_wait_sec: float = 30.0,
        since_event_id: str | None = None,
    ) -> dict[str, Any]:
        headers = {"Accept": "text/event-stream"}
        if since_event_id is not None:
            headers["Last-Event-ID"] = since_event_id
        timeout = httpx.Timeout(
            connect=10.0, read=max_wait_sec, write=10.0, pool=10.0
        )
        events: list[dict[str, Any]] = []
        last_event_id: str | None = since_event_id
        deadline = time.monotonic() + max_wait_sec
        async with self._client.stream(
            "GET",
            f"/lens/sessions/consumer/{session_id}",
            headers=headers,
            timeout=timeout,
        ) as resp:
            if resp.status_code >= 400:
                body_bytes = await resp.aread()
                try:
                    body: Any = json.loads(body_bytes)
                except Exception:
                    body = body_bytes.decode("utf-8", errors="replace")
                raise ArchetypeError(resp.status_code, body)
            current_meta: dict[str, str] = {}
            data_lines: list[str] = []
            try:
                async for line in resp.aiter_lines():
                    if line == "":
                        if data_lines:
                            data = "\n".join(data_lines)
                            try:
                                parsed: Any = json.loads(data)
                            except Exception:
                                parsed = data
                            evt: dict[str, Any] = {"data": parsed}
                            if "event" in current_meta:
                                evt["event"] = current_meta["event"]
                            if "id" in current_meta:
                                evt["id"] = current_meta["id"]
                                last_event_id = current_meta["id"]
                            events.append(evt)
                        current_meta = {}
                        data_lines = []
                        if len(events) >= max_events:
                            break
                        if time.monotonic() >= deadline:
                            break
                        continue
                    if line.startswith(":"):
                        continue
                    field, _, value = line.partition(":")
                    if value.startswith(" "):
                        value = value[1:]
                    if field == "data":
                        data_lines.append(value)
                    elif field in ("event", "id", "retry"):
                        current_meta[field] = value
                    if time.monotonic() >= deadline:
                        break
            except httpx.ReadTimeout:
                pass
        return {
            "events": events,
            "count": len(events),
            "last_event_id": last_event_id,
            "max_events_reached": len(events) >= max_events,
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
