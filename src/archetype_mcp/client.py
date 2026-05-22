from __future__ import annotations

import base64
import mimetypes
import os
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
