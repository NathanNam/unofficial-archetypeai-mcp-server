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

        return websockets.connect(
            session_endpoint,
            additional_headers={"Authorization": f"Bearer {self._api_key}"},
            open_timeout=10.0,
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

    async def consume_session_sse(
        self,
        session_id: str,
        max_events: int = 50,
        max_wait_sec: float = 30.0,
        since_event_id: str | None = None,
        include_heartbeats: bool = False,
    ) -> dict[str, Any]:
        headers = {"Accept": "text/event-stream"}
        if since_event_id is not None:
            headers["Last-Event-ID"] = since_event_id
        timeout = httpx.Timeout(
            connect=10.0, read=max_wait_sec, write=10.0, pool=10.0
        )
        events: list[dict[str, Any]] = []
        last_event_id: str | None = since_event_id
        stream_ended = False
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
                                parsed = {"raw": data}
                            evt_type = (
                                parsed.get("type") if isinstance(parsed, dict) else None
                            )
                            if evt_type == "sse.stream.end":
                                stream_ended = True
                            keep = True
                            if evt_type == "sse.stream.heartbeat" and not include_heartbeats:
                                keep = False
                            if keep:
                                evt: dict[str, Any] = {"data": parsed}
                                if "event" in current_meta:
                                    evt["event"] = current_meta["event"]
                                if "id" in current_meta:
                                    evt["id"] = current_meta["id"]
                                    last_event_id = current_meta["id"]
                                events.append(evt)
                            elif "id" in current_meta:
                                last_event_id = current_meta["id"]
                        current_meta = {}
                        data_lines = []
                        if stream_ended:
                            break
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
            "stream_ended": stream_ended,
        }

    async def poll_session_read(
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

    async def run_session_with_video(
        self,
        file_id: str,
        lens_id: str = "lns-1286e5d1d1b84a77-af311d579cc14869",
        max_outputs: int = 20,
        max_wait_sec: float = 120.0,
    ) -> dict[str, Any]:
        import asyncio

        s = await self.request(
            "POST", "/lens/sessions/create", json={"lens_id": lens_id}
        )
        session_id = s["session_id"]
        session_endpoint = s["session_endpoint"]

        outputs: list[Any] = []
        err_holder: dict[str, Exception] = {}
        stream_started = asyncio.Event()
        stream_ended = False

        async def sse_reader() -> None:
            nonlocal stream_ended
            deadline = time.monotonic() + max_wait_sec
            try:
                async with self._client.stream(
                    "GET",
                    f"/lens/sessions/consumer/{session_id}",
                    headers={"Accept": "text/event-stream"},
                    timeout=httpx.Timeout(
                        connect=10.0, read=max_wait_sec, write=10.0, pool=10.0
                    ),
                ) as resp:
                    if resp.status_code >= 400:
                        body_bytes = await resp.aread()
                        try:
                            body: Any = json.loads(body_bytes)
                        except Exception:
                            body = body_bytes.decode("utf-8", errors="replace")
                        err_holder["err"] = ArchetypeError(resp.status_code, body)
                        return

                    data_lines: list[str] = []
                    try:
                        async for line in resp.aiter_lines():
                            if line == "":
                                if data_lines:
                                    try:
                                        payload: Any = json.loads("\n".join(data_lines))
                                    except Exception:
                                        payload = None
                                    if isinstance(payload, dict):
                                        t = payload.get("type")
                                        if t == "sse.stream.start":
                                            stream_started.set()
                                        elif t == "sse.stream.heartbeat":
                                            pass
                                        elif t == "sse.stream.end":
                                            stream_ended = True
                                            return
                                        else:
                                            outputs.append(payload)
                                            if len(outputs) >= max_outputs:
                                                return
                                data_lines = []
                                if time.monotonic() >= deadline:
                                    return
                                continue
                            if line.startswith(":"):
                                continue
                            field, _, value = line.partition(":")
                            if value.startswith(" "):
                                value = value[1:]
                            if field == "data":
                                data_lines.append(value)
                    except httpx.ReadTimeout:
                        return
            except Exception as e:
                err_holder["err"] = e

        # Built-in lenses (Activity Monitor, Machine State, etc.) ship with NO
        # output_streams configured — outputs are produced internally (counted
        # in num_outputs) but not routed to any consumer until we bind a
        # writer. Set the SSE writer BEFORE opening the consumer so the
        # subscription has something to subscribe to.
        output_resp: Any = None
        try:
            output_resp = await self.send_session_websocket_event(
                session_endpoint,
                {
                    "type": "output_stream.set",
                    "event_data": {
                        "stream_type": "server_sent_events_writer",
                        "stream_config": {},
                    },
                },
                timeout_sec=15.0,
            )
        except Exception as e:
            err_holder["err"] = e

        sse_task = asyncio.create_task(sse_reader())
        input_resp: Any = None
        try:
            try:
                await asyncio.wait_for(stream_started.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pass

            input_event = {
                "type": "input_stream.set",
                "event_data": {
                    "stream_type": "video_file_reader",
                    "stream_config": {"file_id": file_id},
                },
            }
            input_resp = await self.send_session_websocket_event(
                session_endpoint, input_event, timeout_sec=15.0
            )
            await sse_task
        finally:
            if not sse_task.done():
                sse_task.cancel()
                try:
                    await sse_task
                except (asyncio.CancelledError, Exception):
                    pass
            try:
                await self.request(
                    "POST",
                    "/lens/sessions/destroy",
                    json={"session_id": session_id},
                )
            except Exception:
                pass

        if "err" in err_holder:
            raise err_holder["err"]

        return {
            "session_id": session_id,
            "lens_id": lens_id,
            "file_id": file_id,
            "outputs": outputs,
            "count": len(outputs),
            "max_outputs_reached": len(outputs) >= max_outputs,
            "sse_stream_started": stream_started.is_set(),
            "sse_stream_ended": stream_ended,
            "output_stream_response": output_resp,
            "input_stream_response": input_resp,
        }

    async def _run_batch_job(
        self,
        payload: dict[str, Any],
        max_wait_sec: float = 600.0,
        poll_interval_sec: float = 5.0,
        include_events: int = 50,
    ) -> dict[str, Any]:
        import asyncio

        job = await self.request("POST", "/batch/jobs", json=payload)
        job_id = job["id"]
        terminal = {"COMPLETED", "FAILED", "CANCELLED"}
        deadline = time.monotonic() + max_wait_sec
        timed_out = False
        while True:
            if job["status"] in terminal:
                break
            if time.monotonic() >= deadline:
                timed_out = True
                break
            await asyncio.sleep(poll_interval_sec)
            job = await self.request("GET", f"/batch/jobs/{job_id}")
        outputs: Any = None
        if job["status"] == "COMPLETED":
            try:
                outputs = await self.request("GET", f"/batch/jobs/{job_id}/outputs")
            except Exception:
                outputs = None
        events: Any = []
        if include_events > 0:
            try:
                ev_resp = await self.request(
                    "GET",
                    f"/batch/jobs/{job_id}/events",
                    params={"limit": include_events},
                )
                events = ev_resp.get("events", []) if isinstance(ev_resp, dict) else []
            except Exception:
                events = []
        return {
            "job_id": job_id,
            "status": job["status"],
            "pipeline_key": job.get("pipeline_key"),
            "pipeline_version": job.get("pipeline_version"),
            "queue_position": job.get("queue_position"),
            "queue_depth": job.get("queue_depth"),
            "created_at": job.get("created_at"),
            "started_at": job.get("started_at"),
            "completed_at": job.get("completed_at"),
            "failed_at": job.get("failed_at"),
            "cancelled_at": job.get("cancelled_at"),
            "error": job.get("error"),
            "outputs": outputs,
            "events": events,
            "timed_out": timed_out,
        }

    async def run_batch_machine_state_classification(
        self,
        inference_file_ids: list[str],
        n_shots: list[dict[str, Any]],
        data_columns: list[str],
        timestamp_column: str = "timestamp",
        window_size: int = 128,
        step_size: int = 1,
        n_neighbors: int = 11,
        metric: str = "euclidean",
        weights: str = "uniform",
        model_type: str = "omega_1_4_base",
        normalize_embeddings: bool = False,
        batch_size: int = 32,
        flush_every_n_iteration: int = 150,
        parallelism: int = 1,
        name: str | None = None,
        max_wait_sec: float = 900.0,
        poll_interval_sec: float = 5.0,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": name or "machine-state-classification",
            "pipeline_type": "batch",
            "pipeline_key": "machine-state-classification",
            "inputs": {
                "worker.inference": [{"file_id": fid} for fid in inference_file_ids],
                "worker.n_shots": [
                    {"file_id": s["file_id"], "metadata": {"class": s["class_label"]}}
                    for s in n_shots
                ],
            },
            "parameters": {
                "worker": {
                    "parallelism": parallelism,
                    "config": {
                        "batch_size": batch_size,
                        "classifier_config": {
                            "metric": metric,
                            "n_neighbors": n_neighbors,
                            "normalize_embeddings": normalize_embeddings,
                            "weights": weights,
                        },
                        "flush_every_n_iteration": flush_every_n_iteration,
                        "model_type": model_type,
                        "reader_config": {
                            "data_columns": data_columns,
                            "step_size": step_size,
                            "timestamp_column": timestamp_column,
                            "window_size": window_size,
                        },
                    },
                }
            },
        }
        return await self._run_batch_job(
            payload, max_wait_sec=max_wait_sec, poll_interval_sec=poll_interval_sec
        )

    async def run_batch_activity_detection(
        self,
        data_file_ids: list[str],
        model_variant: str = "newton/c:2.5.1-8b-base",
        max_video_frames: int = 32,
        max_new_tokens: int = 256,
        temperature: float = 0.7,
        top_p: float = 0.8,
        top_k: int = 20,
        repetition_penalty: float = 1.0,
        do_sample: bool = True,
        parallelism: int = 1,
        name: str | None = None,
        max_wait_sec: float = 900.0,
        poll_interval_sec: float = 5.0,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": name or "activity-detection",
            "pipeline_type": "batch",
            "pipeline_key": "activity-detection",
            "inputs": {
                "worker.data": [{"file_id": fid} for fid in data_file_ids],
            },
            "parameters": {
                "worker": {
                    "parallelism": parallelism,
                    "config": {
                        "model_variant": model_variant,
                        "max_video_frames": max_video_frames,
                        "generation": {
                            "max_new_tokens": max_new_tokens,
                            "temperature": temperature,
                            "top_p": top_p,
                            "top_k": top_k,
                            "repetition_penalty": repetition_penalty,
                            "do_sample": do_sample,
                        },
                    },
                }
            },
        }
        return await self._run_batch_job(
            payload, max_wait_sec=max_wait_sec, poll_interval_sec=poll_interval_sec
        )

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
