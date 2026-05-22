from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from archetype_mcp.client import ArchetypeClient

load_dotenv()

mcp = FastMCP("archetype-ai")

_client: ArchetypeClient | None = None


def _c() -> ArchetypeClient:
    global _client
    if _client is None:
        _client = ArchetypeClient()
    return _client


# ---------------------------------------------------------------------------
# Files API — https://api.u1.archetypeai.app/v0.5/files
# ---------------------------------------------------------------------------


@mcp.tool()
async def files_upload(file_path: str) -> Any:
    """Upload a local file (CSV, JSON, image, video, plain text) to Archetype.

    Returns the file_id needed by other endpoints (lens sessions, batch jobs, query).
    Max size 512 MB. Use the direct-to-cloud flow for larger files.
    """
    return await _c().upload_file(file_path)


@mcp.tool()
async def files_list() -> Any:
    """List metadata for every file uploaded under the current API key."""
    return await _c().request("GET", "/files/metadata")


@mcp.tool()
async def files_get_metadata(file_id: str) -> Any:
    """Get metadata (size, mime type, timestamps) for a single uploaded file."""
    return await _c().request("GET", f"/files/metadata/{file_id}")


@mcp.tool()
async def files_info() -> Any:
    """Get an aggregate storage summary (counts and bytes used) for the account."""
    return await _c().request("GET", "/files/info")


@mcp.tool()
async def files_delete(file_id: str) -> Any:
    """Permanently delete an uploaded file."""
    return await _c().request("DELETE", f"/files/delete/{file_id}")


@mcp.tool()
async def files_upload_base64(file_path: str) -> Any:
    """Upload a file as base64-encoded multipart/form-data.

    Use this when a raw binary upload is not possible (e.g. when bridging
    through a JSON-only channel). Supports .jpg/.png/.txt/.csv/.json up to
    512 MB after encoding. For larger files use the direct-to-cloud flow.
    """
    return await _c().upload_file_base64(file_path)


@mcp.tool()
async def files_download(file_id: str, dest_path: str) -> dict[str, Any]:
    """Stream a previously uploaded file to a local path.

    `dest_path` is created (along with parent dirs) and overwritten if it
    already exists. Returns the path and total bytes written. The file must
    be in a downloadable state — files still uploading or corrupt return 400.
    """
    return await _c().download_file(file_id, dest_path)


@mcp.tool()
async def files_upload_initiate(
    filename: str,
    file_type: str,
    num_bytes: int,
    resume_if_started: bool | None = None,
) -> Any:
    """Begin a direct-to-cloud upload and receive presigned URLs for each part.

    Step 1 of the multipart flow. The response includes `upload_id`, `num_parts`,
    and a `parts` array of presigned URLs. Clients then PUT each part's bytes
    directly to its URL (capturing the response `ETag` as `part_token`), then
    optionally checkpoint and finally call `files_upload_complete`.
    Supports files up to 250 GB.
    """
    body: dict[str, Any] = {
        "filename": filename,
        "file_type": file_type,
        "num_bytes": num_bytes,
    }
    if resume_if_started is not None:
        body["resume_if_started"] = resume_if_started
    return await _c().request("POST", "/files/uploads/initiate", json=body)


@mcp.tool()
async def files_upload_part_urls(upload_id: str, part_numbers: list[int]) -> Any:
    """Refresh presigned URLs for specific parts of an in-progress upload.

    Use when a previously issued URL is about to expire and the part has not
    yet been PUT. The returned `parts` array preserves the input order.
    """
    return await _c().request(
        "POST",
        f"/files/uploads/{upload_id}/parts/urls",
        json={"part_numbers": part_numbers},
    )


@mcp.tool()
async def files_upload_checkpoint_parts(
    upload_id: str, parts: list[dict[str, Any]]
) -> Any:
    """Persist part_tokens for completed parts so they can be skipped on resume.

    Each entry in `parts` is `{"part_number": int, "part_token": str}` where
    `part_token` is the ETag returned by the part's PUT request. Checkpointed
    parts do not need to be re-supplied to `files_upload_complete`.
    """
    return await _c().request(
        "POST",
        f"/files/uploads/{upload_id}/parts/checkpoint",
        json={"parts": parts},
    )


@mcp.tool()
async def files_upload_complete(upload_id: str, parts: list[dict[str, Any]]) -> Any:
    """Finalize a direct-to-cloud upload after every part has been PUT.

    Only include parts that were NOT previously checkpointed. The server
    assembles the final file and registers it with the organization, returning
    the new `file_uid`.
    """
    return await _c().request(
        "POST", f"/files/uploads/{upload_id}/complete", json={"parts": parts}
    )


@mcp.tool()
async def files_upload_abort(upload_id: str) -> Any:
    """Abort an in-progress direct-to-cloud upload and release its resources.

    Has no effect on already-completed uploads (returns 409). Use this on
    crash recovery or when the source file is no longer available.
    """
    return await _c().request("POST", f"/files/uploads/{upload_id}/abort")


# ---------------------------------------------------------------------------
# Lens API — register lenses and run sessions
# ---------------------------------------------------------------------------


@mcp.tool()
async def lens_register(lens_config: dict[str, Any]) -> Any:
    """Register a new lens (inference pipeline). Returns `lens_id` (prefixed "lns-...").

    `lens_config` accepts:

    - `lens_name` (required) — human-readable name
    - `model_pipeline` — list of processor configs. Each entry is
      `{processor_name, processor_config}`. Common processors:
      `lens_camera_processor` (video frames), `lens_timeseries_state_processor`
      (CSV time-series), `lens_sensor_logs_processor` (sensor JSONL).
    - `model_parameters` — model settings: `model_version` (e.g.
      `"Newton::c2_4_7b_251215a172f6d7"`), `instruction`, `focus`,
      `template_name`, `max_new_tokens`, etc.
    - `input_streams` — bind input(s) to the lens at registration time so
      every session inherits them. Each entry is `{stream_type, stream_config}`;
      see `lens_session_send_event` for the valid stream_type values.
    - `output_streams` — bind output(s); typically
      `[{"stream_type": "server_sent_events_writer"}]` so results flow to the
      SSE consumer (use `lens_session_consume` to read them) or the WebSocket
      mailbox writer for `lens_session_poll_read`.

    Lenses are immutable once registered. To change inputs/outputs for an
    existing built-in lens (like Activity Monitor) either clone+modify
    (`lens_clone` then `lens_modify`) or set them per-session via
    `lens_session_send_event` with an `input_stream.set` / `output_stream.set`
    event.
    """
    return await _c().request("POST", "/lens/register", json={"lens_config": lens_config})


@mcp.tool()
async def lens_session_create(lens_id: str) -> Any:
    """Create a new lens session backed by the given lens_id.

    The response includes `session_id` and a `session_endpoint` WebSocket URL.
    Note: this MCP server does not handle the WebSocket/SSE stream itself —
    connect to `session_endpoint` from your own code to receive results.
    """
    return await _c().request("POST", "/lens/sessions/create", json={"lens_id": lens_id})


@mcp.tool()
async def lens_session_process_event(session_id: str, event: dict[str, Any]) -> Any:
    """Send a single event to a lens session over REST (no WebSocket required).

    `event` must include a `type` field (and typically `event_data`). Accepts
    the same event types as `lens_session_send_event` (see that tool's
    docstring for the full reference of session.* events, input_stream.set
    stream_types, output_stream.set, and model.query).

    Use this REST variant for one-off setup events (`input_stream.set`,
    `output_stream.set`) when you don't want to manage a WebSocket. For
    request-heavy interaction or `session.read` polling, prefer the
    WebSocket-based `lens_session_send_event` to avoid per-event TCP setup.
    """
    return await _c().request(
        "POST",
        "/lens/sessions/events/process",
        json={"session_id": session_id, "event": event},
    )


@mcp.tool()
async def lens_session_destroy(session_id: str) -> Any:
    """Tear down a running lens session and release its resources."""
    return await _c().request(
        "POST", "/lens/sessions/destroy", json={"session_id": session_id}
    )


@mcp.tool()
async def lens_session_run_video(
    file_id: str,
    lens_id: str,
    max_outputs: int = 20,
    max_wait_sec: float = 120.0,
) -> dict[str, Any]:
    """Run an uploaded video through a lens and collect inference outputs.

    One-shot, end-to-end. Use this for "describe this video" style tasks
    against video lenses like Activity Monitor C2.5
    (`lns-1286e5d1d1b84a77-af311d579cc14869`, backed by
    `Newton::c2_5_8b_260413b723a9ab` — current recommended vision lens).
    The older `lns-fd669361822b07e2-bc608aa3fdf8b4f9` (`Newton::c2_4_7b_…`)
    also works.

    Internally orchestrates the full workflow in the correct order so
    outputs are not lost to the SSE-live-only race condition:

    1. Creates a session for `lens_id`
    2. Opens the SSE consumer FIRST and waits for `sse.stream.start`
    3. Sends `input_stream.set` with
       `stream_type=video_file_reader`, `stream_config.file_id=<file_id>`
    4. Drains the SSE stream until any of: `max_outputs` inference events
       collected, `max_wait_sec` elapsed, `sse.stream.end` received
    5. Destroys the session

    Why this exists: SSE-routed lenses emit results live; if you open the
    SSE consumer after the lens has already finished, those outputs are
    gone. Sequential MCP calls (`set input` → `then read output`) can race
    the lens on short videos. This tool keeps the consumer open across the
    input-set call so nothing is missed.

    `file_id` is the filename-style id returned by `files_upload`
    (e.g. `"Ring_Dashcam_Traffic.mp4"`), not the UUID-style `file_uid`.

    For non-video inputs (CSV time-series, RTSP cameras, live sensors) or
    long-running sessions where you want fine-grained control, fall back to
    the primitives: `lens_session_create` + `lens_session_send_event`
    (`input_stream.set`) + `lens_session_consume` / `lens_session_poll_read`
    + `lens_session_destroy`.

    Returns:
        outputs: list of inference event payloads (envelope events
                 `sse.stream.*` are filtered out)
        count: number of inference events
        max_outputs_reached: true if we stopped because of max_outputs
        sse_stream_started / sse_stream_ended: SSE lifecycle flags
        session_id: id of the (now-destroyed) session
        input_stream_response: server's reply to the `input_stream.set` event
    """
    return await _c().run_session_with_video(
        lens_id, file_id, max_outputs, max_wait_sec
    )


@mcp.tool()
async def lens_session_send_event(
    session_endpoint: str,
    event: dict[str, Any],
    timeout_sec: float = 30.0,
) -> Any:
    """Send one event to a lens session WebSocket and return the response.

    Matches the `send_and_receive_event(socket, event)` pattern from the docs:
    opens a WebSocket to `session_endpoint`, sends `event`, reads exactly one
    response, closes. `event` must include a `type` field. Authenticates via
    `Authorization: Bearer <API_KEY>` on the WebSocket upgrade.

    ## Event type reference

    ### Session control

    - `session.status` — returns the full session record DIRECTLY (unique
      shape — does not use the `.response` envelope; this is the one
      exception)
    - `session.validate` — health-check; returns `{is_valid, error_messages}`
    - `session.read` — drains the WebSocket message mailbox. Pass
      `event_data: {client_id: "<unique-string>"}`. Returns `event_data: null`
      when the mailbox is empty. NOTE: only works for lenses with mailbox-
      style outputs; for lenses using `server_sent_events_writer` output, use
      `lens_session_consume` instead — `session.read` will keep returning
      `null` forever even while the SSE channel is emitting frames.
    - `session.destroy` — destroys the session through the WS channel

    ### Input streams (`input_stream.set`)

    Configure where the session pulls inputs from. Pass
    `event_data: {stream_type: <type>, stream_config: {...}}`. Valid types:

    - `video_file_reader` — `stream_config: {file_id: "<filename>"}`. Replays
      a previously-uploaded video (`POST /files`). `file_id` is the
      filename-style value returned by upload (e.g. `"my_video.mp4"`), NOT
      the UUID-style `file_uid` that appears alongside it.
    - `csv_file_reader` — `stream_config: {file_id: "<filename>"}`. Same
      file_id semantics as the video reader.
    - `rtsp_video_streamer` — `stream_config: {rtsp_url, target_image_size:
      [H, W], target_frame_rate_hz}`. For live RTSP cameras.
    - `sensor_streamer` — configuration is sensor-type specific.

    ### Output streams (`output_stream.set`)

    Configure where the session writes outputs. Pass
    `event_data: {stream_type, stream_config: {}}`. Common type:
    `server_sent_events_writer` (which `lens_session_consume` reads from).
    Built-in lenses (Activity Monitor, Machine State) already have outputs
    wired at registration time — only set this if you need to override.

    ### Direct queries (`model.query`)

    One-shot inference. Pass an `event_data` object with `model_version`,
    `template_name`, `instruction`, `focus`, `max_new_tokens`, and `data`
    (e.g. `[{"type": "base64_img", "base64_img": "<b64>"}]`).

    Known template names: `image_qa_template_task` (natural-language image
    description), `image_bbox_template_task` (object detection with bboxes).

    `model.query` only works on lenses that include a model-query processor.
    Activity Monitor (`lens_camera_processor`) and Machine State
    (`lens_timeseries_state_processor`) expect inputs via `input_stream.set`
    and will return `{type: "model.query.response", message: "Response timed
    out for query"}` instead.

    ## Response envelope

    Every event except `session.status` returns
    `{type: "<event>.response", event_data: {...}}`. When `event_data` would
    be empty, the server returns it as `null`.
    """
    return await _c().send_session_websocket_event(
        session_endpoint, event, timeout_sec
    )


@mcp.tool()
async def lens_session_consume(
    session_id: str,
    max_events: int = 50,
    max_wait_sec: float = 30.0,
    since_event_id: str | None = None,
    include_heartbeats: bool = False,
) -> dict[str, Any]:
    """Consume the lens session's SSE output stream.

    Opens `GET /lens/sessions/consumer/{session_id}` and bounded-collects
    events written by lens output processors of type `server_sent_events_writer`
    — the route used by Activity Monitor and most cookbook lenses for
    inference results. This matches `lens.create_sse_consumer()` in the
    official Python SDK.

    Distinct channel from the WebSocket `session.read` mailbox: if your
    lens writes outputs to one, they do not appear on the other. If this
    tool returns no events but `lens_sessions_metadata` shows non-zero
    `num_outputs`, try `lens_session_poll_read` instead.

    `sse.stream.heartbeat` envelope events are filtered out by default;
    set `include_heartbeats=True` to keep them. An `sse.stream.end` event
    means the server closed the stream — it is included in `events` and
    `stream_ended` is set true.

    Pass `since_event_id` (from a previous call's `last_event_id`) as a
    cursor — sent as the standard `Last-Event-ID` request header.
    """
    return await _c().consume_session_sse(
        session_id, max_events, max_wait_sec, since_event_id, include_heartbeats
    )


@mcp.tool()
async def lens_session_poll_read(
    session_endpoint: str,
    client_id: str | None = None,
    max_messages: int = 50,
    max_wait_sec: float = 30.0,
    poll_interval_sec: float = 2.0,
) -> dict[str, Any]:
    """Drain a lens session's WebSocket message mailbox via `session.read`.

    Opens a WebSocket to `session_endpoint` and repeatedly sends
    `{"type": "session.read", "event_data": {"client_id": <id>}}`,
    aggregating the `messages` from each response (inference.result,
    log.info, frame.processed, stream.status, error, etc.) until
    `max_messages` is reached or `max_wait_sec` elapses.

    Distinct channel from the SSE consumer: if your lens uses
    `server_sent_events_writer` for outputs (Activity Monitor, most
    cookbook lenses), use `lens_session_consume` instead — those outputs
    never appear in `session.read` responses.

    Pass the same `client_id` across calls to preserve message stream
    playback position; pass `None` to generate a fresh client_id (which
    resets playback). Between empty polls, the tool sleeps
    `poll_interval_sec` before sending the next `session.read`.
    """
    return await _c().poll_session_read(
        session_endpoint,
        client_id,
        max_messages,
        max_wait_sec,
        poll_interval_sec,
    )


@mcp.tool()
async def lens_modify(
    lens_id: str,
    lens_name: str | None = None,
    lens_config: dict[str, Any] | None = None,
) -> Any:
    """Modify an existing lens template, overwriting previous settings.

    Only lenses with `lens_modifiable: true` (see `lens_metadata`) can be
    modified. `lens_config` accepts `model_pipeline` and/or `model_parameters`.
    """
    body: dict[str, Any] = {"lens_id": lens_id}
    if lens_name is not None:
        body["lens_name"] = lens_name
    if lens_config is not None:
        body["lens_config"] = lens_config
    return await _c().request("POST", "/lens/modify", json=body)


@mcp.tool()
async def lens_clone(lens_id: str) -> Any:
    """Clone an existing lens template, returning a new modifiable lens_id.

    Subsequent changes to the original do not propagate to the clone. The
    clone's config includes an `origin_lens_id` reference back to the source.
    """
    return await _c().request("POST", "/lens/clone", json={"lens_id": lens_id})


@mcp.tool()
async def lens_delete(lens_id: str) -> Any:
    """Delete a lens template (active sessions based on it are not affected).

    Built-in (non-modifiable) lenses cannot be deleted — the response's
    `is_valid` field will be false with a 409-flavoured error message.
    """
    return await _c().request("POST", "/lens/delete", json={"lens_id": lens_id})


@mcp.tool()
async def lens_info() -> Any:
    """Get a summary of all lenses in your organization (counts, last update)."""
    return await _c().request("GET", "/lens/info")


@mcp.tool()
async def lens_metadata(
    lens_id: str | None = None,
    shard_index: int | None = None,
    max_items_per_shard: int | None = None,
) -> Any:
    """Get detailed metadata for every lens in your org (or one if filtered).

    Pagination via `shard_index` / `max_items_per_shard` — pass `-1` for no
    limit. The response includes `lens_id`, `lens_name`, `lens_modifiable`,
    and the full `lens_config` for each lens.
    """
    return await _c().request(
        "GET",
        "/lens/metadata",
        params={
            "lens_id": lens_id,
            "shard_index": shard_index,
            "max_items_per_shard": max_items_per_shard,
        },
    )


@mcp.tool()
async def lens_sessions_info() -> Any:
    """Get a summary of all lens sessions in your org (active / total counts)."""
    return await _c().request("GET", "/lens/sessions/info")


@mcp.tool()
async def lens_sessions_metadata(
    session_id: str | None = None,
    shard_index: int | None = None,
    max_items_per_shard: int | None = None,
) -> Any:
    """Get detailed metadata for every active lens session (or one if filtered).

    Pagination via `shard_index` / `max_items_per_shard` — pass `-1` for no limit.
    """
    return await _c().request(
        "GET",
        "/lens/sessions/metadata",
        params={
            "session_id": session_id,
            "shard_index": shard_index,
            "max_items_per_shard": max_items_per_shard,
        },
    )


# ---------------------------------------------------------------------------
# Query API — direct natural-language queries against a model
# ---------------------------------------------------------------------------


@mcp.tool()
async def query(
    model: str,
    query: str,
    system_prompt: str | None = None,
    instruction_prompt: str | None = None,
    response_start_prompt: str | None = None,
    template_name: str | None = None,
    file_ids: list[str] | None = None,
    events: list[dict[str, Any]] | None = None,
    max_new_tokens: int | None = None,
    max_frames: int | None = None,
    temperature: float | None = None,
    do_sample: bool | None = None,
    repetition_penalty: float | None = None,
    top_p: float | None = None,
    top_k: int | None = None,
    presence_penalty: float | None = None,
    normalize_input: bool | None = None,
    multi_image: bool | None = None,
    render: bool | None = None,
    query_metadata: dict[str, Any] | None = None,
    max_query_size_mb: float | None = None,
    max_wait_time_sec: float | None = None,
    sanitize_response: bool | None = None,
) -> Any:
    """Run a one-shot query against a Newton or Omega model.

    `model` is a versioned identifier like `Newton::c2_4_7b_251215a172f6d7`
    or `OmegaEncoder::omega_embeddings_01`. For numeric encoders pass an empty
    string for `query`. Ground the prompt with `file_ids` (previously uploaded)
    or inline `events`.
    """
    body: dict[str, Any] = {"model": model, "query": query}
    for k, v in {
        "system_prompt": system_prompt,
        "instruction_prompt": instruction_prompt,
        "response_start_prompt": response_start_prompt,
        "template_name": template_name,
        "file_ids": file_ids,
        "events": events,
        "max_new_tokens": max_new_tokens,
        "max_frames": max_frames,
        "temperature": temperature,
        "do_sample": do_sample,
        "repetition_penalty": repetition_penalty,
        "top_p": top_p,
        "top_k": top_k,
        "presence_penalty": presence_penalty,
        "normalize_input": normalize_input,
        "multi_image": multi_image,
        "render": render,
        "query_metadata": query_metadata,
        "max_query_size_mb": max_query_size_mb,
        "max_wait_time_sec": max_wait_time_sec,
        "sanitize_response": sanitize_response,
    }.items():
        if v is not None:
            body[k] = v
    return await _c().request("POST", "/query", json=body)


# ---------------------------------------------------------------------------
# Batch Processing API — jobs
# ---------------------------------------------------------------------------


@mcp.tool()
async def batch_job_create(
    name: str,
    pipeline_key: str,
    pipeline_type: str = "batch",
    pipeline_version: str | None = None,
    inputs: dict[str, Any] | None = None,
    parameters: dict[str, Any] | None = None,
) -> Any:
    """Create a new batch or training job.

    `pipeline_type` is "batch" or "training". `pipeline_key` is a registry
    identifier (e.g. `machine-state-classification`, `activity-detection`).
    `inputs` maps port name -> {"file_id": ..., "metadata": {...}}.
    `parameters` is component-level config with `parallelism` and `config`.
    """
    body: dict[str, Any] = {
        "name": name,
        "pipeline_type": pipeline_type,
        "pipeline_key": pipeline_key,
    }
    if pipeline_version is not None:
        body["pipeline_version"] = pipeline_version
    if inputs is not None:
        body["inputs"] = inputs
    if parameters is not None:
        body["parameters"] = parameters
    return await _c().request("POST", "/batch/jobs", json=body)


@mcp.tool()
async def batch_job_list(
    pipeline_key: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> Any:
    """List batch jobs with optional filters."""
    return await _c().request(
        "GET",
        "/batch/jobs",
        params={
            "pipeline_key": pipeline_key,
            "status": status,
            "limit": limit,
            "cursor": cursor,
        },
    )


@mcp.tool()
async def batch_job_get(job_id: str) -> Any:
    """Get a single batch job by id."""
    return await _c().request("GET", f"/batch/jobs/{job_id}")


@mcp.tool()
async def batch_job_cancel(job_id: str) -> Any:
    """Cancel a running batch job."""
    return await _c().request("POST", f"/batch/jobs/{job_id}/cancel")


@mcp.tool()
async def batch_job_retry(job_id: str) -> Any:
    """Retry a failed or cancelled batch job."""
    return await _c().request("POST", f"/batch/jobs/{job_id}/retry")


@mcp.tool()
async def batch_job_delete(job_id: str) -> Any:
    """Permanently delete a batch job and its associated data."""
    return await _c().request("DELETE", f"/batch/jobs/{job_id}")


@mcp.tool()
async def batch_job_events(
    job_id: str,
    level: str | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> Any:
    """List events emitted during job execution. `level` ∈ INFO|WARN|ERROR|SUCCESS|FAILED."""
    return await _c().request(
        "GET",
        f"/batch/jobs/{job_id}/events",
        params={"level": level, "limit": limit, "cursor": cursor},
    )


@mcp.tool()
async def batch_job_progress(job_id: str) -> Any:
    """Get step-by-step progress entries with metrics for a job."""
    return await _c().request("GET", f"/batch/jobs/{job_id}/progress")


@mcp.tool()
async def batch_job_inputs(
    job_id: str,
    port: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> Any:
    """List input files for a batch job. Filter by port name or processing status
    (pending|processing|completed|failed|reference)."""
    return await _c().request(
        "GET",
        f"/batch/jobs/{job_id}/inputs",
        params={"port": port, "status": status, "limit": limit, "cursor": cursor},
    )


@mcp.tool()
async def batch_job_inputs_progress(job_id: str) -> Any:
    """List inputs paired with their latest progress entry."""
    return await _c().request("GET", f"/batch/jobs/{job_id}/inputs/progress")


@mcp.tool()
async def batch_job_inputs_progress_counts(job_id: str) -> Any:
    """Get per-status counts of tracked inputs for a job."""
    return await _c().request("GET", f"/batch/jobs/{job_id}/inputs/progress/counts")


@mcp.tool()
async def batch_job_inputs_progress_traces(job_id: str) -> Any:
    """Get bucketed cumulative file and per-kind item counters over time."""
    return await _c().request("GET", f"/batch/jobs/{job_id}/inputs/progress/traces")


@mcp.tool()
async def batch_job_outputs(job_id: str) -> Any:
    """List output files for a job, including presigned download URLs.

    URLs expire — re-fetch this endpoint to refresh links.
    """
    return await _c().request("GET", f"/batch/jobs/{job_id}/outputs")


@mcp.tool()
async def batch_queue() -> Any:
    """Get global queue depths grouped by pipeline type."""
    return await _c().request("GET", "/batch/jobs/queue")


# ---------------------------------------------------------------------------
# Batch Processing API — pipeline registry
# ---------------------------------------------------------------------------


@mcp.tool()
async def batch_pipeline_list(limit: int | None = None, cursor: str | None = None) -> Any:
    """List registered batch pipelines (paginated)."""
    return await _c().request(
        "GET", "/batch/registry/pipelines", params={"limit": limit, "cursor": cursor}
    )


@mcp.tool()
async def batch_pipeline_get(pipeline_id: str) -> Any:
    """Get pipeline specification (versions, ports, components)."""
    return await _c().request("GET", f"/batch/registry/pipelines/{pipeline_id}")


@mcp.tool()
async def batch_pipeline_schema(pipeline_id: str) -> Any:
    """Get the configuration schema for a pipeline — required to discover input ports."""
    return await _c().request("GET", f"/batch/registry/pipelines/{pipeline_id}/schema")


# ---------------------------------------------------------------------------
# Fine-tune API — train custom models on uploaded datasets
# ---------------------------------------------------------------------------


@mcp.tool()
async def finetune_job_create(
    name: str,
    config: dict[str, Any],
    id: str | None = None,
) -> Any:
    """Create a fine-tune job.

    `config` must include `datasets` (list of `{name, split, file_ids}` where
    `split` is "train" or "eval") and optionally `model` (default
    `Newton::c2_4_7b_251215a172f6d7`; also `Newton::c2_4_3b_251215dfe6a746`).
    `id` is an optional client-supplied identifier (1-64 chars); omit to let
    the server generate one.
    """
    body: dict[str, Any] = {"name": name, "config": config}
    if id is not None:
        body["id"] = id
    return await _c().request("POST", "/fine-tune/jobs", json=body)


@mcp.tool()
async def finetune_job_get(job_id: str) -> Any:
    """Fetch a fine-tune job's full record by ID."""
    return await _c().request("GET", f"/fine-tune/jobs/{job_id}")


@mcp.tool()
async def finetune_job_list(
    limit: int | None = None,
    offset: int | None = None,
    statuses: list[str] | None = None,
) -> Any:
    """List fine-tune jobs in the org.

    `statuses` filters by status (one of UNKNOWN, REGISTERED, STARTING, RUNNING,
    COMPLETED, FINALIZING, STOPPING, STOPPED, CANCELLED, FAILED). `limit=-1`
    returns all matching jobs.
    """
    return await _c().request(
        "GET",
        "/fine-tune/jobs",
        params={"limit": limit, "offset": offset, "statuses": statuses},
    )


@mcp.tool()
async def finetune_job_cancel(job_id: str) -> Any:
    """Cancel a fine-tune job immediately, without finalizing.

    Cancellation discards in-progress checkpoints. Use `finetune_job_stop`
    instead if you want the runner to wind down gracefully and preserve the
    latest checkpoint.
    """
    return await _c().request("PUT", f"/fine-tune/jobs/{job_id}/cancel")


@mcp.tool()
async def finetune_job_stop(job_id: str) -> Any:
    """Stop a running fine-tune job gracefully (preserves latest checkpoint).

    Transitions through STOPPING → STOPPED. Use `finetune_job_cancel` to
    abandon a job without finalizing.
    """
    return await _c().request("PUT", f"/fine-tune/jobs/{job_id}/stop")


@mcp.tool()
async def finetune_job_delete(job_id: str) -> Any:
    """Delete a fine-tune job (does not gracefully stop a running job).

    If the job is still running, call `finetune_job_stop` or
    `finetune_job_cancel` first.
    """
    return await _c().request("DELETE", f"/fine-tune/jobs/{job_id}")


@mcp.tool()
async def finetune_job_metrics(
    job_id: str, limit: int | None = None, offset: int | None = None
) -> Any:
    """Get per-step training and eval metrics for a fine-tune job.

    Returns `last_checkpoint_step` plus an array of metric records. Typical
    record keys: step, loss, eval_loss, learning_rate, wall_time_sec.
    `limit=-1` (default) returns all entries.
    """
    return await _c().request(
        "GET",
        f"/fine-tune/jobs/{job_id}/metrics",
        params={"limit": limit, "offset": offset},
    )


@mcp.tool()
async def finetune_node_status() -> Any:
    """Get the fine-tune runner node's current status.

    Status is one of UNKNOWN, READY, BUSY, STALE, ERROR.
    """
    return await _c().request("GET", "/fine-tune/status")


def main() -> None:
    transport = os.environ.get("ARCHETYPE_MCP_TRANSPORT", "stdio")
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
