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


# ---------------------------------------------------------------------------
# Lens API — register lenses and run sessions
# ---------------------------------------------------------------------------


@mcp.tool()
async def lens_register(lens_config: dict[str, Any]) -> Any:
    """Register a new lens (inference pipeline) from a config object.

    `lens_config` must include `lens_name`. Optional: `model_pipeline` (processor
    configs) and `model_parameters` (model settings).
    Returns `lens_id` (prefixed "lns-...").
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
async def lens_session_process_events(session_id: str, events: list[dict[str, Any]]) -> Any:
    """Send one or more inline data events to a running lens session.

    Each event is an object describing input data (text, JSON, base64 image,
    numeric array). Use this for short-lived synchronous pushes; for high-rate
    streaming, connect directly to the session WebSocket endpoint instead.
    """
    return await _c().request(
        "POST",
        "/lens/sessions/events/process",
        json={"session_id": session_id, "events": events},
    )


@mcp.tool()
async def lens_session_destroy(session_id: str) -> Any:
    """Tear down a running lens session and release its resources."""
    return await _c().request(
        "POST", "/lens/sessions/destroy", json={"session_id": session_id}
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


def main() -> None:
    transport = os.environ.get("ARCHETYPE_MCP_TRANSPORT", "stdio")
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
