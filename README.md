# Unofficial Archetype AI MCP Server

An [MCP](https://modelcontextprotocol.io) server that exposes the
[Archetype AI Newton Platform](https://docs.archetypeai.app) — Files, Lens,
Query, Batch Processing, and Fine-tune APIs — as tools that any MCP client
can call.

This is an **unofficial** project, not affiliated with or endorsed by Archetype
AI. It wraps the public REST API documented at
`https://api.u1.archetypeai.app/v0.5`.

## What it gives you

53 tools covering every documented REST endpoint, both lens session
output channels (WebSocket and SSE), and one high-level composite for
the most common end-to-end flow.

| Group | Tools |
| --- | --- |
| Files (12) | `files_upload`, `files_upload_base64`, `files_list`, `files_get_metadata`, `files_info`, `files_delete`, `files_download`, `files_upload_initiate`, `files_upload_part_urls`, `files_upload_checkpoint_parts`, `files_upload_complete`, `files_upload_abort` |
| Lens (15) | `lens_register`, `lens_modify`, `lens_clone`, `lens_delete`, `lens_info`, `lens_metadata`, `lens_session_create`, `lens_session_process_event`, `lens_session_send_event`, `lens_session_consume`, `lens_session_poll_read`, `lens_session_run_video`, `lens_session_destroy`, `lens_sessions_info`, `lens_sessions_metadata` |
| Query (1) | `query` |
| Batch jobs (14) | `batch_job_create`, `batch_job_list`, `batch_job_get`, `batch_job_cancel`, `batch_job_retry`, `batch_job_delete`, `batch_job_events`, `batch_job_progress`, `batch_job_inputs`, `batch_job_inputs_progress`, `batch_job_inputs_progress_counts`, `batch_job_inputs_progress_traces`, `batch_job_outputs`, `batch_queue` |
| Batch registry (3) | `batch_pipeline_list`, `batch_pipeline_get`, `batch_pipeline_schema` |
| Fine-tune (8) | `finetune_job_create`, `finetune_job_get`, `finetune_job_list`, `finetune_job_cancel`, `finetune_job_stop`, `finetune_job_delete`, `finetune_job_metrics`, `finetune_node_status` |

### For most video / file-driven analysis: use the composite tool

`lens_session_run_video(file_id, lens_id, max_outputs=20, max_wait_sec=120)`
runs the full workflow in the correct order in one call:

1. Creates a session for `lens_id`
2. Binds the SSE writer (`output_stream.set` →
   `server_sent_events_writer`) — most built-in lenses (Activity Monitor,
   etc.) ship with **no `output_streams` configured**, so outputs are
   counted but routed nowhere until a writer is attached
3. Opens the SSE consumer and waits for `sse.stream.start`
4. Sends `input_stream.set` with `stream_type: video_file_reader`
5. Drains the SSE stream until `max_outputs` collected, `max_wait_sec`
   elapses, or the server sends `sse.stream.end`
6. Destroys the session

Use this whenever you have an uploaded video and a vision lens —
Activity Monitor C2.5 (`lns-1286e5d1d1b84a77-af311d579cc14869`, backed by
`Newton::c2_5_8b_260413b723a9ab`) is the current recommendation; the
older `lns-fd669361822b07e2-bc608aa3fdf8b4f9` (`Newton::c2_4_7b_…`) still
works if you need it. For other input types (RTSP cameras, CSV
time-series, sensor streams) or long-running sessions where you need
fine-grained control, fall back to the primitives below.

### Lens session output channels — two paths, pick the right one

When you assemble the workflow from primitives, the platform writes lens
outputs through **two distinct channels** depending on the lens's output
processor configuration. Each lens uses one or the other, never both:

- **SSE consumer** at `GET /lens/sessions/consumer/{session_id}` — for
  lenses with `server_sent_events_writer` output processors (Activity
  Monitor, most cookbook lenses, or any lens after you bind a writer with
  `output_stream.set`). Read with **`lens_session_consume`**. Mirrors
  `lens.create_sse_consumer()` in the official Python SDK.
- **WebSocket `session.read` mailbox** — for lenses with mailbox-style
  outputs. Read with **`lens_session_poll_read`**. Sends repeated
  `session.read` events on the WS connection and aggregates the returned
  messages.

If `lens_session_consume` returns nothing but `lens_sessions_metadata`
shows non-zero `num_outputs`, you're reading the wrong channel — switch to
`lens_session_poll_read`. SSE is also **live-only**: it surfaces events
that occur after you connect, so opening the consumer after the lens has
finished processing returns nothing. The composite tool sidesteps both
gotchas; the primitives expose them to the caller.

### Lens session WebSocket RPC

The WebSocket itself is **request-response RPC** — each event you send
gets exactly one `<event>.response` reply, no unsolicited push.

- **`lens_session_send_event(session_endpoint, event, timeout_sec=30)`** —
  generic RPC primitive. Opens a WebSocket, sends one event, reads one
  response, closes. Use for `session.status`, `session.validate`,
  `input_stream.set`, `model.query`, `session.destroy`, etc. Authenticates
  via the standard `Authorization: Bearer <API_KEY>` request header on
  the WebSocket upgrade — the same pattern the Python SDK uses. (The
  server also accepts `Sec-WebSocket-Protocol` subprotocol auth, but only
  browser clients need that workaround.)

For true real-time push or many events on a single long-lived connection,
connect to the WebSocket directly from your own code — both `send_event`
and `poll_read` open a fresh connection per call.

### Direct-to-cloud multipart upload

Covered as five discrete tools (`files_upload_initiate` → PUT parts to
presigned URLs → `files_upload_complete`). Driving the PUTs to the
presigned URLs is still up to the client — those go to the storage
provider, not the Archetype API.

## Install

Requires Python 3.10+. Verify with:

```bash
python3 --version
```

**1. Clone the repo.**

```bash
git clone <this repo>
cd unofficial-archetypeai-mcp-server
```

**2. Create a virtual environment.** This isolates the server's dependencies
from your system Python so nothing else on your machine is affected.

```bash
python3 -m venv .venv
```

This creates a `.venv/` directory in the project root (already listed in
`.gitignore`).

**3. Activate the virtual environment.** You'll need to do this in every new
shell session before running the server.

```bash
# macOS / Linux (bash, zsh)
source .venv/bin/activate

# Fish
source .venv/bin/activate.fish

# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Windows (cmd.exe)
.venv\Scripts\activate.bat
```

Once activated, your shell prompt will be prefixed with `(.venv)`. To leave the
environment later, run `deactivate`.

**4. Install the project in editable mode.**

```bash
pip install -e .
```

This pulls in `mcp`, `httpx`, and `python-dotenv`, and registers the
`archetype-mcp` console script.

## Configure

Get an API key from [archetypeai.io](https://www.archetypeai.io/), then:

```bash
cp .env.example .env
# edit .env and set ATAI_API_KEY
```

Or export it directly:

```bash
export ATAI_API_KEY=...
```

Optional: override the base URL with `ATAI_API_ENDPOINT` (defaults to
`https://api.u1.archetypeai.app/v0.5`). The name matches the `api_endpoint`
argument used by the official `archetypeai` Python SDK.

## Run

```bash
archetype-mcp
```

Speaks MCP over stdio by default. To switch transports, set
`ARCHETYPE_MCP_TRANSPORT=sse` (or any transport the
[`mcp`](https://pypi.org/project/mcp/) SDK supports).

## Use it from Claude Desktop

Add this to `~/Library/Application Support/Claude/claude_desktop_config.json`
(macOS) — or the equivalent path on Windows / Linux:

```json
{
  "mcpServers": {
    "archetype-ai": {
      "command": "/absolute/path/to/.venv/bin/archetype-mcp",
      "env": {
        "ATAI_API_KEY": "your_api_key_here",
        "ATAI_API_ENDPOINT": "https://api.u1.archetypeai.app/v0.5"
      }
    }
  }
}
```

- `command` **must be an absolute path** to the `archetype-mcp` script inside
  your virtualenv (find it with `which archetype-mcp` while the venv is
  activated). Claude Desktop spawns the server with its own working directory,
  so a relative path or bare command name will not resolve.
- `ATAI_API_KEY` is required. It has to live in this `env` block (and not just
  your project's `.env`) because Claude Desktop launches the server from a
  different working directory, where `python-dotenv` cannot find your `.env`
  file.
- `ATAI_API_ENDPOINT` is optional — omit it to use the default
  `https://api.u1.archetypeai.app/v0.5`.

Restart Claude Desktop after editing the config. The tools listed above will
appear under the `archetype-ai` server.

## Use it from Claude Code

```bash
claude mcp add archetype-ai /absolute/path/to/.venv/bin/archetype-mcp \
  --env ATAI_API_KEY=your_api_key_here
```

## Example flows

### One-shot query against a vision model

```
query(
  model="Newton::c2_4_7b_251215a172f6d7",
  query="Describe what is happening in this dashboard.",
  file_ids=["screenshot.png"],
)
```

### Time-series classification via batch job

```
files_upload("/path/to/sensor_data.csv")
batch_pipeline_schema("machine-state-classification")  # discover input ports
batch_job_create(
  name="hrv-stress-run-1",
  pipeline_key="machine-state-classification",
  inputs={"data": {"file_id": "<id from upload>"}},
)
batch_job_get("<job id>")
batch_job_outputs("<job id>")  # presigned URLs, refresh as they expire
```

### Video analysis (one-call composite)

```python
# 1. Upload the video. `file_id` is the filename returned in the response;
#    the lens stream config takes that, NOT the UUID-style `file_uid`.
files_upload("/path/to/Ring_Dashcam_Traffic.mp4")
# {"is_valid": true,
#  "file_id": "Ring_Dashcam_Traffic.mp4",
#  "file_uid": "fil_32b6x7g3nt8q4vwmnf73rc9cqb"}

# 2. Run the lens. One call. Creates session, binds the SSE writer
#    (built-in lenses ship with none), connects the consumer, fires
#    input_stream.set, drains, destroys.
#    `lens_id` defaults to Activity Monitor C2.5; pass it explicitly to
#    override (e.g. lens_id="lns-fd669361822b07e2-bc608aa3fdf8b4f9" for
#    the older Activity Monitor / Newton::c2_4_7b_...).
lens_session_run_video(
  file_id="Ring_Dashcam_Traffic.mp4",
  max_outputs=20,
  max_wait_sec=120,
)
```

The response shape:

```python
{
  "session_id": "lsn-...",          # session was created and destroyed for you
  "lens_id": "lns-1286e5d1d1...",
  "file_id": "Ring_Dashcam_Traffic.mp4",
  "outputs": [                       # SSE envelope frames filtered out;
    {"type": "stream.start", ...},   # lens-emitted events kept as-is
    {"type": "inference.reset", ...},
    {"type": "inference.start", ...},
    {"type": "inference.result",
     "event_data": {
       "response": [
         "The video is a dashcam recording from a vehicle driving on a sunny day. "
         "The road is multi-lane with moderate traffic, including cars and a white "
         "van ahead. Trees, palm trees, and other vegetation line the sides..."
       ],
       "query_id": "qry-...", "query_time_sec": 4.21, "sensor_timestamp": "00:00:04",
       ...
     }},
    ...
  ],
  "count": 11,
  "max_outputs_reached": False,      # True if you hit the cap before sse.stream.end
  "sse_stream_started": True,
  "sse_stream_ended": True,          # server signaled end-of-stream
  "output_stream_response": {"type": "output_stream.set.response", ...},
  "input_stream_response":  {"type": "input_stream.set.response",  ...},
}
```

**Timing for the example above:** Activity Monitor C2.5 emits one
`inference.result` per 5 seconds of video (the lens's `camera_buffer_size`
and `camera_buffer_step_size` parameters control this — see
[`lens-processors`](https://docs.archetypeai.app/core-concepts/lenses/lens-processors)).
A ~24-second clip yields 4 results in ~89 seconds wall-clock. Bump
`max_wait_sec` for longer videos; the lens runs to completion or until
the cap, whichever comes first.

**Filtering tip:** the LLM usually only cares about
`type == "inference.result"` events; the others (`stream.start`,
`inference.reset`, `inference.start`, `stream.end`) are lifecycle markers
that are useful for debugging timing but noise for the answer.

### Real-time lens session (manual orchestration)

```
lens_session_create("lns-1d519091822706e2-bc108andqxf8b4os")
# Connect to the returned session_endpoint (WebSocket) from your own code
# to stream sensor data in and receive inference events, or use
# lens_session_send_event / lens_session_consume / lens_session_poll_read
# from this server. Bind a writer first with output_stream.set if the
# lens has no output_streams configured (most built-in lenses don't).
lens_session_destroy("<session id>")
```

## License

Apache-2.0
