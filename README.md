# Unofficial Archetype AI MCP Server

An [MCP](https://modelcontextprotocol.io) server that exposes the
[Archetype AI Newton Platform](https://docs.archetypeai.app) — Files, Lens,
Query, Batch Processing, and Fine-tune APIs — as tools that any MCP client
can call.

This is an **unofficial** project, not affiliated with or endorsed by Archetype
AI. It wraps the public REST API documented at
`https://api.u1.archetypeai.app/v0.5`.

## What it gives you

52 tools covering every documented REST endpoint plus both lens session
output channels (WebSocket and SSE).

| Group | Tools |
| --- | --- |
| Files (12) | `files_upload`, `files_upload_base64`, `files_list`, `files_get_metadata`, `files_info`, `files_delete`, `files_download`, `files_upload_initiate`, `files_upload_part_urls`, `files_upload_checkpoint_parts`, `files_upload_complete`, `files_upload_abort` |
| Lens (14) | `lens_register`, `lens_modify`, `lens_clone`, `lens_delete`, `lens_info`, `lens_metadata`, `lens_session_create`, `lens_session_process_event`, `lens_session_send_event`, `lens_session_consume`, `lens_session_poll_read`, `lens_session_destroy`, `lens_sessions_info`, `lens_sessions_metadata` |
| Query (1) | `query` |
| Batch jobs (14) | `batch_job_create`, `batch_job_list`, `batch_job_get`, `batch_job_cancel`, `batch_job_retry`, `batch_job_delete`, `batch_job_events`, `batch_job_progress`, `batch_job_inputs`, `batch_job_inputs_progress`, `batch_job_inputs_progress_counts`, `batch_job_inputs_progress_traces`, `batch_job_outputs`, `batch_queue` |
| Batch registry (3) | `batch_pipeline_list`, `batch_pipeline_get`, `batch_pipeline_schema` |
| Fine-tune (8) | `finetune_job_create`, `finetune_job_get`, `finetune_job_list`, `finetune_job_cancel`, `finetune_job_stop`, `finetune_job_delete`, `finetune_job_metrics`, `finetune_node_status` |

### Lens session output channels — two paths, pick the right one

The platform writes lens outputs through **two distinct channels**
depending on the lens's output processor configuration. Each lens uses one
or the other, never both:

- **SSE consumer** at `GET /lens/sessions/consumer/{session_id}` — used by
  lenses with `server_sent_events_writer` output processors (Activity
  Monitor, most cookbook lenses). Read with **`lens_session_consume`**.
  Mirrors `lens.create_sse_consumer()` in the official Python SDK.
- **WebSocket `session.read` mailbox** — used by lenses with mailbox-style
  outputs. Read with **`lens_session_poll_read`**. Sends repeated
  `session.read` events on the WS connection and aggregates the returned
  messages.

If `lens_session_consume` returns nothing but `lens_sessions_metadata`
shows non-zero `num_outputs`, you're reading the wrong channel — switch to
`lens_session_poll_read` (or vice versa).

### Lens session WebSocket RPC

The WebSocket itself is **request-response RPC** — each event you send
gets exactly one `<event>.response` reply, no unsolicited push.

- **`lens_session_send_event(session_endpoint, event, timeout_sec=30)`** —
  generic RPC primitive. Opens a WebSocket, sends one event, reads one
  response, closes. Use for `session.status`, `session.validate`,
  `input_stream.set`, `model.query`, `session.destroy`, etc. Authenticates
  via `Sec-WebSocket-Protocol` subprotocols
  (`authenticationauthorization.bearer.<API_KEY>` + `event-protocol-v1`).

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

### Real-time lens session

```
lens_session_create("lns-1d519091822706e2-bc108andqxf8b4os")
# Connect to the returned session_endpoint (WebSocket) from your own code
# to stream sensor data in and receive inference events.
lens_session_destroy("<session id>")
```

## License

Apache-2.0
