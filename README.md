<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="SnerdMQ Python Logo" />
  <h1>🚀 SnerdMQ Python SDK v0.3.5</h1>
  <p>The official Python SDK for SnerdMQ. Execute robust, C-speed background jobs in Python without Redis, Celery, or complex config.</p>

  [![PyPI version](https://img.shields.io/pypi/v/snerdmq-python)](https://pypi.org/project/snerdmq-python)
  [![License](https://img.shields.io/pypi/l/snerdmq-python)](https://github.com/speed-nerd/snerdmq-python/blob/main/LICENSE)
  [![Docs](https://img.shields.io/badge/docs-speed--nerd.github.io-blue)](https://speed-nerd.github.io/docs/)
</div>

This is the official Python client for **SnerdMQ**. It acts as a lightweight, elegant wrapper over the underlying Rust background daemon. It handles all JSON-RPC communication, standard I/O piping, and event loop orchestration so you can write background jobs natively in Python using `asyncio`.

## ✨ v0.3.5 AI Features
- **Smart API Rate-Limiting**: Natively tracks `rate_limit_group` execution velocity to prevent 429 "Too Many Requests" API errors.
- **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
- **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
- **Progress Streaming & Live Dashboard**: Handlers can stream progress updates to a built-in React UI dashboard served by the SDK.
- **The Celery Killer**: No Redis, no RabbitMQ, no ports, no messy worker nodes. Just start enqueuing jobs.
- **Zero Rust Required**: Our CLI tool automatically downloads the pre-compiled C-speed Rust binary for your OS.
- **Native Asyncio**: Written to seamlessly integrate with modern Python `async/await` applications (like FastAPI or Sanic).

### ⚙️ Advanced Task Configuration (v0.3.5)
To power complex AI workflows, tasks can now be configured with advanced orchestration parameters:

* **`auto_dedupe` (`bool`)**: If set to `True`, the daemon computes a cryptographic hash of the `task_type` and `data`. If an identical payload is currently sitting in the queue pending execution, this new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users!
* **`urgency_score` (`float`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a true Binary Max-Heap to continually float tasks with the highest urgency score to the very front of the execution line. Standard tasks default to `0.0`.
* **`rate_limit_group` (`str`)**: A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control.
* **`max_per_minute` (`int`)**: Used in conjunction with `rate_limit_group`. If the queue processes more tasks in this group than the allowed limit within a 60-second rolling window, further tasks in this group are temporarily paused. This natively prevents 429 "Too Many Requests" errors when bursting third-party APIs.
* **`execute_at` (`str` | `datetime`)**: A timestamp of when the job should be executed in the future.
* **`retry_after_hours` (`float`)**: Backoff in **hours** before a failed job is retried (default `0.0`). See *Cron Jobs vs. Retryable Jobs* below.
* **`cron` (`str`)**: A cron expression (e.g. `"0 * * * *"`) for recurring jobs. Shorthands like `"2h"` or `"10m"` are also supported.
* **`webhook_url` (`str`)**: By providing a webhook URL, SnerdMQ will bypass your local Python async handlers and dispatch the task payload via an HTTP POST request directly to the specified URL.
* **`max_execution_seconds` (`int`)**: Optional hard timeout in seconds. If execution takes longer, it's marked as failed.

### Note on Hard Timeouts (`max_execution_seconds`)
When `max_execution_seconds` is provided, the Python SDK wraps the execution of your async handler in `asyncio.wait_for`. If the task takes longer than the timeout, it will be cancelled via `asyncio.exceptions.TimeoutError` and marked as failed. The background Rust daemon also enforces this timeout at the IPC level.

### 🌐 HTTP Webhooks (Serverless Execution)
You can configure a task to execute externally via an HTTP POST request. By setting a `webhook_url`, the internal background processor will skip any registered handlers (`queue.register_handler`) and directly invoke the HTTP endpoint.

If the HTTP endpoint returns a non-200 status code, it triggers a retry. If it permanently fails (reaches `max_retries`), the Dead Letter Queue event is automatically fired via a final HTTP POST to the same `webhook_url` but with the header `X-SnerdMQ-Event: MaxRetriesReached`.

### 🕒 Cron Jobs vs. Retryable Jobs
When using the new scheduling features, it is important to understand the difference between Cron and Retry behaviors:
> - **A Cron Job** is a *Repeatable Job* that executes again **only after a success**, on a fixed schedule.
> - **A Retryable Job** is a *Recovery Job* that executes again **only after a failure**, attempting to recover using the `retry_after_hours` backoff.
> - **Combined:** If a Cron Job fails, it temporarily uses `retry_after_hours` to retry until it recovers. Once it succeeds, it goes back to ticking on its standard cron schedule!

## 📦 Installation

Installing the SDK is a simple two-step process:

**1. Install the package via pip:**
```bash
pip install snerdmq-python
```

**2. Download the Rust Engine:**
Because modern Python Wheels discourage arbitrary post-install scripts, we provide a clean CLI tool. Run this immediately after pip installing to fetch the correct SnerdMQ binary for your operating system (macOS/Linux/Windows):
```bash
snerdmq-install
```

---

## ⚡ Quickstart

Using the SDK is incredibly simple. Initialize the queue, register your async handlers, and start the event loop!

```python
import asyncio
from snerdmq import SnerdQueue

async def send_email(data):
    print(f"Sending email to {data['to']} with subject: {data['subject']}...")
    # ... your logic here (e.g., hitting SendGrid API)

async def main():
    # 1. Initialize the daemon in the background
    queue = SnerdQueue()

    # 2. Register your background job logic
    queue.register_handler('send_email', send_email)

    # 3. Enqueue a job from anywhere in your codebase
    await queue.enqueue(
        task_id='email-123',
        task_type='send_email',
        data={'to': 'john@wick.com', 'subject': 'Continental Update'},
        max_retries=3,
        retry_after_hours=0.5,       # Wait 30 minutes before retrying a failed job
        rate_limit_group='email_api',
        max_per_minute=100,
    )

    # Need scheduling, deduplication, or serverless execution? All orchestration
    # options are opt-in — combine only what you need:
    await queue.enqueue(
        task_id='email-digest-1',
        task_type='send_email',
        data={'to': 'john@wick.com', 'subject': 'Daily Digest'},
        cron='0 8 * * *',            # Run every day at 08:00
        auto_dedupe=True,            # Drop identical pending payloads
        urgency_score=0.99,          # Float to the front of the queue
        webhook_url='https://api.example.com/webhook',  # Execute via HTTP instead of local handlers
        max_execution_seconds=300,   # Hard timeout
    )

    # 4. Start the event loop (listens to the Rust daemon indefinitely)
    print("SnerdMQ Python SDK is listening for jobs...")
    try:
        await queue.start_listening()
    except asyncio.CancelledError:
        pass
    finally:
        print("Shutting down SnerdMQ...")
        await queue.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
```

### ☠️ Dead Letter Queue (Handling Permanent Failures)

When a task fails repeatedly and exhausts its `max_retries`, the SnerdMQ daemon permanently moves it to the Dead Letter Queue. You can hook into this event to alert your team, update your database, or send a Slack message by registering a Max Retry Handler.

```python
# 5. Catch tasks that have permanently failed (Dead Letter Queue)
async def handle_failed_email(data):
    print(f"Email task failed after all retries! Data: {data}")

queue.register_max_retry_handler('send_email', handle_failed_email)
```

---

## 📊 Live Dashboard

SnerdMQ ships with a built-in **React UI dashboard** served directly by the SDK — no extra services or ports to manage in your infrastructure. It gives you a real-time window into your queue:

- **Live stats**: total enqueued, processed, and failed jobs
- **Recent Jobs table**: per-task status (`queued`, `active`, `completed`, `failed`, `dead_letter`), retry counts, and badges showing which features a task uses (cron / webhook / timeout)
- **Real-time Progress Stream**: live output from `yield_progress` calls in your handlers

```python
queue = SnerdQueue()

# Start the built-in dashboard on http://localhost:9090
queue.start_dashboard(9090)

# ... register handlers, start listening, enqueue jobs ...
```

Then open **http://localhost:9090** in your browser. The dashboard automatically falls back to HTTP polling if a WebSocket connection cannot be established, and it also exposes a small JSON API (`/api/stats`, `/api/tasks`, `/api/progress`) if you want to build your own tooling on top.

> **Note:** `start_dashboard` only serves the UI — your jobs keep running whether or not the dashboard is open.

---

## 📡 Progress Reporting

Long-running handlers can stream live updates to the Dashboard's Progress Stream (ideal for streaming LLM tokens or multi-step ETL work):

```python
async def generate_report(data):
    for step in range(1, 11):
        await do_work(step)
        await queue.yield_progress_async(f"Step {step}/10 complete")

queue.register_handler('generate_report', generate_report)
```

> There is also a fire-and-forget sync variant, `queue.yield_progress(...)`, which schedules the update on the running event loop. Both must be called **inside a task handler** so the SDK knows which job the update belongs to.

---

## 🧩 Queue Topology: One Queue or Many?

### ✅ Recommended: one queue, all job types (singleton)

Each `SnerdQueue` client spawns its own Rust daemon and **exclusively owns** its storage directory (`.snerdata` by default). The recommended pattern is **one client per application process**: register every job type on it and serve a single shared dashboard:

```python
import asyncio
from snerdmq import SnerdQueue

async def process_image(data):
    print(f"Processing image: {data['image_id']}")

async def send_otp_email(data):
    print(f"Sending OTP to: {data['to']}")

async def main():
    # ONE queue client for the whole app
    queue = SnerdQueue()

    # Job type #1: image processing
    queue.register_handler('process_image', process_image)

    # Job type #2: OTP emails — same queue, same daemon
    queue.register_handler('send_otp_email', send_otp_email)

    await queue.start_listening()

    # Both job types flow through the exact same queue
    await queue.enqueue(task_id='img-1', task_type='process_image', data={'image_id': 'abc123'}, max_retries=3, retry_after_hours=0.5)
    await queue.enqueue(task_id='otp-1', task_type='send_otp_email', data={'to': 'john@wick.com'}, max_retries=3, retry_after_hours=0.5)

    # ONE dashboard shows every job type
    queue.start_dashboard(8080)

asyncio.run(main())
```

All job types share everything: the same persistent job log, retry/DLQ pipeline, rate-limit state, stats — and one dashboard at `http://localhost:8080` showing all of them.

### 🚫 Same storage twice = fails fast

The daemon takes an **exclusive OS-level lock** on its storage directory at startup. A second client on the same storage fails instead of silently double-executing your jobs:

```python
first = SnerdQueue()   # ✅ owns .snerdata
second = SnerdQueue()  # ❌ daemon refuses to start:
# "Another daemon is already running on storage '.snerdata'"
```

This applies across processes too — with **Gunicorn/Uvicorn multi-worker setups, every worker is a separate process** that spawns its own daemon, so each worker needs its own `storage_path` (or run a single dedicated worker process for jobs).

### 🔀 Need multiple queues? Give each one its own storage

```python
images = SnerdQueue(storage_path='.snerdata-images')
emails = SnerdQueue(storage_path='.snerdata-emails')

images.start_dashboard(8080)  # separate dashboards, so separate ports
emails.start_dashboard(8081)
```

Now you have two fully independent engines: separate job logs, separate rate-limit state, separate dashboards. Only split when you actually need isolation (different teams, different retention, independent monitoring) — otherwise the singleton is simpler and recommended.

---

## 🌍 Advanced: Distributed Scaling

Because the daemon exclusively locks its storage directory, scaling horizontally means **one queue per server**, each with its own storage. Your load balancer routes requests across servers, and every server processes the jobs it enqueued:

```python
from snerdmq import SnerdQueue

# Each server runs its own daemon on its own storage dir (local disk works fine)
queue = SnerdQueue(storage_path='/var/data/snerd')  # per-server storage
```

A shared network drive (AWS EFS or NFS) is still a good home for that storage when a single instance needs durable state — e.g. a container that restarts but must keep its queue. Native OS file locking (`flock`) keeps writes safe — no Redis required.

*Built with ❤️ for John Wick tier engineering.*
