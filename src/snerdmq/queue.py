import asyncio
import json
import os
import sys
from typing import Callable, Awaitable, Dict, Any, Optional
import contextvars
from aiohttp import web

# Store the current task ID for yield_progress
_current_task_id = contextvars.ContextVar('current_task_id', default=None)


class SnerdQueue:
    def __init__(self, binary_path: Optional[str] = None, storage_path: Optional[str] = None):
        self.handlers: Dict[str, Callable[[Any], Awaitable[None]]] = {}
        self.max_retry_handlers: Dict[str, Callable[[Any], Awaitable[None]]] = {}
        self.process: Optional[asyncio.subprocess.Process] = None
        self.is_shutting_down = False
        self.engine_alive = False
        self.pending_enqueues: Dict[str, asyncio.Future] = {}
        self.progress_listeners = set()
        self.dashboard_runner = None
        
        if not binary_path:
            package_dir = os.path.dirname(os.path.abspath(__file__))
            ext = '.exe' if os.name == 'nt' else ''
            binary_path = os.path.join(package_dir, 'bin', f'snerdmq{ext}')

        if not os.path.exists(binary_path):
            raise FileNotFoundError(f"[Snerd] Binary not found at {binary_path}. Please run 'snerdmq-install' or provide binary_path.")

        self.binary_path = binary_path
        self.storage_path = storage_path


    async def start_listening(self):
        """Starts the rust daemon and the event loop to listen to its output."""
        args = [self.storage_path] if self.storage_path else []
        self.process = await asyncio.create_subprocess_exec(
            self.binary_path, *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        if not self.process.stdout or not self.process.stderr:
            raise RuntimeError("[Snerd] Failed to establish standard I/O pipes.")

        # Re-send all registrations in case we are reconnecting
        for task_type in self.handlers.keys():
            await self._send({"action": "register", "task_type": task_type})

        # Run stdout and stderr readers concurrently as tasks
        loop = asyncio.get_running_loop()
        self.engine_alive = True
        loop.create_task(self._read_stdout())
        loop.create_task(self._read_stderr())

    async def _read_stdout(self):
        assert self.process and self.process.stdout
        while not self.is_shutting_down:
            line = await self.process.stdout.readline()
            if not line:
                break
            line_str = line.decode().strip()
            if not line_str:
                continue

            try:
                msg = json.loads(line_str)
                # Dispatch handler without blocking the read loop
                asyncio.create_task(self._handle_engine_message(msg))
            except json.JSONDecodeError:
                pass

        if not self.is_shutting_down:
            print("[Snerd] Engine process terminated unexpectedly.", file=sys.stderr)

        # Engine is gone — it can never ack. Reject all pending enqueues
        # so callers fail fast instead of awaiting forever.
        self.engine_alive = False
        for task_id, future in list(self.pending_enqueues.items()):
            if not future.done():
                future.set_exception(RuntimeError(f"[Snerd] Engine terminated before ack for task '{task_id}'"))
            del self.pending_enqueues[task_id]

    async def _read_stderr(self):
        assert self.process and self.process.stderr
        while not self.is_shutting_down:
            line = await self.process.stderr.readline()
            if not line:
                break
            print(f"[Snerd Engine Error]: {line.decode().strip()}", file=sys.stderr)

    async def _handle_engine_message(self, msg: dict):
        if msg.get('action') == 'ack':
            task_id = msg.get('task_id')
            if task_id and task_id in self.pending_enqueues:
                self.pending_enqueues[task_id].set_result(None)
                del self.pending_enqueues[task_id]
        elif msg.get('action') == 'error':
            task_id = msg.get('task_id')
            if task_id and task_id in self.pending_enqueues:
                self.pending_enqueues[task_id].set_exception(RuntimeError(msg.get('message')))
                del self.pending_enqueues[task_id]
            else:
                print(f"[Snerd] Error from engine: {msg.get('message')}", file=sys.stderr)
        elif msg.get('action') == 'execute':
            task_type = msg.get('task_type')
            task_id = msg.get('task_id')
            task_data = msg.get('task_data')
            max_execution_seconds = msg.get('max_execution_seconds')
            
            if isinstance(task_data, str):
                try:
                    task_data = json.loads(task_data)
                except json.JSONDecodeError:
                    pass

            handler = self.handlers.get(task_type)
            if not handler:
                await self._send({'action': 'result', 'task_id': task_id, 'status': 'error', 'error_msg': 'No handler registered.'})
                return

            try:
                _current_task_id.set(task_id)
                if max_execution_seconds is not None:
                    await asyncio.wait_for(handler(task_data), timeout=max_execution_seconds)
                else:
                    await handler(task_data)
                await self._send({'action': 'result', 'task_id': task_id, 'status': 'success'})
            except asyncio.TimeoutError:
                await self._send({'action': 'result', 'task_id': task_id, 'status': 'error', 'error_msg': f"Task execution timed out after {max_execution_seconds} seconds"})
            except Exception as e:
                await self._send({'action': 'result', 'task_id': task_id, 'status': 'error', 'error_msg': str(e)})

        elif msg.get('action') == 'progress':
            for ws in list(self.progress_listeners):
                try:
                    # msg is already a dict, just forward it
                    asyncio.create_task(ws.send_json(msg))
                except Exception:
                    self.progress_listeners.discard(ws)

        elif msg.get('action') == 'max_retries_reached':
            task_type = msg.get('task_type')
            task_id = msg.get('task_id')
            handler = self.max_retry_handlers.get(task_type)
            if handler:
                task_data = msg.get('task_data')
                if isinstance(task_data, str):
                    try:
                        task_data = json.loads(task_data)
                    except json.JSONDecodeError:
                        pass
                try:
                    await handler(task_data)
                except Exception as e:
                    print(f"[Snerd] Error in max retry handler for task {task_id}: {e}", file=sys.stderr)
            else:
                print(f"[Snerd] Dead Letter Queue: Task {task_id} ({task_type}) permanently failed.", file=sys.stderr)

    async def _send(self, msg: dict):
        if self.process and self.process.stdin and not self.is_shutting_down:
            try:
                self.process.stdin.write((json.dumps(msg) + '\n').encode())
                await self.process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass  # Daemon died, pipe broken — silently ignore

    def register_handler(self, task_type: str, handler: Callable[[Any], Awaitable[None]]):
        """Registers an async function to handle a specific task type."""
        self.handlers[task_type] = handler
        if self.process:
            asyncio.create_task(self._send({"action": "register", "task_type": task_type}))

    def register_max_retry_handler(self, task_type: str, handler: Callable[[Any], Awaitable[None]]):
        """Registers an async function to handle permanently failed tasks of a specific type."""
        self.max_retry_handlers[task_type] = handler

    async def enqueue(self, task_id: str, task_type: str, data: Any, max_retries: int = 3, retry_after_hours: float = 0.0, rate_limit_group: Optional[str] = None, max_per_minute: Optional[int] = None, auto_dedupe: Optional[bool] = None, urgency_score: Optional[float] = None, execute_at: Optional[str] = None, cron: Optional[str] = None, webhook_url: Optional[str] = None, max_execution_seconds: Optional[int] = None):
        """Enqueues a new background job."""
        if not self.process:
            raise RuntimeError("[Snerd] Cannot enqueue task: Queue is not running. Call start_listening() first.")
        if not self.engine_alive:
            raise RuntimeError("[Snerd] Cannot enqueue task: engine is not running.")
        
        payload = {
            'action': 'enqueue',
            'task_id': task_id,
            'task_type': task_type,
            'task_data': json.dumps(data),
            'max_retries': max_retries,
            'retry_after_hours': retry_after_hours
        }

        if rate_limit_group is not None:
            payload['rate_limit_group'] = rate_limit_group
        if max_per_minute is not None:
            payload['max_per_minute'] = max_per_minute
        if auto_dedupe is not None:
            payload['auto_dedupe'] = auto_dedupe
        if urgency_score is not None:
            payload['urgency_score'] = urgency_score
        if execute_at is not None:
            # Handle datetime objects by converting to ISO string
            if hasattr(execute_at, 'isoformat'):
                payload['execute_at'] = execute_at.isoformat()
            else:
                payload['execute_at'] = execute_at
        if cron is not None:
            payload['cron'] = cron
        if webhook_url is not None:
            payload['webhook_url'] = webhook_url
        if max_execution_seconds is not None:
            payload['max_execution_seconds'] = max_execution_seconds

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_enqueues[task_id] = future
        await self._send(payload)
        await future

    def shutdown(self):
        """Gracefully kills the Rust daemon."""
        if self.is_shutting_down:
            return
        self.is_shutting_down = True
        if self.process:
            try:
                self.process.terminate()
            except ProcessLookupError:
                pass


    async def yield_progress_async(self, data: Any):
        task_id = _current_task_id.get()
        if not task_id:
            raise RuntimeError("[Snerd] yield_progress must be called within a task handler context.")
        await self._send({
            'action': 'progress',
            'task_id': task_id,
            'data': data
        })

    def yield_progress(self, data: Any):
        """Yields progress. If inside an event loop, schedules the send."""
        task_id = _current_task_id.get()
        if not task_id:
            raise RuntimeError("[Snerd] yield_progress must be called within a task handler context.")
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._send({
                'action': 'progress',
                'task_id': task_id,
                'data': data
            }))
        except RuntimeError:
            pass

    def start_dashboard(self, port: int = 8080):
        """Starts the embedded dashboard and API server."""
        async def handle_ws(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            self.progress_listeners.add(ws)
            try:
                async for msg in ws:
                    pass
            finally:
                self.progress_listeners.discard(ws)
            return ws

        async def handle_stats(request):
            tasks_map = {}
            storage_dir = self.storage_path or './.snerdata'
            tasks_file = os.path.join(storage_dir, 'tasks', 'tasks.log')
            if os.path.exists(tasks_file):
                with open(tasks_file, 'r') as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            t = json.loads(line)
                            if 'taskId' in t:
                                tasks_map[t['taskId']] = t
                        except json.JSONDecodeError:
                            pass
            enqueued = 0
            processed = 0
            failed = 0
            for t in tasks_map.values():
                enqueued += 1
                if t.get('deletedAt'):
                    if t.get('LastJobError'):
                        failed += 1
                    else:
                        processed += 1
            return web.json_response({'enqueued': enqueued, 'processed': processed, 'failed': failed}, headers={'Access-Control-Allow-Origin': '*'})

        async def handle_tasks(request):
            tasks_map = {}
            storage_dir = self.storage_path or './.snerdata'
            tasks_file = os.path.join(storage_dir, 'tasks', 'tasks.log')
            if os.path.exists(tasks_file):
                with open(tasks_file, 'r') as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            t = json.loads(line)
                            if 'taskId' in t:
                                tasks_map[t['taskId']] = t
                        except json.JSONDecodeError:
                            pass
            
            res = []
            for t in tasks_map.values():
                import time as _time
                if t.get('deletedAt'):
                    if t.get('LastJobError') and t.get('retryCount', 0) >= t.get('maxRetries', 3):
                        status = 'dead_letter'
                    elif t.get('LastJobError'):
                        status = 'failed'
                    else:
                        status = 'completed'
                elif t.get('LastJobError'):
                    status = 'failed'
                else:
                    exec_at = t.get('executeAt', '')
                    try:
                        from datetime import datetime, timezone
                        exec_time = datetime.fromisoformat(exec_at.replace('Z', '+00:00')).timestamp()
                        status = 'active' if exec_time <= _time.time() else 'queued'
                    except (ValueError, AttributeError):
                        status = 'queued'
                res.append({
                    'id': t['taskId'],
                    'type': t['taskType'],
                    'status': status,
                    'progress': 0,
                    'retryCount': t.get('retryCount', 0),
                    'maxRetries': t.get('maxRetries', 3),
                    'retryAfterTime': t.get('retryAfterTime'),
                    'cronExpression': t.get('cronExpression'),
                    'webhookUrl': t.get('webhookUrl'),
                    'maxExecutionSeconds': t.get('maxExecutionSeconds')
                })
            
            return web.json_response(res, headers={'Access-Control-Allow-Origin': '*'})

        async def handle_index(request):
            # Try to find static/index.html
            static_file = os.path.join(os.path.dirname(__file__), '..', '..', 'static', 'index.html')
            if not os.path.exists(static_file):
                static_file = os.path.join(os.getcwd(), 'static', 'index.html')
                
            if os.path.exists(static_file):
                return web.FileResponse(static_file)
            return web.Response(text="Dashboard UI not found in static folder.", status=404)

        async def start_app():
            app = web.Application()
            app.router.add_get('/', handle_index)
            app.router.add_get('/api/stats', handle_stats)
            app.router.add_get('/api/tasks', handle_tasks)
            app.router.add_get('/ws', handle_ws)
            
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', port)
            await site.start()
            self.dashboard_runner = runner
            print(f"[Snerd] Dashboard running on http://localhost:{port}")

        loop = asyncio.get_event_loop()
        loop.create_task(start_app())
