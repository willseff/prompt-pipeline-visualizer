import threading
import time
import random
from queue import Empty

from metrics import get_collector


class APIError(Exception):
    def __init__(self, status_code):
        super().__init__(f"API error {status_code}")
        self.status_code = status_code


class FakeClaudeClient:
    def __init__(self, base_latency=1.0, fatal_rate=0.1, transient_rate=0.2):
        self._base_latency = base_latency
        self._fatal_rate = fatal_rate
        self._transient_rate = transient_rate

    def messages_create(self, prompt: str) -> str:
        time.sleep(self._base_latency)
        roll = random.random()
        if roll < self._fatal_rate:
            raise APIError(400)
        if roll < self._fatal_rate + self._transient_rate:
            raise APIError(503)
        return f"Here is the answer for: {prompt}"


class ClaudeScheduler(threading.Thread):
    _instance_counter = 0
    _counter_lock = threading.Lock()

    def __init__(self, input_queue, output_queues, **kwargs):
        super().__init__()
        self._input_queue = input_queue
        self._output_queues = output_queues
        self._client = FakeClaudeClient()
        self._metrics = get_collector()
        with ClaudeScheduler._counter_lock:
            ClaudeScheduler._instance_counter += 1
            self._worker_id = f"ClaudeWorker[{ClaudeScheduler._instance_counter}]"
        self._metrics.worker_state(self._worker_id, "claude", "starting")
        self.start()

    def run(self):
        while True:
            self._metrics.worker_state(
                self._worker_id, "claude", "idle", "waiting for prompt"
            )
            try:
                prompt = self._input_queue.get(timeout=10)
            except Empty:
                print("Timeout reached in Claude, scheduler stopping")
                self._metrics.worker_state(self._worker_id, "claude", "done")
                break
            self._metrics.started(prompt)
            record = self._process(prompt)
            for q in self._output_queues:
                q.put(record)

    def _process(self, p, num_try=4, multiplier=2, wait=1):
        if wait > 1:
            self._metrics.worker_state(
                self._worker_id, "claude", "backoff", f"sleeping {wait}s before retry"
            )
        time.sleep(wait)
        if num_try == 0:
            self._metrics.max_retries(p)
            return (p, "Max number of retries exceeded", time.ctime())
        self._metrics.worker_state(self._worker_id, "claude", "calling api", p[:60])
        try:
            res = self._client.messages_create(p)
            self._metrics.success(p, res)
            return (p, res, time.ctime())
        except APIError as e:
            if e.status_code == 400:
                self._metrics.fatal(p)
                return (p, "Fatal Error 400", time.ctime())
            else:
                self._metrics.retry(p, attempt=6 - num_try)
                return self._process(
                    p,
                    num_try=num_try - 1,
                    multiplier=multiplier,
                    wait=wait * multiplier,
                )
