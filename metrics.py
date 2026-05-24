import threading
from collections import deque
from time import time


class StatsCollector:
    def __init__(self, recent_size=20):
        self._lock = threading.Lock()
        self._recent_size = recent_size
        self.reset()

    def reset(self):
        with self._lock:
            self.started_count = 0
            self.success_count = 0
            self.fatal_count = 0
            self.max_retries_count = 0
            self.retry_count = 0
            self.recent = deque(maxlen=self._recent_size)
            self.start_time = time()
            self.workers = {}

    def worker_state(self, worker_id, kind, state, detail=""):
        with self._lock:
            existing = self.workers.get(worker_id, {})
            self.workers[worker_id] = {
                "id": worker_id,
                "kind": kind,
                "state": state,
                "detail": detail,
                "updated": time(),
                "processed": existing.get("processed", 0),
                "last_busy": existing.get("last_busy", 0.0),
            }

    def worker_tick(self, worker_id):
        with self._lock:
            w = self.workers.get(worker_id)
            if w is None:
                return
            w["processed"] += 1
            w["last_busy"] = time()

    def started(self, prompt):
        with self._lock:
            self.started_count += 1
            self.recent.appendleft({"type": "started", "prompt": prompt, "t": time()})

    def success(self, prompt, response):
        with self._lock:
            self.success_count += 1
            self.recent.appendleft(
                {"type": "success", "prompt": prompt, "response": response, "t": time()}
            )

    def fatal(self, prompt):
        with self._lock:
            self.fatal_count += 1
            self.recent.appendleft({"type": "fatal", "prompt": prompt, "t": time()})

    def max_retries(self, prompt):
        with self._lock:
            self.max_retries_count += 1
            self.recent.appendleft(
                {"type": "max_retries", "prompt": prompt, "t": time()}
            )

    def retry(self, prompt, attempt):
        with self._lock:
            self.retry_count += 1
            self.recent.appendleft(
                {"type": "retry", "prompt": prompt, "attempt": attempt, "t": time()}
            )

    def snapshot(self):
        with self._lock:
            elapsed = time() - self.start_time
            finished = self.success_count + self.fatal_count + self.max_retries_count
            return {
                "started": self.started_count,
                "success": self.success_count,
                "fatal": self.fatal_count,
                "max_retries": self.max_retries_count,
                "retries": self.retry_count,
                "finished": finished,
                "in_flight": self.started_count - finished,
                "elapsed": elapsed,
                "throughput": finished / elapsed if elapsed > 0 else 0.0,
                "recent": list(self.recent),
                "workers": list(self.workers.values()),
            }


_collector = StatsCollector()


def get_collector():
    return _collector
