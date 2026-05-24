import threading
import json
import os
from queue import Empty

from metrics import get_collector


class JsonLinesScheduler(threading.Thread):
    def __init__(self, input_queue, output_queues=None, **kwargs):
        super().__init__()
        self._input_queue = input_queue
        self._output_path = os.environ.get("JSONL_OUTPUT", "output/prompts.jsonl")
        os.makedirs(os.path.dirname(self._output_path), exist_ok=True)
        self._metrics = get_collector()
        self._worker_id = "JsonLines"
        self._metrics.worker_state(
            self._worker_id, "jsonl", "starting", self._output_path
        )
        self.start()

    def run(self):
        while True:
            self._metrics.worker_state(
                self._worker_id, "jsonl", "idle", "waiting for record"
            )
            try:
                prompt, response, response_time = self._input_queue.get(timeout=10)
            except Empty:
                print("Timeout reached in JsonLines, scheduler stopping")
                self._metrics.worker_state(self._worker_id, "jsonl", "done")
                break
            self._metrics.worker_state(self._worker_id, "jsonl", "writing", prompt[:60])
            line = (
                json.dumps(
                    {
                        "prompt": prompt,
                        "response": response,
                        "response_time": response_time,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            with open(self._output_path, "a", encoding="utf-8") as f:
                f.write(line)
            self._metrics.worker_tick(self._worker_id)
