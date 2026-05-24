import threading
import json
import os
from queue import Empty


class JsonLinesScheduler(threading.Thread):
    def __init__(self, input_queue, output_queues=None, **kwargs):
        super().__init__()
        self._input_queue = input_queue
        self._output_path = os.environ.get("JSONL_OUTPUT", "output/prompts.jsonl")
        os.makedirs(os.path.dirname(self._output_path), exist_ok=True)
        self.start()

    def run(self):
        while True:
            try:
                prompt, response, response_time = self._input_queue.get(timeout=10)
            except Empty:
                print("Timeout reached in JsonLines, scheduler stopping")
                break
            line = json.dumps(
                {"prompt": prompt, "response": response, "response_time": response_time},
                ensure_ascii=False,
            ) + "\n"
            with open(self._output_path, "a", encoding="utf-8") as f:
                f.write(line)
