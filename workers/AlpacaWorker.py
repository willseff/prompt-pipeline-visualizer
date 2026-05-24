import threading
import os
import pandas as pd

from metrics import get_collector


class AlpacaProducer(threading.Thread):
    def __init__(self, input_queue=None, output_queues=None, **kwargs):
        super().__init__()
        self._output_queues = output_queues
        self._num_prompts = int(os.environ.get("NUM_PROMPTS", "20"))
        # Source: hf://datasets/yahma/alpaca-cleaned/alpaca_data_cleaned.json
        self._dataset_path = "data/alpaca_data_cleaned.json"
        self._metrics = get_collector()
        self._worker_id = "AlpacaProducer"

        self._metrics.worker_state(
            self._worker_id, "alpaca", "loading", f"NUM_PROMPTS={self._num_prompts}"
        )
        print(f"AlpacaProducer loading dataset (NUM_PROMPTS={self._num_prompts})")
        df = pd.read_json(self._dataset_path)
        if self._num_prompts > 0:
            df = df.head(self._num_prompts)
        self._instructions = df["instruction"].tolist()
        print(f"AlpacaProducer loaded {len(self._instructions)} instructions")
        self._metrics.worker_state(
            self._worker_id,
            "alpaca",
            "loaded",
            f"{len(self._instructions)} instructions",
        )

        self.start()

    def run(self):
        for i, instruction in enumerate(self._instructions, start=1):
            self._metrics.worker_state(
                self._worker_id,
                "alpaca",
                "queueing",
                f"{i}/{len(self._instructions)}",
            )
            for q in self._output_queues:
                q.put(instruction)
        self._metrics.worker_state(self._worker_id, "alpaca", "done")
