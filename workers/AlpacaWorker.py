import threading
import os
import pandas as pd


class AlpacaProducer(threading.Thread):
    def __init__(self, input_queue=None, output_queues=None, **kwargs):
        super().__init__()
        self._output_queues = output_queues
        self._num_prompts = int(os.environ.get("NUM_PROMPTS", "20"))
        self._dataset_url = (
            "hf://datasets/yahma/alpaca-cleaned/alpaca_data_cleaned.json"
        )

        print(f"AlpacaProducer loading dataset (NUM_PROMPTS={self._num_prompts})")
        df = pd.read_json(self._dataset_url)
        if self._num_prompts > 0:
            df = df.head(self._num_prompts)
        self._instructions = df["instruction"].tolist()
        print(f"AlpacaProducer loaded {len(self._instructions)} instructions")

        self.start()

    def run(self):
        for instruction in self._instructions:
            for q in self._output_queues:
                q.put(instruction)
