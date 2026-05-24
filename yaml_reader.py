import yaml
import importlib

from multiprocessing import Queue


class YamlPipelineExecutor:
    def __init__(self, pipeline_location):
        self._pipeline_location = pipeline_location
        self._queues = {}
        self._workers = {}

    def _load_pipeline(self):
        with open(self._pipeline_location) as inFile:
            self._yaml_data = yaml.safe_load(inFile)

    def _initialize_queues(self):
        for queue in self._yaml_data["queues"]:
            queue_name = queue["name"]
            print(queue_name)
            self._queues[queue_name] = Queue()

    def _initialize_workers(self):
        for worker in self._yaml_data["workers"]:
            worker_name = worker["name"]
            WorkerClass = getattr(
                importlib.import_module(worker["location"]), worker["class"]
            )  # access the class from the module
            input_queue = worker.get("input_queue")
            output_queues = worker.get("output_queues")
            number_instances = worker.get("instances", 1)

            init_params = {
                "input_queue": self._queues[input_queue]
                if input_queue is not None
                else None,
                "output_queues": [
                    self._queues[output_queue] for output_queue in output_queues
                ]
                if output_queues is not None
                else None,
            }

            self._workers[worker_name] = []
            for i in range(number_instances):
                self._workers[worker_name].append(WorkerClass(**init_params))
                print(
                    f"Initialized worker {worker_name} instance {i + 1}/{number_instances}"
                )

    def _join_workers(self):
        for worker_name in self._workers:
            for worker_thread in self._workers[worker_name]:
                worker_thread.join()

    def process_pipeline(self):
        self._load_pipeline()
        self._initialize_queues()
        self._initialize_workers()
        self._join_workers()


if __name__ == "__main__":
    yamlPipelineExecutor = YamlPipelineExecutor("pipelines/alpaca_claude_pipeline.yaml")
    yamlPipelineExecutor.process_pipeline()
