import threading
from sqlalchemy import create_engine
import os
from sqlalchemy.sql import text
from queue import Empty


class PostgresMasterScheduler(threading.Thread):
    def __init__(self, input_queue, **kwargs):
        super().__init__()
        self._input_queue = input_queue

        self.start()

    def run(self):
        while True:
            try:
                val = self._input_queue.get(timeout=10)  # if no value in 10 seconds
                prompt, response, response_time = val
                postgresWorker = PostgresWorker(prompt, response, response_time)
                postgresWorker.insert_into_db()
            except Empty:
                print("Timeout reached in postgres, scheduler stopping")
                break


class PostgresWorker:
    def __init__(self, prompt, response, response_time):
        self._prompt = prompt
        self._response = response
        self._response_time = response_time

        self._PG_USER = os.environ.get("PG_USER") or "williamli"
        self._PG_PW = os.environ.get("PG_PW") or ""
        self._PG_HOST = os.environ.get("PG_HOST") or "localhost"
        self._PG_DB = os.environ.get("PG_DB") or "postgres"

        self._engine = create_engine(
            f"postgresql://{self._PG_USER}:{self._PG_PW}@{self._PG_HOST}/{self._PG_DB}"
        )

    def _create_insert_query(self):
        SQL = """INSERT INTO prompts (prompt, response, response_time) VALUES (:prompt, :response, :response_time)"""
        return SQL

    def insert_into_db(self):
        insert_query = self._create_insert_query()

        with self._engine.connect() as conn:
            print("inserting values to db")
            try:
                conn.execute(
                    text(insert_query),
                    {
                        "prompt": self._prompt,
                        "response": self._response,
                        "response_time": self._response_time,
                    },
                )

                conn.commit()

                print(
                    f"{self._prompt}, {self._response}, {self._response_time} inserted to db"
                )

            except Exception as e:
                print(e)
