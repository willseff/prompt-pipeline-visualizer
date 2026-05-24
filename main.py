import os
from sqlalchemy import create_engine, text
from yaml_reader import YamlPipelineExecutor


def ensure_prompts_table():
    PG_USER = os.environ.get("PG_USER") or "williamli"
    PG_PW = os.environ.get("PG_PW") or ""
    PG_HOST = os.environ.get("PG_HOST") or "localhost"
    PG_DB = os.environ.get("PG_DB") or "postgres"

    engine = create_engine(f"postgresql://{PG_USER}:{PG_PW}@{PG_HOST}/{PG_DB}")

    ddl = """
        CREATE TABLE IF NOT EXISTS prompts (
            id BIGSERIAL PRIMARY KEY,
            prompt TEXT NOT NULL,
            response TEXT NOT NULL,
            response_time TEXT NOT NULL
        )
    """

    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
        print("prompts table ready")


if __name__ == "__main__":
    ensure_prompts_table()
    executor = YamlPipelineExecutor("pipelines/alpaca_claude_pipeline.yaml")
    executor.process_pipeline()
