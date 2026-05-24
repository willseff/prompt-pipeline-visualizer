import os
import time
import threading
from datetime import datetime

import pandas as pd
import streamlit as st

from metrics import get_collector


st.set_page_config(page_title="Prompt Pipeline", layout="wide")

collector = get_collector()

if "pipeline_thread" not in st.session_state:
    st.session_state.pipeline_thread = None


def pipeline_running():
    t = st.session_state.pipeline_thread
    return t is not None and t.is_alive()


def start_pipeline(num_prompts):
    os.environ["NUM_PROMPTS"] = str(num_prompts)
    collector.reset()

    from workers.ClaudeWorker import ClaudeScheduler
    from workers.PostgresWorker import PostgresMasterScheduler

    ClaudeScheduler._instance_counter = 0
    PostgresMasterScheduler._instance_counter = 0

    def target():
        try:
            from main import ensure_prompts_table

            ensure_prompts_table()
        except Exception as e:
            print(f"warning: postgres unavailable ({e})")
        from yaml_reader import YamlPipelineExecutor

        executor = YamlPipelineExecutor("pipelines/alpaca_claude_pipeline.yaml")
        executor.process_pipeline()

    t = threading.Thread(target=target, daemon=True)
    t.start()
    st.session_state.pipeline_thread = t


st.title("Prompt Pipeline")
st.caption("Alpaca prompts → fake Claude (recursive backoff) → Postgres + JSONL")

with st.sidebar:
    st.header("Controls")
    num_prompts = st.number_input(
        "NUM_PROMPTS", min_value=1, max_value=10000, value=20, step=1
    )
    running = pipeline_running()
    if st.button("Run Pipeline", disabled=running, type="primary"):
        start_pipeline(num_prompts)
        st.rerun()
    if running:
        st.success("Pipeline running")
    else:
        st.info("Idle")


EVENT_ICONS = {
    "started": "▶",
    "success": "✓",
    "fatal": "✗",
    "max_retries": "⊘",
    "retry": "↻",
}

KIND_ORDER = {"alpaca": 0, "claude": 1, "postgres": 2, "jsonl": 3}

STATE_BADGES = {
    "starting": "🟡 starting",
    "loading": "🟡 loading",
    "loaded": "🟢 loaded",
    "queueing": "🔵 queueing",
    "idle": "⚪ idle",
    "calling api": "🔵 calling api",
    "inserting": "🔵 inserting",
    "writing": "🔵 writing",
    "backoff": "🟠 backoff",
    "done": "✅ done",
}


def render(snap):
    cols = st.columns(6)
    cols[0].metric("Started", snap["started"])
    cols[1].metric("Success", snap["success"])
    cols[2].metric("Fatal 400", snap["fatal"])
    cols[3].metric("Max retries", snap["max_retries"])
    cols[4].metric("Retries", snap["retries"])
    cols[5].metric("Throughput", f"{snap['throughput']:.2f}/s")

    progress_total = max(snap["started"], 1)
    finished = snap["finished"]
    st.progress(
        min(finished / progress_total, 1.0),
        text=f"Finished {finished} / started {snap['started']} (in flight: {snap['in_flight']}) · elapsed {snap['elapsed']:.1f}s",
    )

    st.subheader("Workers")
    if not snap["workers"]:
        st.write("_(no workers registered yet)_")
    else:
        now = time.time()
        workers = sorted(
            snap["workers"],
            key=lambda w: (KIND_ORDER.get(w["kind"], 99), w["id"]),
        )
        wrows = [
            {
                "worker": w["id"],
                "state": STATE_BADGES.get(w["state"], w["state"]),
                "detail": w["detail"],
                "updated": f"{now - w['updated']:.1f}s ago",
            }
            for w in workers
        ]
        st.dataframe(
            pd.DataFrame(wrows),
            hide_index=True,
            use_container_width=True,
            height=min(48 + 35 * len(wrows), 400),
        )

    left, right = st.columns([1, 2])
    with left:
        st.subheader("Outcomes")
        outcomes = pd.DataFrame(
            {"count": [snap["success"], snap["fatal"], snap["max_retries"]]},
            index=["Success", "Fatal 400", "Max retries"],
        )
        st.bar_chart(outcomes, height=260)

    with right:
        st.subheader("Recent events")
        if not snap["recent"]:
            st.write("_(no events yet)_")
        else:
            rows = []
            for e in snap["recent"]:
                ts = datetime.fromtimestamp(e["t"]).strftime("%H:%M:%S")
                kind = e["type"]
                prompt = (e.get("prompt") or "")[:80]
                detail = ""
                if kind == "success":
                    detail = (e.get("response") or "")[:60]
                elif kind == "retry":
                    detail = f"attempt {e.get('attempt')}"
                rows.append(
                    {
                        "time": ts,
                        " ": EVENT_ICONS.get(kind, "?"),
                        "type": kind,
                        "prompt": prompt,
                        "detail": detail,
                    }
                )
            st.dataframe(
                pd.DataFrame(rows),
                hide_index=True,
                use_container_width=True,
                height=320,
            )


placeholder = st.empty()

while pipeline_running():
    with placeholder.container():
        render(collector.snapshot())
    time.sleep(0.5)

with placeholder.container():
    render(collector.snapshot())
