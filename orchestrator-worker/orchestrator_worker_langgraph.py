"""
Orchestrator-Worker Pattern — Pure LangGraph StateGraph
=======================================================
No message broker. No queues. Communication is entirely through
the shared TypedDict State passed along graph edges.

Flow:
  orchestrate_node
      └─► fan_out_edge  (dynamic: one edge per task)
          worker_node (runs N times, one per partition)
              └─► aggregate_node (barrier: waits for all workers)
                      └─► END
"""

from __future__ import annotations

import asyncio
import operator
import pathlib
import uuid
from typing import Annotated, Any
from typing_extensions import TypedDict

from dotenv import load_dotenv
load_dotenv()

from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

from langgraph.graph import StateGraph, END
from langgraph.types import Send          # used for dynamic fan-out


# ---------------------------------------------------------------------------
# 1.  Shared State  (the only communication channel between nodes)
# ---------------------------------------------------------------------------

class TaskItem(TypedDict):
    task_id: str
    partition: int
    payload: str


class ResultItem(TypedDict):
    task_id: str
    partition: int
    worker_id: int
    result: str
    error: str | None


class OrchestratorState(TypedDict):
    """
    Full graph state.  Annotated[list, operator.add] means each worker
    *appends* its result rather than overwriting — the built-in fan-in reducer.
    """
    goal: str                                          # set once by caller
    tasks: list[TaskItem]                              # written by orchestrate_node
    results: Annotated[list[ResultItem], operator.add] # appended by each worker_node
    final_answer: str                                  # written by aggregate_node


# ---------------------------------------------------------------------------
# 2.  LLM (swap freely)
# ---------------------------------------------------------------------------

LLM = ChatAnthropic(model="claude-sonnet-4-6", temperature=0)
NUM_WORKERS = 3


# ---------------------------------------------------------------------------
# 3.  Node: orchestrate
#     Reads:  state["goal"]
#     Writes: state["tasks"]
# ---------------------------------------------------------------------------

_decompose_chain = (
    ChatPromptTemplate.from_messages([
        ("system",
         "You are an orchestrator. Break the user's goal into exactly {n} "
         "independent sub-tasks. Output one sub-task per line, no numbering."),
        ("human", "{goal}"),
    ])
    | LLM
    | StrOutputParser()
)


async def orchestrate_node(state: OrchestratorState) -> dict:
    raw = await _decompose_chain.ainvoke({"goal": state["goal"], "n": NUM_WORKERS})
    lines = [l.strip() for l in raw.strip().splitlines() if l.strip()]
    lines = (lines + ["(no task)"] * NUM_WORKERS)[:NUM_WORKERS]

    tasks = [
        TaskItem(task_id=str(uuid.uuid4()), partition=i, payload=p)
        for i, p in enumerate(lines)
    ]
    print(f"[orchestrate_node] decomposed into {len(tasks)} tasks")
    return {"tasks": tasks}


# ---------------------------------------------------------------------------
# 4.  Fan-out edge
#     Returns a list of Send() objects — one per task.
#     Each Send() injects a *subset* of state into worker_node.
# ---------------------------------------------------------------------------

def fan_out_edge(state: OrchestratorState) -> list[Send]:
    return [
        Send("worker_node", {"task": task, "worker_id": task["partition"] + 1})
        for task in state["tasks"]
    ]


# ---------------------------------------------------------------------------
# 5.  Node: worker
#     Receives:  {"task": TaskItem, "worker_id": int}  (injected by Send)
#     Writes:    one ResultItem appended to state["results"]
# ---------------------------------------------------------------------------

_worker_chain = (
    ChatPromptTemplate.from_messages([
        ("system",
         "You are Worker Agent #{worker_id}. Complete the assigned task concisely."),
        ("human", "{payload}"),
    ])
    | LLM
    | StrOutputParser()
)


async def worker_node(state: dict) -> dict:
    task: TaskItem = state["task"]
    worker_id: int = state["worker_id"]

    try:
        result_text = await _worker_chain.ainvoke({
            "worker_id": worker_id,
            "payload": task["payload"],
        })
        print(f"[worker_node #{worker_id}] partition {task['partition']} done")
        result = ResultItem(
            task_id=task["task_id"],
            partition=task["partition"],
            worker_id=worker_id,
            result=result_text,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        result = ResultItem(
            task_id=task["task_id"],
            partition=task["partition"],
            worker_id=worker_id,
            result="",
            error=str(exc),
        )

    # operator.add reducer automatically merges this list into state["results"]
    return {"results": [result]}


# ---------------------------------------------------------------------------
# 6.  Node: aggregate
#     Reads:  state["results"]  (all workers have written by now — fan-in barrier)
#     Writes: state["final_answer"]
# ---------------------------------------------------------------------------

_AGGREGATE_PROMPT_VERBOSE = (
    "You are an aggregator. First, produce a brief table showing each worker's "
    "contribution and its usefulness. Then synthesize all results into one coherent final answer."
)
_AGGREGATE_PROMPT_SILENT = (
    "You are an aggregator. Synthesize the worker results into one coherent answer. "
    "Do not describe the workers or their individual outputs — just write the final answer."
)

def _make_aggregate_chain(show_provenance: bool):
    prompt = _AGGREGATE_PROMPT_VERBOSE if show_provenance else _AGGREGATE_PROMPT_SILENT
    return (
        ChatPromptTemplate.from_messages([
            ("system", prompt),
            ("human", "Worker results:\n{results}"),
        ])
        | LLM
        | StrOutputParser()
    )

_aggregate_chain = _make_aggregate_chain(show_provenance=False)


async def aggregate_node(state: OrchestratorState) -> dict:
    sorted_results = sorted(state["results"], key=lambda r: r["partition"])
    joined = "\n\n".join(
        f"[Worker #{r['worker_id']} / partition {r['partition']}]\n"
        + (r["result"] if not r["error"] else f"ERROR: {r['error']}")
        for r in sorted_results
    )
    final = await _aggregate_chain.ainvoke({"results": joined})
    print("[aggregate_node] synthesis complete")
    return {"final_answer": final}


# ---------------------------------------------------------------------------
# 7.  Build the graph
# ---------------------------------------------------------------------------

def build_graph() -> Any:
    g = StateGraph(OrchestratorState)

    g.add_node("orchestrate_node", orchestrate_node)
    g.add_node("worker_node",      worker_node)
    g.add_node("aggregate_node",   aggregate_node)

    g.set_entry_point("orchestrate_node")

    # orchestrate_node → fan_out_edge → (N × worker_node) in parallel
    g.add_conditional_edges("orchestrate_node", fan_out_edge, ["worker_node"])

    # all worker_node branches converge on aggregate_node (automatic barrier)
    g.add_edge("worker_node", "aggregate_node")
    g.add_edge("aggregate_node", END)

    return g.compile()


# ---------------------------------------------------------------------------
# 8.  Entry point
# ---------------------------------------------------------------------------

async def main(show_provenance: bool = False) -> None:
    # Rebuild aggregate chain with the chosen mode, then compile the graph.
    global _aggregate_chain
    _aggregate_chain = _make_aggregate_chain(show_provenance)

    graph = build_graph()

    goal = (
        "Analyse the credit risk of a mid-market manufacturing company: "
        "assess its leverage ratios, liquidity position, and qualitative moat."
    )
    payload = (pathlib.Path(__file__).parent / "payload.md").read_text()

    initial_state: OrchestratorState = {
        "goal": f"{goal}\n\n{payload}",
        "tasks": [],
        "results": [],
        "final_answer": "",
    }

    final_state = await graph.ainvoke(initial_state)

    print("\n" + "-" * 60)
    print("FINAL ANSWER")
    print("-" * 60)
    print(final_state["final_answer"])


if __name__ == "__main__":
    import sys
    verbose = "--provenance" in sys.argv
    asyncio.run(main(show_provenance=verbose))
