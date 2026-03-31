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

import operator
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
    show_provenance: bool                              # set once by caller (optional)
    tasks: list[TaskItem]                              # written by orchestrate_node
    results: Annotated[list[ResultItem], operator.add] # appended by each worker_node
    final_answer: str                                  # written by aggregate_node


# ---------------------------------------------------------------------------
# 2.  LLM (swap freely)
# ---------------------------------------------------------------------------

LLM = ChatAnthropic(model="claude-sonnet-4-6", temperature=0)


# ---------------------------------------------------------------------------
# 3.  Node: orchestrate
#     Reads:  state["goal"]
#     Writes: state["tasks"]
# ---------------------------------------------------------------------------

_decompose_chain = (
    ChatPromptTemplate.from_messages([
        ("system",
         "You are an orchestrator. Break the user's goal into the minimum number "
         "of independent sub-tasks needed. Each sub-task should map to one natural "
         "unit of work (e.g. one item, one entity, one category) — do NOT split a "
         "single unit into multiple sub-tasks. Output one sub-task per line, no numbering."),
        ("human", "{goal}"),
    ])
    | LLM
    | StrOutputParser()
)


async def orchestrate_node(state: OrchestratorState) -> dict:
    raw = await _decompose_chain.ainvoke({"goal": state["goal"]})
    lines = [l.strip() for l in raw.strip().splitlines() if l.strip()]
    if not lines:
        lines = [state["goal"]]

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


async def aggregate_node(state: OrchestratorState) -> dict:
    sorted_results = sorted(state["results"], key=lambda r: r["partition"])
    joined = "\n\n".join(
        f"[Worker #{r['worker_id']} / partition {r['partition']}]\n"
        + (r["result"] if not r["error"] else f"ERROR: {r['error']}")
        for r in sorted_results
    )
    chain = _make_aggregate_chain(state.get("show_provenance", False))
    final = await chain.ainvoke({"results": joined})
    print("[aggregate_node] synthesis complete")
    return {"final_answer": final}


# ---------------------------------------------------------------------------
# 7.  Build the graph
# ---------------------------------------------------------------------------

def build_graph(name: str) -> Any:
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

    return g.compile(name=name)


# ---------------------------------------------------------------------------
# 8.  Graph export for LangGraph Studio
#     langgraph.json points to: orchestrator_worker_langgraph.py:agent
#     Provide {"goal": "..."} as the initial state in the Studio UI.
# ---------------------------------------------------------------------------

graph = build_graph(name="ratings_workers")
