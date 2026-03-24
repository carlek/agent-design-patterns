"""
Orchestrator-Worker Pattern — LangChain Skeleton
================================================
Architecture (from diagram):
  Orchestrator
    └─► Orchestration Topic (Partitions 1..N)  ← fan-out
          Partition 1 ──► Worker Agent #1
          Partition 2 ──► Worker Agent #2
          Partition 3 ──► Worker Agent #3
                              └─► Response Topic ──► Orchestrator (aggregates)

Streaming platform abstraction: thin wrapper so you can swap
asyncio queues (default here) for Kafka, Pulsar, Redis Streams, etc.
"""

from __future__ import annotations

import asyncio

import uuid
from dataclasses import dataclass, field
from typing import Any

from langchain_core.messages import HumanMessage, AIMessage, BaseMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.output_parsers import StrOutputParser
from langchain_anthropic import ChatAnthropic          # swap for ChatOpenAI etc.
# from langchain_openai import ChatOpenAI

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# 1.  Data contract
# ---------------------------------------------------------------------------

@dataclass
class Task:
    """A unit of work written to a partition of the Orchestration Topic."""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    partition: int = 0
    payload: str = ""                  # the actual work instruction
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskResult:
    """What a Worker writes back to the Response Topic."""
    task_id: str = ""
    partition: int = 0
    worker_id: int = 0
    result: str = ""
    error: str | None = None


# ---------------------------------------------------------------------------
# 2.  Streaming platform abstraction (asyncio queues as stand-in)
# ---------------------------------------------------------------------------

class StreamingPlatform:
    """
    Replace `_partitions` / `_response_topic` with real Kafka producers/consumers,
    Redis Streams XADD/XREAD, or any other transport.
    """

    def __init__(self, num_partitions: int = 3) -> None:
        self.num_partitions = num_partitions
        # Orchestration Topic: one queue per partition
        self._partitions: list[asyncio.Queue[Task]] = [
            asyncio.Queue() for _ in range(num_partitions)
        ]
        # Response Topic: single queue (workers write, orchestrator reads)
        self._response_topic: asyncio.Queue[TaskResult] = asyncio.Queue()

    # -- Orchestration Topic (producer side) ---------------------------------
    async def publish_task(self, task: Task) -> None:
        await self._partitions[task.partition].put(task)

    # -- Orchestration Topic (consumer side, per-partition) ------------------
    async def consume_partition(self, partition: int) -> Task:
        return await self._partitions[partition].get()

    # -- Response Topic (producer side, workers) -----------------------------
    async def publish_result(self, result: TaskResult) -> None:
        await self._response_topic.put(result)

    # -- Response Topic (consumer side, orchestrator) ------------------------
    async def consume_response(self) -> TaskResult:
        return await self._response_topic.get()


# ---------------------------------------------------------------------------
# 3.  Worker Agent
# ---------------------------------------------------------------------------

class WorkerAgent:
    """
    Consumes tasks from its assigned partition.
    Runs an LLM chain, then writes results to the Response Topic.
    """

    def __init__(
        self,
        worker_id: int,
        partition: int,
        platform: StreamingPlatform,
        llm=None,
    ) -> None:
        self.worker_id = worker_id
        self.partition = partition
        self.platform = platform

        # --- LangChain chain ------------------------------------------------
        _llm = llm or ChatAnthropic(model="claude-sonnet-4-20250514")

        self._chain = (
            ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        "You are Worker Agent #{worker_id}. "
                        "Complete the assigned task concisely.",
                    ),
                    MessagesPlaceholder("history"),
                    ("human", "{task_payload}"),
                ]
            ).partial(worker_id=str(worker_id))
            | _llm
            | StrOutputParser()
        )

        self._history: list[BaseMessage] = []

    async def run(self, max_tasks: int | None = None) -> None:
        """Poll partition until max_tasks consumed (None = run forever)."""
        processed = 0
        while max_tasks is None or processed < max_tasks:
            task = await self.platform.consume_partition(self.partition)
            try:
                result_text = await self._chain.ainvoke(
                    {
                        "history": self._history,
                        "task_payload": task.payload,
                    }
                )
                # Optionally maintain per-worker history
                self._history.append(HumanMessage(content=task.payload))
                self._history.append(AIMessage(content=result_text))

                await self.platform.publish_result(
                    TaskResult(
                        task_id=task.task_id,
                        partition=task.partition,
                        worker_id=self.worker_id,
                        result=result_text,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                await self.platform.publish_result(
                    TaskResult(
                        task_id=task.task_id,
                        partition=task.partition,
                        worker_id=self.worker_id,
                        error=str(exc),
                    )
                )
            processed += 1


# ---------------------------------------------------------------------------
# 4.  Orchestrator
# ---------------------------------------------------------------------------

class Orchestrator:
    """
    - Receives a high-level goal.
    - Decomposes it into Tasks and distributes them across partitions.
    - Collects results from the Response Topic and aggregates.
    """

    def __init__(self, platform: StreamingPlatform, llm=None) -> None:
        self.platform = platform

        _llm = llm or ChatAnthropic(model="claude-sonnet-4-20250514")

        # Chain 1: decompose goal → sub-tasks (one per line)
        self._decompose_chain = (
            ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        "You are an orchestrator. "
                        "Break the user's goal into exactly {n} independent sub-tasks. "
                        "Output one sub-task per line, no numbering.",
                    ),
                    ("human", "{goal}"),
                ]
            )
            | _llm
            | StrOutputParser()
        )

        # Chain 2: aggregate results → final answer
        self._aggregate_chain = (
            ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        "You are an orchestrator. "
                        "Synthesize the worker results below into a single coherent answer.",
                    ),
                    ("human", "Worker results:\n{results}"),
                ]
            )
            | _llm
            | StrOutputParser()
        )

    async def run(self, goal: str) -> str:
        n = self.platform.num_partitions

        # --- Decompose ------------------------------------------------------
        raw = await self._decompose_chain.ainvoke({"goal": goal, "n": n})
        sub_tasks = [line.strip() for line in raw.strip().splitlines() if line.strip()]
        # Pad / truncate to exactly n tasks
        sub_tasks = (sub_tasks + ["(no task)"] * n)[:n]

        # --- Publish to Orchestration Topic (fan-out) -----------------------
        task_ids: list[str] = []
        for partition, payload in enumerate(sub_tasks):
            task = Task(partition=partition, payload=payload)
            task_ids.append(task.task_id)
            await self.platform.publish_task(task)
            print(f"[Orchestrator] → Partition {partition}: {payload!r}")

        # --- Collect from Response Topic ------------------------------------
        results: dict[str, TaskResult] = {}
        pending = set(task_ids)
        while pending:
            res = await self.platform.consume_response()
            if res.task_id in pending:
                pending.discard(res.task_id)
                results[res.task_id] = res
                status = f"Worker #{res.worker_id} | partition {res.partition}"
                if res.error:
                    print(f"[Orchestrator] ✗ {status} — ERROR: {res.error}")
                else:
                    print(f"[Orchestrator] ✓ {status} — {res.result[:80]}…")

        # --- Aggregate ------------------------------------------------------
        joined = "\n\n".join(
            f"[Worker #{r.worker_id} / partition {r.partition}]\n"
            + (r.result if not r.error else f"ERROR: {r.error}")
            for r in sorted(results.values(), key=lambda x: x.partition)
        )
        final = await self._aggregate_chain.ainvoke({"results": joined})
        return final


# ---------------------------------------------------------------------------
# 5.  Bootstrap
# ---------------------------------------------------------------------------

async def main() -> None:
    NUM_PARTITIONS = 3

    platform = StreamingPlatform(num_partitions=NUM_PARTITIONS)

    # Spin up one Worker Agent per partition
    workers = [
        WorkerAgent(worker_id=i + 1, partition=i, platform=platform)
        for i in range(NUM_PARTITIONS)
    ]

    orchestrator = Orchestrator(platform=platform)

    goal = (
        "Analyse the credit risk of a mid-market manufacturing company: "
        "assess its leverage ratios, liquidity position, and qualitative moat."
    )

    # Run workers as background tasks (each processes exactly 1 task here)
    worker_tasks = [
        asyncio.create_task(worker.run(max_tasks=1)) for worker in workers
    ]

    # Orchestrate
    final_answer = await orchestrator.run(goal)

    # Wait for all workers to finish
    await asyncio.gather(*worker_tasks)

    print("\n" + "-" * 60)
    print("FINAL ANSWER")
    print("-" * 60)
    print(final_answer)


if __name__ == "__main__":
    asyncio.run(main())
