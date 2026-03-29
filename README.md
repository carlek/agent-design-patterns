# Agent Design Patterns

Practical implementations of multi-agent design patterns using LangChain and LangGraph.

Each pattern is self-contained with working code, a detailed write-up, and a sample payload.
Patterns are drawn from the architecture described in
*A Guide to Event-Driven Design for Agents and Multi-Agent Systems* (Confluent, 2025),
which covers how data streaming platforms enable scalable, loosely coupled agentic systems.


---

## Patterns

### 1. Orchestrator-Worker

> A central orchestrator decomposes a goal into independent sub-tasks, fans them out to
> worker agents in parallel, collects their results, and synthesizes a final answer.

The orchestrator-worker pattern maps directly to Kafka's partitioned topic model:
the orchestrator publishes one task per partition, worker agents consume from their
assigned partition, and results flow back on a response topic. In the implementations
here, asyncio queues stand in for Kafka — swapping transports requires no changes to
agent logic.

Two implementations are provided:

| File | Description |
|------|-------------|
| [`orchestrator_worker.py`](orchestrator-worker/orchestrator_worker.py) | LangChain skeleton with `StreamingPlatform` abstraction (asyncio queues), `Orchestrator`, and `WorkerAgent` classes |
| [`orchestrator_worker_langgraph.py`](orchestrator-worker/orchestrator_worker_langgraph.py) | Pure LangGraph `StateGraph` version — fan-out via `Send()`, fan-in via `operator.add` reducer, no message broker |

**CLI version** — runs a credit risk analysis of a fictional company, Acme Precision Parts Inc. The financial data (income statement, balance sheet, cash flows, debt schedule, and peer comparisons) is provided as a document payload in [`orchestrator-worker/payload.md`](orchestrator-worker/payload.md).

**LangGraph Studio version** ([`studio/`](orchestrator-worker/studio/)) — a general-purpose version that takes any goal as free-text input and does not require a document payload. In the Studio input panel, provide only the `goal` field as JSON, e.g. `{"goal": "Rate the top 3 sci-fi movies of 2025"}`. All other state fields are populated automatically by the graph.

![Studio example](orchestrator-worker/studio/studio_screenshot.png)

See [`orchestrator-worker/orchestrator_worker.md`](orchestrator-worker/orchestrator_worker.md)
and [`orchestrator-worker/orchestrator_worker_langgraph.md`](orchestrator-worker/orchestrator_worker_langgraph.md)
for detailed architecture notes.

> **Disclaimer:** The credit risk analysis produced by this demo is generated entirely by an LLM
> and is provided for illustrative purposes only. Outputs may vary, contain errors, or reflect
> model hallucinations. This is not financial or investment advice. Any production-ready
> credit risk system would require validated models, human oversight, regulatory compliance,
> and rigorous backtesting before use in real decision-making.

#### Quick start

```bash
cd orchestrator-worker
cp .env.example .env          # add your ANTHROPIC_API_KEY
pip install -r requirements.txt

# LangChain version
python orchestrator_worker.py

# LangGraph version (plain output)
python orchestrator_worker_langgraph.py

# LangGraph version (show worker attribution table)
python orchestrator_worker_langgraph.py --provenance

# LangGraph Studio 
cd studio
langgraph dev
```


---

## Reference

Falconer, S. (2025). *A Guide to Event-Driven Design for Agents and Multi-Agent Systems*.
Confluent, Inc.

Key concepts from the paper used across these patterns:
- **Orchestrator-Worker** — partitioned topic fan-out with consumer-group workers
- **Hierarchical Agent** — recursive orchestrator-worker applied across agent tiers
- **Blackboard** — shared streaming topic as a live knowledge base
- **Market-Based** — decentralized bid/ask event streams with a market-maker agent

Additional patterns (Hierarchical, Blackboard, Market-Based) will be added as separate
directories following the same structure.
