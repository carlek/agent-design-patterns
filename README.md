# Agent Design Patterns

Practical implementations of multi-agent design patterns using LangChain and LangGraph.

Each pattern is self-contained with working code, a detailed write-up, and a sample payload. Patterns are drawn from the architecture described in

- *A Guide to Event-Driven Design for Agents and Multi-Agent Systems* (Confluent, 2025),

which covers how data streaming platforms enable scalable, loosely coupled agentic systems.


---

## Patterns

1. [**Orchestrator-Worker**](orchestrator-worker/README.md) — fan-out sub-tasks to parallel workers, fan-in results


---

## Reference

Falconer, S. (2025). *A Guide to Event-Driven Design for Agents and Multi-Agent Systems*. Confluent, Inc.

Key concepts from the paper used across these patterns:
- **Orchestrator-Worker** — partitioned topic fan-out with consumer-group workers
- **Hierarchical Agent** — recursive orchestrator-worker applied across agent tiers
- **Blackboard** — shared streaming topic as a live knowledge base
- **Market-Based** — decentralized bid/ask event streams with a market-maker agent

Additional patterns (Hierarchical, Blackboard, Market-Based) will be added as separate directories following the same structure.
