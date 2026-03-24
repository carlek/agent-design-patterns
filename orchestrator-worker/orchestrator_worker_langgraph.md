# Orchestrator-Worker-LangGraph

This version uses LangGraph as the messaging contract instead of the asyncio queues

**What replaced what:**
- `StreamingPlatform` + asyncio queues → **`OrchestratorState` TypedDict** — the state dict *is* the message bus
- `publish_task` / `consume_partition` → **`Send()`** in `fan_out_edge` — injects per-task state directly into each worker invocation
- Manual `task_ids` correlation loop → **`operator.add` reducer** on `results` — LangGraph merges all worker outputs automatically before `aggregate_node` runs; that's your fan-in barrier for free
- `asyncio.create_task` worker loop → **`add_conditional_edges(..., fan_out_edge, ["worker_node"])`** — graph runtime dispatches workers in parallel

**Critical design points:**
- `Annotated[list[ResultItem], operator.add]` is the whole fan-in trick — without the reducer annotation, the last worker would overwrite the others
- `Send("worker_node", {...})` passes a *local* state slice to each worker; workers don't see each other's payloads
- The edge `worker_node → aggregate_node` is a natural barrier — LangGraph won't run `aggregate_node` until all `Send`-dispatched branches resolve
- Scaling: change `NUM_WORKERS` only — the graph wiring is unchanged