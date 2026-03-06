# ADR 0001 - Implementation Deviations

## Context

Repository was built from `fastgraphs_clone_single_agent_build_spec.md` in offline-first mode.

## Deviations

1. Repository/project display name is **AlphaLens** while Python package command remains `fg`.
2. Frozen fixtures are compact representative payloads rather than full raw vendor responses.
3. Live-source integration exists but default behavior prioritizes fixture-backed deterministic execution.
4. Storage and fixtures use Parquet when an engine is available and transparently fall back to CSV in constrained environments (same table contracts).
5. Refresh callback orchestration is synchronous in local v1 (no true background worker progress stream/cancel semantics yet).

No other intentional deviations are known at this time.
