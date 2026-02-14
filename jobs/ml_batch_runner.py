"""Backward-compatible redirect to modular batch package.

The monolith has been split into:
  jobs/batch/config.py       — RunnerConfig
  jobs/batch/db_queries.py   — SQL helpers
  jobs/batch/prediction.py   — Prediction + threshold logic
  jobs/batch/enterprise.py   — Enterprise bridge
  jobs/batch/runner.py       — Orchestrator

This file re-exports for backward compatibility.
"""

from .batch.config import RunnerConfig
from .batch.runner import run_once, main

__all__ = ["RunnerConfig", "run_once", "main"]

if __name__ == "__main__":
    main()
