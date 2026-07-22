"""Local point-in-time research, Rust analytics, and deterministic model training."""

from .data import DatasetResult, UtcNanoseconds, open_dataset
from .training import TrainingRun

__all__ = ["DatasetResult", "TrainingRun", "UtcNanoseconds", "open_dataset"]
__version__ = "0.1.0"
