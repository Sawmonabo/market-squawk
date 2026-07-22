"""Local point-in-time research, Rust analytics, and deterministic model training."""

from .bundle import BundleAuthorityRef
from .data import DatasetResult, UtcNanoseconds, open_dataset
from .training import TrainingProposal, TrainingRun

__all__ = [
    "BundleAuthorityRef",
    "DatasetResult",
    "TrainingProposal",
    "TrainingRun",
    "UtcNanoseconds",
    "open_dataset",
]
__version__ = "0.1.0"
