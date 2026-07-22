"""Local point-in-time research, Rust analytics, and deterministic model training."""

from .bundle import BundleAuthorityRef
from .data import DatasetResult, UtcNanoseconds, open_dataset
from .finance import OperationContext
from .training import TrainingProposal, TrainingRun

__all__ = [
    "BundleAuthorityRef",
    "DatasetResult",
    "OperationContext",
    "TrainingProposal",
    "TrainingRun",
    "UtcNanoseconds",
    "open_dataset",
]
__version__ = "0.1.0"
