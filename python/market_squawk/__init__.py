"""Local point-in-time research, Rust analytics, and deterministic model training."""

from . import market_squawk as _native
from .bundle import BundleAuthorityRef
from .data import DatasetResult, UtcNanoseconds, open_dataset
from .finance import OperationContext
from .training import (
    TrainingEnvironmentReceipt,
    TrainingProposal,
    TrainingRun,
    training_environment_receipt,
)

__all__ = [
    "BundleAuthorityRef",
    "DatasetResult",
    "OperationContext",
    "TrainingProposal",
    "TrainingRun",
    "TrainingEnvironmentReceipt",
    "UtcNanoseconds",
    "open_dataset",
    "training_environment_receipt",
]
__version__ = _native.__version__
__market_squawk_build_identity__ = _native.__market_squawk_build_identity__
