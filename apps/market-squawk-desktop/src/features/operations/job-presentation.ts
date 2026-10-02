export function jobKindLabel(kind: string): string {
  const knownJobKinds: Record<string, string> = {
    "research.ingest-source.v1": "Import research data",
    "market.prepare-history.v1": "Load price history",
    "research.prepare-investment-financials.v1": "Load financial information",
    "research.phase-one-derived-generation-job.v1": "Build research dataset",
    "analysis.phase-one-feature-derived-generation-job.v1": "Build analysis dataset",
    "research.dataset-export.v1": "Export dataset",
    "analysis.scenario-batch.v1": "Compare scenarios",
    "analysis.backtest.v1": "Run backtest",
    "model.forecast-generation.v1": "Prepare forecast",
    "model.training.v1": "Train model",
    "decision.screen-run.v1": "Find opportunities",
    "operations.product-backup.v1": "Create or check backups",
    "operations.recovery.v1": "Recover workspace or program",
    "operations.trusted-update.v1": "Update program",
  }
  return Object.hasOwn(knownJobKinds, kind) ? knownJobKinds[kind]! : "Background job"
}

export function jobStateLabel(state: string): string {
  const labels: Record<string, string> = {
    queued: "Queued", preparing: "Preparing", running: "Running",
    awaiting_confirmation: "Needs confirmation", cancelling: "Cancelling",
    completed: "Completed", failed: "Failed", cancelled: "Cancelled",
    interrupted: "Interrupted", recovering: "Recovering",
  }
  return Object.hasOwn(labels, state) ? labels[state]! : "Status unavailable"
}

export function jobPhaseLabel(phase: string): string {
  const labels: Record<string, string> = {
    "validating-inputs": "Checking inputs",
    "resolving-inputs": "Preparing inputs",
    "preparing-adjusted-history": "Preparing price history",
    "preparing-investment-financials": "Preparing financial information",
    "building-phase-one-derived-generation": "Building dataset",
    "evaluating-screen": "Finding opportunities",
    "validated-admitted-operation": "Checks complete",
    "executing-lifecycle-operation": "Applying changes",
    "training-model": "Training model",
    "evaluating-candidate": "Evaluating model",
    "exporting-candidate": "Saving model",
    "candidate-staged": "Model prepared",
    "training-cancelled": "Training cancelled",
    "training-failed": "Training failed",
  }
  return Object.hasOwn(labels, phase) ? labels[phase]! : "Progress step unavailable"
}

export function jobFailureLabel(failure: string): string {
  const labels: Record<string, string> = {
    recovery: "Recovery failed",
    "decision-screen-failure": "Opportunity search failed",
    "operations-backup-failure": "Backup failed",
    "operations-lifecycle-failure": "Program or workspace change failed",
    "application-operation": "Job could not finish",
    backtest: "Backtest failed",
    "model-training": "Model training failed",
    "operations-recovery-failure": "Recovery failed",
    "operations-update-failure": "Program update failed",
    "forecast-progress-unavailable": "Forecast progress unavailable",
    "forecast-recovery-authority-unavailable": "Forecast recovery unavailable",
  }
  return Object.hasOwn(labels, failure) ? labels[failure]! : "Job could not finish"
}
