//! Canonical terminal-index encoding, validation, and command identities.

use market_squawk_domain::{InstrumentId, SourceId, SourceIdentifier, Timestamp};
use market_squawk_platform::LocalAuthorityStateStore;
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::{
    GovernedBacktestRepositoryLimits, ProductionGovernedBacktestRepositoryError, strictly_ordered,
};
use crate::application::{
    analysis::{BacktestScope, GovernedBacktestCommand, GovernedBacktestRecord},
    domain_support::encode_hex,
};

const TERMINAL_INDEX_SCHEMA_VERSION: u16 = 1;

#[derive(Clone)]
pub(super) struct TerminalIndex {
    pub(super) entries: Vec<StoredTerminal>,
}

impl TerminalIndex {
    pub(super) const fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub(super) fn decode(
        bytes: &[u8],
        limits: GovernedBacktestRepositoryLimits,
    ) -> Result<Self, ProductionGovernedBacktestRepositoryError> {
        if bytes.len() > limits.maximum_index_bytes {
            return Err(ProductionGovernedBacktestRepositoryError::CorruptIndex);
        }
        let wire: TerminalIndexWire = serde_json::from_slice(bytes)
            .map_err(|_| ProductionGovernedBacktestRepositoryError::CorruptIndex)?;
        if wire.schema_version != TERMINAL_INDEX_SCHEMA_VERSION
            || wire.entries.len() > limits.maximum_terminals
        {
            return Err(ProductionGovernedBacktestRepositoryError::CorruptIndex);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(wire.entries.len())
            .map_err(|_| ProductionGovernedBacktestRepositoryError::ResourceExhausted)?;
        for wire_entry in wire.entries {
            let command = wire_entry.command.into_command()?;
            let record = GovernedBacktestRecord::try_from_persisted(wire_entry.record)
                .map_err(|_| ProductionGovernedBacktestRepositoryError::CorruptIndex)?;
            if wire_entry.run_id != record.run_id()
                || !super::canonical_run_id(&wire_entry.command_digest)
                || wire_entry.command_digest
                    != command_digest(&command)
                        .map_err(|_| ProductionGovernedBacktestRepositoryError::CorruptIndex)?
                || !super::canonical_run_id(&wire_entry.record_digest)
                || wire_entry.record_digest
                    != value_digest(record.content())
                        .map_err(|_| ProductionGovernedBacktestRepositoryError::CorruptIndex)?
            {
                return Err(ProductionGovernedBacktestRepositoryError::CorruptIndex);
            }
            entries.push(StoredTerminal {
                command,
                command_digest: wire_entry.command_digest,
                record_digest: wire_entry.record_digest,
                record,
            });
        }
        if entries
            .windows(2)
            .any(|pair| pair[0].record.run_id() >= pair[1].record.run_id())
        {
            return Err(ProductionGovernedBacktestRepositoryError::CorruptIndex);
        }
        let index = Self { entries };
        if index
            .encode(limits)
            .map_err(|_| ProductionGovernedBacktestRepositoryError::CorruptIndex)?
            != bytes
        {
            return Err(ProductionGovernedBacktestRepositoryError::CorruptIndex);
        }
        Ok(index)
    }

    pub(super) fn encode(
        &self,
        limits: GovernedBacktestRepositoryLimits,
    ) -> Result<Vec<u8>, ProductionGovernedBacktestRepositoryError> {
        if self.entries.len() > limits.maximum_terminals {
            return Err(ProductionGovernedBacktestRepositoryError::ResourceExhausted);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(self.entries.len())
            .map_err(|_| ProductionGovernedBacktestRepositoryError::ResourceExhausted)?;
        for terminal in &self.entries {
            entries.push(TerminalEntryView {
                run_id: terminal.record.run_id(),
                command_digest: &terminal.command_digest,
                command: CommandWire::from_command(&terminal.command),
                record_digest: &terminal.record_digest,
                record: terminal.record.content(),
            });
        }
        let bytes = serde_json::to_vec(&TerminalIndexView {
            schema_version: TERMINAL_INDEX_SCHEMA_VERSION,
            entries,
        })
        .map_err(|_| ProductionGovernedBacktestRepositoryError::CorruptIndex)?;
        if bytes.len() > limits.maximum_index_bytes
            || bytes.len() > LocalAuthorityStateStore::maximum_payload_bytes()
        {
            return Err(ProductionGovernedBacktestRepositoryError::ResourceExhausted);
        }
        Ok(bytes)
    }

    pub(super) fn insert(&mut self, terminal: StoredTerminal) -> Result<(), ServiceError> {
        let position = self
            .entries
            .binary_search_by(|candidate| candidate.record.run_id().cmp(terminal.record.run_id()))
            .unwrap_or_else(|position| position);
        self.entries
            .try_reserve_exact(1)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        self.entries.insert(position, terminal);
        Ok(())
    }

    pub(super) fn get(&self, run_id: &str) -> Option<&GovernedBacktestRecord> {
        self.entries
            .binary_search_by(|candidate| candidate.record.run_id().cmp(run_id))
            .ok()
            .and_then(|position| self.entries.get(position))
            .map(|terminal| &terminal.record)
    }
}

#[derive(Clone)]
pub(super) struct StoredTerminal {
    pub(super) command: GovernedBacktestCommand,
    pub(super) command_digest: String,
    pub(super) record_digest: String,
    pub(super) record: GovernedBacktestRecord,
}

impl PartialEq for StoredTerminal {
    fn eq(&self, other: &Self) -> bool {
        self.command == other.command
            && self.command_digest == other.command_digest
            && self.record_digest == other.record_digest
            && self.record.content() == other.record.content()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TerminalIndexView<'a> {
    schema_version: u16,
    entries: Vec<TerminalEntryView<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TerminalEntryView<'a> {
    run_id: &'a str,
    command_digest: &'a str,
    command: CommandWire,
    record_digest: &'a str,
    record: &'a Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TerminalIndexWire {
    schema_version: u16,
    entries: Vec<TerminalEntryWire>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TerminalEntryWire {
    run_id: String,
    command_digest: String,
    command: CommandWire,
    record_digest: String,
    record: Value,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CommandWire {
    strategy_id: SourceIdentifier,
    input_id: SourceIdentifier,
    instruments: Vec<InstrumentId>,
    time_range: Option<TimeRangeWire>,
    sources: Vec<SourceId>,
}

impl CommandWire {
    fn from_command(command: &GovernedBacktestCommand) -> Self {
        Self {
            strategy_id: command.strategy_id().clone(),
            input_id: command.input_id().clone(),
            instruments: command.scope().instruments().to_vec(),
            time_range: command
                .scope()
                .time_range()
                .map(|(starts_at, ends_at)| TimeRangeWire {
                    starts_at_unix_nanos: starts_at.unix_nanos(),
                    ends_at_unix_nanos: ends_at.unix_nanos(),
                }),
            sources: command.scope().sources().to_vec(),
        }
    }

    fn into_command(
        self,
    ) -> Result<GovernedBacktestCommand, ProductionGovernedBacktestRepositoryError> {
        if !strictly_ordered(&self.instruments) || !strictly_ordered(&self.sources) {
            return Err(ProductionGovernedBacktestRepositoryError::CorruptIndex);
        }
        let time_range = self.time_range.map(TimeRangeWire::into_range).transpose()?;
        Ok(GovernedBacktestCommand::new(
            self.strategy_id,
            self.input_id,
            BacktestScope::new(self.instruments, time_range, self.sources),
        ))
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TimeRangeWire {
    starts_at_unix_nanos: i64,
    ends_at_unix_nanos: i64,
}

impl TimeRangeWire {
    fn into_range(
        self,
    ) -> Result<(Timestamp, Timestamp), ProductionGovernedBacktestRepositoryError> {
        if self.starts_at_unix_nanos >= self.ends_at_unix_nanos {
            return Err(ProductionGovernedBacktestRepositoryError::CorruptIndex);
        }
        Ok((
            Timestamp::from_unix_nanos(self.starts_at_unix_nanos),
            Timestamp::from_unix_nanos(self.ends_at_unix_nanos),
        ))
    }
}

pub(super) fn command_digest(command: &GovernedBacktestCommand) -> Result<String, ServiceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/governed-backtest-command/v1");
    hash_text(&mut hash, command.strategy_id().as_str())?;
    hash_text(&mut hash, command.input_id().as_str())?;
    hash_count(&mut hash, command.scope().instruments().len())?;
    for instrument in command.scope().instruments() {
        hash.update(instrument.as_uuid().as_bytes());
    }
    match command.scope().time_range() {
        Some((starts_at, ends_at)) => {
            hash.update([1]);
            hash.update(starts_at.unix_nanos().to_be_bytes());
            hash.update(ends_at.unix_nanos().to_be_bytes());
        }
        None => hash.update([0]),
    }
    hash_count(&mut hash, command.scope().sources().len())?;
    for source in command.scope().sources() {
        hash_text(&mut hash, source.as_str())?;
    }
    Ok(encode_hex(hash.finalize().into()))
}

fn hash_text(hash: &mut Sha256, value: &str) -> Result<(), ServiceError> {
    hash_count(hash, value.len())?;
    hash.update(value.as_bytes());
    Ok(())
}

fn hash_count(hash: &mut Sha256, value: usize) -> Result<(), ServiceError> {
    let value = u64::try_from(value).map_err(|_| ServiceError::ResourceExhausted)?;
    hash.update(value.to_be_bytes());
    Ok(())
}

pub(super) fn value_digest(value: &Value) -> Result<String, ServiceError> {
    let bytes = serde_json::to_vec(value).map_err(|_| ServiceError::InvalidResult)?;
    Ok(encode_hex(Sha256::digest(bytes).into()))
}
