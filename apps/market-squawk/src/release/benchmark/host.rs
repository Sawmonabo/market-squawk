use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use serde::Serialize;

use super::EvidenceAuthority;

const MEMORY_SAMPLE_INTERVAL: Duration = Duration::from_millis(10);
const MAXIMUM_PROBE_BYTES: usize = 64 * 1024;

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HostEvidence {
    operating_system: String,
    kernel: String,
    architecture: &'static str,
    logical_cpus: usize,
    cpu_model: String,
    physical_memory_bytes: u64,
    load_state: String,
    power_state: String,
    thermal_state: String,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ToolchainEvidence {
    rustc_verbose: String,
    cargo_version: String,
    stable_release_required: &'static str,
}

pub(super) fn toolchain_evidence(authority: EvidenceAuthority) -> Result<ToolchainEvidence> {
    let rustc = command_output("rustc", &["--version", "--verbose"])?;
    let cargo = command_output("cargo", &["--version"])?;
    if matches!(authority, EvidenceAuthority::ExactHead)
        && !rustc.lines().next().is_some_and(|line| {
            line == "rustc 1.97.1 (stable)" || line.starts_with("rustc 1.97.1 ")
        })
    {
        bail!("exact-head performance evidence requires stable Rust 1.97.1");
    }
    Ok(ToolchainEvidence {
        rustc_verbose: rustc,
        cargo_version: cargo,
        stable_release_required: "1.97.1",
    })
}

pub(super) fn host_evidence() -> Result<HostEvidence> {
    let logical_cpus = std::thread::available_parallelism()
        .context("logical CPU count is unavailable")?
        .get();
    #[cfg(target_os = "macos")]
    let state = macos_host_state()?;
    #[cfg(not(target_os = "macos"))]
    let state = portable_host_state()?;
    Ok(HostEvidence {
        operating_system: state.operating_system,
        kernel: state.kernel,
        architecture: std::env::consts::ARCH,
        logical_cpus,
        cpu_model: state.cpu_model,
        physical_memory_bytes: state.physical_memory_bytes,
        load_state: state.load_state,
        power_state: state.power_state,
        thermal_state: state.thermal_state,
    })
}

struct HostState {
    operating_system: String,
    kernel: String,
    cpu_model: String,
    physical_memory_bytes: u64,
    load_state: String,
    power_state: String,
    thermal_state: String,
}

#[cfg(target_os = "macos")]
fn macos_host_state() -> Result<HostState> {
    let cpu_model = command_output_optional("sysctl", &["-n", "machdep.cpu.brand_string"])?
        .or(command_output_optional("sysctl", &["-n", "hw.model"])?)
        .context("macOS CPU model is unavailable")?;
    Ok(HostState {
        operating_system: format!("macOS {}", command_output("sw_vers", &["-productVersion"])?),
        kernel: command_output("uname", &["-srv"])?,
        cpu_model,
        physical_memory_bytes: command_output("sysctl", &["-n", "hw.memsize"])?
            .parse::<u64>()
            .context("macOS physical memory is invalid")?,
        load_state: command_output("sysctl", &["-n", "vm.loadavg"])?,
        power_state: command_output("pmset", &["-g", "batt"])?,
        thermal_state: command_output("pmset", &["-g", "therm"])?,
    })
}

#[cfg(not(target_os = "macos"))]
fn portable_host_state() -> Result<HostState> {
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_else(|_| format!("{} {}", std::env::consts::OS, std::env::consts::ARCH));
    let cpu_model = cpu
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .map(|(_, value)| value.trim().to_owned())
        })
        .unwrap_or_else(|| format!("{} {}", std::env::consts::OS, std::env::consts::ARCH));
    let memory = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let physical_memory_bytes = memory
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|value| value.split_ascii_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|kib| kib.checked_mul(1024))
        .unwrap_or(0);
    Ok(HostState {
        operating_system: std::env::consts::OS.to_owned(),
        kernel: command_output_optional("uname", &["-srv"])?
            .unwrap_or_else(|| "unavailable".to_owned()),
        cpu_model,
        physical_memory_bytes,
        load_state: std::fs::read_to_string("/proc/loadavg")
            .unwrap_or_else(|_| "unavailable".to_owned())
            .trim()
            .to_owned(),
        power_state: "not exposed by this operating system".to_owned(),
        thermal_state: "not exposed by this operating system".to_owned(),
    })
}

fn command_output(program: &str, arguments: &[&str]) -> Result<String> {
    command_output_optional(program, arguments)?
        .with_context(|| format!("host probe {program} returned no state"))
}

fn command_output_optional(program: &str, arguments: &[&str]) -> Result<Option<String>> {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .with_context(|| format!("failed to execute host probe {program}"))?;
    if output.stdout.len() > MAXIMUM_PROBE_BYTES || output.stderr.len() > MAXIMUM_PROBE_BYTES {
        bail!("host probe {program} exceeded its output bound");
    }
    if !output.status.success() {
        return Ok(None);
    }
    let value = String::from_utf8(output.stdout).context("host probe output is not UTF-8")?;
    let value = value.trim();
    Ok((!value.is_empty()).then(|| value.to_owned()))
}

pub(super) struct MemorySampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicUsize>,
    worker: Option<JoinHandle<Result<()>>>,
}

impl MemorySampler {
    pub(super) fn start() -> Result<Self> {
        let initial = current_rss()?;
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicUsize::new(initial));
        let worker_stop = Arc::clone(&stop);
        let worker_peak = Arc::clone(&peak);
        let worker = std::thread::Builder::new()
            .name("release-rss-sampler".to_owned())
            .spawn(move || {
                while !worker_stop.load(Ordering::Acquire) {
                    worker_peak.fetch_max(current_rss()?, Ordering::AcqRel);
                    std::thread::sleep(MEMORY_SAMPLE_INTERVAL);
                }
                Ok(())
            })
            .context("resident-memory sampler could not start")?;
        Ok(Self {
            stop,
            peak,
            worker: Some(worker),
        })
    }

    pub(super) fn reset_peak(&mut self, baseline: u64) -> Result<()> {
        self.peak.store(
            usize::try_from(baseline).context("RSS baseline exceeds addressable memory")?,
            Ordering::Release,
        );
        Ok(())
    }

    pub(super) fn finish(&mut self) -> Result<u64> {
        self.stop.store(true, Ordering::Release);
        let worker = self
            .worker
            .take()
            .context("resident-memory sampler was already stopped")?;
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("resident-memory sampler panicked"))??;
        u64::try_from(self.peak.load(Ordering::Acquire))
            .context("peak RSS exceeds the report representation")
    }
}

impl Drop for MemorySampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _joined = worker.join();
        }
    }
}

pub(super) fn rss_plateau() -> Result<u64> {
    let mut maximum = 0_usize;
    for _ in 0..5 {
        maximum = maximum.max(current_rss()?);
        std::thread::sleep(MEMORY_SAMPLE_INTERVAL);
    }
    u64::try_from(maximum).context("resident-memory plateau exceeds the report representation")
}

pub(super) fn memory_sample_interval_millis() -> u64 {
    u64::try_from(MEMORY_SAMPLE_INTERVAL.as_millis()).unwrap_or(u64::MAX)
}

fn current_rss() -> Result<usize> {
    Ok(memory_stats::memory_stats()
        .context("resident-memory measurement is unavailable")?
        .physical_mem)
}
