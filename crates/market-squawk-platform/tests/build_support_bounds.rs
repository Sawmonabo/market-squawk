//! Adversarial bounds for build-script filesystem and subprocess inputs.

#[path = "../build_support.rs"]
mod build_support;

use std::fs;
#[cfg(unix)]
use std::sync::Mutex;
use std::time::Duration;

use tempfile::tempdir;

#[cfg(unix)]
static BOUNDED_COMMAND_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn benchmark_backend_selection_is_closed_and_binds_distinct_sources()
-> Result<(), Box<dyn std::error::Error>> {
    use build_support::{BenchmarkBackend, bind_benchmark_backend_sources};

    assert_eq!(
        BenchmarkBackend::parse("standard")?,
        BenchmarkBackend::Standard
    );
    assert_eq!(
        BenchmarkBackend::parse("candidate")?,
        BenchmarkBackend::Candidate
    );
    assert!(BenchmarkBackend::parse("STANDARD").is_err());
    assert!(BenchmarkBackend::parse("standard,candidate").is_err());
    assert!(BenchmarkBackend::parse("").is_err());

    let package = tempdir()?;
    let benchmark = package.path().join("benches/capture_admission");
    let selected = benchmark.join("backend");
    fs::create_dir_all(&selected)?;
    fs::write(benchmark.join("backend.rs"), b"closed dispatcher")?;
    fs::write(selected.join("standard.rs"), b"bounded standard channel")?;
    fs::write(selected.join("candidate.rs"), b"bounded fixed ring")?;

    let standard =
        bind_benchmark_backend_sources(package.path(), BenchmarkBackend::Standard, 4_096)?;
    let candidate =
        bind_benchmark_backend_sources(package.path(), BenchmarkBackend::Candidate, 4_096)?;
    assert_eq!(standard.backend(), BenchmarkBackend::Standard);
    assert_eq!(candidate.backend(), BenchmarkBackend::Candidate);
    assert_eq!(
        standard.selected_source_relative_path(),
        "benches/capture_admission/backend/standard.rs"
    );
    assert_eq!(
        candidate.selected_source_relative_path(),
        "benches/capture_admission/backend/candidate.rs"
    );
    assert_ne!(
        standard.selected_source_sha256(),
        candidate.selected_source_sha256()
    );
    assert_ne!(standard.backend_sha256(), candidate.backend_sha256());
    assert_eq!(standard.dispatcher_sha256(), candidate.dispatcher_sha256());

    fs::write(selected.join("candidate.rs"), b"bounded standard channel")?;
    assert!(
        bind_benchmark_backend_sources(package.path(), BenchmarkBackend::Candidate, 4_096).is_err()
    );
    Ok(())
}

#[test]
fn benchmark_backend_environment_selection_is_closed() {
    use build_support::{BenchmarkBackend, select_benchmark_backend};

    assert_eq!(
        select_benchmark_backend(false, None, None),
        Ok(BenchmarkBackend::Standard)
    );
    assert_eq!(
        select_benchmark_backend(false, None, Some("candidate")),
        Ok(BenchmarkBackend::Candidate)
    );
    assert!(select_benchmark_backend(false, Some("candidate"), None).is_err());
    assert!(select_benchmark_backend(false, None, Some("standard,candidate")).is_err());
    assert_eq!(
        select_benchmark_backend(true, Some("standard"), None),
        Ok(BenchmarkBackend::Standard)
    );
    assert_eq!(
        select_benchmark_backend(true, Some("candidate"), None),
        Ok(BenchmarkBackend::Candidate)
    );
    assert!(select_benchmark_backend(true, None, None).is_err());
    assert!(select_benchmark_backend(true, Some("standard,candidate"), None).is_err());
    assert!(select_benchmark_backend(true, Some("standard"), Some("candidate")).is_err());
}

#[test]
fn descriptor_hash_rejects_oversized_and_symlink_inputs() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let regular = directory.path().join("regular.rs");
    fs::write(&regular, b"bounded")?;
    assert_eq!(build_support::hash_regular_file(&regular, 7)?.len(), 64);
    assert!(build_support::hash_regular_file(&regular, 6).is_err());
    assert!(
        build_support::hash_regular_file_with_test_mutation(&regular, 7, || {
            let _result = fs::write(&regular, b"mutated");
        })
        .is_err()
    );

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&regular, directory.path().join("linked.rs"))?;
        assert!(build_support::hash_regular_file(&directory.path().join("linked.rs"), 7).is_err());
    }
    Ok(())
}

#[test]
fn traversal_rejects_symlinks_excess_entries_and_depth() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    fs::write(directory.path().join("one.rs"), b"one")?;
    fs::write(directory.path().join("two.rs"), b"two")?;
    let files: Vec<build_support::BoundSourceFile> =
        build_support::collect_rust_files(directory.path(), 2, 4, 16, 32)?;
    assert_eq!(files.len(), 2);
    assert!(build_support::collect_rust_files(directory.path(), 1, 4, 16, 32).is_err());

    let nested = directory.path().join("a");
    fs::create_dir(&nested)?;
    let deeper = nested.join("b");
    fs::create_dir(&deeper)?;
    assert!(build_support::collect_rust_files(directory.path(), 16, 1, 16, 32).is_err());

    let oversized = directory.path().join("oversized.rs");
    fs::write(&oversized, b"seventeen-bytes!!")?;
    assert!(build_support::collect_rust_files(directory.path(), 32, 4, 16, 64).is_err());
    fs::remove_file(oversized)?;

    fs::write(directory.path().join("three.rs"), b"three")?;
    assert!(build_support::collect_rust_files(directory.path(), 32, 4, 16, 10).is_err());

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&nested, directory.path().join("linked-directory"))?;
        assert!(build_support::collect_rust_files(directory.path(), 16, 4, 16, 64).is_err());

        let root_race_parent = tempdir()?;
        let root = root_race_parent.path().join("root");
        let displaced = root_race_parent.path().join("displaced");
        fs::create_dir(&root)?;
        fs::write(root.join("original.rs"), b"original")?;
        assert!(
            build_support::collect_rust_files_with_test_root_replacement(
                &root,
                16,
                4,
                16,
                64,
                || {
                    fs::rename(&root, &displaced).map_err(|_error| ()).ok();
                    fs::create_dir(&root).map_err(|_error| ()).ok();
                    fs::write(root.join("replacement.rs"), b"replacement")
                        .map_err(|_error| ())
                        .ok();
                },
            )
            .is_err()
        );

        let raced_root = tempdir()?;
        let raced = raced_root.path().join("raced");
        fs::create_dir(&raced)?;
        fs::write(raced.join("inside.rs"), b"inside")?;
        let moved = raced_root.path().join("moved");
        let outside = tempdir()?;
        fs::write(outside.path().join("outside.rs"), b"outside")?;
        let mut replaced = false;
        assert!(
            build_support::collect_rust_files_with_test_replacement(
                raced_root.path(),
                16,
                4,
                16,
                64,
                |path| {
                    if !replaced && path == raced {
                        fs::rename(&raced, &moved).map_err(|_error| ()).ok();
                        std::os::unix::fs::symlink(outside.path(), &raced)
                            .map_err(|_error| ())
                            .ok();
                        replaced = true;
                    }
                },
            )
            .is_err()
        );
    }
    Ok(())
}

#[test]
fn authoritative_group_control_rejects_unsupported_platforms()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(
        build_support::validate_process_group_support(
            build_support::CommandPolicy::DevelopmentIsolated,
            false,
        )
        .is_err()
    );
    assert!(
        build_support::validate_process_group_support(
            build_support::CommandPolicy::AuthoritativeInheritOuter {
                expected_process_group: 1,
            },
            false,
        )
        .is_ok()
    );
    assert!(
        build_support::validate_process_group_support(
            build_support::CommandPolicy::DevelopmentIsolated,
            true,
        )
        .is_ok()
    );
    #[cfg(unix)]
    {
        let group_id = build_support::current_process_group_id_for_test();
        let group = group_id.to_string();
        assert_eq!(
            build_support::authoritative_command_policy(Some("inherit-outer-v1"), Some(&group),)?,
            build_support::CommandPolicy::AuthoritativeInheritOuter {
                expected_process_group: group_id,
            }
        );
        assert!(build_support::authoritative_command_policy(None, Some(&group)).is_err());
        assert!(
            build_support::authoritative_command_policy(Some("new-group-v1"), Some(&group))
                .is_err()
        );
        assert!(
            build_support::authoritative_command_policy(Some("inherit-outer-v1"), Some("1"))
                .is_err()
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn bounded_reader_cancels_and_joins_a_non_eof_reader() -> Result<(), Box<dyn std::error::Error>> {
    let (pipe_reader, _pipe_writer) = std::os::unix::net::UnixStream::pair()?;
    let deadline = std::time::Instant::now() + Duration::from_millis(20);
    assert!(build_support::cancel_non_eof_reader_for_test(pipe_reader, 32, deadline).is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn authoritative_child_inherits_the_bound_outer_process_group()
-> Result<(), Box<dyn std::error::Error>> {
    let _process_test_guard = match BOUNDED_COMMAND_TEST_LOCK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let directory = tempdir()?;
    let script = directory.path().join("inherited-group");
    fs::write(&script, b"#!/bin/sh\nexec /bin/ps -o pgid= -p $$\n")?;
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
    let expected = build_support::current_process_group_id_for_test();
    let output = build_support::run_command_with_post_spawn_deadline(
        &script,
        directory.path(),
        &[],
        32,
        32,
        Duration::from_secs(1),
        build_support::authoritative_command_policy(
            Some("inherit-outer-v1"),
            Some(&expected.to_string()),
        )?,
    )?;
    assert_eq!(
        std::str::from_utf8(&output.stdout)?.trim(),
        expected.to_string()
    );
    assert!(output.stderr.is_empty());
    Ok(())
}

#[cfg(unix)]
#[test]
fn bounded_command_rejects_output_flood_and_extinguishes_pipe_holding_descendants()
-> Result<(), Box<dyn std::error::Error>> {
    let _process_test_guard = match BOUNDED_COMMAND_TEST_LOCK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let directory = tempdir()?;
    let bounded = directory.path().join("bounded-git");
    fs::write(
        &bounded,
        b"#!/bin/sh\n[ -z \"${HOME+x}\" ] || exit 71\n[ \"$GIT_CONFIG_NOSYSTEM\" = 1 ] || exit 72\n[ \"$GIT_CONFIG_GLOBAL\" = /dev/null ] || exit 73\n[ \"$GIT_OPTIONAL_LOCKS\" = 0 ] || exit 74\n[ \"$GIT_TERMINAL_PROMPT\" = 0 ] || exit 75\n[ \"$GIT_NO_REPLACE_OBJECTS\" = 1 ] || exit 76\n[ \"$LC_ALL\" = C ] || exit 77\n[ -z \"$PATH\" ] || exit 78\nprintf stdout\nprintf stderr >&2\n",
    )?;
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(&bounded, fs::Permissions::from_mode(0o700))?;
    let output = build_support::run_command_with_post_spawn_deadline(
        &bounded,
        directory.path(),
        &[],
        32,
        32,
        Duration::from_secs(1),
        build_support::authoritative_command_policy(
            Some("inherit-outer-v1"),
            Some(&build_support::current_process_group_id_for_test().to_string()),
        )?,
    )?;
    assert_eq!(output.stdout, b"stdout");
    assert_eq!(output.stderr, b"stderr");

    let script = directory.path().join("fake-git");
    let descendant_pid = directory.path().join("descendant.pid");
    fs::write(
        &script,
        b"#!/bin/sh\n/bin/sleep 30 &\nprintf '%s\\n' \"$!\" > \"$1\"\nwhile :; do printf xxxxxxxxxxxxxxxx; done\n",
    )?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
    let started = std::time::Instant::now();
    assert!(
        build_support::run_command_with_post_spawn_deadline(
            &script,
            directory.path(),
            &[descendant_pid.to_str().ok_or("non-UTF-8 PID path")?],
            32,
            32,
            Duration::from_millis(500),
            build_support::CommandPolicy::DevelopmentIsolated,
        )
        .is_err()
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_process_absent(&descendant_pid)?;

    let exited = directory.path().join("exited-git");
    let exited_descendant_pid = directory.path().join("exited-descendant.pid");
    fs::write(
        &exited,
        b"#!/bin/sh\n/bin/sleep 30 &\nprintf '%s\\n' \"$!\" > \"$1\"\nexit 0\n",
    )?;
    fs::set_permissions(&exited, fs::Permissions::from_mode(0o700))?;
    let output = build_support::run_command_with_post_spawn_deadline(
        &exited,
        directory.path(),
        &[exited_descendant_pid.to_str().ok_or("non-UTF-8 PID path")?],
        32,
        32,
        Duration::from_millis(500),
        build_support::CommandPolicy::DevelopmentIsolated,
    )?;
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    assert_process_absent(&exited_descendant_pid)?;

    let closed = directory.path().join("closed-git");
    let closed_descendant_pid = directory.path().join("closed-descendant.pid");
    fs::write(
        &closed,
        b"#!/bin/sh\n/bin/sh -c 'trap \"\" TERM; exec /bin/sleep 30' >/dev/null 2>&1 &\nprintf '%s\n' \"$!\" > \"$1\"\nexit 0\n",
    )?;
    fs::set_permissions(&closed, fs::Permissions::from_mode(0o700))?;
    let started = std::time::Instant::now();
    let output = build_support::run_command_with_post_spawn_deadline(
        &closed,
        directory.path(),
        &[closed_descendant_pid.to_str().ok_or("non-UTF-8 PID path")?],
        32,
        32,
        Duration::from_millis(500),
        build_support::CommandPolicy::DevelopmentIsolated,
    )?;
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_process_absent(&closed_descendant_pid)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn bounded_command_cleans_up_after_setup_poll_and_read_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let _process_test_guard = match BOUNDED_COMMAND_TEST_LOCK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let directory = tempdir()?;
    let script = directory.path().join("faulting-git");
    fs::write(
        &script,
        b"#!/bin/sh\nprintf '%s\n' \"$$\" > \"$1\"\n/bin/sleep 30 &\nprintf '%s\n' \"$!\" > \"$2\"\nwhile :; do /bin/sleep 1; done\n",
    )?;
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;

    for fault in ["second_reader_setup", "poll", "stdout_read"] {
        let leader_pid = directory.path().join(format!("{fault}-leader.pid"));
        let descendant_pid = directory.path().join(format!("{fault}-descendant.pid"));
        let timeout = Duration::from_millis(500);
        let maximum = build_support::maximum_enforced_post_spawn_duration_for_test(timeout)
            .ok_or("fixture post-spawn deadline overflowed")?;
        let result = build_support::run_command_with_post_spawn_deadline_and_test_fault(
            &script,
            directory.path(),
            &[
                leader_pid.to_str().ok_or("non-UTF-8 leader PID path")?,
                descendant_pid
                    .to_str()
                    .ok_or("non-UTF-8 descendant PID path")?,
            ],
            timeout,
            build_support::CommandPolicy::DevelopmentIsolated,
            fault,
            &descendant_pid,
        );
        let error = match result {
            Ok(_output) => return Err(format!("{fault} unexpectedly succeeded").into()),
            Err(error) => error,
        };
        assert_eq!(maximum, Duration::from_millis(2_100));
        if fault == "second_reader_setup" {
            assert!(
                error
                    .to_string()
                    .contains("deadline elapsed during stderr reader initialization"),
                "unexpected setup failure: {error}"
            );
        }
        assert_process_absent(&leader_pid)?;
        assert_process_absent(&descendant_pid)?;
    }
    Ok(())
}

#[test]
fn cleanup_grace_is_derived_from_the_documented_phase_constants() {
    let (term, group_kill, leader_reap, pipe_finish, total) =
        build_support::cleanup_grace_milliseconds_for_test();
    assert_eq!(
        (term, group_kill, leader_reap, pipe_finish),
        (100, 500, 500, 500)
    );
    assert_eq!(total, term + group_kill + leader_reap + pipe_finish);
    assert_eq!(total, 1_600);
}

#[cfg(unix)]
#[derive(Debug, Default)]
struct FailingProcessGroupControl {
    signals: Mutex<Vec<build_support::ProcessGroupSignal>>,
}

#[cfg(unix)]
impl build_support::ProcessGroupControl for FailingProcessGroupControl {
    fn signal(
        &self,
        _process_id: rustix::process::Pid,
        signal: build_support::ProcessGroupSignal,
    ) -> Result<(), String> {
        match self.signals.lock() {
            Ok(mut signals) => signals.push(signal),
            Err(poisoned) => poisoned.into_inner().push(signal),
        }
        Err("injected signal failure".to_owned())
    }

    fn exists(&self, _process_id: rustix::process::Pid) -> Result<bool, String> {
        Ok(true)
    }
}

#[cfg(unix)]
#[test]
fn process_group_signal_failures_are_bounded_and_reported() -> Result<(), Box<dyn std::error::Error>>
{
    let controller = FailingProcessGroupControl::default();
    let process_id = rustix::process::Pid::from_raw(42).ok_or("fixture PID must be nonzero")?;
    let started = std::time::Instant::now();
    assert!(build_support::terminate_process_group(&controller, process_id, || Ok(())).is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
    let signals = match controller.signals.lock() {
        Ok(signals) => signals.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(
        signals,
        [
            build_support::ProcessGroupSignal::Terminate,
            build_support::ProcessGroupSignal::Kill,
        ]
    );
    Ok(())
}

#[cfg(unix)]
fn assert_process_absent(pid_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !pid_path.exists() {
        if std::time::Instant::now() >= deadline {
            return Err("fake bound tool did not publish its descendant PID".into());
        }
        std::thread::yield_now();
    }
    let pid = fs::read_to_string(pid_path)?;
    let raw_pid = pid.trim().parse::<i32>()?;
    let process_id = rustix::process::Pid::from_raw(raw_pid).ok_or("descendant PID was zero")?;
    while rustix::process::test_kill_process(process_id).is_ok() {
        if std::time::Instant::now() >= deadline {
            return Err("bounded build command left a live descendant".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}
