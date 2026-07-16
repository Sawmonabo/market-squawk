from __future__ import annotations

from pathlib import Path
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[2]
POLICY = ROOT / "deny.toml"


class DependencyPolicyTests(unittest.TestCase):
    def test_policy_checks_all_features_on_documented_tier_one_targets(self) -> None:
        policy = tomllib.loads(POLICY.read_text())
        graph = policy["graph"]
        self.assertTrue(graph["all-features"])
        self.assertEqual(
            set(graph["targets"]),
            {
                "aarch64-apple-darwin",
                "aarch64-unknown-linux-gnu",
                "x86_64-apple-darwin",
                "x86_64-pc-windows-msvc",
                "x86_64-unknown-linux-gnu",
                "x86_64-unknown-linux-musl",
            },
        )

    def test_advisory_and_source_failures_are_denied_without_ignores(self) -> None:
        policy = tomllib.loads(POLICY.read_text())
        advisories = policy["advisories"]
        self.assertEqual(advisories["unmaintained"], "all")
        self.assertEqual(advisories["unsound"], "all")
        self.assertEqual(advisories["yanked"], "deny")
        self.assertEqual(advisories["ignore"], [])

        sources = policy["sources"]
        self.assertEqual(sources["unknown-registry"], "deny")
        self.assertEqual(sources["unknown-git"], "deny")
        self.assertEqual(sources["allow-git"], [])

    def test_wildcards_native_tls_openssl_and_telemetry_are_denied(self) -> None:
        policy = tomllib.loads(POLICY.read_text())
        bans = policy["bans"]
        self.assertEqual(bans["wildcards"], "deny")
        denied = {
            entry if isinstance(entry, str) else entry["crate"]
            for entry in bans["deny"]
        }
        self.assertTrue(
            {
                "native-tls",
                "native-tls-sys",
                "openssl",
                "openssl-sys",
                "opentelemetry",
                "opentelemetry-otlp",
                "opentelemetry_sdk",
                "tracing-opentelemetry",
            }.issubset(denied)
        )


if __name__ == "__main__":
    unittest.main()
