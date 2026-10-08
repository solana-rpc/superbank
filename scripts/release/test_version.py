"""Tests for scripts/release/version.py. Run from the repo root:
python3 -m unittest discover -s scripts/release -p 'test_*.py'
"""

import unittest
from pathlib import Path

import version as v

ROOT = Path(__file__).resolve().parents[2]

CARGO = """[workspace]
members = ["crates/superbank"]

[workspace.package]
version = "0.6.0"
edition = "2024"
rust-version = "1.98.1"
authors = [
    "Triton One Limited",
]

[workspace.dependencies]
clap = { version = "4.5.40" }
"""

TAGS = ["v0.6.0", "v0.6.0-rc1", "v0.6.0-rc2", "v0.6.1", "v0.7.0", "v0.7.1-rc1", "v0.7.1-rc2", "v0.5.0-solparq.18", "nightly"]


class SemverTest(unittest.TestCase):
    def test_ordering(self):
        ordered = ["0.6.0-rc1", "0.6.0-rc2", "0.6.0-rc10", "0.6.0", "0.6.1", "0.7.0-alpha", "0.7.0-alpha.1", "0.7.0-beta", "0.7.0", "1.0.0"]
        self.assertEqual(sorted(reversed(ordered), key=v.parse), ordered)

    def test_numeric_identifiers_compare_as_numbers(self):
        self.assertTrue(v.is_newer("1.0.0-rc.10", "1.0.0-rc.2"))
        self.assertTrue(v.is_newer("1.0.0-alpha", "1.0.0-1"), "numeric identifiers sort before alphanumeric")

    def test_rejects_non_semver(self):
        for bad in ["0.6", "v0.6.0", "0.6.0.1", "01.2.3", "1.2.3-", "1.2.3+build", "1.2.3-rc..1", "", "latest"]:
            with self.assertRaises(v.VersionError, msg=bad):
                v.parse(bad)


class CargoTomlTest(unittest.TestCase):
    def test_reads_only_the_workspace_package_version(self):
        self.assertEqual(v.workspace_version(CARGO), "0.6.0")

    def test_set_changes_only_that_line(self):
        out = v.set_workspace_version(CARGO, "0.7.1-rc3")
        self.assertEqual(v.workspace_version(out), "0.7.1-rc3")
        self.assertIn('clap = { version = "4.5.40" }', out)
        self.assertIn('rust-version = "1.98.1"', out)
        self.assertEqual(out.replace('"0.7.1-rc3"', '"0.6.0"', 1), CARGO)

    def test_missing_table_or_version(self):
        with self.assertRaises(v.VersionError):
            v.workspace_version("[package]\nversion = \"1.0.0\"\n")
        with self.assertRaises(v.VersionError):
            v.workspace_version("[workspace.package]\nedition = \"2024\"\n")

    def test_the_real_cargo_toml_parses(self):
        v.parse(v.workspace_version((ROOT / "Cargo.toml").read_text()))


class ReadmeTest(unittest.TestCase):
    def test_docker_tags(self):
        readme = "docker build -t superbank:0.6.0 .\n  superbank:0.6.0-rc1\nimage: superbank:local\nsuperbank-rpc:0.6.0\n"
        out = v.set_readme_docker_tags(readme, "0.7.1")
        self.assertEqual(out, "docker build -t superbank:0.7.1 .\n  superbank:0.7.1\nimage: superbank:local\nsuperbank-rpc:0.6.0\n")


class TagTest(unittest.TestCase):
    def test_latest_release_ignores_solparq_and_non_release_tags(self):
        self.assertEqual(v.latest_release(TAGS), "0.7.1-rc2")
        self.assertIsNone(v.latest_release(["nightly", "v0.5.0-solparq.18"]))

    def test_check_flags_main_behind_a_release(self):
        with self.assertRaisesRegex(v.VersionError, "behind the latest release"):
            v.check("0.6.0", TAGS)
        v.check("0.7.1-rc2", TAGS)
        v.check("0.7.1", TAGS)
        v.check("0.1.0", [])

    def test_new_version_must_move_forward(self):
        v.check_new_version("0.7.1-rc3", "0.7.1-rc2", TAGS)
        v.check_new_version("0.7.1", "0.7.1-rc2", TAGS)
        with self.assertRaisesRegex(v.VersionError, "newer than the current"):
            v.check_new_version("0.7.1-rc2", "0.7.1-rc2", TAGS)
        with self.assertRaisesRegex(v.VersionError, "latest release tag"):
            v.check_new_version("0.7.0", "0.6.0", TAGS)
        with self.assertRaises(v.VersionError):
            v.check_new_version("not-a-version", "0.6.0", TAGS)

    def test_catch_up_may_equal_the_latest_tag(self):
        v.check_new_version("0.7.1-rc2", "0.6.0", TAGS, allow_existing=True)
        with self.assertRaisesRegex(v.VersionError, "older than the latest release"):
            v.check_new_version("0.7.0", "0.6.0", TAGS, allow_existing=True)


class CliTest(unittest.TestCase):
    def test_verify_tag(self):
        current = v.workspace_version((ROOT / "Cargo.toml").read_text())
        self.assertEqual(v.main(["verify-tag", f"v{current}"]), 0)
        self.assertEqual(v.main(["verify-tag", "v999.0.0"]), 1)


if __name__ == "__main__":
    unittest.main()
