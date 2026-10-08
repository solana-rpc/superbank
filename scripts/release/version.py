#!/usr/bin/env python3
"""Keeps the Superbank workspace version and release tags in step.

The single source of truth is `version` under `[workspace.package]` in the
root Cargo.toml; every crate inherits it (`version.workspace = true`). A
release tag is `v<version>`. Tags matching `v*-solparq.*` are solparq-only
releases with their own numbering and are ignored here.

Commands (run from anywhere in the repo):
  show              print the workspace version
  check             fail if a release tag is newer than the workspace version
                    (main is behind a release)
  set VERSION       set the workspace version in Cargo.toml and the README
                    docker tags; it must be newer than the current version and
                    every release tag. Run `cargo update --workspace` after.
  verify-tag TAG    fail unless TAG is v<workspace version>

Tags come from `git ls-remote --tags origin`; pass --local-tags to read the
local repository's tags instead.

Pre-release ordering follows SemVer, except that identifiers mixing letters
and digits compare naturally, so `rc10` sorts after `rc2` as people expect
(strict SemVer compares them as text).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

_IDENT = r"(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)"
SEMVER = re.compile(
    rf"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-({_IDENT}(?:\.{_IDENT})*))?$"
)
RELEASE_TAG = re.compile(r"^v(.+)$")
SOLPARQ_TAG = re.compile(r"-solparq\.")
WORKSPACE_TABLE = re.compile(r"^\[workspace\.package\][^\S\n]*\n(.*?)(?=^\[|\Z)", re.M | re.S)
VERSION_LINE = re.compile(r'^(version\s*=\s*")([^"]*)(")', re.M)
README_DOCKER_TAG = re.compile(r"\bsuperbank:(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)\b")


class VersionError(Exception):
    pass


# --- SemVer ---------------------------------------------------------------


def parse(version: str) -> tuple:
    """Returns a sortable key for a SemVer version (no build metadata)."""
    m = SEMVER.match(version)
    if not m:
        raise VersionError(f"not a SemVer version (X.Y.Z or X.Y.Z-pre): {version!r}")
    major, minor, patch, pre = m.groups()
    core = (int(major), int(minor), int(patch))
    if pre is None:
        # A release sorts after every pre-release of the same core version.
        return core + ((1,),)
    return core + ((0, tuple(_ident_key(part) for part in pre.split("."))),)


def _ident_key(ident: str) -> tuple:
    if ident.isdigit():
        return (0, int(ident))  # numeric identifiers sort before alphanumeric
    # Natural order inside an identifier: rc2 < rc10.
    return (1, tuple((0, int(chunk)) if chunk.isdigit() else (1, chunk) for chunk in re.findall(r"\d+|\D+", ident)))


def is_newer(a: str, b: str) -> bool:
    return parse(a) > parse(b)


# --- Repository state -------------------------------------------------------


def workspace_version(cargo_toml: str) -> str:
    table = WORKSPACE_TABLE.search(cargo_toml)
    if not table:
        raise VersionError("Cargo.toml has no [workspace.package] table")
    line = VERSION_LINE.search(table.group(1))
    if not line:
        raise VersionError("[workspace.package] has no version")
    return line.group(2)


def set_workspace_version(cargo_toml: str, version: str) -> str:
    table = WORKSPACE_TABLE.search(cargo_toml)
    if not table:
        raise VersionError("Cargo.toml has no [workspace.package] table")
    body, count = VERSION_LINE.subn(lambda m: f"{m.group(1)}{version}{m.group(3)}", table.group(1), count=1)
    if count != 1:
        raise VersionError("[workspace.package] has no version")
    return cargo_toml[: table.start(1)] + body + cargo_toml[table.end(1) :]


def set_readme_docker_tags(readme: str, version: str) -> str:
    return README_DOCKER_TAG.sub(f"superbank:{version}", readme)


def release_versions(tag_names) -> list:
    """Versions of the release tags among `tag_names` (others are skipped)."""
    versions = []
    for name in tag_names:
        m = RELEASE_TAG.match(name)
        if not m or SOLPARQ_TAG.search(name) or not SEMVER.match(m.group(1)):
            continue
        versions.append(m.group(1))
    return versions


def latest_release(tag_names):
    versions = release_versions(tag_names)
    return max(versions, key=parse) if versions else None


def check(version: str, tag_names) -> None:
    """Raises when a release tag is newer than the workspace version."""
    parse(version)
    latest = latest_release(tag_names)
    if latest and is_newer(latest, version):
        raise VersionError(
            f"main is behind the latest release: Cargo.toml says {version} but tag v{latest} exists. "
            f"Run the 'Prepare release' workflow (or scripts/release/version.py set) to bring the "
            f"workspace version up to at least {latest}."
        )


def check_new_version(new: str, current: str, tag_names, allow_existing: bool = False) -> None:
    parse(new)
    latest = latest_release(tag_names)
    if allow_existing:
        if latest and is_newer(latest, new):
            raise VersionError(f"{new} is older than the latest release tag v{latest}")
        return
    if not is_newer(new, current):
        raise VersionError(f"{new} must be newer than the current version {current}")
    if latest and not is_newer(new, latest):
        raise VersionError(f"{new} must be newer than the latest release tag v{latest}")


# --- git -----------------------------------------------------------------------


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout


def tag_commits(local: bool) -> dict:
    """Tag name -> commit it points to (annotated tags are peeled)."""
    if local:
        out = _git("for-each-ref", "--format=%(refname:short) %(*objectname) %(objectname)", "refs/tags")
        commits = {}
        for line in out.splitlines():
            name, peeled, obj = (line.split(" ") + ["", ""])[:3]
            commits[name] = peeled or obj
        return commits
    out = _git("ls-remote", "--tags", "origin")
    commits = {}
    for line in out.splitlines():
        sha, ref = line.split("\t")
        name = ref.removeprefix("refs/tags/")
        if name.endswith("^{}"):
            commits[name[:-3]] = sha  # peeled commit wins over the tag object
        else:
            commits.setdefault(name, sha)
    return commits


# --- CLI ------------------------------------------------------------------------


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--local-tags", action="store_true", help="read tags from the local repository, not origin")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("show")
    sub.add_parser("check")
    p_set = sub.add_parser("set")
    p_set.add_argument("version")
    p_set.add_argument("--allow-existing-tag", action="store_true", help="allow a version equal to the latest release tag")
    p_verify = sub.add_parser("verify-tag")
    p_verify.add_argument("tag")
    args = parser.parse_args(argv)

    cargo_path = ROOT / "Cargo.toml"
    cargo = cargo_path.read_text()
    current = workspace_version(cargo)
    try:
        if args.command == "show":
            parse(current)
            print(current)
        elif args.command == "check":
            check(current, tag_commits(args.local_tags))
            print(f"ok: workspace version {current} is not behind any release tag")
        elif args.command == "set":
            check_new_version(args.version, current, tag_commits(args.local_tags), args.allow_existing_tag)
            cargo_path.write_text(set_workspace_version(cargo, args.version))
            readme_path = ROOT / "README.md"
            readme_path.write_text(set_readme_docker_tags(readme_path.read_text(), args.version))
            print(f"set workspace version {current} -> {args.version}; now run: cargo update --workspace")
        elif args.command == "verify-tag":
            if args.tag != f"v{current}":
                raise VersionError(f"tag {args.tag} does not match the workspace version {current} (expected v{current})")
            print(f"ok: {args.tag} matches Cargo.toml")
    except VersionError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
