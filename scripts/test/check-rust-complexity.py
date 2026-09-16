#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Gate changed Rust functions: new McCabe <=10, existing complexity cannot rise.

Requires rust-code-analysis-cli 0.0.25. Pass --base to compare another Git revision.
No repository files are rewritten. Closures are measured as separate functions.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


def git(*args):
    return subprocess.check_output(["git", *args], text=True)


def analyze(path):
    tool = os.environ.get("RUST_CODE_ANALYSIS", "rust-code-analysis-cli")
    tree = json.loads(subprocess.check_output([tool, "-p", str(path), "-m", "-O", "json"], text=True))
    lines = path.read_text().splitlines()
    result = {}

    def visit(node, parents):
        kind, name = node["kind"], node["name"]
        key = (*parents, name)
        if kind == "function":
            nested = sum(child["metrics"]["cyclomatic"]["sum"] for child in node["spaces"])
            complexity = node["metrics"]["cyclomatic"]["sum"] - nested
            body = "\n".join(lines[node["start_line"] - 1:node["end_line"]])
            result[key] = (complexity, body, node["start_line"])
        for child in node["spaces"]:
            visit(child, parents if kind == "unit" else key)

    visit(tree, ())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="HEAD")
    args = parser.parse_args()
    changed = set(git("diff", "--name-only", args.base).splitlines())
    changed.update(git("ls-files", "--others", "--exclude-standard").splitlines())
    failures = []
    checked = 0
    with tempfile.TemporaryDirectory(prefix="superbank-complexity-") as directory:
        for filename in sorted(changed):
            path = Path(filename)
            if path.suffix != ".rs" or not path.is_file():
                continue
            previous = subprocess.run(["git", "show", f"{args.base}:{filename}"], capture_output=True, text=True)
            baseline = {}
            if previous.returncode == 0:
                old = Path(directory) / path.name
                old.write_text(previous.stdout)
                baseline = analyze(old)
            for name, (score, body, line) in analyze(path).items():
                prior = baseline.get(name)
                if prior and prior[1] == body:
                    continue
                checked += 1
                limit = prior[0] if prior else 10
                if score > limit:
                    failures.append(f"{filename}:{line}: {'::'.join(name)} McCabe {score:g} > {limit:g}")
    print(f"Checked {checked} changed Rust functions")
    for failure in failures:
        print(failure)
    raise SystemExit(bool(failures))


if __name__ == "__main__":
    main()
