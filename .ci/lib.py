"""Shared helpers for product-repo quality CI glue (not package code)."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
CI_DIR = REPO_ROOT / ".ci"
BASELINES_DIR = CI_DIR / "baselines"
OUT_DIR = CI_DIR / "out"
SEMGREP_BASELINE = BASELINES_DIR / "semgrep-findings.json"
MUTMUT_BASELINE = BASELINES_DIR / "mutmut-score.json"
PYPROJECT = REPO_ROOT / "pyproject.toml"

def ensure_out_dir() -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUT_DIR


def run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        check=check,
        text=True,
        capture_output=False,
    )


def git_changed_src_files(base_ref: str | None) -> list[str]:
    """Return changed ``src/**/*.py`` paths relative to repo root."""
    if base_ref:
        cmd = ["git", "diff", "--name-only", "--diff-filter=ACMR", f"{base_ref}...HEAD"]
    else:
        # Local hooks: staged + unstaged vs HEAD, plus untracked under src/
        cmd = ["git", "diff", "--name-only", "--diff-filter=ACMR", "HEAD"]
    result = subprocess.run(cmd, cwd=REPO_ROOT, check=True, text=True, capture_output=True)
    paths = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    if not base_ref:
        staged = subprocess.run(
            ["git", "diff", "--name-only", "--diff-filter=ACMR", "--cached"],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        paths.update(line.strip() for line in staged.stdout.splitlines() if line.strip())
        untracked = subprocess.run(
            ["git", "ls-files", "--others", "--exclude-standard", "src"],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        paths.update(line.strip() for line in untracked.stdout.splitlines() if line.strip())

    return sorted(
        p
        for p in paths
        if p.startswith("src/") and p.endswith(".py") and (REPO_ROOT / p).is_file()
    )


def finding_id(check_id: str, path: str, message: str) -> str:
    return f"{check_id}|{path}|{message}"


def findings_from_semgrep_json(payload: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for item in payload.get("results", []):
        check_id = str(item.get("check_id", "unknown"))
        path = str(item.get("path", ""))
        start_line = int(item.get("start", {}).get("line", 0))
        message = str(item.get("extra", {}).get("message", ""))
        findings.append(
            {
                "id": finding_id(check_id, path, message),
                "check_id": check_id,
                "path": path,
                "start_line": start_line,
                "message": message,
            }
        )
    findings.sort(key=lambda f: f["id"])
    return findings


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def mutation_score(stats: dict[str, Any]) -> int:
    """Integer percent score matching mutmut badge formula (floor)."""
    total = int(stats.get("total", 0))
    skipped = int(stats.get("skipped", 0))
    tested = total - skipped
    if tested <= 0:
        return 0
    killed = int(stats.get("killed", 0))
    timeout = int(stats.get("timeout", 0))
    return int(((killed + timeout) / tested) * 100)


def collect_survived_mutants() -> list[dict[str, str]]:
    """Survived mutants from ``mutants/**/*.meta`` (mutmut exit code 0)."""
    mutants_root = REPO_ROOT / "mutants"
    survivors: list[dict[str, str]] = []
    if not mutants_root.is_dir():
        return survivors
    for meta_path in sorted(mutants_root.rglob("*.meta")):
        # mutants/src/foo.py.meta → src/foo.py
        source_path = str(meta_path.relative_to(mutants_root))[: -len(".meta")]
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        for key, exit_code in sorted(payload.get("exit_code_by_key", {}).items()):
            if exit_code == 0:
                survivors.append({"id": key, "path": source_path})
    return survivors


@contextmanager
def temporary_only_mutate(patterns: list[str]) -> Iterator[None]:
    """Temporarily set ``[tool.mutmut].only_mutate`` in pyproject.toml."""
    original = PYPROJECT.read_text(encoding="utf-8")
    data = tomllib.loads(original)
    mutmut = data.setdefault("tool", {}).setdefault("mutmut", {})
    mutmut["only_mutate"] = patterns
    # Rewrite only the mutmut table section via a stable append/replace approach:
    # keep original body and inject an only_mutate line under [tool.mutmut].
    lines = original.splitlines(keepends=True)
    out: list[str] = []
    in_mutmut = False
    wrote = False
    for line in lines:
        if line.strip() == "[tool.mutmut]":
            in_mutmut = True
            out.append(line)
            continue
        if in_mutmut and line.startswith("["):
            if not wrote:
                out.append("only_mutate = " + json.dumps(patterns) + "\n")
                wrote = True
            in_mutmut = False
        if in_mutmut and line.strip().startswith("only_mutate"):
            continue
        out.append(line)
    if in_mutmut and not wrote:
        out.append("only_mutate = " + json.dumps(patterns) + "\n")
    PYPROJECT.write_text("".join(out), encoding="utf-8")
    try:
        yield
    finally:
        PYPROJECT.write_text(original, encoding="utf-8")


def empty_sarif(tool_name: str) -> dict[str, Any]:
    driver: dict[str, Any] = {"name": tool_name}
    if tool_name == "mutmut":
        driver["informationUri"] = "https://github.com/boxed/mutmut"
    return {
        "version": "2.1.0",
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "runs": [
            {
                "tool": {"driver": driver},
                "results": [],
            }
        ],
    }


def die(message: str, code: int = 1) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)
