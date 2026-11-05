#!/usr/bin/env python3
"""Build one aggregated Slack chat.postMessage payload; --dry-run never posts."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from lib import OUT_DIR, ensure_out_dir, load_json, write_json

SURVIVOR_CAP = 30
CHANNEL_PLACEHOLDER = "C_PLACEHOLDER"


def short_mutant_id(mutant_id: str) -> str:
    """Prefer the trailing ``x_…__mutmut_N`` segment for Slack link labels."""
    if "." in mutant_id:
        return mutant_id.rsplit(".", 1)[-1]
    return mutant_id


def blob_url(*, server_url: str, repo: str, sha: str, path: str) -> str:
    base = server_url.rstrip("/")
    return f"{base}/{repo}/blob/{sha}/{path}"


def format_survivor_lines(
    survivors: list[dict[str, Any]],
    *,
    server_url: str,
    repo: str,
    sha: str,
    cap: int = SURVIVOR_CAP,
) -> list[str]:
    if not survivors:
        return []
    lines = ["Survivors:"]
    shown = survivors[:cap]
    can_link = bool(server_url and repo and sha)
    for item in shown:
        mutant_id = str(item.get("id", ""))
        path = str(item.get("path", ""))
        label = short_mutant_id(mutant_id) or mutant_id or path or "?"
        if can_link and path:
            url = blob_url(server_url=server_url, repo=repo, sha=sha, path=path)
            lines.append(f"• <{url}|{label}> (`{path}`)")
        else:
            lines.append(f"• `{label}` (`{path}`)" if path else f"• `{label}`")
    remaining = len(survivors) - len(shown)
    if remaining > 0:
        lines.append(f"… and {remaining} more")
    return lines


def build_payload(
    *,
    factors: list[str],
    run_url: str,
    survivors: list[dict[str, Any]] | None = None,
    server_url: str = "",
    repo: str = "",
    sha: str = "",
) -> dict[str, Any]:
    lines = [
        "*github-bmad-loop quality worsen*",
        f"Run: <{run_url}|Actions run>",
    ]
    if server_url and repo:
        scan = f"{server_url.rstrip('/')}/{repo}/security/code-scanning"
        lines.append(f"Code scanning: <{scan}|open alerts>")
    lines.append("Factors:")
    for factor in factors:
        lines.append(f"• {factor}")
    lines.extend(
        format_survivor_lines(
            survivors or [],
            server_url=server_url,
            repo=repo,
            sha=sha,
        )
    )
    channel = os.environ.get("SLACK_CHANNEL_ID", "").strip() or CHANNEL_PLACEHOLDER
    return {"channel": channel, "text": "\n".join(lines)}


def collect_factors(worsen_paths: list[Path]) -> list[str]:
    factors: list[str] = []
    for path in worsen_paths:
        if not path.is_file():
            continue
        data = load_json(path)
        if not data.get("worsened"):
            continue
        tool = data.get("tool", path.stem)
        if tool == "semgrep":
            n = len(data.get("new_findings", []))
            factors.append(f"semgrep: {n} new finding(s) vs baseline")
        elif tool == "mutmut":
            factors.append(
                f"mutmut: score {data.get('baseline_score')} → {data.get('current_score')} "
                f"(drop {data.get('drop')}, survived_delta {data.get('survived_delta', 0)}, "
                f"no_tests_delta {data.get('no_tests_delta', 0)})"
            )
        else:
            factors.append(f"{tool}: worsened")
    return factors


def collect_survivors(worsen_paths: list[Path]) -> list[dict[str, Any]]:
    survivors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in worsen_paths:
        if not path.is_file():
            continue
        data = load_json(path)
        if not data.get("worsened") or data.get("tool") != "mutmut":
            continue
        for item in data.get("survivors") or []:
            if not isinstance(item, dict):
                continue
            key = str(item.get("id", ""))
            if not key or key in seen:
                continue
            seen.add(key)
            survivors.append({"id": key, "path": str(item.get("path", ""))})
    return survivors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--worsen",
        nargs="*",
        type=Path,
        default=[],
        help="Paths to worsen JSON files from Semgrep/mutmut helpers",
    )
    parser.add_argument(
        "--factor",
        action="append",
        default=[],
        help="Explicit factor string (repeatable); used with fixtures",
    )
    parser.add_argument(
        "--run-url",
        default="https://example.invalid/actions/runs/0",
        help="Workflow run URL (placeholder for dry-run)",
    )
    parser.add_argument(
        "--server-url",
        default="",
        help="GitHub server URL for blob / code-scanning links",
    )
    parser.add_argument(
        "--repo",
        default="",
        help="owner/name repository for blob links",
    )
    parser.add_argument(
        "--sha",
        default="",
        help="Commit SHA for blob links",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write payload to stdout and .ci/out/slack-payload.json; do not POST",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUT_DIR / "slack-payload.json",
    )
    args = parser.parse_args(argv)
    ensure_out_dir()

    factors = list(args.factor)
    factors.extend(collect_factors(args.worsen))
    # de-dupe preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for f in factors:
        if f not in seen:
            seen.add(f)
            unique.append(f)

    if not unique:
        print("No worsen factors; skipping Slack payload")
        write_json(args.output, {"skipped": True, "reason": "no_factors"})
        return 0

    survivors = collect_survivors(args.worsen)
    payload = build_payload(
        factors=unique,
        run_url=args.run_url,
        survivors=survivors,
        server_url=args.server_url,
        repo=args.repo,
        sha=args.sha,
    )
    write_json(args.output, payload)
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if args.dry_run:
        print(f"[dry-run] wrote {args.output}; not posting to Slack")
        return 0

    # Live post is owned by slackapi/slack-github-action in workflows.
    # This script only prepares the payload file for that action.
    print(f"Wrote Slack payload for workflow action: {args.output}")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    raise SystemExit(main())
