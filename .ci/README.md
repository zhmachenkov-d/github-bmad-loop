# Product-repo quality CI glue

Helpers under `.ci/` wire Semgrep (AST-diff / diff-aware findings) and mutmut
(mutation testing) for local use and GitHub Actions. They are **not** part of
the `github-bmad-loop` package.

## Baselines (`.ci/baselines/`)

| File | Meaning |
| --- | --- |
| `semgrep-findings.json` | Accepted Semgrep finding ids (`check_id\|path\|start_line\|message`) |
| `mutmut-score.json` | Last accepted full mutmut integer score (+ counts) |

**Intentional bump rule:** only update a baseline when the team accepts a new
Semgrep finding or a lower mutation score / higher survived or no_tests count.
Prefer regenerating via:

```bash
uv run python .ci/run_semgrep.py --write-baseline
uv run python .ci/run_mutmut_full.py --write-baseline
```

Never bump baselines solely to silence a real regression.

On PR/push, Semgrep uses `--baseline-commit` (AST-diff slice). Worsen
`current_count` is that slice size, not a full-repo finding count; new ids in
the slice still fail the gate against the committed baseline.

## Worsen / Slack

- Semgrep: any finding id not in the Semgrep baseline (PR/push job).
- Mutmut (scheduled full): score drop ≥ 1 **or** `survived` / `no_tests`
  growth vs mutmut baseline.
- One aggregated Slack payload via `slack_notify.py` +
  `slackapi/slack-github-action` `chat.postMessage`
  (`SLACK_BOT_TOKEN` + `SLACK_CHANNEL_ID`). Channel id is written into the
  payload from env (`payload-templated: false`). Mutmut worsen includes a capped
  survivor list with blob links to `src/` at the run SHA. Clean/improved runs
  do not Slack. Missing secrets skip notify (do not fail the job).

HITL dry-run (no bot token):

```bash
uv run python .ci/run_worsen_fixture.py
```

## Outputs

Generated artifacts land in `.ci/out/` (gitignored): SARIF, worsen JSON, Slack
payload drafts.
