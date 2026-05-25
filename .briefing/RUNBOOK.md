# Runbook — nest-kafka-native

Operator guide for this project. All decision logic lives in
`.briefing/PROMPT.md`; the agent runs it. You run the agent.

## Briefing folder

```
AI_CODING_GUIDELINES.md             (the constitution; tracked at repo root)
.briefing/
├── BRIEF.md                        (the implementation brief)
├── PROMPT.md                       (the agent's full instructions)
├── RUNBOOK.md                      (this file)
├── run.sh                          (one milestone)
├── loop.sh                         (until done or blocked)
├── STATE.json                      (created on first run; gitignored)
└── LAST_RUN.json                   (written after each invocation; gitignored)
```

`AI_CODING_GUIDELINES.md` evolves via PRs (subject to safety check #5).

## Run

- One milestone: `.briefing/run.sh`
- Until done or blocked: `.briefing/loop.sh`

Both write logs to `runs/` and update `.briefing/STATE.json` and
`.briefing/LAST_RUN.json`. The outcome is the `status` field of
`LAST_RUN.json`:

- `MILESTONE_MERGED` — re-run for the next milestone.
- `COMPLETE_ALL` — all milestones done. Move to manual release work.
- `BLOCKED` — read `reason`, see "When BLOCKED" below.
- `PRE_FLIGHT_FAILED` — fix the environment, re-run.

## When BLOCKED

Read `.briefing/LAST_RUN.json` for the reason. Common cases:

| Reason | Action |
| --- | --- |
| `Constitutional change requires manual review. PR=<num>` | Expected when the agent edited `AI_CODING_GUIDELINES.md`. Review the diff; if approved, `gh pr merge <num> --squash --delete-branch` and re-run (the agent auto-detects the merge via Phase 1 Case D and advances). If declined, `gh pr close <num>` and edit STATE.json to set `status="pending"` with `current_branch` and `current_pr` cleared. |
| `PR <n> was closed without merging` | Either re-open the PR (then re-run) OR edit STATE.json: set `status="pending"`, clear `current_branch` and `current_pr`. |
| `CI failing after 3 fix attempts` | Inspect logs in `runs/`. Fix manually (push a commit yourself) OR update BRIEF.md to disambiguate. Then edit STATE.json: set `status="in_pr"` to re-poll. |
| `All milestones complete` | You're done. Inspect, decide on release. |
| Architectural question | Update BRIEF.md to answer it. Edit STATE.json: set `status="pending"`. Re-run. |

## When unsure what the agent did

```bash
# state
jq . .briefing/STATE.json

# last invocation
jq . .briefing/LAST_RUN.json

# what's on GitHub
gh pr list --state all --limit 10
git log origin/main --oneline -10

# the actual run transcript
ls -lt runs/ | head
less runs/<latest>.log
```

## Manual overrides

```bash
# Pause: stop the loop from picking up the next milestone
jq '.status = "blocked" | .reason = "paused by operator"' \
  .briefing/STATE.json > /tmp/s && mv /tmp/s .briefing/STATE.json

# Force restart from milestone 1 (DESTRUCTIVE — wipes state)
rm .briefing/STATE.json .briefing/LAST_RUN.json

# Skip a milestone (use only if you've completed it manually)
jq '.current_milestone += 1 | .status = "pending"' \
  .briefing/STATE.json > /tmp/s && mv /tmp/s .briefing/STATE.json

# Switch to interactive mode for one milestone (observe + intervene)
claude --dangerously-skip-permissions < .briefing/PROMPT.md
```

## Branch protection note

Auto-merge requires the gh-authenticated user to have merge permission
with no blocking approval rules. If you've added required reviewers,
either disable for these bootstrap repos or grant the agent user a
bypass.
