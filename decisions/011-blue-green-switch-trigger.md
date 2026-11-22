# 011: What triggers the blue-green switch

Stage: S3 · Date: 2026-11-22 · Author: Dreamstick9 · Drafted by agent: yes

## Choice

The CD workflow flips traffic automatically, with no person in the loop, once the new colour passes a smoke test. On every push to `main` that passes CI, `.github/workflows/cd.yaml`:
1. deploys the new image to the idle colour (blue or green),
2. runs `deploy/web.sh smoke <colour> <commit>` against that colour only (pinned with the `X-Lab-Color` header): `/version` must show the commit just built, and `/predict` must answer,
3. switches all app traffic to it with `deploy/web.sh switch`,
4. checks the public `/version` shows the new commit.

The old colour keeps running. Rollback is the same workflow with `rollback` ticked: it smoke-tests the other colour and switches back, without building or pulling anything.

Measured (2026-11-22): rollback green to blue took 1.6 s on the host, 8 s for the job, 18 s from pressing the button.

## Rejected alternative

Smoke-test automatically, then wait for a person to approve the switch in a GitHub environment (like `production` for promotions). Safer: a human looks before riders get the new version.

## Why

<!-- Student: rewrite in your own words. Point to confirm: you chose automatic after smoke test on 2026-11-22. -->
The switch is cheap to undo, so it doesn't need a person in front of it. The old colour is still running, and going back takes under 2 seconds on the host and under 20 seconds from the button. An approval would make every deploy wait for someone, for a risk that a 20-second rollback already covers. The smoke test catches the failures that matter most at deploy time: the wrong commit, an app that doesn't start, or `/predict` failing. Model changes are different: they go through `promote.yaml`, which does need approval, because a bad champion is harder to spot than a broken app.

The cost: the smoke test is one request. A bug it doesn't exercise (another endpoint, a slow memory leak, a rare input) reaches every rider straight away, until someone notices and rolls back.

## What would show this was wrong

A bad deploy that passed the smoke test and stayed live for a long time before anyone rolled back: that would mean the smoke test is too thin, or that nobody is watching after a switch. S5's alerts and S8's canary are where that would show up. Also: a deploy that fails its smoke test leaves the broken build on the idle colour without switching. Riders are fine, but the previous good version is no longer standing by, so a rollback right after would have nothing to go back to (its own smoke test would refuse). If that bites, the next good deploy restores it.
