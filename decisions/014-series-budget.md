# 014: Series budget

Stage: S5 · Date: 2026-11-22 · Author: Dreamstick9 · Drafted by agent: yes

## Choice

"Moderate": keep everything riders and the SLOs depend on, keep enough host and container data to debug an incident, drop the rest. The rules are in `observability/alloy/budget.alloy`:
- **Services (app, serving, Traefik):** keep every metric in `contracts/metrics.yaml` with all its labels; drop only Go and Python garbage-collector internals (`go_.+`, `python_gc_.+`).
- **Hosts (node exporter):** keep CPU, memory, pressure, filesystems, disks, load, OOM kills and boot time; only the real disk (drop `tmpfs`, `ramfs`, `vfat`) and the real network card `ens5` (drop `lo`, `docker*`, `veth*`, `br-*`); drop `node_scrape_collector_*` and everything else.
- **Containers (cAdvisor):** keep CPU, memory, network, start time, OOM events and health; drop filesystem detail (`container_fs_*`) and memory page-fault counts; drop the labels that change on every deploy (`id`, `image`, `container_label_*`), keeping `name` to tell containers apart.

Estimated result: about 970 active series for the app and platform hosts together (from about 3,439 measured before), against Grafana Cloud's free limit of 10,000.

## Rejected alternative

"Light": drop only the obvious waste (`node_scrape_collector_*` and the per-deploy `id`/`image` labels) and keep nearly everything else, about 4,000 series.

## Why

<!-- Student: rewrite in your own words. Points to confirm: you chose "Moderate" on 2026-11-22; it came out at about 970 instead of the ~2,500 estimated when choosing, and the cheap things to add back if wanted are container pressure, node_netstat_* and container_fs_*. -->
The limit is for all hosts together, and S6 adds the LLM host, which unpruned would add about another 1,500 to 2,500. The series that matter are the ones a dashboard, an SLO or an incident would use; most of the rest (scrape-collector timings, virtual network cards, per-file-system IO counters) nobody queries. The labels that change every deploy matter most: with `image` carrying the commit tag (`app:sha-d6a0a40`), every blue-green deploy would create a new set of container series while the old ones stay active for a while, so a burst of deploys multiplies the count. Dropping them keeps the count flat across deploys. The cost: less to dig into during an incident (no per-container disk IO, no TCP stats), and container network is only reported for the whole machine.

## What would show this was wrong

An incident where the answer was in a series we dropped (for example a full disk inside a container, or TCP retransmits). Or the count still rising after three deploys in a row (S5 step 3): that would mean a per-deploy label is still getting through. Or, after S6, the total getting close to 10,000.
