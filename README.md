seamm_slurm
===========
[//]: # (Badges)
[![GitHub Actions Build Status](https://github.com/molssi-seamm/seamm_slurm/workflows/CI/badge.svg)](https://github.com/molssi-seamm/seamm_slurm/actions?query=workflow%3ACI)

A small library that wraps the SLURM command-line tools (`sbatch`, `squeue`,
`sacct`, `scancel`) behind one interface, `SlurmBackend`, with two transports:

- `LocalSlurm` -- runs the SLURM CLI directly on the current host (the host
  running the code is itself a SLURM submit host, e.g. a cluster head/login
  node).
- `SshSlurm` -- runs the same commands on a remote host over passwordless
  SSH (the host running the code is *not* a SLURM submit host).

Both transports share the same submit/poll/cancel logic and SLURM-version
handling (some clusters support `squeue --json`/`sacct --json`, others only
have plain columnar output -- this library prefers JSON when available and
falls back to `--parsable2`/`--format=` text parsing otherwise, transparently
to the caller).

`submit()` takes the full script text (shebang, `#SBATCH` directives,
payload) and feeds it to `sbatch --parsable` on stdin -- it never needs the
script to exist as a file on the target host, so submission works identically
whether the caller and the SLURM cluster share a filesystem or not.
`seamm_slurm.script.build_script()` is a small helper for building that
script text from a directives dict (partition/account/qos/nodes/ntasks/time/
mem/gpus/...) plus a payload command, matching the
`<root>/<jobserver-name>.ini` config shape used by `seamm_jobserver`.

`poll_many()` takes a batch of SLURM job IDs (not one call per job) and
returns a `JobStatus` per ID, merging live state from `squeue` with
historical/terminal state from `sacct` -- `sacct` is the source of truth once
a job has left the live queue.

`seamm_slurm.config` reads a JobServer's `<root>/<jobserver-name>.ini`
(`load_slurm_config`/`SlurmSection`) -- the same system/machine config file
`seamm_jobserver` itself uses, moved here so any dependency-light consumer
(a future job-submission UI, for instance) can read and validate it without
pulling in the rest of the SEAMM stack. Includes an optional
`[<section>.limits]` companion section and `SlurmSection.merge_overrides()`,
for sites that want to let a job override some of the section's defaults
(cores, memory, walltime, ...) within enumerated choices or numeric/size/
time bounds -- secure by default, nothing is overridable unless a site's
`.limits` section says so.

This package intentionally has no SEAMM-core/`molsystem`/dashboard
dependency and no notion of SEAMM's own job-status vocabulary -- it only
speaks SLURM's. `seamm_jobserver`'s whole-flowchart SLURM submission mode is
the first consumer, validated end-to-end against a real cluster; a future
`seamm_exec` `Slurm` executor (per-step submission) is expected to reuse the
same backend rather than duplicating the SLURM-CLI handling.

See `docs/developer_guide/campaigns/2026-08-06/index.rst` in this repo, and
`seamm_jobserver`'s own `docs/developer_guide/campaigns/2026-08-05/` (the
full cross-repo SLURM-integration campaign, moved there from a
workspace-level scratch doc), for the design rationale.
