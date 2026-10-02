seamm_scheduler
===============
[//]: # (Badges)
[![GitHub Actions Build Status](https://github.com/molssi-seamm/seamm_scheduler/workflows/CI/badge.svg)](https://github.com/molssi-seamm/seamm_scheduler/actions?query=workflow%3ACI)

Queueing systems for SEAMM: one module per queueing system behind a shared
`Scheduler` interface, with local and ssh transports and staging.

- `seamm_scheduler.slurm.Slurm` -- SLURM: `#SBATCH` directives, `sbatch`,
  `squeue`/`sacct` (with `--json` where the cluster has it, text otherwise),
  `scancel`.
- `seamm_scheduler.pbs.Pbs` -- PBS Professional / OpenPBS: `#PBS` directives
  with one `select` statement, `qsub`, `qstat`, `qdel`. Tested against
  recorded output only so far.
- `QueueBackend(scheduler, transport)` -- submit a script, poll many jobs in as
  few commands as the scheduler allows, cancel, count the user's jobs.
- `LocalTransport` / `SshTransport` -- run the commands on this host, or on a
  login node over passwordless ssh.
- `LocalStager` / `RsyncStager` -- move job (or task) directories when the
  caller and the cluster share no filesystem.
- `seamm_scheduler.config` -- the JobServer's `<root>/<jobserver-name>.ini`:
  each section is a *target*, describing where a job's flowchart evaluator
  runs and, with the optional task keys, where its tasks run.

Every scheduler translates the same scheduler-neutral resources (`ntasks`,
`cpus_per_task`, `mem_per_cpu`, `ngpus`, `walltime`, `partition`, `account`,
`qos`, `nodes`) into its own directives, and reports job states in one small
vocabulary (pending, running, completed, cancelled, failed).

The package has no dependencies. It is used by `seamm_jobserver` (whole
flowcharts as batch jobs) and `seamm_exec`'s task layer (bundles of tasks as
batch jobs). `seamm_slurm`, which this package generalizes, remains as a
compatibility shim that re-exports from here.

Features
--------

- SLURM 20.11 (no `--json`) to 25.11 (nested JSON) handled transparently
- Per-user queue limits (`count_jobs()`)
- Poll failures (an ssh outage) are reported as such, never as vanished jobs
- One `rsync` for many directories (`push`/`pull`)

Acknowledgements
----------------

Developed by the Molecular Sciences Software Institute (MolSSI),
which receives funding from the National Science Foundation under
award CHE-2136142.
