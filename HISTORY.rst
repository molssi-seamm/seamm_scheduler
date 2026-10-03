=======
History
=======

2026.10.2 -- Initial release: queueing systems for SEAMM
    * Generalizes ``seamm_slurm`` into one module per queueing system behind a
      shared ``Scheduler`` interface: SLURM (``slurm.py``) and PBS
      (``pbs.py``, tested against recorded output only).
    * ``QueueBackend(scheduler, transport)`` with the local and ssh
      transports; the stagers gained ``push``/``pull`` of many directories in
      one ``rsync``.
    * Targets in ``<root>/<jobserver-name>.ini`` gained the optional task keys
      (``tasks``, ``scheduler``, ``shared_filesystem``, ``bundle_tasks``,
      ``bundle_walltime``, ``max_queued_tasks``, ``inline_below``,
      ``remote_python``, ``remote_seamm_root``, ``poll_interval``, ``url``)
      used by ``seamm_exec``'s task layer. Sections without them behave as
      before.
    * SLURM 25.11's ``sacct --json`` exit codes; poll failures reported as
      ``poll_failed`` rather than as jobs that disappeared.
    * A bare SLURM time (``30``) is minutes, as SLURM reads it.
    * ``find_jobs(name)`` finds a user's job by name, queued, running or (from
      accounting) finished, so a submission whose answer was lost can be checked.
