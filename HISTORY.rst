=======
History
=======
2026.10.5 -- The TaskServer: a queue for a machine without one
    * ``seamm-taskserver`` shares one machine's cores and memory among everything
      SEAMM runs on it -- the calculations of several flowcharts, and the flowcharts
      themselves -- like a small queueing system: ``submit``, ``status``, ``cancel``,
      ``queue``, ``config``. There is nothing to keep running; each job has a runner
      of its own, so restarts and upgrades never touch running jobs. Time limits do
      not count a sleeping laptop's sleep. Memory is enforced: a job using far more
      than it asked for is stopped, and the newest job when the machine runs low.
      The capacity is in ``<root>/taskserver.ini`` (by default the physical cores
      and half the memory). See the new TaskServer page.
    * The ``seamm`` scheduler drives the TaskServer exactly as SLURM and PBS are
      driven, locally or over ssh with the job's files staged, for the JobServer
      (``type = queue``, ``scheduler = seamm``) and for a flowchart's calculations
      (``tasks = queue``). Flowcharts take no cores and never hold up their own
      calculations; their memory charge can exceed the capacity, capped by the
      JobServer's ``max_concurrent_jobs``.
    * ``max_walltime`` in a queue section: the longest time a retry may ask for.
    * Bugfix: copying a job back now removes stale database logs (and a parallel
      loop's finished ``loop_entry.db``) in subdirectories too; with macOS's rsync
      only those at the top of the job were removed.
    * Requires psutil.
2026.10.4 -- Bugfix: stale database logs after copying a job back; timeouts
    * Copying a job's directory back from a cluster could leave an old SQLite log
      (``seamm.db-wal``) from an earlier copy beside the job's newer database, and
      SQLite replayed it over the database, so the job looked as it was earlier
      (e.g. with fewer table rows). The log files (``-wal``, ``-shm``,
      ``-journal``) are now made to match the other copy, in both directions; no
      other file or directory is deleted.
    * ``JobStatus.timed_out`` says whether the queue stopped a job for running past
      its time limit (SLURM ``TIMEOUT``; PBS exit status -29 or a "walltime ...
      exceeded" comment).

2026.10.3 -- PBS validated on a real site; flowcharts as batch jobs on any scheduler
    * PBS was validated on a real OpenPBS 23.06 site: dependencies are passed as
      ``-W depend``, the job starts with a ``cd`` into its directory (PBS starts
      jobs in the home directory), ``export = ALL`` becomes ``-V``, finished jobs
      are found again after a restart (``qselect -x``), and a garbled ``qstat``
      reply is a failed poll rather than "no jobs".
    * The resources of a job or bundle are merged into the section's ``select``
      chunk, so the section's memory is kept; SLURM spellings in a PBS section
      (``ntasks``, ``mem``, ``time`` ...) are translated rather than dropped.
    * A section can run the flowchart itself as a batch job on any scheduler:
      ``type = queue`` with ``scheduler = pbs`` or ``slurm``. ``type = slurm``
      still works.
    * The host where a flowchart runs as a batch job needs this version too:
      older versions do not know ``type = queue``.
    * Output recorded from real SLURM 20.11 and OpenPBS 23.06 sites is replayed
      by the tests.

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
