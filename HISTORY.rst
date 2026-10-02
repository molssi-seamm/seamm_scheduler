=======
History
=======

2026.8.13 -- Internal: a shared, reusable way to compute a job's remote scratch path
    * ``SlurmSection`` gained ``remote_wdir_for(local_wdir)``, factored out
      of ``seamm_jobserver``'s own private ``_remote_wdir()`` helper. It's
      a pure function of a job's local working directory and the
      section's own ``remote_root``, so any caller that knows both --
      not just whichever process originally staged the job in -- can
      recompute the same remote path independently. Lays the groundwork
      for a Dashboard to pull a still-running ``transport = ssh`` job's
      files back on demand, rather than only after SLURM reports it
      terminal.
    * ``stage.py`` gained a ``STAGE_LOCK_FILENAME`` constant: a shared
      filename (not a full path) two independent processes can agree to
      lock under a job's own working directory before calling
      ``stage_out()``, so a real end-of-run pull and an on-demand
      mid-run pull can't race each other. A plain string constant, not
      locking logic itself, to keep this package dependency-free.
    * No behavior change to any existing code path.

2026.8.12 -- Bugfix: ``mem_per_cpu``/other per-unit memory overrides crashed at startup
    * ``merge_overrides()`` validates a job's requested SLURM directive
      overrides against ``[section.limits]``, parsing each value into a
      comparable number first. That parsing only recognized the literal
      field name ``mem`` as a unit-suffixed size (e.g. ``"1900M"``) --
      ``mem_per_cpu`` (and any other ``mem_per_*`` directive) fell through
      to a plain ``float()``, which raised
      ``ValueError: could not convert string to float: '1900M'`` for any
      job that explicitly requested one, even a value matching the
      section's own default. The job failed immediately with no SLURM
      script ever generated. ``mem_per_cpu`` and other ``mem_per_*``
      fields now parse the same unit-suffixed way ``mem`` already did.

2026.8.11 -- A per-queue ``setup`` field for raw shell commands before a job runs
    * ``SlurmSection`` gained a new ``setup`` field: raw shell commands
      (e.g. ``module load ORCA``, or several via ini continuation lines)
      run at the top of the generated sbatch script, before
      ``run_from_jobserver``. For a queue whose own submission
      environment doesn't already carry whatever a code's
      ``installation = modules`` setting needs (e.g. a JobServer
      dispatching over a bare, non-interactive ssh connection with no
      ``MODULEPATH``/Lmod set up). Blank/unset by default -- no effect on
      any existing config.

2026.8.10 -- Multiple queues per JobServer instance, and a scheduler-free queue type
    * ``SlurmSection`` gained a ``type`` field (``"slurm"`` by default, so
      every existing config is unaffected) and a new ``type = local``
      value: a queue with no scheduler at all, for a JobServer that wants
      to route some jobs to a plain local subprocess and others to one or
      more real SLURM clusters from the same instance. ``build_backend()``/
      ``build_stager()`` raise a clear error if called on a ``type=local``
      section, since it has neither -- callers route those jobs through
      their own local-subprocess path instead.
    * New ``list_sections(root, jobserver_name)`` enumerates every
      cluster/queue section defined in a ``<root>/<jobserver-name>.ini``
      file, not just the one ``load_slurm_config()`` resolves via its
      ``default=``/single-section fallback -- needed by anything that
      wants to route jobs across more than one queue, or to advertise
      "what queues exist" to a submission UI. Returns ``{}`` if the ini
      file doesn't exist, the same "feature doesn't exist" convention
      ``load_slurm_config()`` already uses (returning ``None``).
    * Both functions now share one internal per-section parser, so they
      stay consistent as the ini format grows.

2026.8.8 -- Stage a job's files to/from a remote host with no shared filesystem
    * New ``LocalStager``/``RsyncStager`` (``seamm_slurm.stage``), paired with
      the existing ``LocalSlurm``/``SshSlurm`` transports: lets a JobServer
      that shares no filesystem with the SLURM submit host (e.g. a laptop
      reaching a cluster over ssh) push a job's working directory there
      before submission and pull results back after, over
      ``rsync -e ssh``. ``LocalStager`` is a no-op for the existing
      shared-storage case.
    * ``SlurmSection`` gained ``build_stager()`` (alongside
      ``build_backend()``) and three new optional ini keys:
      ``remote_root`` (base directory for a job's remote scratch tree),
      ``remote_run_from_jobserver`` (explicit absolute path to invoke on
      the remote host, preferred), and ``remote_conda_env`` (falls back
      to ``conda run -n <env>`` when the absolute path isn't known/stable).
    * Live-validated: a real job staged to a remote cluster over actual
      ssh/rsync, ran there for real, and staged back correctly.

2026.8.6.1 -- Add seamm_slurm.config: ini parsing, moved from seamm_jobserver
    * ``SlurmSection``/``load_slurm_config`` (previously
      ``seamm_jobserver.slurm_config``) now live here, so any
      dependency-light consumer -- a future job-submission UI, for instance
      -- can read/validate a JobServer's ``<root>/<jobserver-name>.ini``
      without pulling in the rest of the SEAMM stack.
    * Added an optional ``[<section>.limits]`` companion ini section and
      ``SlurmSection.merge_overrides()``: sites can now say which SLURM
      directives a job may override per-job, and within what bounds
      (enumerated choices, or numeric/size/time ranges). Secure by default
      -- no ``.limits`` section means nothing is overridable.

2026.8.6 -- Initial release
    * ``SlurmBackend`` (submit/poll_many/cancel), ``LocalSlurm`` and
      ``SshSlurm`` transports, JSON-with-text-fallback parsing for
      ``squeue``/``sacct`` (SLURM versions differ in ``--json`` support), and
      ``script.build_script()`` for turning a directives dict into a full
      ``sbatch`` script.
    * Wired into ``seamm_jobserver``'s whole-flowchart SLURM submission mode
      and validated end-to-end against a real cluster: real
      ``sbatch``-to-completion runs, a genuine resubmit-and-give-up sequence
      under repeated real SLURM failures, and a deterministic kill/restart
      test.
