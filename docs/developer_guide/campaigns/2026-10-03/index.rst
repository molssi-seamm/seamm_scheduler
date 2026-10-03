2026-10-03 -- MolSSI10: from SLURM to OpenPBS
=============================================

Status: **design note for Paul and the design session; nothing on MolSSI10 has
been changed.** Phase 7 of the parallel-execution campaign (seamm_exec
``docs/developer_guide/campaigns/2026-10-02/``) includes "PBS validated on a
real PBS site". ``seamm_scheduler``'s PBS back end has only been tested against
mocked ``qsub``/``qstat`` output. The plan is to make MolSSI10 that site by
replacing its SLURM with a permanent single-node OpenPBS. SLURM stays covered by
ChemAI (local) and TinkerCliffs (ssh).

MolSSI10 today (checked read-only, 2026-10-03)
----------------------------------------------

- Debian 11.11, kernel 5.10, 6 cores, 125 GB, 689 GB free on ``/``.
  Passwordless sudo for ``psaxe``.
- SLURM 20.11.4 (Debian ``slurm-wlm``, ``slurmctld``, ``slurmd``, ``slurmdbd``)
  with munge. One node and one partition, ``batch``; ``select/cons_tres`` with
  ``CR_Core_Memory``. Accounting goes through ``slurmdbd`` into MariaDB 10.5,
  whose only database besides the system ones is ``slurm_acct_db``.
- PostgreSQL 13 runs a ``pubchemqc`` cluster on port 5437 (``/mnt/ssd1``). It
  is unrelated to SEAMM and must not be touched.
- SEAMM runs as ``psaxe``: ``seamm-jobserver`` and ``seamm-webui`` from
  ``~/SEAMM`` (``venv`` → ``/mnt/hdd2/psaxe/SEAMM/venv``).
- ``~/SEAMM/molssi10.ini`` has two queues: ``[molssi10]`` (this host's SLURM,
  ``transport = local``) and ``[tinkercliffs]`` (ssh). **The default queue is
  ``molssi10``.** So every production job submitted without a queue is an
  sbatch on this host's SLURM: 37 jobs since 2026-09-19.
- ``~/SEAMM_DEV/PaulVT.local.ini`` on the Mac also has a ``[molssi10]`` queue
  (ssh to this host's SLURM). Its ``remote_run_from_jobserver`` still points to
  the old conda env ``/home/psaxe/miniconda3/envs/seamm``.

What shapes the plan
--------------------

1. **The JobServer cannot run a flowchart as a PBS job.** A target section's
   ``type`` is ``slurm`` (the whole flowchart is one batch job) or ``local``
   (a subprocess). PBS is accepted only for a flowchart's *tasks*
   (``tasks = queue``, ``scheduler = pbs``). The comment in
   ``seamm_scheduler/config.py`` says so: a PBS evaluator needs a new type
   *and* a JobServer change. Because MolSSI10's default queue is its local
   SLURM, removing SLURM without that change breaks production submissions
   that don't name a queue. See decision A.
2. **OpenPBS has no Debian packages, and its last release is v23.06.06
   (June 2023).** We would build it from source: autotools, with hwloc (2.4.1
   is installed), libical, libedit, Tcl/Tk, swig, expat, OpenSSL and the
   PostgreSQL development files. Its data service runs its *own* PostgreSQL
   instance (by default on port 15007, data under ``PBS_HOME/datastore``) using
   the system's PostgreSQL server binaries, so it sits beside the ``pubchemqc``
   cluster without touching it. If the build turns out to need much patching
   on Debian 11, the fallback is a container (decision C).
3. **Gaps in the PBS back end** that only a real site can settle:

   - A PBS job starts in ``$HOME``, not the submit directory as with SLURM. The
     batch script must ``cd "$PBS_O_WORKDIR"`` or ``-d``/``-w`` the directory.
     ``build_script`` adds nothing today.
   - ``qstat -x`` needs the server attribute ``job_history_enable = True``
     (and a ``job_history_duration``), or finished jobs vanish and adoption
     after a restart cannot tell completed from lost.
   - The environment: ``qsub`` without ``-V`` passes only ``PBS_O_*``. That is
     the behaviour we want (SLURM's ``export=NONE`` lesson: ORCA's clobbered
     ``LD_LIBRARY_PATH``), but the payload must then set up its own
     environment, as the bundle worker already does for SLURM.
   - The real ``qstat -f -F json`` and ``qsub`` output (ids like
     ``12.molssi10``), the ``Exit_status`` of a ``qdel``-ed or killed job, and
     ``qselect``, to replace the mocks with recorded fixtures.
   - ``mem`` in a ``select`` chunk and the ``mpiprocs``/``ncpus`` accounting
     under ``cgroups`` (or none) on one node.

Plan
----

Each step that changes the host is done only with Paul's OK, in a window with
no SEAMM jobs running there.

0. **Decisions** (below).
1. **Freeze SLURM 20.11** (read-only, no host change). Record real ``sinfo``,
   ``squeue``, ``sacct`` (text, since 20.11 has no JSON) and
   ``scontrol show job`` output for running, finished, failed and cancelled
   test jobs, as ``seamm_scheduler`` fixtures, so the old-SLURM text path stays
   tested without a host. Snapshot ``/etc/slurm``, the munge key, both
   ``[molssi10]`` ini sections and ``mysqldump slurm_acct_db``.
2. **Build OpenPBS** in a scratch directory on MolSSI10 (``apt install`` of the
   build dependencies only, nothing started). This proves the build before
   SLURM is touched. Install prefix ``/opt/pbs``, ``PBS_HOME``
   ``/var/spool/pbs``.
3. **Swap** (the window): set the JobServer's default queue so new jobs don't
   go to SLURM (decision A). Wait for running jobs to finish. Stop and disable
   ``slurmctld``, ``slurmd``, ``slurmdbd`` and ``munge``, then
   ``apt remove`` the SLURM and munge packages (configuration kept with
   ``remove``, not ``purge``). Install OpenPBS, with the server, scheduler,
   communication daemon and MoM on this host, through its systemd unit. Set up
   one queue ``workq`` with 6 CPUs and the node's memory as resources,
   ``job_history_enable = True`` (7 days), and no ``sudo`` needed for
   submission.
4. **Make the back end real** (``seamm_scheduler``): the working directory,
   the environment, recorded fixtures for qsub/qstat/qdel/qselect and
   job-history states, adoption of queued bundles after an evaluator restart,
   and a ``pbs`` variant in seamm-manager's ini templates if needed. Keep the
   mocks for unit tests.
5. **Validate:**

   (a) the scheduler suite from the Mac over ssh;
   (b) a table-writing MOPAC flowchart from MolSSI10's own JobServer with its
       tasks on the local PBS queue;
   (c) with decision A, the same flowchart as a PBS job;
   (d) a MolSSI10 → TinkerCliffs job, proving the production ssh path still
       works;
   (e) the Mac's SEAMM_DEV ``[molssi10]`` queue as PBS over ssh.

6. **Record** in these notes, the design doc's phase 7 and memory.

Rollback: stop and disable PBS, then ``apt install`` the same SLURM and munge
packages. The configuration is kept by ``remove``, and the snapshot covers
``/etc/slurm`` and the munge key; restore ``slurm_acct_db`` from the dump.
Then restore the ini sections and restart the JobServer.

Budget: about a day for the build and swap, and a day for the back-end work and
validation.

Decisions for Paul
------------------

A. **PBS evaluators in the JobServer.** Today MolSSI10's default queue runs each
   flowchart as a SLURM job. Either:

   (1) extend the JobServer in this campaign so a section can run the
       evaluator as a PBS job: a scheduler-neutral ``type = queue`` with
       ``scheduler = pbs``. This is the honest production test, but it is a
       JobServer change. **Recommended.**
   (2) make MolSSI10's default queue ``local`` (flowcharts run as
       subprocesses, as on the Mac) with ``tasks = queue``,
       ``scheduler = pbs``, so only tasks go through PBS, and leave (1) for
       later.

   Either way, ``[molssi10]``'s default changes during the window.
B. **MariaDB** is used only for SLURM's accounting. Remove it with SLURM
   (after the dump), or leave it installed and stopped?
C. **On the host or in a container?** OpenPBS in a container on MolSSI10
   (images exist for development) would leave the host untouched, but a
   container's PBS submitting the JobServer's jobs on the host is not the
   "would it work in production" check we want. **Recommended: on the host**,
   with the container only if the build fails.
D. **The window:** when MolSSI10 can have no jobs running (its production
   JobServer submits to TinkerCliffs as well as locally).
