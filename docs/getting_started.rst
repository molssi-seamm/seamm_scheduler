Getting Started
===============

``seamm_scheduler`` talks to queueing systems -- SLURM and PBS today -- through
one interface. It has no SEAMM-core dependency and no notion of SEAMM's own job
states; it only speaks the queueing systems'.

Installing
----------

The library has no runtime dependencies::

    pip install seamm-scheduler

Submitting and polling
----------------------

.. code-block:: python

    from seamm_scheduler import QueueBackend, LocalTransport, SshTransport
    from seamm_scheduler import build_script, get_scheduler

    slurm = get_scheduler("slurm")
    backend = QueueBackend(slurm, SshTransport("tinkercliffs"))
    # or, on a login node: QueueBackend(slurm, LocalTransport())

    directives = slurm.directives(
        {"ntasks": 4, "mem_per_cpu": 2 * 1024**3, "walltime": 3600},
        extra={"partition": "normal_q", "account": "seamm", "export": "NONE"},
    )
    script = build_script(directives, "module load ORCA\norca orca.inp > orca.out")
    job_id = backend.submit(script, job_name="demo")

    status = backend.poll_many([job_id])[job_id]
    print(status.state, status.category, status.task_state)

``poll_many`` takes many ids and uses as few commands as the scheduler allows.
A job the scheduler no longer knows is absent from the result; if the queue
could not be asked at all (``backend.scheduler.poll_failed``), absence means
nothing.

The historical SLURM classes are still here: ``LocalSlurm()`` and
``SshSlurm(host)`` are ``QueueBackend`` subclasses with SLURM built in.

Targets
-------

``seamm_scheduler.config`` reads a JobServer's ``<root>/<jobserver-name>.ini``.
Each section is a target:

.. code-block:: ini

    [DEFAULT]
    default = local

    [local]
    type = local
    tasks = pool

    [arc]
    type = local                 ; the evaluator runs on the JobServer's host
    tasks = queue                ; its tasks go to a queue
    scheduler = slurm
    transport = ssh
    host = tinkercliffs
    remote_root = /projects/seamm/psaxe/tasks
    remote_python = /projects/seamm/SEAMM/venv/bin/python
    account = seamm
    partition = normal_q
    qos = tc_normal_short
    export = NONE
    bundle_tasks = 8
    max_queued_tasks = 800
    max_walltime = 3-00:00:00    ; retries never ask for more time than this

.. code-block:: python

    from seamm_scheduler import list_sections

    sections = list_sections("~/SEAMM", "chemai")
    arc = sections["arc"]
    queue = arc.build_task_backend()     # the QueueBackend for its tasks
    stager = arc.build_task_stager()     # RsyncStager, or LocalStager if shared

A section without ``tasks =`` means exactly what it did before the task keys
existed. See ``seamm_exec``'s task layer for what the keys do.

Where the flowchart itself runs is the section's ``type``:

``local``
    as a subprocess on the JobServer's host;
``queue``
    as a batch job on the queueing system named by ``scheduler`` (``slurm``,
    the default, ``pbs``, or ``seamm`` for a machine's own TaskServer -- see
    :doc:`taskserver`), through the section's ``transport``;
``slurm``
    the original spelling of ``queue`` with ``scheduler = slurm``; still
    accepted.

A PBS section may give PBS's own directives or the portable spellings:

.. code-block:: ini

    [molssi10]
    type = queue
    scheduler = pbs
    transport = ssh
    host = molssi10
    remote_root = /home/psaxe/seamm_jobs
    remote_run_from_jobserver = /home/psaxe/SEAMM/venv/bin/run_from_jobserver
    queue = workq                ; or partition =
    walltime = 01:00:00          ; or time =, in SLURM's forms too
    select = 1:ncpus=1:mem=20gb  ; or ntasks =, mem =, mem_per_cpu = ...

The resources become one ``select`` chunk, merged into the section's
``select``: a job's or a bundle's cores replace the section's, its memory is
kept unless the job sets one. A PBS job starts in the home directory, so the
script begins with a ``cd`` into the job's directory. Like SLURM's default, a
PBS flowchart job gets the JobServer's environment (``-V``); with
``export = NONE`` it gets only PBS's ``PBS_O_*`` variables, so the section must
then use an absolute ``remote_run_from_jobserver`` (or a ``setup =`` that
prepares the environment), not ``remote_conda_env``.

The remote host needs this version of ``seamm_scheduler`` (or later) too: an
older one does not know ``type = queue`` and would treat an evaluator running
inside a PBS job as if it had to reach the cluster over ssh.

Adding a queueing system
------------------------

Subclass ``seamm_scheduler.scheduler.Scheduler`` in a new module (see
``pbs.py``), implement the directive, command and parsing methods, and add it
to ``SCHEDULERS`` in ``scheduler.py``.
