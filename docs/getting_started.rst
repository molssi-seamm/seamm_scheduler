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

.. code-block:: python

    from seamm_scheduler import list_sections

    sections = list_sections("~/SEAMM", "chemai")
    arc = sections["arc"]
    queue = arc.build_task_backend()     # the QueueBackend for its tasks
    stager = arc.build_task_stager()     # RsyncStager, or LocalStager if shared

A section without ``tasks =`` means exactly what it did before the task keys
existed. See ``seamm_exec``'s task layer for what the keys do.

Adding a queueing system
------------------------

Subclass ``seamm_scheduler.scheduler.Scheduler`` in a new module (see
``pbs.py``), implement the directive, command and parsing methods, and add it
to ``SCHEDULERS`` in ``scheduler.py``.
