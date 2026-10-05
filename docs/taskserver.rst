The TaskServer
==============

A machine without a queueing system -- a laptop, a workstation, a second Mac --
can still share its cores and memory fairly among everything SEAMM runs on it:
the calculations of several flowcharts at once, and the flowcharts themselves.
The TaskServer is a small queue for that machine, driven exactly like SLURM or
PBS, locally or over ssh, through the ``seamm`` scheduler.

There is nothing to start or keep running: the queue is one file,
``<root>/taskserver/queue.db``, and every command that uses it also starts the
jobs that fit. Each job runs under a small runner of its own, so restarting or
upgrading anything never touches running jobs, and a laptop that sleeps simply
carries on when it wakes (time limits do not count the sleep).

Capacity
--------

``<root>/taskserver.ini`` (``~/SEAMM`` by default) says what the queue may use::

    [taskserver]
    cores = 8
    memory = 8 GB

``seamm-manager install`` writes it with the machine's physical cores and half
its memory, leaving the rest for the desktop and everything else. A job asking
for more than this is refused rather than run. A job's memory is enforced, not
just counted: a job using more than 125 % of what it asked for, for 30 seconds,
is stopped, and if the machine itself runs low on memory the newest job is
stopped.

The flowcharts themselves (the JobServer's jobs) take no cores and are never
counted against calculations, so they can never hold what their own calculations
wait for.

Using it
--------

In the JobServer's ``<root>/<hostname>.ini``, a section that sends a flowchart's
calculations to this machine's queue::

    [local]
    type = queue                ; the JobServer runs the flowcharts there too
    scheduler = seamm
    transport = local
    tasks = queue

or to another machine's, over ssh, with its files staged there and back::

    [workstation]
    type = local
    tasks = queue
    scheduler = seamm
    transport = ssh
    host = workstation.local
    remote_root = /home/me/seamm_tasks
    remote_python = /home/me/SEAMM/venv/bin/python

``remote_seamm_root`` names the root of the queue there, if it is not
``~/SEAMM``. The other machine needs SEAMM installed (``remote_python``) and the
same passwordless ssh the SLURM and PBS sections use.

Looking at it
-------------

``seamm-taskserver queue`` lists what is queued and running, with the cores and
memory each takes; ``seamm-taskserver status`` reports jobs (``--json`` for
programs), ``seamm-taskserver cancel ID`` cancels one, and ``seamm-taskserver
config`` shows the capacity. Over ssh, run them as ``<python> -m
seamm_scheduler.taskserver ...``.
