Getting Started
===============

``seamm_scheduler`` wraps the SLURM command-line tools (``sbatch``, ``squeue``,
``sacct``, ``scancel``) behind one interface, ``SlurmBackend``, with two
transports -- ``LocalSlurm`` (SLURM CLI on the current host) and
``SshSlurm`` (SLURM CLI on a remote host over passwordless SSH). It has no
SEAMM-core dependency and no notion of SEAMM's own job-status vocabulary; it
only speaks SLURM's.

Installing
----------

The library has no runtime dependencies::

    pip install seamm_scheduler

Basic usage
-----------

.. code-block:: python

    from seamm_scheduler import LocalSlurm, SshSlurm
    from seamm_scheduler.script import build_script

    backend = LocalSlurm()
    # or, from a host that is not itself a SLURM submit host:
    # backend = SshSlurm("molssi10")

    script = build_script(
        {
            "job_name": "my-flowchart",
            "partition": "batch",
            "nodes": 1,
            "ntasks": 1,
            "time": "01:00:00",
            "chdir": "/home/psaxe/SEAMM/Jobs/projects/demo/Job_00123",
        },
        payload=(
            "source /home/psaxe/miniconda3/etc/profile.d/conda.sh\n"
            "conda activate seamm\n"
            "run_from_jobserver 123 "
            "/home/psaxe/SEAMM/Jobs/projects/demo/Job_00123 "
            "/home/psaxe/SEAMM/Jobs/seamm.db\n"
        ),
    )
    job_id = backend.submit(script)

    statuses = backend.poll_many([job_id])
    print(statuses[job_id].category)  # "pending" | "running" | "completed" | ...

Staging files when there's no shared filesystem
-------------------------------------------------

``SshSlurm`` above only runs SLURM CLI commands remotely -- it assumes the
job's own working directory is already visible to the remote host (e.g. a
shared/NFS filesystem, or the caller runs on a login node). If it is not --
a laptop reaching a cluster it shares no filesystem with, for instance --
pair it with a stager, mirroring the transport with ``LocalStager`` (a
no-op, for the shared-filesystem case above) or ``RsyncStager``:

.. code-block:: python

    from seamm_scheduler import RsyncStager

    stager = RsyncStager("molssi10")
    # Pushes local_wdir to molssi10 over rsync -e ssh, mkdir -p first.
    # Use the returned path for the sbatch script's chdir directive and
    # for any command line built for the remote host, not local_wdir.
    remote_wdir = stager.stage_in(local_wdir, remote_wdir)
    ...
    # After SLURM reports the job terminal, pull results back:
    stager.stage_out(remote_wdir, local_wdir)

``SlurmSection.build_stager()`` picks the right one automatically from a
``<root>/<jobserver-name>.ini`` section's ``transport`` key, the same way
``build_backend()`` picks the transport -- see ``seamm_jobserver``'s user
guide for the full ini format, including ``remote_root``,
``remote_run_from_jobserver``/``remote_conda_env``, and ``setup`` (raw
shell commands, e.g. ``module load ORCA``, run at the top of the
generated sbatch script before ``run_from_jobserver`` -- for a queue
whose submission environment doesn't otherwise carry whatever a code's
own ``installation = modules`` needs).

``remote_wdir`` above (a scratch path on the remote host, distinct from
``local_wdir``) is not something a caller invents -- ``SlurmSection`` also
computes it, deterministically, from ``remote_root`` and the local
directory's own name:

.. code-block:: python

    remote_wdir = section.remote_wdir_for(local_wdir)

Because it's a pure function of ``local_wdir`` and the section's own
config, any caller who knows both can recompute the same path
independently -- not just whichever process originally called
``stage_in()``. This is what lets something like a Dashboard pull a
still-*running* job's files back on demand (``stager.stage_out(section.
remote_wdir_for(local_wdir), local_wdir)``) without needing to ask the
process that submitted the job where it put things.

See the design doc under :doc:`developer_guide/campaigns/2026-08-06/index`
for the full rationale, and the workspace-root
``~/Work/SEAMM/jobserver-slurm-plan.md`` living plan for how this fits into
the larger ``seamm_jobserver`` SLURM integration.

Multiple queues per config file
--------------------------------

A ``<root>/<jobserver-name>.ini`` file can describe more than one
cluster/queue target, one section each. ``load_slurm_config(root,
jobserver_name)`` resolves a *single* section (via that file's
``[DEFAULT] default =`` key, or the sole section if there is only one);
``list_sections(root, jobserver_name)`` instead returns every section as a
``{name: SlurmSection}`` dict, for a caller that routes jobs across more
than one queue or wants to advertise "what queues exist" (e.g. to a
submission UI):

.. code-block:: python

    from seamm_scheduler.config import list_sections

    sections = list_sections(root, "molssi10")
    for name, section in sections.items():
        print(name, section.type, section.transport)

A section's ``type`` (default ``"slurm"``) can also be ``"local"`` -- no
scheduler at all, for a queue that just means "run this as a plain local
subprocess" rather than a submission target of its own.
``build_backend()``/``build_stager()`` raise clearly if called on a
``type = local`` section, since it has neither; a caller routes those jobs
through its own local-subprocess path instead. See ``seamm_jobserver``'s
design doc under ``docs/developer_guide/campaigns/2026-08-10/`` (multi-queue
routing) for how this is used to let one JobServer instance route jobs to
several queues -- some local, some real SLURM clusters -- at once.
