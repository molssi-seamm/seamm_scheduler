# -*- coding: utf-8 -*-

"""``QueueBackend``: submit, poll and cancel jobs on a queueing system.

A backend pairs a :class:`~seamm_scheduler.scheduler.Scheduler` (which commands,
which syntax) with a transport (how to run them: on this host, or over ssh).
Command construction and output parsing live in the scheduler, so every
transport behaves identically.
"""

import logging

from .scheduler import SchedulerError, SubmitError

logger = logging.getLogger("seamm_scheduler")


class QueueBackend:
    """Talks to one queueing system through one transport.

    Parameters
    ----------
    scheduler : seamm_scheduler.scheduler.Scheduler
    transport : LocalTransport or SshTransport, optional
        How commands run. A subclass may instead override :meth:`_run`.
    """

    def __init__(self, scheduler, transport=None):
        self.scheduler = scheduler
        self.transport = transport

    @property
    def host(self):
        return getattr(self.transport, "host", None)

    # ---- transport hook ---------------------------------------------------
    def _run(self, argv, input_text=None):
        """Run a command through the transport: ``(returncode, stdout, stderr)``."""
        if self.transport is None:
            raise NotImplementedError(
                f"{type(self).__name__} has no transport and does not override _run"
            )
        return self.transport.run(argv, input_text=input_text)

    # ---- public API -------------------------------------------------------
    def submit(self, script, *, job_name=None):
        """Submit a script (its full text, directives and shebang included --
        see ``seamm_scheduler.script.build_script``), fed on the submit
        command's standard input, so the script never needs to exist as a file
        the target host can see.

        Returns
        -------
        str
            The job id.
        """
        argv = self.scheduler.submit_cmd(job_name=job_name)
        rc, out, err = self._run(argv, input_text=script)
        if rc != 0:
            raise self._submit_error(f"{argv[0]} failed ({rc}): {err.strip()}")
        try:
            return self.scheduler.parse_submit(out)
        except ValueError:
            raise self._submit_error(
                f"{argv[0]} returned no job id: {out!r} {err!r}"
            ) from None

    def cancel(self, job_id):
        """Cancel a submitted job."""
        self.cancel_many([job_id])

    def cancel_many(self, job_ids):
        """Cancel several jobs in one command."""
        job_ids = [str(j) for j in job_ids]
        if not job_ids:
            return
        argv = self.scheduler.cancel_cmd(job_ids)
        rc, out, err = self._run(argv)
        if rc != 0:
            raise self._error(
                f"{argv[0]} {' '.join(job_ids)} failed ({rc}): {err.strip()}"
            )

    def poll_many(self, job_ids):
        """The current status of a batch of jobs, in as few commands as the
        scheduler allows.

        Returns
        -------
        {str: JobStatus}
            Keyed by job id. A job the scheduler has no record of at all (e.g.
            purged from accounting) is absent; callers should treat "missing"
            as "can't confirm this job still exists".
        """
        return self.scheduler.poll(self._run, [str(j) for j in job_ids])

    def find_jobs(self, job_name):
        """The ids of the user's queued or running jobs called ``job_name``,
        or None if that cannot be known now (the queue could not be asked)."""
        return self.scheduler.find(self._run, job_name)

    def count_jobs(self):
        """How many jobs the user has in the queue now, or None if unknown."""
        argv = self.scheduler.count_cmd()
        if argv is None:
            return None
        rc, out, err = self._run(argv)
        if rc != 0:
            logger.warning(f"Could not count the queued jobs: {err.strip()}")
            return None
        return len([line for line in out.splitlines() if line.strip()])

    # ---- errors -------------------------------------------------------------
    # Subclasses (seamm_slurm's SlurmBackend) raise their own historical types.
    def _error(self, message):
        return SchedulerError(message)

    def _submit_error(self, message):
        return SubmitError(message)
