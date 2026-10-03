# -*- coding: utf-8 -*-

"""Build a full batch script from a directives dict and a payload.

Matches the ``<root>/<jobserver-name>.ini`` config shape used by
``seamm_jobserver``: each target section's directive keys (``partition``,
``account``, ``qos``, ``nodes``, ``ntasks``, ``time``, ``mem``, ``gpus``, ...)
become the scheduler's directive lines. A blank/``None`` value means "don't pass
that directive" -- let the scheduler's own defaults apply.
"""

from .scheduler import get_scheduler


def build_script(directives, payload, *, shell="/bin/bash", scheduler="slurm"):
    """Build a full batch script: shebang, directive lines, then payload.

    Parameters
    ----------
    directives : dict(str, any)
        Submission options in the scheduler's own vocabulary (for SLURM, the
        plain names such as ``"partition"`` -- see
        :meth:`seamm_scheduler.slurm.Slurm.directive_lines`).
    payload : str
        The body of the script -- the commands to run. Used verbatim, after the
        directive block.
    shell : str = "/bin/bash"
        The shebang interpreter.
    scheduler : str or Scheduler = "slurm"
        Whose directive syntax to write.

    Returns
    -------
    str
        The full script text, ready to hand to ``QueueBackend.submit()``.
    """
    if isinstance(scheduler, str):
        scheduler = get_scheduler(scheduler)

    lines = [f"#!{shell}"]
    lines.extend(scheduler.directive_lines(directives))
    lines.append("")
    prologue = scheduler.prologue_lines(directives)
    if prologue:
        lines.extend(prologue)
        lines.append("")
    lines.append(payload.rstrip("\n"))
    lines.append("")

    return "\n".join(lines)
