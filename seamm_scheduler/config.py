# -*- coding: utf-8 -*-

"""Load a JobServer's ``<root>/<jobserver-name>.ini``: its *targets*.

System/machine config, not a user preference -- lives at ``<root>``
(``~/SEAMM`` by default), alongside ``orca.ini``/``lammps.ini``/
``dashboards.ini``, not in ``~/.seamm.d/seamm.ini``. Named after the JobServer
instance (its ``--name``, default hostname), one section per target.

A target says where a job's flowchart *evaluator* runs (``type``: ``local`` for
a subprocess of the JobServer, ``slurm`` for a batch job of its own) and, with
the keys added for the task layer, where the evaluator's *tasks* run
(``tasks = pool | taskserver | queue``). Every key the task layer added is
optional: a section without ``tasks =`` means exactly what it meant before
(whole-flowchart submission, the codes running inside the evaluator).

A section is copied verbatim into each job's ``target.json`` (``setup`` text
included), which the Dashboard can show, so a section must never hold secrets.

Lives here, not in ``seamm_jobserver``, so any dependency-light consumer --
``seamm_jobserver``, ``seamm_webui``'s queue list, ``seamm_exec``'s task layer
-- can read and validate the file without pulling in the rest of the SEAMM
stack. See ``seamm_exec``'s ``docs/developer_guide/campaigns/2026-10-02/``.
"""

import configparser
import re
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path, PurePosixPath
from typing import Optional

from .backend import QueueBackend
from .local import LocalTransport
from .scheduler import get_scheduler
from .slurm import LocalSlurm, SshSlurm
from .ssh import TASK_SSH_OPTIONS, SshTransport
from .stage import LocalStager, RsyncStager

# The task layer's ssh: fail fast, and give up a hung command (see
# TASK_SSH_OPTIONS). rsync of a large bundle may take long, so it gets more.
_TASK_COMMAND_TIMEOUT = 300
_TASK_STAGE_TIMEOUT = 3600

# Section keys that describe JobServer- or task-layer behavior rather than a
# submission directive -- not forwarded to the scheduler's directives.
_NON_DIRECTIVE_KEYS = {
    "type",
    "transport",
    "host",
    "max_concurrent_jobs",
    "max_resubmits",
    "default",
    "remote_root",
    "remote_conda_env",
    "remote_run_from_jobserver",
    "setup",
    # The task layer (2026-10-02)
    "tasks",
    "scheduler",
    "shared_filesystem",
    "bundle_tasks",
    "bundle_walltime",
    "max_queued_tasks",
    "inline_below",
    "url",
    "remote_python",
    "remote_seamm_root",
    "poll_interval",
}

# Recognized values for a section's "type" key: where the *evaluator* runs.
# "local" means "no scheduler -- the JobServer's local-subprocess path";
# "queue" means the JobServer submits the whole flowchart as a batch job to the
# queueing system named by `scheduler` (slurm, the default, or pbs); "slurm" is
# the original spelling of "queue" with SLURM.
_VALID_TYPES = {"slurm", "queue", "local"}

# Recognized values for "tasks": where the evaluator's tasks run.
_VALID_TASKS = {"pool", "taskserver", "queue"}

_TRUE = {"1", "yes", "true", "on"}
_FALSE = {"0", "no", "false", "off"}


@dataclass
class FieldLimits:
    """Constraints on a per-job override of one directive.

    All fields optional: a directive can be listed as overridable with no
    constraint at all, meaning "any value the scheduler itself accepts."
    """

    choices: Optional[list] = None
    minimum: Optional[str] = None
    maximum: Optional[str] = None


@dataclass
class TargetSection:
    """One target from a ``<root>/<jobserver-name>.ini`` file."""

    name: str
    transport: str
    host: Optional[str]
    type: str = "slurm"
    directives: dict = field(default_factory=dict)
    max_concurrent_jobs: int = 20
    max_resubmits: int = 3
    limits: dict = field(default_factory=dict)  # {directive: FieldLimits}
    # Only meaningful for transport=ssh, where the JobServer shares no
    # filesystem with the submit host (see RsyncStager): the base directory
    # under which each job's remote scratch tree is created, and how to invoke
    # run_from_jobserver on the remote host, since its own local install can't
    # be reused there. Either remote_run_from_jobserver (an explicit absolute
    # path, preferred) or remote_conda_env (falls back to ``conda run -n <env>
    # run_from_jobserver``) must be set for a type=slurm, transport=ssh
    # section; neither is meaningful for transport=local.
    remote_root: Optional[str] = None
    remote_conda_env: Optional[str] = None
    remote_run_from_jobserver: Optional[str] = None
    # Raw shell commands run at the top of the batch script -- e.g. "module
    # load ORCA" -- before the payload. Ini multi-line syntax (indented
    # continuation lines) allows more than one command. Not templated/escaped
    # in any way -- trusted config, not job input.
    setup: Optional[str] = None

    # ---- the task layer (all optional; None = as before) -------------------
    # Where the evaluator's tasks run: "pool" (its own LocalPool), "queue"
    # (a scheduler), "taskserver". None: the codes run in the evaluator, as
    # before the task layer existed.
    tasks: Optional[str] = None
    # The queueing system for tasks = queue. Default: slurm.
    scheduler: Optional[str] = None
    # Whether the evaluator and the cluster see the same storage, so task
    # directories need no staging. Default: yes for transport=local (or an
    # evaluator inside the cluster, type=slurm), no for transport=ssh.
    shared_filesystem: Optional[bool] = None
    # Bundling: tasks per allocation, and the most walltime (seconds) a bundle
    # may add up to from its tasks' estimates.
    bundle_tasks: Optional[int] = None
    bundle_walltime: Optional[float] = None
    # Most queued + running jobs of this user at once (TinkerCliffs: 1,000,
    # every array element counting).
    max_queued_tasks: Optional[int] = None
    # Tasks estimated below this many seconds run in the evaluator's pool.
    inline_below: Optional[float] = None
    # tasks = taskserver: the TaskServer's URL.
    url: Optional[str] = None
    # transport=ssh: a Python with seamm_exec on the cluster, which runs the
    # task worker and resolves each program from the cluster's own
    # <program>.ini, and the SEAMM root holding those files (default: the
    # root the venv belongs to).
    remote_python: Optional[str] = None
    remote_seamm_root: Optional[str] = None
    # Seconds between status polls of the queue.
    poll_interval: Optional[float] = None

    # ------------------------------------------------------------------
    # The evaluator's back end (the JobServer's whole-flowchart submission)
    # ------------------------------------------------------------------
    @property
    def is_batch(self):
        """Whether the JobServer runs this section's evaluators as batch jobs."""
        return self.type in ("slurm", "queue")

    @property
    def batch_scheduler(self):
        """The queueing system the evaluator jobs are submitted to."""
        if self.type == "slurm":
            return "slurm"
        return (self.scheduler or "slurm").lower()

    def build_backend(self):
        """Construct the backend that submits this section's evaluator jobs."""
        if not self.is_batch:
            raise RuntimeError(
                f"section '{self.name}' has type=local; it has no queueing "
                "back end to build -- route jobs for it through the "
                "JobServer's existing local-subprocess path instead"
            )
        return self._backend(self.transport, self.host, self.batch_scheduler)

    def build_stager(self):
        """Construct the stager for this section's evaluator jobs -- paired
        with the transport the same way ``build_backend()`` picks a
        backend."""
        if self.type == "local":
            raise RuntimeError(
                f"section '{self.name}' has type=local; it has no stager "
                "to build -- route jobs for it through the "
                "JobServer's existing local-subprocess path instead"
            )
        return self._stager(self.transport, self.host)

    # ------------------------------------------------------------------
    # The tasks' back end
    # ------------------------------------------------------------------
    @property
    def task_transport(self):
        """How the evaluator reaches the queue for its tasks.

        An evaluator that is itself a batch job (type=slurm or queue) runs inside
        the cluster and submits its tasks there with local commands, whatever
        transport the JobServer uses to reach the cluster.
        """
        if self.is_batch:
            return "local"
        return self.transport

    @property
    def tasks_share_filesystem(self):
        """Whether task directories need no staging."""
        if self.shared_filesystem is not None:
            return self.shared_filesystem
        return self.task_transport == "local"

    @property
    def task_scheduler(self):
        """The name of the queueing system for tasks."""
        return (self.scheduler or "slurm").lower()

    def build_task_backend(self):
        """The ``QueueBackend`` the evaluator submits its tasks through."""
        if self.tasks != "queue":
            raise RuntimeError(
                f"section '{self.name}' has tasks={self.tasks}; only tasks=queue "
                "submits tasks to a queueing system"
            )
        return self._backend(
            self.task_transport,
            self.host,
            self.task_scheduler,
            ssh_options=TASK_SSH_OPTIONS,
            timeout=_TASK_COMMAND_TIMEOUT,
            drop_env_prefixes=("SLURM_", "PBS_"),
        )

    def build_task_stager(self):
        """The stager for task directories: none on a shared filesystem."""
        if self.tasks_share_filesystem:
            return LocalStager()
        return self._stager(
            self.task_transport,
            self.host,
            ssh_options=TASK_SSH_OPTIONS,
            timeout=_TASK_STAGE_TIMEOUT,
        )

    def task_settings(self):
        """This section as a JSON-serializable dict, for ``<job>/target.json``.

        The JobServer writes it into each job's directory so that an evaluator
        can find its target wherever it runs (on the JobServer's host, or as a
        batch job on a cluster that cannot read this ini file).
        """
        data = asdict(self)
        data["limits"] = {k: asdict(v) for k, v in self.limits.items()}
        return data

    @classmethod
    def from_settings(cls, data):
        """The inverse of :meth:`task_settings`."""
        known = {f.name for f in fields(cls)}
        values = {k: v for k, v in data.items() if k in known}
        values["limits"] = {
            k: FieldLimits(**v) for k, v in (data.get("limits") or {}).items()
        }
        return cls(**values)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _backend(
        self,
        transport,
        host,
        scheduler,
        ssh_options=(),
        timeout=None,
        drop_env_prefixes=(),
    ):
        if transport not in ("local", "ssh"):
            raise RuntimeError(
                f"section '{self.name}' has unknown transport "
                f"'{transport}' (expected 'local' or 'ssh')"
            )
        if transport == "ssh" and not host:
            raise RuntimeError(
                f"section '{self.name}' has transport=ssh but no host set"
            )
        if scheduler == "slurm":
            # The historical classes, so isinstance() checks keep working.
            if transport == "local":
                return LocalSlurm(drop_env_prefixes=drop_env_prefixes)
            return SshSlurm(host, ssh_options=ssh_options, timeout=timeout)
        if transport == "local":
            t = LocalTransport(drop_env_prefixes=drop_env_prefixes)
        else:
            t = SshTransport(host, ssh_options=ssh_options, timeout=timeout)
        return QueueBackend(get_scheduler(scheduler), t)

    def _stager(self, transport, host, ssh_options=(), timeout=None):
        if transport == "local":
            return LocalStager()
        elif transport == "ssh":
            if not host:
                raise RuntimeError(
                    f"section '{self.name}' has transport=ssh but no host set"
                )
            return RsyncStager(host, ssh_options=ssh_options, timeout=timeout)
        else:
            raise RuntimeError(
                f"section '{self.name}' has unknown transport "
                f"'{transport}' (expected 'local' or 'ssh')"
            )

    def remote_wdir_for(self, local_wdir):
        """The remote scratch path a ``transport = ssh`` section's job
        should run in (or, for an already-running job, where its files
        currently live), derived from its local working directory's name
        and this section's ``remote_root``.

        Deterministic and side-effect-free -- callers besides the one that
        originally submitted the job (e.g. a Dashboard pulling a running
        job's files back on demand, rather than waiting for the job to
        finish) can recompute the same path independently, as long as they
        agree on ``local_wdir`` and this section's config. Job directory
        names (``Job_NNNNNN``) are unique across the whole datastore, not
        just within a project, so no collision risk.

        Raises
        ------
        RuntimeError
            If this section has no ``remote_root`` set -- only meaningful
            for ``transport = ssh``.
        """
        if not self.remote_root:
            raise RuntimeError(
                f"section '{self.name}' has transport=ssh but "
                "no remote_root is set -- can't determine where to stage "
                f"jobs on {self.host}."
            )
        return str(PurePosixPath(self.remote_root) / Path(local_wdir).name)

    def merge_overrides(self, requested):
        """Merge a job's requested per-directive overrides on top of this
        section's defaults, validating each against ``self.limits``.

        Parameters
        ----------
        requested : dict(str, any)
            Directive overrides a job asked for, e.g. ``{"ntasks": 4}`` --
            typically a job's ``parameters["slurm"]``.

        Returns
        -------
        dict
            The final directives dict (this section's defaults with the
            validated overrides applied), ready for
            ``seamm_scheduler.script.build_script``.

        Raises
        ------
        ValueError
            If a requested field isn't overridable for this section, or its
            value is outside the configured choices/bounds. Never trust a
            caller (e.g. a web UI) to have already enforced this --
            validate here regardless of what constrained the request's
            origin.
        """
        directives = dict(self.directives)
        for key, value in requested.items():
            limits = self.limits.get(key)
            if limits is None:
                raise ValueError(
                    f"'{key}' is not an overridable SLURM directive for "
                    f"section '{self.name}'"
                )
            if limits.choices is not None and str(value) not in limits.choices:
                raise ValueError(
                    f"'{key}={value}' is not one of the allowed choices for "
                    f"section '{self.name}': {limits.choices}"
                )
            if limits.minimum is not None or limits.maximum is not None:
                parsed = _parse_slurm_value(key, value)
                if limits.minimum is not None:
                    lo = _parse_slurm_value(key, limits.minimum)
                    if parsed < lo:
                        raise ValueError(
                            f"'{key}={value}' is below the minimum "
                            f"({limits.minimum}) for section '{self.name}'"
                        )
                if limits.maximum is not None:
                    hi = _parse_slurm_value(key, limits.maximum)
                    if parsed > hi:
                        raise ValueError(
                            f"'{key}={value}' exceeds the maximum "
                            f"({limits.maximum}) for section '{self.name}'"
                        )
            directives[key] = value
        return directives


#: The historical name.
SlurmSection = TargetSection


def load_target(root, jobserver_name, section=None):
    """Load one target from ``<root>/<jobserver_name>.ini``, if it exists.

    Parameters
    ----------
    root : str or Path
        The SEAMM root directory (e.g. ``~/SEAMM``), same as every other
        per-code ``.ini`` file.
    jobserver_name : str
        This JobServer instance's ``--name`` (default: hostname).
    section : str or None
        Which section to use. If ``None``, uses ``[DEFAULT]``'s
        ``default =`` key, falling back to the sole section if there is
        exactly one and no ``default`` is set.

    Returns
    -------
    TargetSection or None
        ``None`` means "no such file" -- the caller should fall back to
        running jobs as local subprocesses, exactly as if this feature did
        not exist.
    """
    ini_path = Path(root).expanduser() / f"{jobserver_name}.ini"
    if not ini_path.exists():
        return None

    config = configparser.ConfigParser(interpolation=None)
    config.read(ini_path)

    if section is None:
        section = config.defaults().get("default")
    if section is None:
        # A ".limits" section is a companion, not a routable target itself.
        candidates = [s for s in config.sections() if not s.endswith(".limits")]
        if len(candidates) == 1:
            section = candidates[0]
        else:
            raise RuntimeError(
                f"{ini_path} has no [DEFAULT] default= and no single, "
                "unambiguous section to use."
            )
    if section not in config:
        raise RuntimeError(f"{ini_path} has no section '{section}'")

    return _build_section(config, section)


#: The historical name.
load_slurm_config = load_target


def list_sections(root, jobserver_name):
    """Load every routable section from ``<root>/<jobserver_name>.ini``, not
    just the one ``load_target()`` resolves via ``default=``/single-section
    fallback.

    For per-job routing (a job asks for a named queue) and for surfacing
    "what queues exist" to a submission UI (e.g. ``seamm_webui``'s
    ``GET /api/queues``).

    Returns
    -------
    dict(str, TargetSection)
        Empty if the config file doesn't exist.
    """
    ini_path = Path(root).expanduser() / f"{jobserver_name}.ini"
    if not ini_path.exists():
        return {}

    config = configparser.ConfigParser(interpolation=None)
    config.read(ini_path)

    names = [s for s in config.sections() if not s.endswith(".limits")]
    return {name: _build_section(config, name) for name in names}


def _build_section(config, section):
    """Parse one ``[section]`` (plus its optional ``[section.limits]``
    companion) of an already-loaded config file into a ``TargetSection``."""
    items = dict(config.items(section))

    section_type = (items.get("type") or "slurm").strip().lower()
    if section_type not in _VALID_TYPES:
        raise RuntimeError(
            f"section '{section}' has unknown type '{section_type}' "
            f"(expected one of {sorted(_VALID_TYPES)})"
        )

    tasks = items.get("tasks") or None
    if tasks is not None:
        tasks = tasks.strip().lower()
        if tasks not in _VALID_TASKS:
            raise RuntimeError(
                f"section '{section}' has unknown tasks '{tasks}' "
                f"(expected one of {sorted(_VALID_TASKS)})"
            )
    scheduler = items.get("scheduler") or None
    if scheduler is not None:
        # Fails early on a typo.
        get_scheduler(scheduler)

    directives = {
        k: v for k, v in items.items() if k not in _NON_DIRECTIVE_KEYS and v != ""
    }

    return TargetSection(
        name=section,
        transport=items.get("transport", "local"),
        host=items.get("host") or None,
        type=section_type,
        directives=directives,
        max_concurrent_jobs=int(items.get("max_concurrent_jobs", 20)),
        max_resubmits=int(items.get("max_resubmits", 3)),
        limits=_load_limits(config, section),
        remote_root=items.get("remote_root") or None,
        remote_conda_env=items.get("remote_conda_env") or None,
        remote_run_from_jobserver=items.get("remote_run_from_jobserver") or None,
        setup=items.get("setup") or None,
        tasks=tasks,
        scheduler=scheduler,
        shared_filesystem=_bool(section, items, "shared_filesystem"),
        bundle_tasks=_int(section, items, "bundle_tasks"),
        bundle_walltime=_seconds(section, items, "bundle_walltime"),
        max_queued_tasks=_int(section, items, "max_queued_tasks"),
        inline_below=_float(section, items, "inline_below"),
        url=items.get("url") or None,
        remote_python=items.get("remote_python") or None,
        remote_seamm_root=items.get("remote_seamm_root") or None,
        poll_interval=_float(section, items, "poll_interval"),
    )


def _bool(section, items, key):
    value = (items.get(key) or "").strip().lower()
    if value == "":
        return None
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise RuntimeError(f"section '{section}': {key} = {value} is not yes or no")


def _int(section, items, key):
    value = (items.get(key) or "").strip()
    if value == "":
        return None
    try:
        return int(value)
    except ValueError:
        raise RuntimeError(
            f"section '{section}': {key} = {value} is not an integer"
        ) from None


def _float(section, items, key):
    value = (items.get(key) or "").strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        raise RuntimeError(
            f"section '{section}': {key} = {value} is not a number"
        ) from None


def _seconds(section, items, key):
    value = (items.get(key) or "").strip()
    if value == "":
        return None
    try:
        return float(_parse_time(value))
    except ValueError:
        raise RuntimeError(
            f"section '{section}': {key} = {value} is not a time "
            "(minutes, MM:SS, HH:MM:SS or D-HH:MM:SS)"
        ) from None


def _load_limits(config, section):
    """Parse the optional ``[<section>.limits]`` companion section.

    Secure by default: absent entirely means nothing is overridable.
    """
    limits_section = f"{section}.limits"
    if limits_section not in config:
        return {}

    items = dict(config.items(limits_section))
    # dict(config.items(...)) inherits [DEFAULT] keys too (e.g. "default")
    items.pop("default", None)

    overridable_raw = items.pop("overridable", "")
    overridable = [f.strip() for f in overridable_raw.split(",") if f.strip()]

    per_field = {}
    for key, value in items.items():
        if "." not in key:
            continue
        field_name, attr = key.rsplit(".", 1)
        if attr not in ("choices", "min", "max"):
            continue
        per_field.setdefault(field_name, {})[attr] = value

    limits = {}
    for field_name in overridable:
        attrs = per_field.get(field_name, {})
        choices = None
        if "choices" in attrs:
            choices = [c.strip() for c in attrs["choices"].split(",") if c.strip()]
        limits[field_name] = FieldLimits(
            choices=choices,
            minimum=attrs.get("min"),
            maximum=attrs.get("max"),
        )
    return limits


# Known unit-aware directive names -- everything else is compared as a plain
# number. Not a general value-type system, just what's needed for the
# directives sites actually bound today (see FieldLimits/.limits).
_SIZE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*([KMGT]?)", re.IGNORECASE)
_SIZE_MULTIPLIER_MB = {"": 1, "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}


def _parse_size(value):
    """Parse a SLURM-style memory size (e.g. ``"100G"``, ``"2048"``,
    ``"20000M"``) into MB -- SLURM's own default unit for ``--mem`` when no
    suffix is given."""
    match = _SIZE_RE.fullmatch(str(value).strip())
    if not match:
        raise ValueError(f"cannot parse SLURM size value: {value!r}")
    number, suffix = match.groups()
    return float(number) * _SIZE_MULTIPLIER_MB[suffix.upper()]


def _parse_time(value):
    """Parse a SLURM time value into seconds.

    SLURM's forms: ``M``, ``M:S``, ``H:M:S``, ``D-H``, ``D-H:M``, ``D-H:M:S``.
    A bare number is minutes.
    """
    text = str(value).strip()
    if "-" in text:
        days_str, text = text.split("-", 1)
        days = int(days_str)
        parts = [int(p) for p in text.split(":")]
        while len(parts) < 3:
            parts.append(0)  # D-H and D-H:M count from the hours
    else:
        days = 0
        parts = [int(p) for p in text.split(":")]
        if len(parts) == 1:
            parts = [0, parts[0], 0]  # minutes
        while len(parts) < 3:
            parts.insert(0, 0)
    hours, minutes, seconds = parts[-3:]
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def _parse_slurm_value(field_name, value):
    """Parse a directive value into a comparable float."""
    if field_name == "mem" or field_name.startswith("mem_per_"):
        return _parse_size(value)
    if field_name in ("time", "walltime"):
        return _parse_time(value)
    return float(value)
