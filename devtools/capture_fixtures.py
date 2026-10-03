#!/usr/bin/env python
"""Record a real queueing system's command output as test fixtures.

Drives seamm_scheduler's own back end against a real site, over ssh, and records
every command it runs with its exit code and output. Scenarios: a job that
succeeds, one that fails (exit 3), one held behind another (pending), and a long
one that is cancelled; polled while queued/running and again after they finish,
plus find and count.

    python devtools/capture_fixtures.py molssi10 slurm tests/fixtures/slurm-20.11 \
        --directive partition=batch --directive mem=1G

The fixtures are JSON: {"site": ..., "scheduler": ..., "calls": [{"argv", "input",
"rc", "stdout", "stderr", "label"}, ...]}.
"""

import argparse
import json
import time
from pathlib import Path

from seamm_scheduler.backend import QueueBackend
from seamm_scheduler.scheduler import get_scheduler
from seamm_scheduler.script import build_script
from seamm_scheduler.ssh import SshTransport


class RecordingTransport:
    def __init__(self, inner):
        self.inner = inner
        self.calls = []
        self.label = ""

    def run(self, argv, input_text=None):
        rc, out, err = self.inner.run(argv, input_text=input_text)
        self.calls.append(
            {
                "label": self.label,
                "argv": [str(a) for a in argv],
                "input": input_text,
                "rc": rc,
                "stdout": out,
                "stderr": err,
            }
        )
        return rc, out, err


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("host")
    parser.add_argument("scheduler", choices=("slurm", "pbs"))
    parser.add_argument("output", help="Directory for the fixture files")
    parser.add_argument("--directive", action="append", default=[], help="key=value")
    parser.add_argument("--workdir", default="/tmp/seamm_fixtures")
    parser.add_argument("--hold-key", default=None, help="directive for a dependency")
    args = parser.parse_args()

    base = dict(d.split("=", 1) for d in args.directive)
    transport = RecordingTransport(SshTransport(args.host))
    scheduler = get_scheduler(args.scheduler)
    backend = QueueBackend(scheduler, transport)
    tag = f"seammfx{int(time.time()) % 100000}"

    transport.run(["mkdir", "-p", args.workdir])

    def submit(name, body, extra=None):
        directives = {**base, **(extra or {})}
        directives.update(scheduler.log_directives(args.workdir))
        script = build_script(
            directives,
            f"cd {args.workdir}\n{body}\n",
            scheduler=scheduler,
        )
        transport.label = f"submit {name}"
        return backend.submit(script, job_name=f"{tag}-{name}")

    ok = submit("ok", "sleep 20\nexit 0")
    fail = submit("fail", "sleep 20\nexit 3")
    long_ = submit("long", "sleep 600")
    if args.scheduler == "slurm":
        dep = {"dependency": f"afterany:{long_}"}
    else:
        dep = {"depend": f"afterany:{long_}"}
    held = submit("held", "sleep 5", dep)
    ids = [ok, fail, long_, held]
    print("submitted", ids)

    time.sleep(8)
    transport.label = "poll running/pending"
    print(backend.poll_many(ids))
    transport.label = "find long"
    backend.find_jobs(f"{tag}-long")
    transport.label = "count"
    backend.count_jobs()

    time.sleep(30)
    transport.label = "cancel long"
    backend.cancel(long_)
    time.sleep(20)
    transport.label = "poll finished"
    print(backend.poll_many(ids))
    transport.label = "find finished ok"
    backend.find_jobs(f"{tag}-ok")
    transport.label = "poll unknown id"
    print(backend.poll_many(["999999"]))

    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "lifecycle.json"
    path.write_text(
        json.dumps(
            {
                "site": args.host,
                "scheduler": args.scheduler,
                "ids": {"ok": ok, "fail": fail, "long": long_, "held": held},
                "calls": transport.calls,
            },
            indent=2,
        )
        + "\n"
    )
    print("wrote", path, len(transport.calls), "calls")


if __name__ == "__main__":
    main()
