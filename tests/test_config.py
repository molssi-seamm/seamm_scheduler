# -*- coding: utf-8 -*-

"""Tests for seamm_slurm.config (load_slurm_config, SlurmSection,
FieldLimits, merge_overrides).

Moved here from seamm_jobserver.slurm_config (2026-08-06) so any
dependency-light consumer can read/validate this config without pulling in
the rest of the SEAMM stack -- see the module docstring.
"""

import pytest

import seamm_slurm
from seamm_slurm.config import (
    FieldLimits,
    SlurmSection,
    list_sections,
    load_slurm_config,
)

# ---- load_slurm_config: basic section/routing behavior (unchanged from
# ---- seamm_jobserver.slurm_config) --------------------------------------


def test_missing_file_returns_none(tmp_path):
    assert load_slurm_config(tmp_path, "molssi10") is None


def test_single_section_no_default_key(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\n" "transport = local\n" "partition = batch\n" "nodes = 1\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section is not None
    assert section.name == "molssi10"
    assert section.transport == "local"
    assert section.host is None
    assert section.directives == {"partition": "batch", "nodes": "1"}
    assert section.max_concurrent_jobs == 20
    assert section.max_resubmits == 3
    assert section.limits == {}


def test_default_key_picks_section_among_several(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[DEFAULT]\n"
        "default = chemai\n"
        "\n"
        "[molssi10]\n"
        "transport = local\n"
        "\n"
        "[chemai]\n"
        "transport = ssh\n"
        "host = seamm-chemai\n"
        "partition = ChemAI\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.name == "chemai"
    assert section.transport == "ssh"
    assert section.host == "seamm-chemai"
    assert section.directives == {"partition": "ChemAI"}


def test_multiple_sections_without_default_raises(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\ntransport = local\n\n[chemai]\ntransport = ssh\nhost = x\n"
    )
    with pytest.raises(RuntimeError, match="no single"):
        load_slurm_config(tmp_path, "molssi10")


def test_limits_section_not_counted_as_a_routable_section(tmp_path):
    # A single real section plus its .limits companion should still count
    # as "exactly one" for the no-default fallback.
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\n"
        "transport = local\n"
        "\n"
        "[molssi10.limits]\n"
        "overridable = ntasks\n"
        "ntasks.min = 1\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.name == "molssi10"


def test_explicit_section_argument(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\ntransport = local\n\n[chemai]\ntransport = ssh\nhost = x\n"
    )
    section = load_slurm_config(tmp_path, "molssi10", section="chemai")
    assert section.name == "chemai"
    assert section.host == "x"


def test_unknown_section_name_raises(tmp_path):
    (tmp_path / "molssi10.ini").write_text("[molssi10]\ntransport = local\n")
    with pytest.raises(RuntimeError, match="no section 'nope'"):
        load_slurm_config(tmp_path, "molssi10", section="nope")


def test_blank_directive_values_are_dropped(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\n"
        "transport = local\n"
        "partition = batch\n"
        "account =\n"
        "qos =\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.directives == {"partition": "batch"}


def test_max_concurrent_and_resubmits_overridable(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\n"
        "transport = local\n"
        "max_concurrent_jobs = 5\n"
        "max_resubmits = 1\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.max_concurrent_jobs == 5
    assert section.max_resubmits == 1


def test_build_backend_local():
    section = SlurmSection(name="x", transport="local", host=None)
    backend = section.build_backend()
    assert isinstance(backend, seamm_slurm.LocalSlurm)


def test_build_backend_ssh():
    section = SlurmSection(name="x", transport="ssh", host="molssi10")
    backend = section.build_backend()
    assert isinstance(backend, seamm_slurm.SshSlurm)
    assert backend.host == "molssi10"


def test_build_backend_ssh_without_host_raises():
    section = SlurmSection(name="x", transport="ssh", host=None)
    with pytest.raises(RuntimeError, match="no host"):
        section.build_backend()


def test_build_backend_unknown_transport_raises():
    section = SlurmSection(name="x", transport="pbs", host=None)
    with pytest.raises(RuntimeError, match="unknown transport"):
        section.build_backend()


def test_build_stager_local():
    section = SlurmSection(name="x", transport="local", host=None)
    stager = section.build_stager()
    assert isinstance(stager, seamm_slurm.LocalStager)


def test_build_stager_ssh():
    section = SlurmSection(name="x", transport="ssh", host="molssi10")
    stager = section.build_stager()
    assert isinstance(stager, seamm_slurm.RsyncStager)
    assert stager.host == "molssi10"


def test_build_stager_ssh_without_host_raises():
    section = SlurmSection(name="x", transport="ssh", host=None)
    with pytest.raises(RuntimeError, match="no host"):
        section.build_stager()


def test_build_stager_unknown_transport_raises():
    section = SlurmSection(name="x", transport="pbs", host=None)
    with pytest.raises(RuntimeError, match="unknown transport"):
        section.build_stager()


def test_remote_root_and_conda_env_parsed_from_ini(tmp_path):
    (tmp_path / "Mac.ini").write_text(
        "[molssi10]\n"
        "transport = ssh\n"
        "host = molssi10\n"
        "remote_root = /home/psaxe/seamm_scratch\n"
        "remote_conda_env = seamm\n"
    )
    section = load_slurm_config(tmp_path, "Mac")
    assert section.remote_root == "/home/psaxe/seamm_scratch"
    assert section.remote_conda_env == "seamm"
    # Neither is a SLURM directive.
    assert "remote_root" not in section.directives
    assert "remote_conda_env" not in section.directives


def test_remote_root_and_conda_env_default_to_none(tmp_path):
    (tmp_path / "molssi10.ini").write_text("[molssi10]\ntransport = local\n")
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.remote_root is None
    assert section.remote_conda_env is None
    assert section.remote_run_from_jobserver is None


def test_remote_run_from_jobserver_parsed_from_ini(tmp_path):
    (tmp_path / "Mac.ini").write_text(
        "[molssi10]\n"
        "transport = ssh\n"
        "host = molssi10\n"
        "remote_run_from_jobserver = "
        "/home/psaxe/miniconda3/envs/seamm/bin/run_from_jobserver\n"
    )
    section = load_slurm_config(tmp_path, "Mac")
    assert (
        section.remote_run_from_jobserver
        == "/home/psaxe/miniconda3/envs/seamm/bin/run_from_jobserver"
    )
    assert "remote_run_from_jobserver" not in section.directives


# ---- "setup" (2026-08-11: module load / remote shell setup) --------------


def test_setup_parsed_from_ini(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[tinkercliffs]\n"
        "transport = ssh\n"
        "host = tinkercliffs\n"
        "setup = module load ORCA\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.setup == "module load ORCA"
    assert "setup" not in section.directives


def test_setup_multiline_parsed_from_ini(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[tinkercliffs]\n"
        "transport = ssh\n"
        "host = tinkercliffs\n"
        "setup = module load ORCA\n"
        "    module load GCC\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.setup == "module load ORCA\nmodule load GCC"


def test_setup_defaults_to_none(tmp_path):
    (tmp_path / "molssi10.ini").write_text("[molssi10]\ntransport = local\n")
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.setup is None


# ---- "type" (2026-08-10 campaign: multi-queue routing) -------------------


def test_type_defaults_to_slurm(tmp_path):
    (tmp_path / "molssi10.ini").write_text("[molssi10]\ntransport = local\n")
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.type == "slurm"


def test_type_local_parsed(tmp_path):
    (tmp_path / "mac.ini").write_text("[local]\ntype = local\n")
    section = load_slurm_config(tmp_path, "mac")
    assert section.type == "local"
    assert "type" not in section.directives


def test_type_unknown_raises(tmp_path):
    (tmp_path / "molssi10.ini").write_text("[molssi10]\ntype = pbs\n")
    with pytest.raises(RuntimeError, match="unknown type 'pbs'"):
        load_slurm_config(tmp_path, "molssi10")


def test_build_backend_type_local_raises():
    section = SlurmSection(name="x", transport="local", host=None, type="local")
    with pytest.raises(RuntimeError, match="type=local"):
        section.build_backend()


def test_build_stager_type_local_raises():
    section = SlurmSection(name="x", transport="local", host=None, type="local")
    with pytest.raises(RuntimeError, match="type=local"):
        section.build_stager()


# ---- remote_wdir_for --------------------------------------------------------


def test_remote_wdir_for_joins_remote_root_and_local_dir_name():
    section = SlurmSection(
        name="molssi10",
        transport="ssh",
        host="molssi10",
        remote_root="/scratch/psaxe/seamm_jobs",
    )
    assert (
        section.remote_wdir_for("/Users/psaxe/SEAMM/Jobs/projects/default/Job_000123")
        == "/scratch/psaxe/seamm_jobs/Job_000123"
    )


def test_remote_wdir_for_no_remote_root_raises():
    section = SlurmSection(name="molssi10", transport="ssh", host="molssi10")
    with pytest.raises(RuntimeError, match="no remote_root is set"):
        section.remote_wdir_for("/Users/psaxe/SEAMM/Jobs/projects/default/Job_000123")


def test_remote_wdir_for_deterministic_across_calls():
    """Two independent callers (e.g. seamm_jobserver at submit time and a
    Dashboard pulling a running job's files on demand later) must compute
    the exact same path from the same inputs."""
    section = SlurmSection(
        name="molssi10",
        transport="ssh",
        host="molssi10",
        remote_root="/scratch/psaxe/seamm_jobs",
    )
    wdir = "/Users/psaxe/SEAMM/Jobs/projects/default/Job_000123"
    assert section.remote_wdir_for(wdir) == section.remote_wdir_for(wdir)


# ---- list_sections ---------------------------------------------------------


def test_list_sections_missing_file_returns_empty_dict(tmp_path):
    assert list_sections(tmp_path, "molssi10") == {}


def test_list_sections_returns_every_routable_section(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[DEFAULT]\n"
        "default = local\n"
        "\n"
        "[local]\n"
        "type = local\n"
        "\n"
        "[molssi10]\n"
        "transport = local\n"
        "\n"
        "[chemai]\n"
        "transport = ssh\n"
        "host = seamm-chemai\n"
        "\n"
        "[molssi10.limits]\n"
        "overridable = ntasks\n"
    )
    sections = list_sections(tmp_path, "molssi10")
    assert set(sections) == {"local", "molssi10", "chemai"}
    assert sections["local"].type == "local"
    assert sections["molssi10"].type == "slurm"
    assert sections["chemai"].host == "seamm-chemai"
    assert sections["molssi10"].limits == {"ntasks": FieldLimits()}


def test_list_sections_single_section(tmp_path):
    (tmp_path / "molssi10.ini").write_text("[molssi10]\ntransport = local\n")
    sections = list_sections(tmp_path, "molssi10")
    assert set(sections) == {"molssi10"}


# ---- [<section>.limits] parsing ------------------------------------------


def test_no_limits_section_means_nothing_overridable(tmp_path):
    (tmp_path / "molssi10.ini").write_text("[molssi10]\ntransport = local\n")
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.limits == {}


def test_limits_overridable_with_no_bounds(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\ntransport = local\n\n[molssi10.limits]\noverridable = partition\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.limits == {"partition": FieldLimits()}


def test_limits_choices(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\ntransport = local\n\n"
        "[molssi10.limits]\n"
        "overridable = partition\n"
        "partition.choices = batch, gpu\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.limits["partition"].choices == ["batch", "gpu"]


def test_limits_min_max(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\ntransport = local\n\n"
        "[molssi10.limits]\n"
        "overridable = ntasks, mem, time\n"
        "ntasks.min = 1\n"
        "ntasks.max = 6\n"
        "mem.max = 100G\n"
        "time.max = 04:00:00\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert section.limits["ntasks"] == FieldLimits(minimum="1", maximum="6")
    assert section.limits["mem"] == FieldLimits(maximum="100G")
    assert section.limits["time"] == FieldLimits(maximum="04:00:00")


def test_limits_only_covers_fields_in_overridable_list(tmp_path):
    # ntasks.min is present, but ntasks isn't in `overridable` -- ignored.
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\ntransport = local\n\n"
        "[molssi10.limits]\n"
        "overridable = partition\n"
        "ntasks.min = 1\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    assert "ntasks" not in section.limits
    assert list(section.limits.keys()) == ["partition"]


# ---- merge_overrides -----------------------------------------------------


def test_merge_overrides_no_requests_returns_defaults():
    section = SlurmSection(
        name="x", transport="local", host=None, directives={"partition": "batch"}
    )
    assert section.merge_overrides({}) == {"partition": "batch"}


def test_merge_overrides_unauthorized_field_raises():
    section = SlurmSection(
        name="x", transport="local", host=None, directives={"partition": "batch"}
    )
    with pytest.raises(ValueError, match="not an overridable"):
        section.merge_overrides({"account": "myaccount"})


def test_merge_overrides_unconstrained_field_passes_through():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"partition": "batch"},
        limits={"partition": FieldLimits()},
    )
    result = section.merge_overrides({"partition": "gpu"})
    assert result == {"partition": "gpu"}


def test_merge_overrides_choices_valid():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"partition": "batch"},
        limits={"partition": FieldLimits(choices=["batch", "gpu"])},
    )
    assert section.merge_overrides({"partition": "gpu"})["partition"] == "gpu"


def test_merge_overrides_choices_invalid_raises():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"partition": "batch"},
        limits={"partition": FieldLimits(choices=["batch", "gpu"])},
    )
    with pytest.raises(ValueError, match="not one of the allowed choices"):
        section.merge_overrides({"partition": "debug"})


def test_merge_overrides_numeric_within_bounds():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"ntasks": "1"},
        limits={"ntasks": FieldLimits(minimum="1", maximum="6")},
    )
    assert section.merge_overrides({"ntasks": 4})["ntasks"] == 4


def test_merge_overrides_numeric_below_minimum_raises():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"ntasks": "1"},
        limits={"ntasks": FieldLimits(minimum="1", maximum="6")},
    )
    with pytest.raises(ValueError, match="below the minimum"):
        section.merge_overrides({"ntasks": 0})


def test_merge_overrides_numeric_above_maximum_raises():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"ntasks": "1"},
        limits={"ntasks": FieldLimits(minimum="1", maximum="6")},
    )
    with pytest.raises(ValueError, match="exceeds the maximum"):
        section.merge_overrides({"ntasks": 7})


def test_merge_overrides_mem_within_bounds():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"mem": "20G"},
        limits={"mem": FieldLimits(maximum="100G")},
    )
    assert section.merge_overrides({"mem": "40G"})["mem"] == "40G"


def test_merge_overrides_mem_above_maximum_raises():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"mem": "20G"},
        limits={"mem": FieldLimits(maximum="100G")},
    )
    with pytest.raises(ValueError, match="exceeds the maximum"):
        section.merge_overrides({"mem": "200G"})


def test_merge_overrides_mem_mixed_units():
    # 100000M < 100G (102400M) -- should pass.
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"mem": "20G"},
        limits={"mem": FieldLimits(maximum="100G")},
    )
    assert section.merge_overrides({"mem": "100000M"})["mem"] == "100000M"


def test_merge_overrides_mem_per_cpu_within_bounds():
    # Regression test: mem_per_cpu (unlike the bare "mem" directive) used to
    # fall through to a plain float(), which crashes on any unit suffix --
    # found via a real ChemAI job (2553) that resubmitted its own defaults
    # ("1900M") as an explicit override and got "startup error" before a
    # single SLURM script was written.
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"mem_per_cpu": "1900M"},
        limits={"mem_per_cpu": FieldLimits(maximum="4000M")},
    )
    assert section.merge_overrides({"mem_per_cpu": "1900M"})["mem_per_cpu"] == "1900M"


def test_merge_overrides_mem_per_cpu_above_maximum_raises():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"mem_per_cpu": "1900M"},
        limits={"mem_per_cpu": FieldLimits(maximum="4000M")},
    )
    with pytest.raises(ValueError, match="exceeds the maximum"):
        section.merge_overrides({"mem_per_cpu": "8G"})


def test_merge_overrides_time_within_bounds():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"time": "01:00:00"},
        limits={"time": FieldLimits(maximum="04:00:00")},
    )
    assert section.merge_overrides({"time": "02:30:00"})["time"] == "02:30:00"


def test_merge_overrides_time_above_maximum_raises():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"time": "01:00:00"},
        limits={"time": FieldLimits(maximum="04:00:00")},
    )
    with pytest.raises(ValueError, match="exceeds the maximum"):
        section.merge_overrides({"time": "05:00:00"})


def test_merge_overrides_time_with_days():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"time": "01:00:00"},
        limits={"time": FieldLimits(maximum="1-00:00:00")},
    )
    with pytest.raises(ValueError, match="exceeds the maximum"):
        section.merge_overrides({"time": "2-00:00:00"})


def test_merge_overrides_multiple_fields_at_once():
    section = SlurmSection(
        name="x",
        transport="local",
        host=None,
        directives={"partition": "batch", "ntasks": "1", "mem": "20G"},
        limits={
            "partition": FieldLimits(choices=["batch", "gpu"]),
            "ntasks": FieldLimits(minimum="1", maximum="6"),
            "mem": FieldLimits(maximum="100G"),
        },
    )
    result = section.merge_overrides({"ntasks": 3, "mem": "40G"})
    assert result == {"partition": "batch", "ntasks": 3, "mem": "40G"}


def test_merge_overrides_end_to_end_from_ini(tmp_path):
    (tmp_path / "molssi10.ini").write_text(
        "[molssi10]\n"
        "transport = local\n"
        "partition = batch\n"
        "ntasks = 1\n"
        "mem = 20G\n"
        "\n"
        "[molssi10.limits]\n"
        "overridable = ntasks, mem\n"
        "ntasks.min = 1\n"
        "ntasks.max = 6\n"
        "mem.max = 100G\n"
    )
    section = load_slurm_config(tmp_path, "molssi10")
    result = section.merge_overrides({"ntasks": 4, "mem": "40G"})
    assert result == {"partition": "batch", "ntasks": 4, "mem": "40G"}

    with pytest.raises(ValueError):
        section.merge_overrides({"ntasks": 10})
