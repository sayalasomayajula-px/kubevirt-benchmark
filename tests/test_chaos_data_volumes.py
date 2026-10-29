import importlib.util
import logging
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("measure_chaos", ROOT / "chaos-benchmark" / "measure-chaos.py")
measure_chaos = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure_chaos)

TEMPLATE = str(ROOT / "examples" / "vm-templates" / "vm-template.yaml")


def build_vm(**overrides):
    """Run the manifest builder with kubectl stubbed out and return the VirtualMachine it would create."""
    sent = []

    class FakeProcess:
        returncode = 0

        def __init__(self, *args, **kwargs):
            pass

        def communicate(self, input=None):
            sent.append(input)
            return "", ""

    args = SimpleNamespace(datasource_name="rhel9", datasource_namespace="images", vm_memory="2048M",
                           vm_cpu_cores=1, **overrides)
    with mock.patch.object(subprocess, "Popen", FakeProcess):
        ok = measure_chaos.create_vm_with_data_volumes(
            "vm-1", "ns", TEMPLATE, "sc-rwx", 1, "30Gi", args, logging.getLogger("test"), 1)
    assert ok
    return yaml.safe_load(sent[0])


def data_volume(vm):
    return next(dv for dv in vm["spec"]["dataVolumeTemplates"] if dv["metadata"]["name"] == "vm-1-data-1")


class ManifestTests(unittest.TestCase):
    def test_data_volumes_default_to_rwx_and_leave_volume_mode_to_the_storage_class(self):
        storage = data_volume(build_vm())["spec"]["storage"]
        self.assertEqual(storage["accessModes"], ["ReadWriteMany"])
        self.assertNotIn("volumeMode", storage)

    def test_rwo_can_still_be_requested(self):
        storage = data_volume(build_vm(data_volume_access_mode="ReadWriteOnce"))["spec"]["storage"]
        self.assertEqual(storage["accessModes"], ["ReadWriteOnce"])

    def test_block_volume_mode(self):
        storage = data_volume(build_vm(data_volume_access_mode="ReadWriteMany", data_volume_mode="Block"))["spec"]["storage"]
        self.assertEqual(storage["accessModes"], ["ReadWriteMany"])
        self.assertEqual(storage["volumeMode"], "Block")

    def test_root_disk_is_not_touched(self):
        vm = build_vm(data_volume_access_mode="ReadWriteMany", data_volume_mode="Block")
        root = next(dv for dv in vm["spec"]["dataVolumeTemplates"] if dv["metadata"]["name"] == "vm-1-volume")
        self.assertNotIn("accessModes", root["spec"]["storage"])


class ParserTests(unittest.TestCase):
    def parse(self, *extra):
        argv = ["measure-chaos.py", "--storage-class", "sc", "--concurrency", "2", *extra]
        with mock.patch.object(sys, "argv", argv):
            return measure_chaos.parse_args()

    def test_defaults(self):
        args = self.parse()
        self.assertEqual(args.data_volume_access_mode, "ReadWriteMany")
        self.assertIsNone(args.data_volume_mode)

    def test_short_and_long_forms_are_accepted_in_any_case(self):
        self.assertEqual(self.parse("--data-volume-access-mode", "rwo").data_volume_access_mode, "ReadWriteOnce")
        self.assertEqual(self.parse("--data-volume-access-mode", "ReadWriteMany").data_volume_access_mode, "ReadWriteMany")
        self.assertEqual(self.parse("--data-volume-mode", "block").data_volume_mode, "Block")

    def test_unknown_values_are_rejected(self):
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            self.parse("--data-volume-access-mode", "ROX")


class CliPassthroughTests(unittest.TestCase):
    def run_cli(self, *extra):
        from click.testing import CliRunner
        from virtbench.cli import cli

        captured = {}

        def fake_run(cmd, **kwargs):
            captured["cmd"] = cmd
            return SimpleNamespace(returncode=0)

        with mock.patch("virtbench.commands.chaos.subprocess.run", fake_run):
            result = CliRunner().invoke(cli, ["chaos-benchmark", "--storage-class", "sc", "--concurrency", "2", *extra])
        return result, captured.get("cmd", [])

    def test_options_reach_the_script(self):
        result, cmd = self.run_cli("--data-volume-access-mode", "RWO", "--data-volume-mode", "Block")
        self.assertIn("--data-volume-access-mode", cmd, result.output)
        self.assertEqual(cmd[cmd.index("--data-volume-access-mode") + 1], "RWO")
        self.assertEqual(cmd[cmd.index("--data-volume-mode") + 1], "Block")

    def test_default_run_uses_rwx_and_omits_volume_mode(self):
        result, cmd = self.run_cli()
        self.assertEqual(cmd[cmd.index("--data-volume-access-mode") + 1], "RWX", result.output)
        self.assertNotIn("--data-volume-mode", cmd)


if __name__ == "__main__":
    unittest.main()
