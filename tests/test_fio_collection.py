import importlib.util
import logging
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

SCRIPT = Path(__file__).resolve().parent.parent / "io-benchmark" / "fio" / "measure-fio-performance.py"
spec = importlib.util.spec_from_file_location("measure_fio_performance", SCRIPT)
fio = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fio)

LOGGER = logging.getLogger("test_fio_collection")


class FakeClock:
    """Each attempt costs one second; sleeping advances the clock by the delay."""

    def __init__(self):
        self.now = 0.0
        self.lock = threading.Lock()

    def tick(self, seconds=1.0):
        with self.lock:
            self.now += seconds
            return self.now

    def sleep(self, seconds):
        self.tick(seconds)


def run(namespaces, attempt, concurrency, retries, delay=20, clock=None):
    clock = clock or FakeClock()
    done = []
    results = fio.collect_all_fio_results(
        namespaces, attempt, LOGGER, concurrency, retries, delay,
        on_done=lambda ns, data: done.append((ns, data)), sleep=clock.sleep,
    )
    return results, done


class CollectAllTests(unittest.TestCase):
    def test_every_vm_is_tried_before_any_vm_is_retried(self):
        namespaces = [f"ns-{i}" for i in range(1, 11)]
        clock = FakeClock()
        calls = []

        def attempt(ns):
            clock.tick()
            calls.append(ns)
            return None, "No output"

        run(namespaces, attempt, concurrency=5, retries=3, clock=clock)

        self.assertEqual(len(calls), 30)
        for round_no in range(3):
            self.assertEqual(sorted(calls[round_no * 10:(round_no + 1) * 10]), sorted(namespaces))

    def test_late_output_does_not_depend_on_worker_slot(self):
        """10 VMs / 5 slots: FIO output appears at t=100; every VM must get the same chance."""
        namespaces = [f"ns-{i}" for i in range(1, 11)]
        clock = FakeClock()

        def attempt(ns):
            if clock.tick() < 100:
                return None, "No output"
            return {"ns": ns}, ""

        results, done = run(namespaces, attempt, concurrency=5, retries=6, clock=clock)

        self.assertTrue(all(results[ns] for ns in namespaces), results)
        self.assertEqual(sorted(ns for ns, _ in done), sorted(namespaces))

    def test_missing_vm_ip_is_retried_not_fatal(self):
        outcomes = {"ns-1": iter([(None, "VM IP not available yet"), (None, "VM IP not available yet"), ({"ok": 1}, "")])}

        results, _ = run(["ns-1"], lambda ns: next(outcomes[ns]), concurrency=1, retries=5, delay=0)

        self.assertEqual(results["ns-1"], {"ok": 1})

    def test_a_vm_that_never_finishes_gets_exactly_max_retries_attempts(self):
        attempts = {"good": 0, "bad": 0}

        def attempt(ns):
            attempts[ns] += 1
            return ({"ok": 1}, "") if ns == "good" else (None, "No output")

        results, done = run(["bad", "good"], attempt, concurrency=2, retries=4, delay=0)

        self.assertEqual(attempts, {"good": 1, "bad": 4})
        self.assertIsNone(results["bad"])
        self.assertEqual(results["good"], {"ok": 1})
        self.assertEqual(sorted(ns for ns, _ in done), ["bad", "good"])

    def test_sleep_happens_between_rounds_not_inside_a_worker(self):
        clock = FakeClock()
        sleeps = []

        def attempt(ns):
            return None, "No output"

        fio.collect_all_fio_results(
            ["a", "b", "c"], attempt, LOGGER, 1, 3, 20, sleep=lambda s: sleeps.append(s),
        )

        self.assertEqual(sleeps, [20, 20])  # between 3 rounds, none after the last


class SingleAttemptTests(unittest.TestCase):
    CONFIG = {"pod": "p", "pod_ns": "d", "user": "u", "password": "x"}

    def test_no_ip_is_reported_as_not_ready(self):
        with mock.patch.object(fio, "get_vmi_ip", return_value=None):
            data, reason = fio.collect_fio_results_once("ns", "vm", self.CONFIG, "/tmp", LOGGER)
        self.assertIsNone(data)
        self.assertIn("IP", reason)

    def test_empty_output_is_reported_as_not_ready(self):
        with mock.patch.object(fio, "get_vmi_ip", return_value="1.2.3.4"), \
                mock.patch.object(fio, "run_ssh_command", side_effect=["0", ""]):
            data, reason = fio.collect_fio_results_once("ns", "vm", self.CONFIG, "/tmp", LOGGER)
        self.assertIsNone(data)
        self.assertEqual(reason, "No output")

    def test_valid_json_is_returned_and_saved(self):
        import json
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(fio, "get_vmi_ip", return_value="1.2.3.4"), \
                mock.patch.object(fio, "run_ssh_command", side_effect=["0", 'noise {"jobs": []} tail']):
            data, reason = fio.collect_fio_results_once("ns", "vm", self.CONFIG, tmp, LOGGER)
            saved = json.loads((Path(tmp) / "per-vm-results" / "ns" / "fio_raw.json").read_text())
        self.assertEqual(data, {"jobs": [], fio.FIO_EXIT_KEY: 0})
        self.assertEqual(saved, {"jobs": []})

    def test_unreadable_exit_status_is_retried(self):
        with mock.patch.object(fio, "get_vmi_ip", return_value="1.2.3.4"), \
                mock.patch.object(fio, "run_ssh_command", return_value=None):
            data, reason = fio.collect_fio_results_once("ns", "vm", self.CONFIG, "/tmp", LOGGER)
        self.assertIsNone(data)
        self.assertIn("exit status", reason)


class ConfigFromResultsTests(unittest.TestCase):
    def test_uses_the_settings_the_vm_actually_ran(self):
        defaults = {"runtime": 300, "bs": "4k", "rw": "randwrite", "iodepth": 64,
                    "numjobs": 4, "size": "10G", "ramp_time": 15, "rwmixread": 70}
        raw = {"global options": {"runtime": "180", "size": "1G", "rw": "randread", "bs": "8k",
                                  "iodepth": "32", "numjobs": "2", "ramp_time": "5", "rwmixread": "50"}}

        config = fio.fio_config_from_results([raw], defaults)

        self.assertEqual(config, {"runtime": 180, "bs": "8k", "rw": "randread", "iodepth": 32,
                                  "numjobs": 2, "size": "1G", "ramp_time": 5, "rwmixread": 50})

    def test_falls_back_to_requested_settings_without_results(self):
        defaults = {"runtime": 300, "bs": "4k"}
        self.assertEqual(fio.fio_config_from_results([], defaults), defaults)
        self.assertEqual(fio.fio_config_from_results([{"jobs": []}], defaults), defaults)


if __name__ == "__main__":
    unittest.main()


class ProfileIterationErrorTests(unittest.TestCase):
    CONFIG = {"pod": "p", "pod_ns": "d", "user": "u", "password": "x"}

    def _run(self, output):
        args = SimpleNamespace(fio_runtime=1, fio_ramp_time=0, fio_iodepth=1, fio_numjobs=1)
        workload = {"name": "randread-4k", "rw": "randread", "bs": "4k", "rwmixread": None}
        with mock.patch.object(fio, "build_profile_fio_options", return_value=["--name=x"]), \
                mock.patch.object(fio, "ssh_exec_command", return_value=(0, output, "")):
            return fio.run_profile_iteration("1.2.3.4", workload, 1, args, self.CONFIG, LOGGER)

    def test_job_error_fails_the_iteration(self):
        with self.assertRaises(RuntimeError) as raised:
            self._run('{"jobs": [{"error": 5}]}')
        self.assertIn("FIO jobs failed", str(raised.exception))

    def test_clean_jobs_pass(self):
        self.assertEqual(self._run('{"jobs": [{"error": 0}]}'), {"jobs": [{"error": 0}]})
