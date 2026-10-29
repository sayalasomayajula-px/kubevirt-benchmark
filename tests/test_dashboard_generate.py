import importlib.util
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "dashboard" / "generate_dashboard.py"
spec = importlib.util.spec_from_file_location("generate_dashboard", SCRIPT)
generate_dashboard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generate_dashboard)


def make_run(base: Path, px: str, stamp: str, with_results: bool) -> Path:
    run = base / px / "1-disk" / f"{stamp}_fio_benchmark_1-2"
    (run / "per-vm-results").mkdir(parents=True)
    if with_results:
        (run / "summary_fio_benchmark.json").write_text(json.dumps({
            "test_type": "fio_benchmark", "total_vms": 2, "successful": 2, "failed": 0,
            "total_test_duration_sec": 120.0, "config": {"rw": "randread", "bs": "4k"},
            "metrics": [{"metric": "read_iops", "avg": 1000.0, "max": 1100.0, "min": 900.0}],
        }))
        (run / "fio_benchmark_results.json").write_text("[]")
    return run


class GenerateTests(unittest.TestCase):
    def test_builds_a_page_and_skips_runs_without_results(self):
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            make_run(tmp / "results", "px-1.0", stamp, with_results=True)
            make_run(tmp / "results", "px-empty", stamp, with_results=False)
            out = generate_dashboard.generate(base_dir=tmp / "results", output=tmp / "out.html")
            html = out.read_text()
        self.assertIn("px-1.0", html)
        self.assertNotIn("px-empty", html)
        self.assertIn("KubeVirt performance dashboard", html)

    def test_vm_pool_runs_get_their_own_tab(self):
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            run = tmp / "results" / "vm-pool" / f"{stamp}-vm-pool-abc12345"
            run.mkdir(parents=True)
            (run / "results.json").write_text(json.dumps({
                "configuration": {"replicas": 5, "storage_class": "sc-fc",
                                  "datasource_namespace": "images", "datasource_name": "rhel9"},
                "results": [
                    {"requested_strategy": "csi-clone", "status": "succeeded", "all_ready_seconds": 20.0,
                     "all_vm_objects_seconds": 1.0, "all_root_disks_seconds": 5.0, "ready_50_seconds": 15.0},
                    {"requested_strategy": "snapshot", "status": "unsupported", "error": "no snapshot class"},
                ],
            }))
            out = generate_dashboard.generate(base_dir=tmp / "results", output=tmp / "out.html")
            html = out.read_text()
        self.assertIn("VM Pool", html)
        self.assertIn("csi-clone", html)
        self.assertIn("Not supported on this cluster", html)

    def test_days_filter_excludes_older_runs(self):
        old = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d-%H%M%S")
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            make_run(tmp / "results", "px-1.0", old, with_results=True)
            self.assertIsNone(generate_dashboard.generate(base_dir=tmp / "results", days=7, output=tmp / "o.html"))
            self.assertIsNotNone(generate_dashboard.generate(base_dir=tmp / "results", output=tmp / "o.html"))

    def test_missing_results_folder_is_not_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(generate_dashboard.generate(base_dir=Path(tmp) / "nope"))


if __name__ == "__main__":
    unittest.main()
