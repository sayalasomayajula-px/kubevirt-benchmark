import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from virtbench.profile_dashboard import (
    DASHBOARD_FILE,
    RESULT_FILE,
    find_runs,
    load_run,
    main,
    render,
    write_run_dashboard,
)


def summary(profile, workload, iops, bw, read_p99=2.0, write_p99=0.0, runs=3, threads=4):
    row = {
        "profile": profile,
        "description": f"{profile} description",
        "iterations": runs,
        "median_total_iops": iops,
        "median_total_bw_mibps": bw,
        "median_read_iops": iops,
        "median_write_iops": 0,
        "median_read_p99_ms": read_p99,
        "median_write_p99_ms": write_p99,
        "thread_budget": threads,
        "median_iops_per_thread": iops / threads,
    }
    if workload:
        row["workload"] = workload
    return row


def iteration(profile, workload, number, iops, bw):
    row = {"profile": profile, "iteration": number, "total_iops": iops, "total_bw_mibps": bw}
    if workload:
        row["workload"] = workload
    return row


def document(workload="randread-4k", spreads=(0, 0)):
    """baseline 1000 IOPS; combined 1300 (+30%); dedicated 1005 (+0.5%)."""
    low, high = spreads
    rows, its = [], []
    for profile, base in (("baseline", 1000), ("dedicated-iothread", 1005), ("combined", 1300)):
        rows.append(summary(profile, workload, base, base / 250,
                            read_p99={"baseline": 4.0, "dedicated-iothread": 4.0, "combined": 2.0}[profile]))
        for number, offset in enumerate((-low, 0, high), 1):
            its.append(iteration(profile, workload, number, base + offset, (base + offset) / 250))
    return {
        "test_type": "fio_kubevirt_profile_comparison",
        "timestamp": "2026-10-20T12:00:00",
        "storage_class": "sc-fc",
        "node_name": "worker-1",
        "vm_cpus": 4,
        "fio_config": {"rw": "randread", "bs": "4k", "iodepth": 64, "numjobs": 4,
                       "runtime": 20, "ramp_time": 5, "size": "2G"},
        "requested_profiles": ["baseline", "dedicated-iothread", "combined"],
        "failures": [],
        "profiles": rows,
        "iterations": its,
    }


def write_run(base, driver, stamp, doc):
    folder = Path(base) / driver / "profile-comparison" / stamp
    folder.mkdir(parents=True)
    (folder / RESULT_FILE).write_text(json.dumps(doc))
    return folder


class RunViewTests(unittest.TestCase):
    def view(self, doc):
        with tempfile.TemporaryDirectory() as tmp:
            folder = write_run(tmp, "sc-fc", "20261020-120000", doc)
            return load_run(folder / RESULT_FILE)

    def rows(self, doc):
        return {row["profile"]: row for row in self.view(doc)["workloads"][0]["rows"]}

    def test_profiles_keep_the_order_they_were_run_not_a_ranking(self):
        doc = document()
        doc["requested_profiles"] = ["baseline", "combined", "dedicated-iothread"]
        profiles = [row["profile"] for row in self.view(doc)["workloads"][0]["rows"]]
        self.assertEqual(profiles, ["baseline", "combined", "dedicated-iothread"])

    def test_change_is_measured_against_baseline_of_the_same_workload(self):
        rows = self.rows(document())
        self.assertEqual(rows["combined"]["primary_change"], 30.0)
        self.assertEqual(rows["combined"]["p99_change"], -50.0)
        self.assertEqual(rows["combined"]["primary_state"], "better")
        self.assertEqual(rows["combined"]["p99_state"], "better")

    def test_small_change_is_not_called_better_or_worse(self):
        self.assertEqual(self.rows(document())["dedicated-iothread"]["primary_state"], "same")

    def test_change_inside_the_run_to_run_spread_is_not_called_better(self):
        # +30% with a roughly 40% spread between runs is indistinguishable from noise
        rows = self.rows(document(spreads=(400, 400)))
        self.assertEqual(rows["combined"]["primary_state"], "same")
        self.assertGreater(rows["baseline"]["cv"], 20)

    def test_lower_throughput_or_higher_latency_is_worse(self):
        doc = document()
        doc["profiles"][2]["median_total_iops"] = 700
        doc["profiles"][2]["median_read_p99_ms"] = 8.0
        row = self.rows(doc)["combined"]
        self.assertEqual((row["primary_state"], row["p99_state"]), ("worse", "worse"))

    def test_spread_is_computed_from_runs_not_taken_from_the_summary(self):
        rows = self.rows(document(spreads=(100, 100)))
        self.assertGreater(rows["baseline"]["cv"], 5)
        self.assertTrue(any("spread" in note for note in rows["baseline"]["notes"]))

    def test_single_run_has_no_spread_and_is_flagged(self):
        doc = document()
        doc["iterations"] = [item for item in doc["iterations"] if item["iteration"] == 1]
        for row in doc["profiles"]:
            row["iterations"] = 1
        rows = self.rows(doc)
        self.assertIsNone(rows["baseline"]["cv"])
        self.assertIn("single iteration, no spread measured", rows["baseline"]["notes"])

    def test_failed_profile_is_shown_with_its_error(self):
        doc = document()
        doc["profiles"] = [row for row in doc["profiles"] if row["profile"] != "combined"]
        doc["failures"] = [{"profile": "combined", "error": "not applied by KubeVirt\nmore detail"}]
        view = self.view(doc)["workloads"][0]
        failed = next(row for row in view["rows"] if row["profile"] == "combined")
        self.assertEqual((failed["status"], failed["error"]), ("failed", "not applied by KubeVirt"))
        self.assertTrue(any("combined failed" in flag for flag in view["flags"]))

    def test_missing_baseline_is_stated_instead_of_inventing_changes(self):
        doc = document()
        doc["profiles"] = [row for row in doc["profiles"] if row["profile"] != "baseline"]
        view = self.view(doc)["workloads"][0]
        self.assertTrue(any("baseline" in flag for flag in view["flags"]))
        self.assertTrue(all(row.get("primary_change") is None for row in view["rows"] if row["status"] == "ok"))

    def test_unverified_profile_is_flagged_and_applied_values_are_carried(self):
        doc = document()
        doc["applied_configuration"] = {
            "baseline": {"verified": True, "iothreads": 0,
                         "benchmark_disk": {"iothread": None, "queues": None, "cache": "none"}, "notes": []},
            "combined": {"verified": True, "iothreads": 2, "notes": [],
                         "benchmark_disk": {"iothread": 2, "queues": 4, "cache": "none"}},
            "dedicated-iothread": {"verified": False, "reason": "no exec permission"},
        }
        rows = self.rows(doc)
        self.assertEqual(rows["combined"]["applied"]["queues"], 4)
        self.assertIn("not verified against the running VM", rows["dedicated-iothread"]["notes"])

    def test_older_results_without_workload_fields_still_load(self):
        doc = document(workload=None)
        view = self.view(doc)
        self.assertEqual(len(view["workloads"]), 1)
        self.assertEqual(view["workloads"][0]["name"], "randread-4k")
        self.assertEqual(len(view["workloads"][0]["rows"]), 3)

    def test_each_workload_is_compared_only_with_its_own_baseline(self):
        doc = document("randread-4k")
        seq = document("seqread-128k")
        for row in seq["profiles"]:
            row["median_total_bw_mibps"] *= 10
        doc["profiles"] += seq["profiles"]
        doc["iterations"] += seq["iterations"]
        doc["workloads"] = [
            {"name": "randread-4k", "label": "4K randread", "primary": "iops", "rw": "randread", "bs": "4k"},
            {"name": "seqread-128k", "label": "128K seqread", "primary": "bw", "rw": "read", "bs": "128k"},
        ]
        view = self.view(doc)
        self.assertEqual([w["unit"] for w in view["workloads"]], ["IOPS", "MiB/s"])
        for workload in view["workloads"]:
            self.assertEqual(len(workload["rows"]), 3)
            self.assertEqual(workload["rows"][0]["primary_change"] if workload["rows"][0]["profile"] != "baseline" else 0, 0)
        combined = {w["name"]: next(r for r in w["rows"] if r["profile"] == "combined") for w in view["workloads"]}
        self.assertEqual(combined["randread-4k"]["primary_change"], 30.0)
        self.assertEqual(combined["seqread-128k"]["primary_change"], 30.0)


class PageTests(unittest.TestCase):
    def test_page_reports_measurements_without_ranking_or_advice(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = write_run(tmp, "sc-fc", "20261020-120000", document())
            html = render([load_run(folder / RESULT_FILE)])
        for forbidden in ("Best for", "OVERALL RECOMMENDATION", "Recommended", "recommended profile"):
            self.assertNotIn(forbidden, html)
        self.assertIn("does not rank or recommend profiles", html)
        self.assertIn("vs baseline", html)

    def test_data_cannot_close_the_script_tag(self):
        doc = document()
        doc["failures"] = [{"profile": "combined", "error": "</script><b>x</b>"}]
        doc["profiles"] = [row for row in doc["profiles"] if row["profile"] != "combined"]
        with tempfile.TemporaryDirectory() as tmp:
            folder = write_run(tmp, "d", "20261020-120000", doc)
            html = render([load_run(folder / RESULT_FILE)])
        self.assertEqual(html.count("</script>"), html.count("<script"))

    def test_run_folder_gets_a_dashboard_next_to_its_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = write_run(tmp, "sc-fc", "20261020-120000", document())
            output = write_run_dashboard(folder)
            self.assertEqual(output, folder / DASHBOARD_FILE)
            self.assertIn("Storage profile comparison", output.read_text())


class DiscoveryTests(unittest.TestCase):
    def test_runs_are_found_newest_first_and_can_be_filtered_by_age(self):
        now = datetime.now()
        old_stamp = (now - timedelta(days=30)).strftime("%Y%m%d-%H%M%S")
        new_stamp = now.strftime("%Y%m%d-%H%M%S")
        with tempfile.TemporaryDirectory() as tmp:
            write_run(tmp, "a", old_stamp, document())
            write_run(tmp, "b", new_stamp, document())
            self.assertEqual([p.parent.name for p in find_runs(Path(tmp))], [new_stamp, old_stamp])
            self.assertEqual([p.parent.name for p in find_runs(Path(tmp), days=7)], [new_stamp])

    def test_command_writes_the_page_and_reports_when_nothing_is_found(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "page.html"
            self.assertEqual(main(["--base-dir", tmp, "--output-html", str(out)]), 1)
            write_run(tmp, "sc-fc", "20261020-120000", document())
            self.assertEqual(main(["--base-dir", tmp, "--output-html", str(out)]), 0)
            self.assertTrue(out.exists())


if __name__ == "__main__":
    unittest.main()
