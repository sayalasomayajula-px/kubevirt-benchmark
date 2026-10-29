import unittest

import yaml
from click.testing import CliRunner

from virtbench.cli import cli
from virtbench.fio_profiles import (
    LABEL_BASELINE,
    LABEL_LATENCY,
    LABEL_THROUGHPUT,
    configure_manifest,
    custom_workload,
    parse_domain_xml,
    parse_iteration,
    parse_profiles,
    parse_workloads,
    rank_profiles,
    recommend,
    resolve_workload,
    summarize_profiles,
    verify_profile,
)


TEMPLATE = """
apiVersion: kubevirt.io/v1
kind: VirtualMachine
metadata:
  name: fio
spec:
  template:
    metadata:
      labels: {app: fio}
    spec:
      domain:
        cpu: {cores: 2}
        resources:
          requests: {cpu: "1", memory: 2Gi}
        devices:
          disks:
            - name: rootdisk
              disk: {bus: virtio}
            - name: scratchdisk
              disk: {bus: virtio}
      volumes: []
"""


class ManifestProfileTests(unittest.TestCase):
    def configured(self, profile):
        return yaml.safe_load(
            configure_manifest(TEMPLATE, profile, 4, "worker-a", 3)
        )

    def test_combined_profile_enables_features_and_equal_cpu_resources(self):
        vm = self.configured("combined")
        spec = vm["spec"]["template"]["spec"]
        domain = spec["domain"]
        self.assertEqual(domain["cpu"], {"cores": 4})
        self.assertEqual(domain["resources"]["requests"]["cpu"], "4")
        self.assertEqual(domain["resources"]["limits"]["cpu"], "4")
        self.assertEqual(domain["ioThreadsPolicy"], "shared")
        self.assertTrue(domain["devices"]["blockMultiQueue"])
        scratch = next(d for d in domain["devices"]["disks"] if d["name"] == "scratchdisk")
        self.assertTrue(scratch["dedicatedIOThread"])
        self.assertEqual(scratch["serial"], "virtbenchfio")
        self.assertEqual(spec["nodeSelector"]["kubernetes.io/hostname"], "worker-a")

    def test_supplemental_profile_sets_requested_pool_size(self):
        domain = self.configured("iothreads-supplemental")["spec"]["template"]["spec"]["domain"]
        self.assertEqual(domain["ioThreadsPolicy"], "supplementalPool")
        self.assertEqual(domain["ioThreads"]["supplementalPoolThreadCount"], 3)

    def test_cache_modes_are_not_profiles(self):
        with self.assertRaises(ValueError):
            parse_profiles("baseline,cache-none")

    def test_profile_validation_rejects_unknown_and_duplicates(self):
        with self.assertRaises(ValueError):
            parse_profiles("baseline,nope")
        with self.assertRaises(ValueError):
            parse_profiles("baseline,baseline")

    def test_scale_actions_still_require_explicit_namespace_range(self):
        result = CliRunner().invoke(
            cli, ["fio", "--action", "run-all", "--storage-class", "fast"]
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("--start and --end are required", result.output)


class ResultTests(unittest.TestCase):
    def raw(self, read_iops=100, write_iops=50, read_p99=2_000_000, write_p99=3_000_000):
        return {
            "jobs": [{
                "read": {
                    "iops": read_iops,
                    "bw_bytes": 1048576,
                    "clat_ns": {"percentile": {
                        "95.000000": 1_000_000,
                        "99.000000": read_p99,
                        "99.900000": 4_000_000,
                    }},
                },
                "write": {
                    "iops": write_iops,
                    "bw_bytes": 524288,
                    "clat_ns": {"percentile": {
                        "95.000000": 2_000_000,
                        "99.000000": write_p99,
                        "99.900000": 5_000_000,
                    }},
                },
            }]
        }

    def test_parser_extracts_tail_latency_and_throughput(self):
        row = parse_iteration(self.raw(), "baseline", 1)
        self.assertEqual(row["total_iops"], 150)
        self.assertEqual(row["total_bw_mibps"], 1.5)
        self.assertEqual(row["read_p99_ms"], 2)
        self.assertEqual(row["write_p999_ms"], 5)

    def test_summary_uses_medians_and_baseline_delta(self):
        rows = [
            parse_iteration(self.raw(100, 50), "baseline", 1),
            parse_iteration(self.raw(120, 60), "baseline", 2),
            parse_iteration(self.raw(150, 75, 1_000_000, 2_000_000), "combined", 1),
            parse_iteration(self.raw(170, 85, 1_000_000, 2_000_000), "combined", 2),
        ]
        summary = summarize_profiles(rows, 4, 4)
        combined = next(item for item in summary if item["profile"] == "combined")
        self.assertEqual(combined["median_total_iops"], 240)
        self.assertEqual(combined["total_iops_change_pct"], 45.45)
        self.assertEqual(combined["read_p99_ms_improvement_pct"], 50)
        self.assertEqual(combined["min_total_iops"], 225)
        self.assertEqual(combined["max_total_iops"], 255)
        self.assertGreater(combined["total_iops_cv_pct"], 0)


def fake_raw(iops, bw_mib, p99_ms, direction="read"):
    stats = {
        "iops": iops,
        "bw_bytes": bw_mib * 1024 * 1024,
        "clat_ns": {"percentile": {"99.000000": p99_ms * 1_000_000}},
    }
    return {"jobs": [{direction: stats}]}


class WorkloadTests(unittest.TestCase):
    def test_named_workloads_validate(self):
        self.assertEqual(
            parse_workloads("randread-4k, seqread-128k"), ["randread-4k", "seqread-128k"]
        )
        with self.assertRaises(ValueError):
            parse_workloads("randread-4k,bogus")
        with self.assertRaises(ValueError):
            parse_workloads("randread-4k,randread-4k")

    def test_primary_metric_follows_block_size(self):
        self.assertEqual(resolve_workload("randread-4k")["primary"], "iops")
        self.assertEqual(resolve_workload("seqwrite-128k")["primary"], "bw")
        self.assertEqual(custom_workload("randwrite", "4k", 70)["primary"], "iops")
        self.assertEqual(custom_workload("read", "1m", 70)["primary"], "bw")

    def test_summary_baseline_is_per_workload(self):
        rows = [
            parse_iteration(fake_raw(1000, 4, 2), "baseline", 1, "randread-4k"),
            parse_iteration(fake_raw(2000, 8, 1), "combined", 1, "randread-4k"),
            parse_iteration(fake_raw(100, 400, 5), "baseline", 1, "seqread-128k"),
            parse_iteration(fake_raw(100, 600, 5), "combined", 1, "seqread-128k"),
        ]
        summary = summarize_profiles(rows, 4, 4)
        by_key = {(r["workload"], r["profile"]): r for r in summary}
        self.assertEqual(by_key[("randread-4k", "combined")]["total_iops_change_pct"], 100)
        self.assertEqual(by_key[("seqread-128k", "combined")]["total_bw_mibps_change_pct"], 50)
        self.assertEqual(by_key[("seqread-128k", "baseline")]["total_iops_change_pct"], 0)


class RecommendationTests(unittest.TestCase):
    def summaries(self, spec):
        rows = []
        for workload, profiles in spec.items():
            for profile, (iops, bw, p99) in profiles.items():
                rows.append(parse_iteration(fake_raw(iops, bw, p99), profile, 1, workload))
        return summarize_profiles(rows, 4, 4)

    def workloads(self, *names):
        return {name: resolve_workload(name) for name in names}

    def test_rank_orders_by_primary_metric(self):
        summaries = self.summaries({"randread-4k": {
            "baseline": (1000, 4, 4), "combined": (3000, 12, 2), "iothreads-auto": (2000, 8, 3),
        }})
        ranked = rank_profiles(summaries, resolve_workload("randread-4k"))
        self.assertEqual([r["profile"] for r in ranked], ["combined", "iothreads-auto", "baseline"])
        self.assertEqual([r["rank"] for r in ranked], [1, 2, 3])

    def test_sequential_workload_is_ranked_by_bandwidth_not_iops(self):
        summaries = self.summaries({"seqread-128k": {
            "baseline": (900, 100, 4), "block-multiqueue": (100, 500, 4),
        }})
        ranked = rank_profiles(summaries, resolve_workload("seqread-128k"))
        self.assertEqual(ranked[0]["profile"], "block-multiqueue")

    def test_near_tie_is_broken_by_tail_latency(self):
        summaries = self.summaries({"randread-4k": {
            "baseline": (1000, 4, 9), "combined": (995, 4, 2),
        }})
        ranked = rank_profiles(summaries, resolve_workload("randread-4k"))
        self.assertEqual(ranked[0]["profile"], "combined")

    def test_overall_recommendation_and_labels(self):
        summaries = self.summaries({
            "randread-4k": {"baseline": (1000, 4, 8), "combined": (3000, 12, 2),
                            "iothreads-auto": (1500, 6, 4)},
            "seqread-128k": {"baseline": (500, 200, 8), "combined": (900, 700, 2),
                             "iothreads-auto": (600, 300, 4)},
        })
        result = recommend(summaries, self.workloads("randread-4k", "seqread-128k"))
        overall = result["overall"]
        self.assertEqual([r["profile"] for r in overall][0], "combined")
        self.assertEqual(overall[0]["rank"], 1)
        self.assertIn(LABEL_LATENCY, overall[0]["best_for"])
        self.assertIn(LABEL_THROUGHPUT, overall[0]["best_for"])
        baseline = next(r for r in overall if r["profile"] == "baseline")
        self.assertEqual(baseline["best_for"], LABEL_BASELINE)
        self.assertEqual(set(result["per_workload"]), {"randread-4k", "seqread-128k"})

    def test_profile_missing_a_workload_is_not_ranked(self):
        summaries = self.summaries({
            "randread-4k": {"baseline": (1000, 4, 8), "combined": (3000, 12, 2)},
            "seqread-128k": {"baseline": (500, 200, 8)},
        })
        result = recommend(summaries, self.workloads("randread-4k", "seqread-128k"))
        combined = next(r for r in result["overall"] if r["profile"] == "combined")
        self.assertIsNone(combined["rank"])
        self.assertEqual(result["overall"][0]["profile"], "baseline")


def domain_xml(iothreads=2, root_iothread=1, bench_iothread=2, queues=4, cache="none", vcpus=4):
    def attrs(iothread, q, c):
        text = f"name='qemu' type='raw' cache='{c}'"
        if iothread is not None:
            text += f" iothread='{iothread}'"
        if q is not None:
            text += f" queues='{q}'"
        return text

    iothread_tag = f"<iothreads>{iothreads}</iothreads>" if iothreads else ""
    return f"""<domain type='kvm'>
  <vcpu placement='static'>{vcpus}</vcpu>
  {iothread_tag}
  <devices>
    <disk type='block' device='disk'>
      <driver {attrs(root_iothread, queues if bench_iothread else None, 'none')}/>
      <target dev='vda' bus='virtio'/>
    </disk>
    <disk type='block' device='disk'>
      <driver {attrs(bench_iothread, queues, cache)}/>
      <target dev='vdb' bus='virtio'/>
      <serial>virtbenchfio</serial>
    </disk>
  </devices>
</domain>"""


class AppliedConfigurationTests(unittest.TestCase):
    def test_domain_xml_is_parsed_into_effective_settings(self):
        observed = parse_domain_xml(domain_xml())
        self.assertEqual(observed["vcpus"], 4)
        self.assertEqual(observed["iothreads"], 2)
        disk = observed["benchmark_disk"]
        self.assertEqual((disk["iothread"], disk["queues"], disk["cache"]), (2, 4, "none"))

    def test_matching_domain_passes_for_combined(self):
        self.assertEqual(verify_profile("combined", parse_domain_xml(domain_xml()), 4, 4), [])

    def test_multiqueue_ignored_by_kubevirt_is_reported(self):
        observed = parse_domain_xml(domain_xml(queues=None))
        problems = verify_profile("block-multiqueue", observed, 4, 4)
        self.assertEqual(len(problems), 1)
        self.assertIn("queues=None", problems[0])
        self.assertIn("explicit CPU", problems[0])

    def test_dedicated_iothread_must_not_share_with_another_disk(self):
        observed = parse_domain_xml(domain_xml(root_iothread=2, bench_iothread=2))
        problems = verify_profile("dedicated-iothread", observed, 4, 4)
        self.assertTrue(any("shares IOThread" in item for item in problems))

    def test_iothread_profiles_need_iothreads_and_auto_respects_pool_limit(self):
        none = parse_domain_xml(domain_xml(iothreads=0, root_iothread=None, bench_iothread=None, queues=None))
        self.assertIn("no IOThreads were created", verify_profile("iothreads-shared", none, 4, 4))
        many = parse_domain_xml(domain_xml(iothreads=9))
        self.assertTrue(verify_profile("iothreads-auto", many, 4, 4))
        self.assertEqual(verify_profile("iothreads-auto", parse_domain_xml(domain_xml(iothreads=8)), 4, 4), [])

    def test_supplemental_pool_needs_the_requested_thread_count(self):
        observed = parse_domain_xml(domain_xml(iothreads=2))
        self.assertTrue(verify_profile("iothreads-supplemental", observed, 4, 4))
        self.assertEqual(verify_profile("iothreads-supplemental", observed, 4, 2), [])

    def test_baseline_must_not_have_multiqueue(self):
        self.assertTrue(verify_profile("baseline", parse_domain_xml(domain_xml()), 4, 4))

    def test_vcpu_mismatch_is_reported(self):
        self.assertTrue(verify_profile("combined", parse_domain_xml(domain_xml(vcpus=2)), 4, 4))

    def test_missing_benchmark_disk_is_reported(self):
        xml = domain_xml().replace("<serial>virtbenchfio</serial>", "")
        self.assertIn("not in the running domain", verify_profile("combined", parse_domain_xml(xml), 4, 4)[0])


class KubeVirtRestrictionTests(unittest.TestCase):
    def scsi_template(self, bus):
        return TEMPLATE.replace(
            "- name: scratchdisk\n              disk: {bus: virtio}",
            f"- name: scratchdisk\n              disk: {{bus: {bus}}}",
        )

    def test_dedicated_iothread_and_multiqueue_are_refused_on_scsi(self):
        for profile in ("dedicated-iothread", "block-multiqueue", "combined"):
            with self.assertRaises(ValueError) as caught:
                configure_manifest(self.scsi_template("scsi"), profile, 4)
            self.assertIn("SCSI", str(caught.exception))

    def test_shared_policy_does_not_need_virtio(self):
        configure_manifest(self.scsi_template("scsi"), "iothreads-shared", 4)

    def test_lun_style_benchmark_disk_is_refused_for_dedicated_threads(self):
        lun = TEMPLATE.replace("- name: scratchdisk\n              disk: {bus: virtio}",
                               "- name: scratchdisk\n              lun: {bus: scsi}")
        with self.assertRaises(ValueError):
            configure_manifest(lun, "dedicated-iothread", 4)


class ThreadBudgetTests(unittest.TestCase):
    def rows(self):
        raw = fake_raw(1000, 4, 2)
        return [parse_iteration(raw, name, 1, "randread-4k") for name in ("baseline", "iothreads-auto")]

    def test_budget_uses_observed_iothreads(self):
        summary = summarize_profiles(self.rows(), 4, 4, {"baseline": 0, "iothreads-auto": 8})
        budgets = {row["profile"]: row["thread_budget"] for row in summary}
        self.assertEqual(budgets, {"baseline": 4, "iothreads-auto": 12})

    def test_budget_falls_back_when_the_domain_was_not_inspected(self):
        summary = summarize_profiles(self.rows(), 4, 4)
        self.assertEqual({row["thread_budget"] for row in summary}, {4})


if __name__ == "__main__":
    unittest.main()
