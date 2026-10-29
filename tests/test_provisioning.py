import time
import unittest

from click.testing import CliRunner

from virtbench.cli import cli
from virtbench.provisioning import (
    CLONE_TYPE_ANNOTATION,
    ROOT_DISK_LABEL,
    RUN_LABEL,
    aggregate_results,
    build_clone_vm,
    build_vm_pool,
    find_root_volume,
    monitor_pool,
    parse_duration,
    percentile,
    strategy_preflight,
)


def example_vm():
    return {
        "apiVersion": "kubevirt.io/v1",
        "kind": "VirtualMachine",
        "metadata": {"name": "original", "resourceVersion": "123"},
        "status": {"printableStatus": "Stopped"},
        "spec": {
            "running": False,
            "dataVolumeTemplates": [
                {
                    "metadata": {"name": "old-root"},
                    "spec": {
                        "sourceRef": {"kind": "DataSource", "name": "old"},
                        "storage": {
                            "resources": {"requests": {"storage": "20Gi"}},
                            "storageClassName": "old-class",
                        },
                    },
                },
                {
                    "metadata": {"name": "data-disk"},
                    "spec": {"source": {"blank": {}}, "storage": {}},
                },
            ],
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {
                    "domain": {
                        "devices": {
                            "disks": [
                                {"name": "rootdisk", "bootOrder": 1},
                                {"name": "datadisk"},
                            ]
                        }
                    },
                    "volumes": [
                        {"name": "rootdisk", "dataVolume": {"name": "old-root"}},
                        {"name": "datadisk", "dataVolume": {"name": "data-disk"}},
                    ],
                },
            },
        },
    }


def source_pvc():
    return {
        "metadata": {
            "name": "golden",
            "namespace": "images",
            "annotations": {"cdi.kubevirt.io/storage.contentType": "kubevirt"},
        },
        "spec": {
            "storageClassName": "fast",
            "accessModes": ["ReadWriteOnce"],
            "volumeMode": "Block",
            "resources": {"requests": {"storage": "10Gi"}},
        },
        "status": {"phase": "Bound"},
    }


class ManifestTests(unittest.TestCase):
    def test_find_root_volume_from_boot_order(self):
        self.assertEqual(find_root_volume(example_vm(), None), "rootdisk")

    def test_build_clone_vm_replaces_only_root_template(self):
        vm = build_clone_vm(
            example_vm(),
            name="bench-vm",
            namespace="bench",
            root_volume="rootdisk",
            source_namespace="images",
            source_name="golden",
            source_pvc=source_pvc(),
            strategy="csi-clone",
            run_id="run-1",
            benchmark="cdi-root-disk",
            data_volume_name="new-root",
        )
        self.assertEqual(vm["metadata"]["name"], "bench-vm")
        self.assertNotIn("resourceVersion", vm["metadata"])
        self.assertNotIn("status", vm)
        self.assertNotIn("running", vm["spec"])
        self.assertEqual(vm["spec"]["runStrategy"], "Always")
        root = next(
            volume
            for volume in vm["spec"]["template"]["spec"]["volumes"]
            if volume["name"] == "rootdisk"
        )
        self.assertEqual(root["dataVolume"]["name"], "new-root")
        templates = vm["spec"]["dataVolumeTemplates"]
        self.assertEqual({item["metadata"]["name"] for item in templates}, {"new-root", "data-disk"})
        generated = next(item for item in templates if item["metadata"]["name"] == "new-root")
        self.assertEqual(generated["metadata"]["annotations"][CLONE_TYPE_ANNOTATION], "csi-clone")
        self.assertEqual(generated["metadata"]["labels"][ROOT_DISK_LABEL], "true")
        self.assertEqual(generated["spec"]["source"]["pvc"], {"namespace": "images", "name": "golden"})
        self.assertEqual(generated["spec"]["storage"]["storageClassName"], "fast")
        self.assertEqual(
            generated["spec"]["storage"]["resources"]["requests"]["storage"],
            "10Gi",
        )

    def test_build_pool_wraps_vm_and_starts_at_requested_size(self):
        vm = build_clone_vm(
            example_vm(),
            name="pool",
            namespace="bench",
            root_volume="rootdisk",
            source_namespace="images",
            source_name="golden",
            source_pvc=source_pvc(),
            strategy="copy",
            run_id="pool-run",
            benchmark="vm-pool",
        )
        pool = build_vm_pool(
            vm, name="my-pool", namespace="bench", replicas=0, run_id="pool-run"
        )
        self.assertEqual(pool["kind"], "VirtualMachinePool")
        self.assertEqual(pool["spec"]["replicas"], 0)
        labels = pool["spec"]["virtualMachineTemplate"]["metadata"]["labels"]
        self.assertEqual(labels["kubevirt.io/vmpool"], "my-pool")
        self.assertEqual(labels[RUN_LABEL], "pool-run")


class FakePreflightClient:
    def get(self, resource, name, namespace=None):
        if resource == "storageclass":
            return {"provisioner": "example.csi.io"}
        return None

    def list(self, resource, namespace=None, labels=None):
        if resource == "volumesnapshotclass":
            return [{"metadata": {"name": "snap"}, "driver": "example.csi.io"}]
        return []

    def can_i(self, verb, resource, namespace):
        return True


class PreflightTests(unittest.TestCase):
    def test_cross_storage_class_disables_optimized_clones_only(self):
        result = strategy_preflight(
            FakePreflightClient(),
            ["csi-clone", "snapshot", "copy"],
            "images",
            "bench",
            source_pvc(),
            "other",
        )
        self.assertFalse(result["csi-clone"]["supported"])
        self.assertFalse(result["snapshot"]["supported"])
        self.assertTrue(result["copy"]["supported"])


class FakePoolClient:
    def list(self, resource, namespace=None, labels=None):
        if resource == "vm":
            return [{"metadata": {"name": "vm-0"}}, {"metadata": {"name": "vm-1"}}]
        if resource == "vmi":
            return [
                {"status": {"conditions": [{"type": "Ready", "status": "True"}]}},
                {"status": {"conditions": [{"type": "Ready", "status": "True"}]}},
            ]
        if resource in {"datavolume", "pvc"}:
            return [
                {
                    "metadata": {"annotations": {CLONE_TYPE_ANNOTATION: "snapshot"}},
                    "status": {"phase": "Succeeded"},
                },
                {
                    "metadata": {"annotations": {CLONE_TYPE_ANNOTATION: "snapshot"}},
                    "status": {"phase": "Succeeded"},
                },
            ]
        return []


class MonitoringTests(unittest.TestCase):
    def test_pool_monitor_records_thresholds_and_observed_strategy(self):
        result = monitor_pool(
            FakePoolClient(),
            namespace="bench",
            run_id="run",
            replicas=2,
            requested_strategy="snapshot",
            ready_when="vmi-ready",
            timeout=1,
            poll_interval=0.01,
            started=time.monotonic(),
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["observed_strategies"], {"snapshot": 2})
        self.assertIsNotNone(result["ready_50_seconds"])
        self.assertIsNotNone(result["all_ready_seconds"])


class UtilityTests(unittest.TestCase):
    def test_duration_and_percentile(self):
        self.assertEqual(parse_duration("4h"), 14400)
        self.assertEqual(parse_duration("2.5m"), 150)
        self.assertEqual(percentile([1, 2, 3, 4], 0.5), 2.5)

    def test_aggregate_excludes_fallback_timings(self):
        summary = aggregate_results(
            [
                {
                    "requested_strategy": "snapshot",
                    "status": "succeeded",
                    "total_seconds": 5,
                },
                {
                    "requested_strategy": "snapshot",
                    "status": "fallback",
                    "total_seconds": 100,
                },
            ],
            "total_seconds",
        )
        self.assertEqual(summary["snapshot"]["p50_seconds"], 5)
        self.assertEqual(summary["snapshot"]["fallback"], 1)

    def test_commands_are_registered(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("cdi-root-disk", result.output)
        self.assertIn("vm-pool", result.output)


if __name__ == "__main__":
    unittest.main()


class GoldenReferenceTests(unittest.TestCase):
    def test_defaults_to_datasource(self):
        from virtbench.provisioning import golden_reference

        self.assertEqual(
            golden_reference(None, "pvc", "rhel9", "openshift-virtualization-os-images"),
            ("openshift-virtualization-os-images/rhel9", "datasource"),
        )

    def test_golden_image_overrides_datasource(self):
        from virtbench.provisioning import golden_reference

        self.assertEqual(
            golden_reference("images/golden", "pvc", "rhel9", "ns"),
            ("images/golden", "pvc"),
        )


class ResolveInputPathTests(unittest.TestCase):
    def test_falls_back_to_repo_root(self):
        import tempfile
        from pathlib import Path
        from virtbench.provisioning import BenchmarkError, resolve_input_path

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "vm.yaml").write_text("x")
            self.assertEqual(resolve_input_path(Path("vm.yaml"), root), root / "vm.yaml")
            with self.assertRaises(BenchmarkError):
                resolve_input_path(Path("missing.yaml"), root)
