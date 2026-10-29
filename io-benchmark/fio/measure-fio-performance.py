#!/usr/bin/env python3
"""
KubeVirt FIO Benchmark - Storage I/O Performance Test

This script runs FIO benchmarks across multiple VMs to measure storage performance.
Results include IOPS, bandwidth, and latency metrics aggregated across all VMs.
SSH access uses password authentication via an existing SSH helper pod.

Supports multiple actions:
  deploy         - Deploy VMs with FIO pre-configured (FIO runs automatically on boot)
  status         - Check VM and FIO status
  gather-results - Collect FIO results from VMs
  cleanup        - Delete VMs and namespaces
  run-all        - Full workflow: deploy, wait for FIO, gather results

Usage:
    # Full workflow (deploy + wait + gather)
    python3 measure-fio-performance.py --action run-all --start 1 --end 10 --storage-class px-csi

    # Step-by-step workflow
    python3 measure-fio-performance.py --action deploy --start 1 --end 10 --storage-class px-csi
    python3 measure-fio-performance.py --action status --start 1 --end 10
    python3 measure-fio-performance.py --action gather-results --start 1 --end 10 --storage-driver portworx-3.6
    python3 measure-fio-performance.py --action cleanup --start 1 --end 10

Author: KubeVirt Benchmark Suite Contributors
License: Apache 2.0
"""

import argparse
import base64
import csv
import os
import sys
import signal
import subprocess
import json
import shlex
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, List, Optional, Dict, Iterable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from utils.common import (
    setup_logging, run_kubectl_command, create_namespace, create_namespaces_parallel,
    delete_namespace, cleanup_test_namespaces, confirm_cleanup,
    print_cleanup_summary, get_vm_disk_count, get_vmi_ip, get_pvc_status,
    ssh_exec_command,
)
from utils.vm_placement import (
    DEFAULT_EXCLUDE_LABEL, get_vm_template_info, get_eligible_nodes, plan_balanced_placement,
    count_per_node, get_pod_requests_by_node, check_capacity, get_storage_class,
    storage_topology_warning, set_vm_cpus, pin_vm_to_node, get_deployed_state, verify_placement,
)
from virtbench.fio_profiles import (
    BENCHMARK_DISK_SERIAL, PROFILE_ORDER, WORKLOADS, configure_manifest,
    custom_workload, parse_domain_xml, parse_iteration, parse_profiles,
    parse_workloads, rank_profiles, recommend, resolve_workload, summarize_profiles,
    verify_profile,
)

# Defaults
DEFAULT_VM_NAME = 'fio-vm'
DEFAULT_VM_TEMPLATE = '../../examples/vm-templates/fio-vm-template.yaml'
DEFAULT_NAMESPACE_PREFIX = 'fio-benchmark'
DEFAULT_CONCURRENCY = 20
DEFAULT_FIO_RUNTIME = 300
DEFAULT_FIO_BS = '4k'
DEFAULT_FIO_RW = 'randwrite'
DEFAULT_FIO_IODEPTH = 64
DEFAULT_FIO_NUMJOBS = 4
DEFAULT_FIO_SIZE = '10G'
DEFAULT_PLACEMENT_TIMEOUT = 600
DEFAULT_VM_CPUS = 4
DEFAULT_PROFILE_ITERATIONS = 3


def parse_args():
    parser = argparse.ArgumentParser(
        description='Run FIO benchmarks across multiple KubeVirt VMs.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Actions:
  deploy         Deploy VMs with FIO pre-configured (FIO runs automatically on boot)
  status         Check VM and FIO status across namespaces
  gather-results Collect FIO results from VMs (requires --ssh-pod)
  cleanup        Delete VMs and namespaces
  run-all        Full workflow: deploy, wait for FIO, gather results (requires --ssh-pod)

Examples:
  # Full workflow (deploy + wait + gather)
  %(prog)s --action run-all --start 1 --end 10 --storage-class px-csi --save-results

  # Step-by-step workflow
  %(prog)s --action deploy --start 1 --end 10 --storage-class px-csi
  %(prog)s --action status --start 1 --end 10
  %(prog)s --action gather-results --start 1 --end 10 --storage-driver portworx-3.6
  %(prog)s --action cleanup --start 1 --end 10

  # VMs are spread evenly over the eligible nodes by default (see --help for
  # how nodes are chosen). Keep FIO off specific nodes by labeling them:
  #   kubectl label node worker-3 virtbench.io/no-fio=true
  # or list them with --exclude-nodes:
  %(prog)s --action run-all --start 1 --end 10 --storage-class px-csi \\
      --exclude-nodes worker-3,worker-4

  # Custom FIO parameters (for deploy or run-all)
  %(prog)s --action deploy --start 1 --end 50 --storage-class px-csi \\
      --fio-runtime 600 --fio-rw randrw --fio-bs 8k
        """
    )

    # Action
    parser.add_argument('--action', '-a', type=str, default='run-all',
                        choices=['deploy', 'status', 'gather-results', 'cleanup', 'run-all',
                                 'compare-profiles'],
                        help='Action to perform (default: run-all)')

    # Required for most actions
    parser.add_argument('-s', '--start', type=int, help='Start namespace index')
    parser.add_argument('-e', '--end', type=int, help='End namespace index')
    parser.add_argument('--storage-class', type=str, help='Storage class name (required for deploy/run-all)')

    # VM config
    parser.add_argument('--vm-name', type=str, default=DEFAULT_VM_NAME)
    parser.add_argument('--vm-template', type=str, default=DEFAULT_VM_TEMPLATE)
    parser.add_argument('--namespace-prefix', type=str, default=DEFAULT_NAMESPACE_PREFIX)
    parser.add_argument('--concurrency', type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument('--vm-cpus', type=positive_int, default=None,
                        help=f'Guest vCPUs per FIO VM (sets cores, 1 socket/1 thread, and the CPU '
                             f'request). Default: {DEFAULT_VM_CPUS}. Templates that use spec.instancetype '
                             f'keep their own size unless --vm-cpus is given, which is then rejected.')
    parser.add_argument('--vm-cpu-request', type=str, default=None,
                        help='CPU request per VM (default: equal to the vCPU count), '
                             'e.g. "1" to reproduce the older 2-vCPU/1-CPU-request setup')

    # Placement
    parser.add_argument('--node-selector', type=str, default='',
                        help='Label selector (key or key=value, comma-separated) choosing candidate '
                             'nodes. Default: nodes with node-role.kubernetes.io/worker, else all '
                             'non-control-plane nodes, else all nodes.')
    parser.add_argument('--exclude-nodes', type=str, default='',
                        help='Comma-separated node names to keep FIO VMs off. NotReady, cordoned and '
                             'KubeVirt-unschedulable nodes, and nodes with a NoSchedule/NoExecute '
                             'taint the VM template does not tolerate, are skipped automatically.')
    parser.add_argument('--exclude-node-label', type=str, default=DEFAULT_EXCLUDE_LABEL,
                        help='Node label (key or key=value) marking nodes FIO VMs must not use '
                             f'(default: {DEFAULT_EXCLUDE_LABEL}, any value). Empty string disables.')
    parser.add_argument('--no-balance', action='store_true',
                        help='Do not pin VMs to nodes; let the scheduler place them')
    parser.add_argument('--skip-capacity-check', action='store_true',
                        help='Do not refuse to deploy when planned VMs exceed a node\'s free CPU/memory')
    parser.add_argument('--placement-timeout', type=int, default=DEFAULT_PLACEMENT_TIMEOUT,
                        help='Seconds to wait for all VMIs to be scheduled before checking placement')

    # FIO config
    parser.add_argument('--fio-runtime', type=int, default=DEFAULT_FIO_RUNTIME, help='FIO runtime (seconds)')
    parser.add_argument('--fio-timeout', type=positive_int, default=None,
                        help='Completion timeout in seconds (default: fio-runtime + 600)')
    parser.add_argument('--fio-bs', type=str, default=DEFAULT_FIO_BS, help='Block size')
    parser.add_argument('--fio-rw', type=str, default=DEFAULT_FIO_RW,
                        choices=['read', 'write', 'randread', 'randwrite', 'randrw', 'rw'])
    parser.add_argument('--fio-iodepth', type=int, default=DEFAULT_FIO_IODEPTH)
    parser.add_argument('--fio-numjobs', type=int, default=DEFAULT_FIO_NUMJOBS)
    parser.add_argument('--fio-size', type=str, default=DEFAULT_FIO_SIZE, help='Test file size')
    parser.add_argument('--fio-ramp-time', type=int, default=15,
                        help='Warm-up time excluded from each measured FIO iteration')
    parser.add_argument('--fio-rwmixread', type=int, default=70,
                        help='Read percentage for randrw/rw workloads')

    # KubeVirt storage profile comparison
    parser.add_argument('--profiles', default=','.join(PROFILE_ORDER),
                        help='Comma-separated KubeVirt storage profiles')
    parser.add_argument('--profile-iterations', type=int, default=DEFAULT_PROFILE_ITERATIONS,
                        help='Measured FIO repetitions per profile')
    parser.add_argument('--node-name', help='Pin all profile VMs to one Kubernetes node')
    parser.add_argument('--supplemental-pool-threads', type=int, default=4,
                        help='IOThread count for the supplemental-pool profile')
    parser.add_argument('--benchmark-disk', default='scratchdisk',
                        help='VM disk to tune and benchmark')
    parser.add_argument('--workloads',
                        help='Comma-separated I/O patterns to run against every profile '
                             f'({", ".join(WORKLOADS)}). Defaults to the single pattern '
                             'given by --fio-rw/--fio-bs')
    parser.add_argument('--recommend', action='store_true',
                        help='Emit ranked per-workload tables and an overall recommendation')
    parser.add_argument('--keep-profile-resources', action='store_true',
                        help='Keep the comparison namespace and VMs after the run')

    # Output
    parser.add_argument('--results-dir', type=str, default='results', help='Base results directory')
    parser.add_argument('--storage-driver', type=str, default='Not-Specified',
                        help='Storage driver for results folder (default: Not-Specified)')
    parser.add_argument('--disks-per-vm', type=str, default='auto',
                        help='Disks per VM for results folder name (default: auto-detect from first VM, fallback: 1-disk)')
    parser.add_argument('--save-results', action='store_true', help='Save results to JSON/CSV')
    parser.add_argument('--cleanup', action='store_true', help='Delete VMs after test (for run-all action)')

    # Collection settings
    parser.add_argument('--collect-retries', type=int, default=8, help='Max retries for collecting results')
    parser.add_argument('--collect-retry-delay', type=int, default=20, help='Delay (seconds) between retries')
    parser.add_argument('--collect-concurrency', type=int, default=5, help='Max concurrent result collections')

    # SSH settings (password-based via existing pod)
    parser.add_argument('--ssh-pod', default='ssh-test-pod', help='SSH helper pod name (must have sshpass installed)')
    parser.add_argument('--ssh-pod-ns', default='default', help='SSH helper pod namespace')
    parser.add_argument('--vm-user', default='cloud-user', help='VM SSH user')
    parser.add_argument('--vm-password', default='changeme', help='VM SSH password')

    # Logging
    parser.add_argument('--log-file', type=str, help='Log file path')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])

    args = parser.parse_args()

    # Validate required args based on action
    if args.action in ['deploy', 'run-all', 'compare-profiles'] and not args.storage_class:
        parser.error(f"--storage-class is required for action '{args.action}'")
    if args.action != 'compare-profiles' and (args.start is None or args.end is None):
        parser.error('--start and --end are required for this action')
    if args.action == 'compare-profiles':
        args.start = args.start or 1
        args.end = args.end or 1
    if args.profile_iterations < 1:
        parser.error('--profile-iterations must be at least 1')
    if args.action == 'compare-profiles' and args.vm_cpus is None:
        args.vm_cpus = DEFAULT_VM_CPUS
    if not 0 <= args.fio_rwmixread <= 100:
        parser.error('--fio-rwmixread must be between 0 and 100')
    try:
        args.profile_list = parse_profiles(args.profiles)
        if args.workloads:
            args.workload_list = [
                resolve_workload(name, args.fio_rwmixread)
                for name in parse_workloads(args.workloads)
            ]
        else:
            args.workload_list = [
                custom_workload(args.fio_rw, args.fio_bs, args.fio_rwmixread)
            ]
    except ValueError as exc:
        parser.error(str(exc))

    return args


def positive_int(value: str) -> int:
    """argparse type: integer >= 1."""
    try:
        n = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid integer: {value!r}")
    if n < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return n


def prepare_vm_yaml(template_path: str, vm_name: str, storage_class: str,
                    fio_config: Dict, vm_password: str, logger) -> str:
    """Prepare VM YAML with substituted values. Returns YAML content."""
    with open(template_path) as f:
        content = f.read()

    replacements = {
        '{{VM_NAME}}': vm_name,
        '{{STORAGE_CLASS_NAME}}': storage_class,
        '{{VM_PASSWORD}}': vm_password,
        '{{FIO_RUNTIME}}': str(fio_config['runtime']),
        '{{FIO_BS}}': fio_config['bs'],
        '{{FIO_RW}}': fio_config['rw'],
        '{{FIO_IODEPTH}}': str(fio_config['iodepth']),
        '{{FIO_NUMJOBS}}': str(fio_config['numjobs']),
        '{{FIO_SIZE}}': fio_config['size'],
        '{{FIO_RAMP_TIME}}': str(fio_config.get('ramp_time', 0)),
        '{{FIO_RWMIXREAD}}': str(fio_config.get('rwmixread', 70)),
    }

    for placeholder, value in replacements.items():
        content = content.replace(placeholder, value)

    return content


def deploy_vm(namespace: str, vm_yaml: str, logger) -> bool:
    """Deploy VM in namespace. Returns success status."""
    try:
        proc = subprocess.run(
            ['kubectl', 'apply', '-f', '-', '-n', namespace],
            input=vm_yaml, capture_output=True, text=True, check=True
        )
        logger.debug(f"[{namespace}] VM deployed")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"[{namespace}] Failed to deploy VM: {e.stderr}")
        return False


def wait_for_vm_running(namespace: str, vm_name: str, timeout: int, logger) -> bool:
    """Wait for VM to reach Running state."""
    start = time.time()
    while time.time() - start < timeout:
        vm_status, vmi_phase = get_vm_and_vmi_status(namespace, vm_name)
        if vmi_phase == 'Running':
            return True
        time.sleep(5)
    return False


def get_vm_and_vmi_status(namespace: str, vm_name: str) -> Tuple[str, str]:
    """Get VM and VMI status. Returns (vm_status, vmi_phase)."""
    vm_status = "Unknown"
    vmi_phase = "NotFound"
    try:
        result = subprocess.run([
            'kubectl', 'get', 'vm', '-n', namespace, vm_name,
            '-o', 'jsonpath={.status.printableStatus}'
        ], capture_output=True, text=True)
        vm_status = result.stdout.strip() or "Unknown"
    except subprocess.CalledProcessError:
        pass
    try:
        result = subprocess.run([
            'kubectl', 'get', 'vmi', '-n', namespace, vm_name,
            '-o', 'jsonpath={.status.phase}'
        ], capture_output=True, text=True)
        vmi_phase = result.stdout.strip() or "NotFound"
    except subprocess.CalledProcessError:
        pass
    return vm_status, vmi_phase


def run_ssh_command(vm_ip: str, command: str, ssh_pod: str, ssh_pod_ns: str,
                    vm_user: str, vm_password: str, timeout: int = 60) -> Optional[str]:
    """Run SSH command on VM via the shared helper. Returns stdout, or None on exec failure."""
    try:
        returncode, stdout, _ = ssh_exec_command(
            vm_ip, command, ssh_pod, ssh_pod_ns, vm_user, vm_password,
            timeout=timeout
        )
        return stdout if returncode == 0 else None
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return None


def wait_for_fio_complete(namespace: str, vm_name: str, ssh_config: Dict,
                          timeout: int, logger) -> bool:
    """Wait for FIO to complete by checking for completion marker."""
    start = time.time()

    # First wait for VM to be Running
    last_status = ("Unknown", "NotFound")
    while time.time() - start < min(300, timeout):
        vm_status, vmi_phase = get_vm_and_vmi_status(namespace, vm_name)
        last_status = (vm_status, vmi_phase)
        if vmi_phase == "Running":
            break
        if vmi_phase == "Failed":
            pvc_status = get_pvc_status(namespace, logger)
            logger.error(f"[{namespace}] VM failed to start. VM={vm_status}, VMI={vmi_phase}, PVCs={pvc_status}")
            return False
        time.sleep(min(15, max(0, timeout - (time.time() - start))))
    else:
        vm_status, vmi_phase = last_status
        pvc_status = get_pvc_status(namespace, logger)
        logger.warning(f"[{namespace}] VM did not reach Running within the startup wait. VM={vm_status}, VMI={vmi_phase}, PVCs={pvc_status}")
        return False

    # Wait for VM to get an IP
    vm_ip = None
    while time.time() - start < min(360, timeout) and not vm_ip:
        vm_ip = get_vmi_ip(vm_name, namespace, logger)
        if not vm_ip:
            time.sleep(min(10, max(0, timeout - (time.time() - start))))

    if not vm_ip:
        logger.warning(f"[{namespace}] VM running but no IP within the startup wait")
        return False

    logger.info(f"[{namespace}] VM running with IP {vm_ip}, waiting for FIO...")

    # Wait for FIO to complete
    while time.time() - start < timeout:
        output = run_ssh_command(
            vm_ip, 'cat /tmp/fio_complete 2>/dev/null',
            ssh_config['pod'], ssh_config['pod_ns'],
            ssh_config['user'], ssh_config['password'],
            timeout=max(1, min(90, timeout - (time.time() - start)))
        )
        if output and 'completed' in output:
            return True
        time.sleep(min(30, max(0, timeout - (time.time() - start))))

    logger.warning(f"[{namespace}] FIO did not complete within timeout")
    return False


def wait_for_vm_ssh(namespace: str, vm_name: str, ssh_config: Dict,
                    timeout: int, logger) -> Optional[str]:
    """Wait for a restarted VM to be running and accept SSH connections."""
    started = time.time()
    while time.time() - started < timeout:
        _, phase = get_vm_and_vmi_status(namespace, vm_name)
        if phase == 'Running':
            vm_ip = get_vmi_ip(vm_name, namespace, logger)
            if vm_ip:
                output = run_ssh_command(
                    vm_ip, 'echo ready', ssh_config['pod'], ssh_config['pod_ns'],
                    ssh_config['user'], ssh_config['password'], timeout=30,
                )
                if output and 'ready' in output:
                    return vm_ip
        time.sleep(10)
    return None


def extract_json_object(text: str) -> Optional[str]:
    """Extract complete JSON object from text with potential noise before/after."""
    start = text.find('{')
    if start == -1:
        return None

    # Find matching closing brace
    depth = 0
    for i, char in enumerate(text[start:], start):
        if char == '{':
            depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0:
                return text[start:i+1]
    return None


FIO_EXIT_MISSING = "VIRTbench_EXIT_CODE_MISSING"


def collect_fio_results_once(namespace: str, vm_name: str, ssh_config: Dict,
                             output_dir: str, logger) -> Tuple[Optional[Dict], str]:
    """Make one attempt to read a VM's FIO results and its guest-recorded exit status.

    Returns (results, "") on success, or (None, reason) when the VM is not ready yet.
    A missing VM IP counts as "not ready", not as a permanent failure: a VMI can be
    Running before its interface address is reported. The exit status is stored in
    the results under FIO_EXIT_KEY (None when the template did not record it).
    """
    vm_ip = get_vmi_ip(vm_name, namespace, logger)
    if not vm_ip:
        return None, "VM IP not available yet"

    try:
        exit_out = run_ssh_command(
            vm_ip, f'if [ -e /tmp/fio_exit_code ]; then cat /tmp/fio_exit_code; else echo {FIO_EXIT_MISSING}; fi',
            ssh_config['pod'], ssh_config['pod_ns'],
            ssh_config['user'], ssh_config['password'],
            timeout=60
        )
        exit_code = parse_exit_code(exit_out)
        if exit_out is None or (exit_code is None and exit_out.strip() != FIO_EXIT_MISSING):
            return None, "Failed to read FIO exit status"

        output = run_ssh_command(
            vm_ip, 'cat /tmp/fio_results.json',
            ssh_config['pod'], ssh_config['pod_ns'],
            ssh_config['user'], ssh_config['password'],
            timeout=120
        )
        if not output:
            if exit_code not in (None, 0):
                return {FIO_EXIT_KEY: exit_code, "jobs": []}, ""
            return None, "No output"

        json_content = extract_json_object(output)
        if not json_content:
            if exit_code not in (None, 0):
                return {FIO_EXIT_KEY: exit_code, "jobs": []}, ""
            return None, "No valid JSON"

        results = json.loads(json_content)
        vm_results_dir = os.path.join(output_dir, "per-vm-results", namespace)
        os.makedirs(vm_results_dir, exist_ok=True)
        with open(os.path.join(vm_results_dir, "fio_raw.json"), 'w') as f:
            f.write(json_content)
        results[FIO_EXIT_KEY] = exit_code
        return results, ""
    except Exception as e:
        return None, str(e)


def collect_all_fio_results(namespaces: List[str], attempt, logger, concurrency: int,
                            max_retries: int, retry_delay: int,
                            on_done=None, sleep=time.sleep) -> Dict[str, Optional[Dict]]:
    """Collect results from every VM, giving each VM the same number of attempts.

    Collection runs in rounds. Within a round each pending VM is tried once (at most
    `concurrency` at a time); between rounds the whole batch waits `retry_delay`.
    A worker slot is never held across a sleep, so a VM's chance of success depends
    on when its FIO run finishes, not on its position in the namespace list.

    attempt(namespace) -> (results or None, reason)
    on_done(namespace, results or None) is called once per VM, from the calling thread,
    as soon as its outcome is final.
    Returns {namespace: results or None}.
    """
    max_retries = max(1, max_retries)
    outcomes: Dict[str, Optional[Dict]] = {}
    pending = list(namespaces)

    for round_no in range(1, max_retries + 1):
        if not pending:
            break
        reasons = {}
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
            futures = {executor.submit(attempt, ns): ns for ns in pending}
            for future in as_completed(futures):
                ns = futures[future]
                try:
                    data, reason = future.result()
                except Exception as e:
                    data, reason = None, str(e)
                if data is not None:
                    outcomes[ns] = data
                    if on_done:
                        on_done(ns, data)
                else:
                    reasons[ns] = reason
        pending = [ns for ns in pending if ns not in outcomes]
        for ns in pending:
            logger.warning(f"[{ns}] Retry {round_no}/{max_retries}: {reasons.get(ns, 'No output')}")
        if pending and round_no < max_retries:
            sleep(retry_delay)

    for ns in pending:
        logger.error(f"[{ns}] Failed to collect results after {max_retries} attempts")
        outcomes[ns] = None
        if on_done:
            on_done(ns, None)
    return outcomes


def fio_config_from_results(raw_results: List[Dict], requested: Dict) -> Dict:
    """The FIO settings the VMs really ran, read from their own output.

    gather-results is run separately from deploy, so the CLI values may differ from
    what was deployed. Falls back to the requested settings when no VM reported any.
    """
    ints = ('runtime', 'iodepth', 'numjobs', 'ramp_time', 'rwmixread')
    for raw in raw_results:
        reported = raw.get("global options") if isinstance(raw, dict) else None
        if not reported:
            continue
        config = dict(requested)
        for key in requested:
            if key not in reported:
                continue
            value = str(reported[key])
            if key in ints:
                digits = ''.join(ch for ch in value if ch.isdigit())
                if not digits:
                    continue
                config[key] = int(digits)
            else:
                config[key] = value
        return config
    return dict(requested)


FIO_EXIT_KEY = "_virtbench_fio_exit_code"


def parse_exit_code(text: Optional[str]) -> Optional[int]:
    """Exit code written by the guest to /tmp/fio_exit_code; None when absent or unreadable."""
    try:
        return int((text or "").strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None


def fio_job_errors(raw_data: Dict) -> Tuple[List[Dict], Optional[str]]:
    """Return every nonzero or invalid raw FIO job error."""
    jobs = raw_data.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        return [], "FIO result has no jobs"

    errors = []
    for index, job in enumerate(jobs):
        if not isinstance(job, dict):
            errors.append({"job": index, "error": None})
            continue
        error = job.get("error")
        if isinstance(error, bool) or not isinstance(error, int) or error < 0:
            errors.append({"job": index, "error": error})
        elif error != 0:
            errors.append({"job": index, "error": error})
    return errors, None


def failed_fio_result(namespace: str, error: str, exit_code: Optional[int] = None,
                      fio_errors: Optional[List[Dict]] = None) -> Dict:
    return {"namespace": namespace, "exit_code": exit_code,
            "fio_errors": fio_errors or [], "error": error, "success": False}


def unverified_fio_result(namespace: str, error: str) -> Dict:
    return {"namespace": namespace, "exit_code": None, "fio_errors": [],
            "error": error, "success": False, "unverified": True}


def parse_fio_results(namespace: str, raw_data: Dict,
                      exit_code: Optional[int] = None) -> Dict:
    """Validate command and per-job status before returning performance metrics."""
    if not isinstance(raw_data, dict):
        return failed_fio_result(namespace, "FIO result is not a JSON object", exit_code)
    if exit_code is None:
        exit_code = raw_data.get(FIO_EXIT_KEY)
    fio_errors, job_error = fio_job_errors(raw_data)
    if isinstance(exit_code, int) and not isinstance(exit_code, bool) and exit_code != 0:
        reasons = [f"FIO command exited with status {exit_code}"]
        if job_error:
            reasons.append(job_error)
        if fio_errors:
            reasons.append("One or more FIO jobs failed or had invalid error fields")
        return failed_fio_result(namespace, "; ".join(reasons), exit_code, fio_errors)
    if job_error:
        return failed_fio_result(namespace, job_error, exit_code, fio_errors)
    if fio_errors:
        return failed_fio_result(namespace, "One or more FIO jobs failed", exit_code, fio_errors)
    if isinstance(exit_code, bool) or not isinstance(exit_code, int) or not 0 <= exit_code <= 255:
        return unverified_fio_result(namespace, "FIO command exit status is unavailable; result cannot be verified")
    ri = wi = rb = wb = rl = wl = 0
    for job in raw_data["jobs"]:
        r, w = job.get("read", {}), job.get("write", {})
        ri += r.get("iops", 0)
        wi += w.get("iops", 0)
        rb += r.get("bw_bytes", 0)
        wb += w.get("bw_bytes", 0)
        if r.get("lat_ns"):
            rl = r["lat_ns"].get("mean", 0)
        if w.get("lat_ns"):
            wl = w["lat_ns"].get("mean", 0)

    return {
        "namespace": namespace,
        "read_iops": round(ri, 2),
        "write_iops": round(wi, 2),
        "read_bw_mibps": round(rb / 1024 / 1024, 2),
        "write_bw_mibps": round(wb / 1024 / 1024, 2),
        "read_lat_ms": round(rl / 1e6, 3),
        "write_lat_ms": round(wl / 1e6, 3),
        "exit_code": exit_code,
        "fio_errors": [],
        "success": True,
    }


def aggregate_results(results: List[Dict], fio_config: Dict,
                      total_duration: float) -> Dict:
    """Aggregate results from all VMs into summary."""
    successful = [r for r in results if r.get("success")]
    unverified = [r for r in results if r.get("unverified")]
    failed = [r for r in results if not r.get("success") and not r.get("unverified")]

    def calc_stats(key):
        values = [r[key] for r in successful if r.get(key, 0) > 0]
        if not values:
            return {"avg": 0, "max": 0, "min": 0}
        return {
            "avg": round(sum(values) / len(values), 2),
            "max": round(max(values), 2),
            "min": round(min(values), 2)
        }

    return {
        "test_type": "fio_benchmark",
        "timestamp": datetime.now().isoformat(),
        "total_vms": len(results),
        "successful": len(successful),
        "failed": len(failed),
        "unverified": len(unverified),
        "failures": [{"namespace": r.get("namespace"), "exit_code": r.get("exit_code"),
                      "fio_errors": r.get("fio_errors", []),
                      "error": r.get("error") or "no results collected"}
                     for r in failed],
        "unverified_results": [{"namespace": r.get("namespace"), "error": r.get("error")}
                                for r in unverified],
        "total_test_duration_sec": round(total_duration, 2),
        "config": fio_config,
        "metrics": [
            {"metric": "read_iops", **calc_stats("read_iops")},
            {"metric": "write_iops", **calc_stats("write_iops")},
            {"metric": "read_bw_mibps", **calc_stats("read_bw_mibps")},
            {"metric": "write_bw_mibps", **calc_stats("write_bw_mibps")},
            {"metric": "read_lat_ms", **calc_stats("read_lat_ms")},
            {"metric": "write_lat_ms", **calc_stats("write_lat_ms")},
        ]
    }


def attach_placement(summary: Dict, all_results: List[Dict], placement: Optional[Dict]):
    """Record the VM-to-node map in the summary and on each per-VM result."""
    if not placement:
        return
    summary['placement'] = placement
    node_map = placement.get('vm_node_map', {})
    for r in all_results:
        r['node'] = node_map.get(r.get('namespace'))


def save_results_to_files(output_dir: str, summary: Dict, all_results: List[Dict], logger):
    """Save results to JSON and CSV files."""
    # Save summary
    summary_path = os.path.join(output_dir, "summary_fio_benchmark.json")
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    # Save all results
    results_path = os.path.join(output_dir, "fio_benchmark_results.json")
    with open(results_path, 'w') as f:
        json.dump(all_results, f, indent=2)

    # Save CSV
    csv_path = os.path.join(output_dir, "fio_benchmark_results.csv")
    with open(csv_path, 'w') as f:
        # 'node' is appended last so positional readers of the older columns keep working.
        headers = ["namespace", "read_iops", "write_iops", "read_bw_mibps",
                   "write_bw_mibps", "read_lat_ms", "write_lat_ms", "success", "node"]
        f.write(",".join(headers) + "\n")
        for r in all_results:
            f.write(",".join("" if r.get(h) is None else str(r.get(h)) for h in headers) + "\n")

    # Save VM-to-node placement
    if summary.get('placement'):
        placement_path = os.path.join(output_dir, "vm_node_placement.json")
        with open(placement_path, 'w') as f:
            json.dump(summary['placement'], f, indent=2)

    logger.info(f"Results saved to {output_dir}")


def print_results_table(summary: Dict):
    """Print results summary table."""
    print("\n" + "=" * 60)
    print("FIO BENCHMARK RESULTS")
    print("=" * 60)
    print(f"Total VMs: {summary['total_vms']} | Successful: {summary['successful']} | Failed: {summary['failed']}")
    print(f"Test Duration: {summary['total_test_duration_sec']:.1f}s")
    print(f"\nFIO Config: {summary['config']['rw']} | bs={summary['config']['bs']} | "
          f"iodepth={summary['config']['iodepth']} | numjobs={summary['config']['numjobs']}")
    print("-" * 60)
    print(f"{'Metric':<20} {'Avg':>12} {'Max':>12} {'Min':>12}")
    print("-" * 60)

    for m in summary['metrics']:
        metric_name = m['metric'].replace('_', ' ').title()
        print(f"{metric_name:<20} {m['avg']:>12,.2f} {m['max']:>12,.2f} {m['min']:>12,.2f}")
    print("=" * 60 + "\n")


def _kubectl(args: List[str], input_text: Optional[str] = None, check: bool = True):
    """Run kubectl while inheriting the caller's KUBECONFIG."""
    return subprocess.run(
        ['kubectl', *args], input=input_text, capture_output=True, text=True, check=check
    )


def create_profile_helper(namespace: str, pod_name: str, logger) -> None:
    """Create the short-lived SSH helper used for profile comparisons."""
    helper = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': pod_name,
            'namespace': namespace,
            'labels': {'virtbench.io/benchmark': 'fio-profile-comparison'},
        },
        'spec': {
            'containers': [{
                'name': 'ssh-client',
                'image': 'alpine:3.20',
                'command': ['/bin/sh', '-c'],
                'args': ['apk add --no-cache openssh-client sshpass && tail -f /dev/null'],
                'resources': {
                    'requests': {'cpu': '50m', 'memory': '64Mi'},
                    'limits': {'cpu': '200m', 'memory': '256Mi'},
                },
            }],
            'restartPolicy': 'Always',
        },
    }
    import yaml
    _kubectl(['apply', '-f', '-'], yaml.safe_dump(helper, sort_keys=False))
    try:
        _kubectl([
            'wait', '--for=condition=Ready', f'pod/{pod_name}', '-n', namespace,
            '--timeout=180s',
        ])
    except subprocess.CalledProcessError as exc:
        logs = _kubectl(['logs', pod_name, '-n', namespace], check=False).stdout
        logger.error('SSH helper did not become ready: %s', logs or exc.stderr)
        raise


def inspect_applied_profile(namespace: str, vm_name: str, profile: str, args, logger) -> Dict:
    """Read the running domain and confirm KubeVirt applied the profile.

    Raises RuntimeError on a mismatch so a profile that silently did nothing is
    reported as failed instead of being measured as if it were tuned. If the
    domain cannot be read at all (for example no pods/exec permission) the
    profile is measured but marked unverified.
    """
    try:
        pods = _kubectl([
            'get', 'pods', '-n', namespace, '-l', 'kubevirt.io=virt-launcher',
            '--field-selector=status.phase=Running', '-o', 'name',
        ]).stdout.split()
        if not pods:
            raise RuntimeError('no running virt-launcher pod')
        domain = _kubectl([
            'exec', '-n', namespace, pods[0], '-c', 'compute', '--',
            'sh', '-c', 'virsh dumpxml "$(virsh list --name | head -1)"',
        ]).stdout
        observed = parse_domain_xml(domain)
    except Exception as exc:  # inspection is best effort; see docstring
        logger.warning('[%s] could not inspect the running domain: %s', profile, exc)
        return {'verified': False, 'reason': str(exc)}
    problems = verify_profile(
        profile, observed, args.vm_cpus, args.supplemental_pool_threads
    )
    if problems:
        raise RuntimeError(
            f'profile {profile!r} was not applied by KubeVirt: ' + '; '.join(problems)
        )
    disk = observed['benchmark_disk']
    notes = []
    if observed['iothreads'] and disk['iothread'] is None:
        notes.append(
            f'{observed["iothreads"]} IOThreads exist but the benchmark disk is not assigned '
            'to any of them, so its I/O is not served by an IOThread'
        )
    return {
        'verified': True,
        'vcpus': observed['vcpus'],
        'iothreads': observed['iothreads'],
        'benchmark_disk': disk,
        'notes': notes,
    }


def build_profile_fio_options(workload: Dict, args, output_path: str) -> List[str]:
    """FIO options for one measured iteration of a named workload."""
    return [
        '--name=virtbench-profile',
        '--directory=/scratch',
        f'--rw={workload["rw"]}',
        f'--rwmixread={workload.get("rwmixread", args.fio_rwmixread)}',
        f'--bs={workload["bs"]}',
        '--direct=1',
        '--ioengine=libaio',
        f'--iodepth={args.fio_iodepth}',
        f'--numjobs={args.fio_numjobs}',
        '--group_reporting=1',
        '--time_based=1',
        f'--runtime={args.fio_runtime}',
        f'--ramp_time={args.fio_ramp_time}',
        f'--size={args.fio_size}',
        '--output-format=json',
        f'--output={output_path}',
    ]


def run_profile_iteration(vm_ip: str, workload: Dict, iteration: int, args,
                          ssh_config: Dict, logger) -> Dict:
    """Run and collect one measured FIO iteration in an already-warmed VM."""
    output_path = f'/var/tmp/fio_profile_{workload["name"]}_{iteration}.json'
    fio_options = build_profile_fio_options(workload, args, output_path)
    fio_command = ' '.join(shlex.quote(option) for option in fio_options)
    command = (
        f"DEVICE=$(readlink -f /dev/disk/by-id/virtio-{BENCHMARK_DISK_SERIAL} 2>/dev/null); "
        "if [ -z \"$DEVICE\" ]; then DEVICE=/dev/$(lsblk -dnbo NAME,SIZE | while read name size; do "
        "if [ \"$size\" -gt 40000000000 ]; then echo \"$name\"; break; fi; done); fi; "
        "test -b \"$DEVICE\" && sudo mkdir -p /scratch && "
        "(mountpoint -q /scratch || sudo mount \"$DEVICE\" /scratch) && "
        f"sudo fio {fio_command} && "
        f"cat {shlex.quote(output_path)}"
    )
    encoded_command = base64.b64encode(command.encode()).decode()
    remote_command = f'echo {encoded_command} | base64 -d | sh'
    returncode, output, error = ssh_exec_command(
        vm_ip, remote_command, ssh_config['pod'], ssh_config['pod_ns'],
        ssh_config['user'], ssh_config['password'], logger=logger,
        timeout=args.fio_runtime + args.fio_ramp_time + 180,
    )
    if returncode != 0:
        detail = (error or output or 'no remote error text').strip()
        raise RuntimeError(f'FIO command failed (exit {returncode}): {detail[-1000:]}')
    if not output:
        raise RuntimeError('FIO produced no output')
    json_content = extract_json_object(output)
    if not json_content:
        logger.debug('Unparsed FIO output: %s', output[-1000:])
        raise RuntimeError('FIO output did not contain valid JSON')
    data = json.loads(json_content)
    job_errors, job_error = fio_job_errors(data)
    if job_error or job_errors:
        raise RuntimeError(job_error or f'One or more FIO jobs failed: {job_errors}')
    return data


def save_profile_comparison(output_dir: str, metadata: Dict, iterations: List[Dict],
                            summaries: List[Dict],
                            recommendation: Optional[Dict] = None) -> None:
    """Persist machine-readable and spreadsheet-friendly comparison results."""
    os.makedirs(output_dir, exist_ok=True)
    document = {**metadata, 'profiles': summaries, 'iterations': iterations}
    if recommendation is not None:
        document['recommendation'] = recommendation
    with open(os.path.join(output_dir, 'fio_profile_comparison.json'), 'w') as handle:
        json.dump(document, handle, indent=2)
    if recommendation is not None:
        with open(os.path.join(output_dir, 'fio_profile_recommendation.csv'), 'w',
                  newline='') as handle:
            workloads = list(recommendation['workloads'])
            fields = ['rank', 'profile', 'throughput_score', 'latency_score',
                      'efficiency_score', 'best_for'] + [
                          f'{name}_{metric}' for name in workloads
                          for metric in ('primary_value', 'worst_p99_ms')]
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for row in recommendation['overall']:
                flat = {key: row.get(key) for key in fields[:6]}
                for name in workloads:
                    values = row['values'].get(name, {})
                    flat[f'{name}_primary_value'] = values.get('primary_value')
                    flat[f'{name}_worst_p99_ms'] = values.get('worst_p99_ms')
                writer.writerow(flat)
    if iterations:
        with open(os.path.join(output_dir, 'fio_profile_iterations.csv'), 'w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(iterations[0]))
            writer.writeheader()
            writer.writerows(iterations)
    if summaries:
        with open(os.path.join(output_dir, 'fio_profile_summary.csv'), 'w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(summaries[0]))
            writer.writeheader()
            writer.writerows(summaries)


def print_profile_comparison(summaries: List[Dict]) -> None:
    """Print the profile comparison, one table per workload."""
    workloads = []
    for row in summaries:
        name = row.get('workload', 'default')
        if name not in workloads:
            workloads.append(name)
    for workload in workloads:
        print('\n' + '=' * 112)
        print(f'KUBEVIRT FIO STORAGE PROFILE COMPARISON - {workload}')
        print('=' * 112)
        print(f"{'Profile':<26} {'Median IOPS':>14} {'vs baseline':>12} "
              f"{'CV':>8} {'MiB/s':>10} {'Read p99':>12} {'Write p99':>12} {'IOPS/thread':>14}")
        print('-' * 112)
        for row in (r for r in summaries if r.get('workload', 'default') == workload):
            delta = row.get('total_iops_change_pct')
            delta_text = 'n/a' if delta is None else f'{delta:+.1f}%'
            print(
                f"{row['profile']:<26} {row['median_total_iops']:>14,.0f} {delta_text:>12} "
                f"{row['total_iops_cv_pct']:>7.1f}% {row['median_total_bw_mibps']:>10,.1f} "
                f"{row['median_read_p99_ms']:>11,.3f}ms "
                f"{row['median_write_p99_ms']:>11,.3f}ms "
                f"{row['median_iops_per_thread']:>14,.0f}"
            )
        print('=' * 112)
    print()


def print_recommendation(recommendation: Dict) -> None:
    """Print ranked per-workload tables and the overall recommendation."""
    for name, ranked in recommendation['per_workload'].items():
        spec = recommendation['workloads'][name]
        unit = 'MiB/s' if spec.get('primary') == 'bw' else 'IOPS'
        print('\n' + '=' * 100)
        print(f"RANKING - {spec.get('label', name)} ({name}), ranked by {unit}")
        print('=' * 100)
        print(f"{'Rank':<5} {'Profile':<26} {'IOPS':>12} {'MiB/s':>10} "
              f"{'p99 (ms)':>10} {'vs baseline':>12}")
        print('-' * 100)
        for row in ranked:
            key = 'total_bw_mibps_change_pct' if spec.get('primary') == 'bw' \
                else 'total_iops_change_pct'
            delta = row.get(key)
            delta_text = 'n/a' if delta is None else f'{delta:+.1f}%'
            print(f"{row['rank']:<5} {row['profile']:<26} {row['median_total_iops']:>12,.0f} "
                  f"{row['median_total_bw_mibps']:>10,.1f} "
                  f"{max(row['median_read_p99_ms'], row['median_write_p99_ms']):>10,.3f} "
                  f"{delta_text:>12}")
    overall = recommendation['overall']
    names = list(recommendation['workloads'])
    width = 28 + 14 * len(names) + 40
    print('\n' + '=' * width)
    print('OVERALL RECOMMENDATION')
    print('=' * width)
    header = f"{'Rank':<5} {'Profile':<26} {'Score':>7} "
    header += ' '.join(f'{name:>13}' for name in names) + f" {'Worst p99':>10}  Best for"
    print(header)
    print('-' * width)
    for row in overall:
        cells = []
        worst = 0.0
        for name in names:
            values = row['values'].get(name)
            if values is None:
                cells.append(f"{'-':>13}")
                continue
            unit = 'MiB/s' if values['primary_metric'].endswith('bw_mibps') else 'IOPS'
            cells.append(f"{values['primary_value']:>8,.0f} {unit:<4}")
            worst = max(worst, values['worst_p99_ms'])
        rank = row['rank'] if row['rank'] is not None else '-'
        score = f"{row['throughput_score'] * 100:.0f}%"
        print(f"{rank:<5} {row['profile']:<26} {score:>7} {' '.join(cells)} "
              f"{worst:>8,.3f}ms  {row['best_for']}")
    print('=' * width)
    print('Score = geometric mean of each profile\'s result relative to the best profile '
          'per workload.\n')


def action_compare_profiles(args, fio_config, ssh_config, logger):
    """Compare KubeVirt storage tuning profiles one at a time on a fixed node."""
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    namespace = f"{args.namespace_prefix}-profiles-{timestamp}"[:63].rstrip('-')
    helper_name = 'virtbench-ssh'
    output_dir = os.path.join(
        args.results_dir,
        args.storage_driver,
        'profile-comparison',
        timestamp,
    )
    template_path = args.vm_template
    if not os.path.isabs(template_path):
        template_path = os.path.abspath(os.path.join(os.path.dirname(__file__), template_path))

    print('\n' + '=' * 72)
    print('FIO - KUBEVIRT STORAGE PROFILE COMPARISON')
    print('=' * 72)
    print(f'Storage class: {args.storage_class}')
    print(f'Profiles: {", ".join(args.profile_list)}')
    print(f'Iterations: {args.profile_iterations} per profile')
    print(f'Pinned node: {args.node_name or "scheduler-selected"}')
    print('Workloads: ' + ', '.join(spec['name'] for spec in args.workload_list))
    print(f'Common FIO settings: iodepth={args.fio_iodepth}, numjobs={args.fio_numjobs}, '
          f'runtime={args.fio_runtime}s, ramp={args.fio_ramp_time}s')
    measured = (len(args.profile_list) * len(args.workload_list)
                * args.profile_iterations * (args.fio_runtime + args.fio_ramp_time))
    print(f'Estimated measurement time: {measured / 60:.0f} min '
          f'(excludes VM boot and restarts)')
    print('=' * 72 + '\n')

    _kubectl(['create', 'namespace', namespace])
    all_iterations = []
    failures = []
    applied = {}
    try:
        create_profile_helper(namespace, helper_name, logger)
        ssh_config = {**ssh_config, 'pod': helper_name, 'pod_ns': namespace}

        vm_name = 'fio-profile'
        for position, profile in enumerate(args.profile_list, 1):
            if position > 1:
                print(f'[{position}/{len(args.profile_list)}] {profile}: restarting with profile...')
                halted = _kubectl([
                    'patch', 'vm', vm_name, '-n', namespace, '--type=merge',
                    '-p', '{"spec":{"runStrategy":"Halted"}}',
                ], check=False)
                if halted.returncode == 0:
                    _kubectl([
                        'wait', '--for=delete', f'vmi/{vm_name}', '-n', namespace,
                        '--timeout=300s',
                    ], check=False)
            else:
                print(f'[{position}/{len(args.profile_list)}] {profile}: provisioning shared VM and disks...')
            base_yaml = prepare_vm_yaml(
                template_path, vm_name, args.storage_class, fio_config,
                ssh_config['password'], logger,
            )
            vm_yaml = configure_manifest(
                base_yaml, profile, args.vm_cpus, args.node_name,
                args.supplemental_pool_threads, args.benchmark_disk,
            )
            try:
                if not deploy_vm(namespace, vm_yaml, logger):
                    raise RuntimeError('VM manifest could not be deployed')
                warmup_timeout = args.fio_runtime + args.fio_ramp_time + 1200
                if position == 1:
                    ready = wait_for_fio_complete(
                        namespace, vm_name, ssh_config, warmup_timeout, logger
                    )
                    vm_ip = get_vmi_ip(vm_name, namespace, logger) if ready else None
                else:
                    vm_ip = wait_for_vm_ssh(
                        namespace, vm_name, ssh_config, 600, logger
                    )
                    ready = bool(vm_ip)
                if not ready:
                    status = _kubectl(
                        ['get', 'vm,vmi,dv,pvc', '-n', namespace, '-o', 'wide'], check=False
                    ).stdout
                    raise RuntimeError(f'VM did not become ready for profile measurement\n{status}')
                if not vm_ip:
                    raise RuntimeError('VM has no pod-network IP')
                if position == 1 and not args.node_name:
                    selected = _kubectl([
                        'get', 'vmi', vm_name, '-n', namespace,
                        '-o', 'jsonpath={.status.nodeName}',
                    ]).stdout.strip()
                    if not selected:
                        raise RuntimeError('KubeVirt did not report the selected worker node')
                    args.node_name = selected
                    print(f'    Scheduler selected {selected}; pinning subsequent profiles there')

                applied[profile] = inspect_applied_profile(
                    namespace, vm_name, profile, args, logger
                )
                if applied[profile]['verified']:
                    disk = applied[profile]['benchmark_disk']
                    print(f'    applied: iothreads={applied[profile]["iothreads"]}, '
                          f'disk iothread={disk["iothread"]}, queues={disk["queues"]}, '
                          f'cache={disk["cache"]}')
                    for note in applied[profile]['notes']:
                        print(f'    note: {note}')
                for workload in args.workload_list:
                    for iteration in range(1, args.profile_iterations + 1):
                        print(f'    {workload["name"]}: measured iteration '
                              f'{iteration}/{args.profile_iterations}...')
                        raw = run_profile_iteration(
                            vm_ip, workload, iteration, args, ssh_config, logger
                        )
                        raw_dir = os.path.join(output_dir, 'raw', profile)
                        os.makedirs(raw_dir, exist_ok=True)
                        with open(os.path.join(
                                raw_dir, f'{workload["name"]}-iteration-{iteration}.json'),
                                'w') as handle:
                            json.dump(raw, handle, indent=2)
                        all_iterations.append(
                            parse_iteration(raw, profile, iteration, workload['name'])
                        )
            except Exception as exc:
                failures.append({'profile': profile, 'error': str(exc)})
                logger.error('[%s] %s', profile, exc)
                print(f'    FAILED: {exc}')
                continue

        observed_iothreads = {
            name: info['iothreads'] for name, info in applied.items() if info.get('verified')
        }
        summaries = summarize_profiles(
            all_iterations, args.vm_cpus, args.supplemental_pool_threads, observed_iothreads
        ) if all_iterations else []
        metadata = {
            'test_type': 'fio_kubevirt_profile_comparison',
            'timestamp': datetime.now().isoformat(),
            'namespace': namespace,
            'storage_class': args.storage_class,
            'node_name': args.node_name,
            'vm_cpus': args.vm_cpus,
            'supplemental_pool_threads': args.supplemental_pool_threads,
            'fio_config': fio_config,
            'requested_profiles': args.profile_list,
            'workloads': args.workload_list,
            'applied_configuration': applied,
            'failures': failures,
        }
        recommendation = None
        if args.recommend and summaries:
            recommendation = recommend(
                summaries, {spec['name']: spec for spec in args.workload_list}
            )
        save_profile_comparison(
            output_dir, metadata, all_iterations, summaries, recommendation
        )
        print_profile_comparison(summaries)
        if recommendation is not None:
            print_recommendation(recommendation)
        print(f'Results saved to: {output_dir}')
        try:
            from virtbench.profile_dashboard import write_run_dashboard
            print(f'Dashboard: {write_run_dashboard(Path(output_dir))}')
        except Exception as exc:  # the dashboard is a convenience, never fail the run
            logger.warning('Could not write the comparison dashboard: %s', exc)
        if failures:
            print(f'Profiles with failures: {", ".join(item["profile"] for item in failures)}')
            return 1
        return 0
    finally:
        if not args.keep_profile_resources:
            print(f'Cleaning comparison namespace {namespace}...')
            for attempt in range(1, 4):
                result = _kubectl(
                    ['delete', 'namespace', namespace, '--wait=false', '--ignore-not-found'],
                    check=False,
                )
                if result.returncode == 0:
                    break
                logger.warning(
                    'Namespace cleanup attempt %s failed: %s', attempt, result.stderr.strip()
                )
                time.sleep(2)
        else:
            print(f'Comparison resources retained in namespace: {namespace}')


def get_output_dir(args, namespaces, logger) -> str:
    """Determine and create output directory."""
    if getattr(args, '_output_dir', None):
        return args._output_dir

    disks_per_vm = args.disks_per_vm
    if disks_per_vm == "auto":
        first_ns = namespaces[0]
        disk_count = get_vm_disk_count(args.vm_name, first_ns, logger)
        if disk_count > 0:
            disks_per_vm = f"{disk_count}-disk"
            if logger:
                logger.info(f"Auto-detected {disk_count} disks from {first_ns}/{args.vm_name} spec")
        else:
            disks_per_vm = "1-disk"
            if logger:
                logger.warning(f"Could not detect disks from {first_ns}/{args.vm_name}, using default: 1-disk")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    num_vms = len(namespaces)
    run_name = f"{timestamp}_fio_benchmark_{num_vms}vms"

    output_dir = os.path.join(
        args.results_dir,
        args.storage_driver,
        disks_per_vm,
        run_name
    )
    os.makedirs(output_dir, exist_ok=True)
    args._output_dir = output_dir
    return output_dir


class DeploymentError(Exception):
    """Raised when the deployment cannot be prepared; nothing has been created yet."""


def prepare_deployment(args, namespaces, fio_config, vm_password, logger) -> Tuple[Dict[str, str], Optional[Dict]]:
    """
    Render the manifest, apply the vCPU override, plan placement and run the
    capacity/topology checks -- all before anything is created on the cluster.

    Returns (manifests per namespace, plan or None). Raises DeploymentError.
    """
    template_path = os.path.join(os.path.dirname(__file__), args.vm_template)
    if not os.path.exists(template_path):
        raise DeploymentError(f"VM template not found: {template_path}")
    vm_yaml = prepare_vm_yaml(template_path, args.vm_name, args.storage_class,
                              fio_config, vm_password, logger)
    try:
        info = get_vm_template_info(vm_yaml)
        cpus = args.vm_cpus
        if cpus is None and not info['instancetype']:
            cpus = DEFAULT_VM_CPUS
        if cpus is not None:
            vm_yaml = set_vm_cpus(vm_yaml, cpus, args.vm_cpu_request)
            info = get_vm_template_info(vm_yaml)
        elif args.vm_cpu_request is not None:
            raise ValueError("--vm-cpu-request cannot be used with an instancetype template")
    except ValueError as e:
        raise DeploymentError(str(e))

    if info['instancetype']:
        print(f"VM size: from instancetype {info['instancetype']}")
    else:
        print(f"VM size: {info['vcpus'] or 'default'} vCPU, CPU request {info['cpu_request']:g}"
              f"{' (from --vm-cpus)' if args.vm_cpus is not None else ' (default)'}")

    plan = build_placement_plan(args, namespaces, info, logger)
    manifests = {
        ns: pin_vm_to_node(vm_yaml, plan[ns]['hostname']) if plan else vm_yaml
        for ns in namespaces
    }
    return manifests, plan


def build_placement_plan(args, namespaces, info: Dict, logger) -> Optional[Dict[str, Dict]]:
    """Pick eligible nodes and assign VMs evenly. Returns None if balancing is disabled."""
    if args.no_balance:
        print("Placement: scheduler default (--no-balance)")
        return None

    if info['constraints']:
        print(f"  WARNING: the VM template has {', '.join(info['constraints'])}; nodes are not "
              f"checked against it, so pinned VMs may stay Pending. Consider --no-balance.")

    exclude = [n.strip() for n in args.exclude_nodes.split(',') if n.strip()]
    try:
        result = get_eligible_nodes(info['tolerations'], exclude, args.exclude_node_label,
                                    info['node_selector'], args.node_selector, logger)
    except Exception as e:
        raise DeploymentError(f"could not determine eligible nodes: {e}. "
                              f"Use --no-balance to let the scheduler place VMs.")

    nodes, skipped = result['eligible'], result['skipped']
    print(f"Candidate nodes: {result['pool']}")
    for name in result['unknown_excludes']:
        print(f"  WARNING: --exclude-nodes name '{name}' matches no node")
    if skipped:
        print("Skipped nodes:")
        for name, reason in sorted(skipped.items()):
            print(f"  - {name}: {reason}")
    if not nodes:
        raise DeploymentError(
            "no eligible nodes for FIO VMs (see the skipped nodes above). Adjust --node-selector, "
            "--exclude-node-label or --exclude-nodes, or re-run with --no-balance to let the "
            "scheduler place VMs.")

    start_index = max(args.start - 1, 0)
    plan = plan_balanced_placement(namespaces, nodes, start_index)
    counts = count_per_node({ns: n['name'] for ns, n in plan.items()})
    print(f"Placement: balanced across {len(nodes)} nodes")
    for node, count in counts.items():
        print(f"  {node:<40} {count} VM(s)")
    if len(namespaces) % len(nodes):
        print(f"  Note: {len(namespaces)} VMs do not divide evenly over {len(nodes)} nodes; "
              f"counts differ by at most 1.")

    # Storage topology
    warning = storage_topology_warning(get_storage_class(args.storage_class, logger), nodes)
    if warning:
        print(f"  WARNING: {warning}")

    # Capacity
    if info['instancetype']:
        print("  Capacity check skipped: VM size comes from an instancetype.")
    else:
        used, err = get_pod_requests_by_node(logger)
        if used is None:
            print(f"  Capacity check skipped: cannot list pods ({err}).")
        else:
            problems = check_capacity(plan, nodes, used, info['cpu_request'], info['memory_request'])
            if problems:
                print("Capacity: some nodes cannot fit their planned VMs:")
                for p in problems:
                    print(f"  - {p}")
                if not args.skip_capacity_check:
                    raise DeploymentError(
                        "not enough free capacity for balanced placement. Use fewer VMs, a smaller "
                        "--vm-cpus/--vm-cpu-request, --exclude-nodes for full nodes, --no-balance, "
                        "or --skip-capacity-check to deploy anyway.")
                print("  Continuing (--skip-capacity-check); affected VMs may stay Pending.")
    return plan


def warn_existing_vms(namespaces, vm_name, logger):
    """Warn when VMs already exist: kubectl apply will not move or resize running VMs."""
    existing = [ns for ns, s in get_deployed_state(namespaces, vm_name, logger)['state'].items()
                if s['exists']]
    if existing:
        print(f"  WARNING: {len(existing)} VM(s) already exist (e.g. {existing[0]}). Running VMs keep "
              f"their current node and CPU until restarted, and old FIO results may be collected. "
              f"Run --action cleanup first for a clean run.")


def apply_manifests(args, namespaces, manifests: Dict[str, str], plan, logger) -> List[str]:
    """Apply each namespace's manifest. Returns the namespaces that failed."""
    failed = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {executor.submit(deploy_vm, ns, manifests[ns], logger): ns for ns in namespaces}
        for future in as_completed(futures):
            ns = futures[future]
            try:
                ok = future.result()
            except Exception as e:
                ok = False
                logger.error(f"[{ns}] {e}")
            target = f" -> {plan[ns]['name']}" if plan and ns in plan else ""
            print(f"  {'✓' if ok else '✗'} {ns}{target}")
            if not ok:
                failed.append(ns)
    return sorted(failed)


def check_placement(args, namespaces, plan: Optional[Dict[str, Dict]], logger,
                    wait: bool = True, failed_to_apply: Iterable[str] = ()) -> Dict:
    """
    Read where each VMI landed and what it runs with (waiting up to
    --placement-timeout for scheduling when a plan exists), print per-node
    counts and return the verification report.
    """
    failed = set(failed_to_apply)
    planned = {ns: n['name'] for ns, n in plan.items() if ns not in failed} if plan else {}
    waiting_for = [ns for ns in namespaces if ns not in failed]
    deadline = time.time() + (args.placement_timeout if (wait and plan) else 0)
    while True:
        deployed = get_deployed_state(namespaces, args.vm_name, logger)
        state = deployed['state']
        if all(state[ns]['node'] for ns in waiting_for) or time.time() >= deadline:
            break
        time.sleep(10)

    report = verify_placement(planned, state, failed)
    report['policy'] = 'balanced' if plan else 'scheduler'
    report['vm_node_map'] = {ns: s['node'] for ns, s in state.items()}
    report['vm_cpus'] = deployed['vm_cpus']
    report['vm_cpu_request'] = deployed['vm_cpu_request']
    if planned:
        report['planned'] = planned
    if deployed['errors']:
        report['read_errors'] = deployed['errors']

    print("VM distribution per node:")
    for node, count in report['per_node_counts'].items():
        print(f"  {node:<40} {count} VM(s)")
    for ns in report['not_created']:
        print(f"  ! {ns}: VM/VMI not created (apply failed or still provisioning)")
    for ns in report['unscheduled']:
        print(f"  ! {ns}: VMI not scheduled (check `kubectl get vmi,pods -n {ns}` and events for "
              f"capacity, taint or volume-topology reasons)")
    for ns in report['terminal']:
        print(f"  ! {ns}: VMI is {state[ns]['phase']}")
    for ns, m in report['mismatches'].items():
        print(f"  ! {ns}: planned {m['planned']} but running on {m['actual']}")
    if report['restart_required']:
        print(f"  ! {len(report['restart_required'])} VM(s) have RestartRequired: the running VMI "
              f"does not match the applied spec (e.g. {report['restart_required'][0]})")
    for e in deployed['errors']:
        print(f"  ! could not read cluster state: {e}")
    if not plan:
        print("  (scheduler placement; not enforced or verified)")
    elif report['balanced']:
        print("  Placement verified: every VM is on its planned node and the spread is even.")
    else:
        print("  WARNING: VMs are NOT all on their planned nodes or not evenly distributed; "
              "results may not be comparable.")
    return report


def action_deploy(args, namespaces, fio_config, logger) -> int:
    """Deploy VMs with FIO pre-configured. Returns the exit code."""
    print("\n" + "=" * 60)
    print("FIO BENCHMARK - DEPLOY")
    print("=" * 60)
    print(f"Namespaces: {namespaces[0]} to {namespaces[-1]} ({len(namespaces)} VMs)")
    print(f"Storage Class: {args.storage_class}")
    print(f"FIO Config: {fio_config['rw']} | bs={fio_config['bs']} | iodepth={fio_config['iodepth']}")
    print("=" * 60 + "\n")

    print("[1/3] Preparing deployment...")
    try:
        manifests, plan = prepare_deployment(args, namespaces, fio_config, args.vm_password, logger)
    except DeploymentError as e:
        print(f"ERROR: {e}")
        return 1
    warn_existing_vms(namespaces, args.vm_name, logger)

    print("[2/3] Creating namespaces...")
    create_namespaces_parallel(namespaces, batch_size=20, logger=logger)

    print("[3/3] Deploying FIO VMs...")
    failed = apply_manifests(args, namespaces, manifests, plan, logger)

    print("Checking VM placement...")
    report = check_placement(args, namespaces, plan, logger, failed_to_apply=failed)

    print(f"\nDeployment complete.")
    print("VMs will start FIO automatically on boot.")
    print(f"Use --action status to check progress.")
    print(f"Use --action gather-results to collect results when complete.")
    if failed:
        print(f"ERROR: {len(failed)} VM(s) failed to apply: {', '.join(failed)}")
        return 1
    if report['balanced'] is False:
        return 1
    return 0


def check_fio_status_in_vm(vm_ip: str, ssh_config: Dict, logger) -> str:
    """Check FIO status inside the VM. Returns: 'running', 'completed', 'not-started', or 'unknown'."""
    if not vm_ip:
        return "no-ip"

    # Check if FIO is currently running
    fio_running = run_ssh_command(
        vm_ip, 'pgrep -x fio >/dev/null 2>&1 && echo "YES" || echo "NO"',
        ssh_config['pod'], ssh_config['pod_ns'],
        ssh_config['user'], ssh_config['password'],
        timeout=30
    )

    if fio_running and 'YES' in fio_running:
        return "running"

    # Check if FIO completed
    fio_complete = run_ssh_command(
        vm_ip, 'cat /tmp/fio_complete 2>/dev/null',
        ssh_config['pod'], ssh_config['pod_ns'],
        ssh_config['user'], ssh_config['password'],
        timeout=30
    )

    if fio_complete and 'completed' in fio_complete:
        return "completed"

    # Check if FIO results exist (in case completion marker wasn't written)
    fio_results = run_ssh_command(
        vm_ip, 'test -f /tmp/fio_results.json && echo "YES" || echo "NO"',
        ssh_config['pod'], ssh_config['pod_ns'],
        ssh_config['user'], ssh_config['password'],
        timeout=30
    )

    if fio_results and 'YES' in fio_results:
        return "completed"

    if fio_running and 'NO' in fio_running:
        return "not-started"

    return "unknown"


def action_status(args, namespaces, ssh_config, logger):
    """Check VM and FIO status."""
    print("\n" + "=" * 70)
    print("FIO BENCHMARK - STATUS")
    print("=" * 70)
    print(f"Checking {len(namespaces)} VMs...")
    print("-" * 70)
    print(f"{'Namespace':<25} {'VM Status':<12} {'VMI Phase':<10} {'VM IP':<16} {'FIO Status':<12}")
    print("-" * 70)

    summary = {'running': 0, 'completed': 0, 'not-started': 0, 'not-running': 0, 'unknown': 0}

    for ns in namespaces:
        vm_status, vmi_phase = get_vm_and_vmi_status(ns, args.vm_name)
        vm_ip = get_vmi_ip(args.vm_name, ns, logger) if vmi_phase == "Running" else None

        if vmi_phase == "Running" and vm_ip:
            fio_status = check_fio_status_in_vm(vm_ip, ssh_config, logger)
        elif vmi_phase == "Running":
            fio_status = "no-ip"
        else:
            fio_status = "-"

        # Update summary
        if fio_status in summary:
            summary[fio_status] += 1
        elif vmi_phase != "Running":
            summary['not-running'] += 1

        # Color coding for display
        ip_display = vm_ip if vm_ip else "-"
        print(f"{ns:<25} {vm_status:<12} {vmi_phase:<10} {ip_display:<16} {fio_status:<12}")

    print("-" * 70)
    print(f"Summary: {summary['completed']} completed, {summary['running']} running, "
          f"{summary['not-started']} not-started, {summary['not-running']} VMs not running")
    print("=" * 70 + "\n")


def collect_results_for_vms(namespaces, args, ssh_config, output_dir, logger,
                            timed_out=()):
    """Collect and parse every VM's FIO results (shared by gather-results and run-all).

    `timed_out` lists namespaces whose FIO run did not complete; they are recorded as
    failed without being collected. Returns (per-VM parsed results, raw FIO outputs of
    the VMs that succeeded).
    """
    all_results = []
    raw_results = []
    for ns in timed_out:
        all_results.append({"namespace": ns, "success": False,
                            "error": "FIO completion timed out or VM unavailable"})
        print(f"  ✗ {ns}: FIO completion timed out or VM unavailable")

    def report(ns, raw_data):
        if raw_data:
            parsed = parse_fio_results(ns, raw_data)
            all_results.append(parsed)
            if parsed["success"]:
                raw_results.append(raw_data)
                print(f"  ✓ {ns}")
            else:
                print(f"  ✗ {ns}: {parsed['error']}")
        else:
            all_results.append({"namespace": ns, "success": False, "error": "no results collected"})
            print(f"  ✗ {ns}")

    skip = set(timed_out)
    collect_all_fio_results(
        [ns for ns in namespaces if ns not in skip],
        lambda ns: collect_fio_results_once(ns, args.vm_name, ssh_config, output_dir, logger),
        logger, args.collect_concurrency, args.collect_retries, args.collect_retry_delay,
        on_done=report,
    )
    return all_results, raw_results


def wait_for_all_fio_complete(namespaces, vm_name, ssh_config, fio_timeout, concurrency, logger):
    """Wait for every VM's FIO completion marker. Returns the namespaces that did not complete."""
    timed_out = []
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        futures = {
            executor.submit(wait_for_fio_complete, ns, vm_name, ssh_config, fio_timeout, logger): ns
            for ns in namespaces
        }
        for future in as_completed(futures):
            ns = futures[future]
            try:
                done = future.result()
            except Exception as e:
                logger.error(f"[{ns}] Waiting for FIO completion failed: {e}")
                done = False
            if not done:
                timed_out.append(ns)
    return timed_out


def action_gather_results(args, namespaces, fio_config, ssh_config, logger):
    """Collect FIO results from existing VMs."""
    print("\n" + "=" * 60)
    print("FIO BENCHMARK - GATHER RESULTS")
    print("=" * 60)
    print(f"Collecting from {len(namespaces)} VMs...")
    print("=" * 60 + "\n")

    test_start = time.time()
    output_dir = get_output_dir(args, namespaces, logger)
    print(f"Output directory: {output_dir}\n")

    fio_timeout = args.fio_timeout if args.fio_timeout is not None else fio_config['runtime'] + 600

    # Wait for completion before collecting.
    timed_out = wait_for_all_fio_complete(namespaces, args.vm_name, ssh_config, fio_timeout,
                                          args.collect_concurrency, logger)
    all_results, raw_results = collect_results_for_vms(
        namespaces, args, ssh_config, output_dir, logger, timed_out=timed_out)

    # Aggregate and save
    test_duration = time.time() - test_start
    summary = aggregate_results(all_results, fio_config_from_results(raw_results, fio_config), test_duration)
    # Describe what is actually deployed and running, not this command's flags.
    # Never let this lose the results collected above.
    try:
        plan = get_deployed_state(namespaces, args.vm_name, logger)['plan']
        placement = check_placement(args, namespaces, plan, logger, wait=False)
    except Exception as e:
        logger.error(f"Could not read placement: {e}")
        placement = None
    summary['vm_cpus'] = placement.get('vm_cpus') if placement else None
    summary['vm_cpu_request'] = placement.get('vm_cpu_request') if placement else None
    attach_placement(summary, all_results, placement)

    if args.save_results:
        save_results_to_files(output_dir, summary, all_results, logger)

    print_results_table(summary)

    if args.save_results:
        print(f"Results saved to: {output_dir}/")

    return report_fio_failures(summary)


def report_fio_failures(summary: Dict) -> int:
    """Print failed or unverified VMs; return 1 unless every run is verified."""
    if not summary.get('failed') and not summary.get('unverified'):
        return 0
    print(f"ERROR: FIO failed on {summary.get('failed', 0)} of {summary['total_vms']} VM(s); "
          f"{summary.get('unverified', 0)} result(s) unverified:")
    for f in summary.get('failures', []):
        print(f"  {f['namespace']}: {f.get('error')}")
    for r in summary.get('unverified_results', []):
        print(f"  {r['namespace']}: {r.get('error')}")
    return 1


def action_cleanup(args, namespaces, logger):
    """Delete VMs and namespaces."""
    print("\n" + "=" * 60)
    print("FIO BENCHMARK - CLEANUP")
    print("=" * 60)
    print(f"Cleaning up {len(namespaces)} namespaces...")
    print("=" * 60 + "\n")

    cleanup_test_namespaces(
        namespace_prefix=args.namespace_prefix,
        start=args.start,
        end=args.end,
        vm_name=args.vm_name,
        delete_namespaces=True,
        batch_size=20,
        logger=logger
    )
    print(f"Cleaned up {len(namespaces)} namespaces")


def action_run_all(args, namespaces, fio_config, ssh_config, logger):
    """Full workflow: deploy, wait for FIO, gather results."""
    print("\n" + "=" * 60)
    print("FIO BENCHMARK - FULL RUN")
    print("=" * 60)
    print(f"Namespaces: {namespaces[0]} to {namespaces[-1]} ({len(namespaces)} VMs)")
    print(f"Storage Class: {args.storage_class}")
    print(f"FIO Config: {fio_config['rw']} | bs={fio_config['bs']} | iodepth={fio_config['iodepth']} | "
          f"numjobs={fio_config['numjobs']} | runtime={fio_config['runtime']}s")
    print("=" * 60 + "\n")

    test_start = time.time()

    # Step 1: Prepare (render, plan, check) before creating anything, then create namespaces
    print("[1/4] Preparing deployment and creating namespaces...")
    try:
        manifests, plan = prepare_deployment(args, namespaces, fio_config, ssh_config['password'], logger)
    except DeploymentError as e:
        print(f"ERROR: {e}")
        return 1
    warn_existing_vms(namespaces, args.vm_name, logger)
    create_namespaces_parallel(namespaces, batch_size=20, logger=logger)

    # Step 2: Deploy VMs
    print("[2/4] Deploying FIO VMs...")
    failed = apply_manifests(args, namespaces, manifests, plan, logger)
    if plan:
        print("Checking VM placement...")
        check_placement(args, namespaces, plan, logger, failed_to_apply=failed)

    # Step 3: Wait for VMs and FIO to complete
    print(f"[3/4] Waiting for VMs to boot and FIO to complete (polling every 30s)...")
    fio_timeout = args.fio_timeout if args.fio_timeout is not None else fio_config['runtime'] + 600

    completed = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {
            executor.submit(
                wait_for_fio_complete, ns, args.vm_name, ssh_config, fio_timeout, logger
            ): ns for ns in namespaces
        }
        for future in as_completed(futures):
            ns = futures[future]
            success = future.result()
            status = "✓" if success else "✗"
            print(f"  {status} {ns}")
            completed.append((ns, success))

    # Record final placement (all VMs have had the chance to start by now)
    try:
        placement = check_placement(args, namespaces, plan, logger, wait=False, failed_to_apply=failed)
    except Exception as e:
        logger.error(f"Could not read placement: {e}")
        placement = None

    # Step 4: Collect results
    print("[4/4] Collecting results...")
    output_dir = get_output_dir(args, namespaces, logger)
    print(f"Output directory: {output_dir}")

    all_results, raw_results = collect_results_for_vms(
        namespaces, args, ssh_config, output_dir, logger,
        timed_out=[ns for ns, success in completed if not success])

    # Aggregate and save
    test_duration = time.time() - test_start
    summary = aggregate_results(all_results, fio_config_from_results(raw_results, fio_config), test_duration)
    summary['vm_cpus'] = placement.get('vm_cpus') if placement else None
    summary['vm_cpu_request'] = placement.get('vm_cpu_request') if placement else None
    attach_placement(summary, all_results, placement)

    if args.save_results:
        save_results_to_files(output_dir, summary, all_results, logger)

    print_results_table(summary)

    if args.save_results:
        print(f"Results saved to: {output_dir}/")

        # Cleanup VMs if requested
        if args.cleanup:
            print("\nCleaning up resources...")
            cleanup_test_namespaces(
                namespace_prefix=args.namespace_prefix,
                start=args.start,
                end=args.end,
                vm_name=args.vm_name,
                delete_namespaces=True,
                batch_size=20,
                logger=logger
            )
            print(f"Cleaned up {len(namespaces)} namespaces")

    rc = report_fio_failures(summary)
    if failed:
        print(f"ERROR: {len(failed)} VM(s) failed to apply: {', '.join(failed)}")
        rc = 1
    if placement and placement.get('balanced') is False:
        print("ERROR: balanced placement was not achieved; see the placement report above.")
        rc = 1
    return rc


def main():
    args = parse_args()
    namespaces = [f"{args.namespace_prefix}-{i}" for i in range(args.start, args.end + 1)]

    if args.save_results and args.action in ['gather-results', 'run-all'] and not args.log_file:
        output_dir = get_output_dir(args, namespaces, logger=None)
        args.log_file = os.path.join(output_dir, "fio-benchmark.log")

    logger = setup_logging(args.log_file, args.log_level)

    fio_config = {
        'runtime': args.fio_runtime,
        'bs': args.fio_bs,
        'rw': args.fio_rw,
        'iodepth': args.fio_iodepth,
        'numjobs': args.fio_numjobs,
        'size': args.fio_size,
        'ramp_time': args.fio_ramp_time,
        'rwmixread': args.fio_rwmixread,
    }

    ssh_config = {
        'pod': args.ssh_pod,
        'pod_ns': args.ssh_pod_ns,
        'user': args.vm_user,
        'password': args.vm_password
    }

    rc = 0
    if args.action == 'deploy':
        rc = action_deploy(args, namespaces, fio_config, logger)
    elif args.action == 'status':
        action_status(args, namespaces, ssh_config, logger)
    elif args.action == 'gather-results':
        rc = action_gather_results(args, namespaces, fio_config, ssh_config, logger)
    elif args.action == 'cleanup':
        action_cleanup(args, namespaces, logger)
    elif args.action == 'run-all':
        rc = action_run_all(args, namespaces, fio_config, ssh_config, logger)
    elif args.action == 'compare-profiles':
        rc = action_compare_profiles(args, fio_config, ssh_config, logger)
    else:
        print(f"Unknown action: {args.action}")
        rc = 1
    sys.exit(rc or 0)


if __name__ == '__main__':
    main()
