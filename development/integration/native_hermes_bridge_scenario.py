"""Create a real, disposable raytsystem task and optionally run the Hermes bridge.

Run with the upstream environment on PYTHONPATH, for example:
  PYTHONPATH=../upstream/src python native_hermes_bridge_scenario.py \
    --root /tmp/anodhas-hermes-native-001
  PYTHONPATH=../upstream/src python native_hermes_bridge_scenario.py \
    --root /tmp/anodhas-hermes-native-002 --execute \
    --owner-approved-synthetic-pilot

Each invocation requires a new root under an explicitly allowlisted directory. The
official bootstrap creates the ledger; a real Git commit and code graph bind the
native task worktree. No fixture mode, synthetic ledgers or mock supervisor.
Without the owner-approved flag the real run stops at its provider approval gate.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml
from raytsystem.bootstrap.service import BootstrapService
from raytsystem.codegraph.projection import CodeGraphProjection
from raytsystem.contracts import TaskStatus, derive_id
from raytsystem.contracts.execution import (
    ExecutionApproval,
    ExecutionRunStatus,
    TaskAssignment,
    TaskWorkspace,
)
from raytsystem.execution.config import load_execution_config
from raytsystem.execution.service import ExecutionService
from raytsystem.tasking import TaskService


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ("git", "-c", "core.hooksPath=/dev/null", "-C", str(root), *args),
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result.stdout.strip()


def _new_root(value: str) -> Path:
    requested = Path(value).expanduser().absolute()
    root = requested.parent.resolve() / requested.name
    temporary_roots = (
        Path(tempfile.gettempdir()).resolve(),
        Path("/tmp").resolve(),
        Path("/data/workspace/.raytsystem/workspaces").resolve(),
    )
    if (
        requested.exists()
        or requested.is_symlink()
        or not any(root.is_relative_to(base) for base in temporary_roots)
    ):
        raise ValueError("root must be new and below an allowlisted directory")
    if not root.parent.is_dir() or root.parent.is_symlink():
        raise ValueError("root parent must be an existing real directory")
    root.mkdir(mode=0o700)
    return root


def _write_catalog(root: Path) -> None:
    config = root / "config" / "raytsystem.toml"
    config.write_text(
        config.read_text(encoding="utf-8")
        + "\n[features]\n"
        + "runtime_execution_enabled = true\n"
        + "hermes_bridge_enabled = true\n",
        encoding="utf-8",
    )
    registry = root / "config" / "runtime-adapters.yaml"
    payload = yaml.safe_load(registry.read_text(encoding="utf-8"))
    payload["adapters"].append(
        {
            "adapter_id": "adapter_hermes_bridge",
            "name": "Local ANODHAS Hermes IPC bridge",
            "version": "0.1.0-local",
            "state": "available",
            "isolation_mode": "managed_worktree",
            "capabilities": ["read_workspace", "staged_write"],
        }
    )
    registry.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")
    pack = root / "packs" / "bridge_demo"
    (pack / "agents").mkdir(parents=True)
    (pack / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "pack_id": "pack_bridge_demo",
                "name": "Local bridge demonstration",
                "version": "1.0.0",
                "description": "Disposable, local-only execution scenario.",
                "license_expression": "Apache-2.0",
                "trust_class": "user",
                "agent_ids": ["agent_bridge_builder"],
                "skill_ids": [],
                "context_paths": ["AGENTS.md"],
                "optional": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (pack / "agents" / "agent_bridge_builder.yaml").write_text(
        yaml.safe_dump(
            {
                "agent_id": "agent_bridge_builder",
                "name": "Builder",
                "role": "software_engineer",
                "description": "Implements a bounded task in its managed Git worktree.",
                "version": "1.0.0",
                "pack_id": "pack_bridge_demo",
                "runtime_adapter_id": "adapter_hermes_bridge",
                "skill_ids": [],
                "context_paths": ["AGENTS.md"],
                "capabilities": ["code_edit"],
                "requested_filesystem_mode": "staging_only",
                "approved_data_classes": ["public"],
                "accent": "#123456",
                "enabled": True,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _setup(root: Path) -> str:
    source = root / "src"
    source.mkdir()
    (source / "service.py").write_text(
        "def greeting() -> str:\n    return 'Hello'\n", encoding="utf-8"
    )
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "ANODHAS Local Scenario")
    _git(root, "config", "user.email", "contact005@example.invalid")
    _git(root, "add", "--", "src/service.py")
    _git(root, "commit", "-m", "Seed local training source")

    bootstrap = BootstrapService(root)
    plan = bootstrap.plan(template="software")
    if not plan.preflight.ok:
        raise RuntimeError(f"bootstrap preflight blocked: {plan.preflight.blockers}")
    bootstrap.apply(confirm=plan.fingerprint, template="software")
    _write_catalog(root)
    _git(root, "add", "-A")
    _git(root, "commit", "-m", "Install raytsystem and local bridge catalog")
    commit = _git(root, "rev-parse", "HEAD")
    graph = CodeGraphProjection(root)
    graph.rebuild()
    status = graph.status(verify_hashes=True)
    if status.state.value != "current":
        raise RuntimeError(
            f"code graph is not current: {status.state.value}/{status.reason}"
        )
    return commit


def _task_and_assignment(root: Path) -> tuple[str, str]:
    tasks = TaskService(root)
    created = tasks.create_task(
        title="Update Python greeting in src/service.py",
        description=(
            "In this isolated training worktree, change greeting() "
            "to return 'Hello, ANODHAS'."
        ),
        actor="user:local_scenario",
        idempotency_key="create-bridge-training-task",
    )
    planned = tasks.transition_task(
        created.task.task_id,
        TaskStatus.PLANNED,
        actor="user:local_scenario",
        idempotency_key="plan-bridge-training-task",
        expected_generation_id=created.generation_id,
    )
    ready = tasks.transition_task(
        created.task.task_id,
        TaskStatus.READY,
        actor="user:local_scenario",
        idempotency_key="ready-bridge-training-task",
        expected_generation_id=planned.generation_id,
    )
    with ExecutionService(root) as service:
        employee = next(
            item
            for item in service.employees.load().employees
            if item.name == "Builder"
            and item.runtime_adapter_id == "adapter_hermes_bridge"
        )
        assignment = service.assign_task(
            task_id=ready.task.task_id,
            employee_id=employee.employee_id,
            expected_generation_id=ready.generation_id,
            idempotency_key="assign-bridge-training-task",
        )
    return ready.task.task_id, assignment.assignment.assignment_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", required=True, help="New isolated path in an allowlisted root"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Invoke the real local IPC bridge after all gates",
    )
    parser.add_argument(
        "--owner-approved-synthetic-pilot",
        action="store_true",
        help="Create one hash-bound approval for this freshly generated synthetic task",
    )
    args = parser.parse_args()
    if args.owner_approved_synthetic_pilot and not args.execute:
        parser.error("--owner-approved-synthetic-pilot requires --execute")
    root = _new_root(args.root)
    report: dict[str, object] = {"root": str(root), "stage": "created"}
    try:
        report["git_commit"] = _setup(root)
        report["graph_state"] = CodeGraphProjection(root).status().state.value
        task_id, assignment_id = _task_and_assignment(root)
        report.update(task_id=task_id, assignment_id=assignment_id, stage="assigned")
        task = next(
            item
            for item in TaskService(root).snapshot().tasks
            if item.task_id == task_id
        )
        report["native_sensitivity"] = task.sensitivity.value
        report["data_scope"] = "fresh synthetic source only; no imported corpus"
        bridge_available = shutil.which("anodhas-hermes-bridge") is not None
        if not args.execute or not bridge_available:
            with ExecutionService(root) as service:
                prepared = service.prepare_workspace(
                    assignment_id,
                    run_id="xrun_preflight_local",
                    fixture_mode=False,
                )
            worktree = root / prepared.preparation.workspace.repo_path
            if _git(worktree, "rev-parse", "HEAD") != report["git_commit"]:
                raise RuntimeError(
                    "prepared Git worktree differs from the committed source"
                )
            report["worktree"] = str(worktree)
            report["stage"] = "worktree_prepared"
        if not args.execute:
            report["next"] = (
                "Use a new --root and --execute when the local bridge is ready"
            )
            return 0
        if not bridge_available:
            report["blocker"] = "anodhas-hermes-bridge is absent from PATH"
            return 3
        with ExecutionService(root, config=load_execution_config(root)) as service:
            waiting = asyncio.run(
                service.manual_heartbeat(
                    assignment_id=assignment_id,
                    idempotency_key="run-bridge-training-task",
                    fixture_mode=False,
                )
            )
            report.update(waiting.receipt())
            report["stage"] = "approval_gate"
            if waiting.policy_decision is not None:
                report["required_approval_scope"] = list(
                    waiting.policy_decision.required_approval_scope
                )
            if not args.owner_approved_synthetic_pilot:
                return 4
            policy = waiting.policy_decision
            if (
                not waiting.approval_required
                or policy is None
                or policy.destination != "provider:openai"
                or policy.required_approval_scope
                != ("private_corpus_egress", "provider_egress", "runtime_execution")
            ):
                raise RuntimeError(
                    "expected scoped provider approval gate was not reached"
                )
            assignment = service.store.get(TaskAssignment, assignment_id)
            workspaces = service.store.list(TaskWorkspace, task_id=task_id)
            if assignment is None or len(workspaces) != 1:
                raise RuntimeError("approval bindings are missing or ambiguous")
            approved_at = datetime.now(UTC) - timedelta(seconds=1)
            seed = ExecutionApproval(
                approval_id="xapr_pending",
                action="execute_runtime",
                payload_sha256=policy.payload_sha256,
                employee_id=assignment.employee_id,
                task_id=task_id,
                run_id=waiting.run_id,
                workspace_id=workspaces[0].workspace_id,
                destination="provider:openai",
                scope=policy.required_approval_scope,
                approved_by="user:owner_synthetic_pilot",
                approved_at=approved_at,
                expires_at=approved_at + timedelta(minutes=5),
            )
            approval = seed.model_copy(
                update={"approval_id": derive_id("xapr", seed.identity_payload())}
            )
            service.store.put(approval, expected_revision=None)
            report["approval_id"] = approval.approval_id
            result = asyncio.run(
                service.manual_heartbeat(
                    assignment_id=assignment_id,
                    idempotency_key="run-bridge-training-task",
                    approval_id=approval.approval_id,
                    fixture_mode=False,
                )
            )
            report.update(result.receipt())
            report["stage"] = "run_attempted"
            successful = {ExecutionRunStatus.REVIEW, ExecutionRunStatus.SUCCEEDED}
            return 0 if result.status in successful else 4
    except Exception as error:
        report["blocker"] = f"{type(error).__name__}: {error}"
        return 2
    finally:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    sys.exit(main())
