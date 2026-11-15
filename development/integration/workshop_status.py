"""Shared presentation adapter. No model execution, queue, network or deployment."""
from datetime import UTC, datetime
import json
import re

from raytsystem.contracts import derive_id
from raytsystem.contracts.execution import CommentKind, ExecutionComment


class WorkshopStatus:
    """External-result import using existing task ledger and execution receipts.

    The imported run is NOT a native ExecutionRun: we have no invocation/lease
    evidence. Its binding is an immutable handoff comment in ExecutionStore.
    No scheduler or alternate queue is created.
    """
    def __init__(self, tasks, store, *, owner_id):
        self.tasks, self.store, self.owner_id = tasks, store, owner_id

    def _owner(self, value):
        if type(value) is not int or value != self.owner_id:
            raise PermissionError("Owner identity required from trusted transport")

    def register_result(self, source, *, native_task_id, project_id, owner_id):
        self._owner(owner_id)
        for key in ("task_id", "run_id"):
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{1,127}", source.get(key, "")):
                raise ValueError("Invalid external identity")
        allowed = {"task_id", "run_id", "status", "updated_at", "exit_code", "reviewed", "changed_files", "checks"}
        if set(source) - allowed:
            raise ValueError("Snapshot contains unsupported fields; sanitize before import")
        for path in source.get("changed_files", []):
            if not isinstance(path, str) or path.startswith("/") or ".." in path.split("/") or "\\" in path:
                raise ValueError("Unsafe changed-file path")
        source = dict(source)
        source["updated_at"] = _stamp(source["updated_at"]).isoformat()
        project_status(source)  # validate timestamp and content before any write
        task = next((t for t in self.tasks.snapshot().tasks if t.task_id == native_task_id), None)
        if task is None:
            raise LookupError("Existing task required; no automatic task creation")
        if task.project_id != project_id:
            raise ValueError("Existing task belongs to another project")
        request = {"source": source, "project_id": project_id, "native_task_id": native_task_id}
        key = derive_id("workshop", {"external_run_id": source["run_id"]})
        binding = {"external_task_id": source["task_id"], "external_run_id": source["run_id"],
                   "native_task_id": task.task_id, "native_run_id": None,
                   "project_id": project_id, "source": source}
        stamp = _stamp(source["updated_at"])
        comment = ExecutionComment(comment_id="comment_pending", task_id=task.task_id,
            kind=CommentKind.HANDOFF, actor="user:telegram_owner", run_id=None,
            body=json.dumps({"workshop_binding": binding}, ensure_ascii=False, sort_keys=True), created_at=stamp)
        comment = comment.model_copy(update={"comment_id": derive_id("comment", comment.identity_payload())})
        return self.store.put_with_receipt(comment, expected_revision=None,
            scope="workshop_import", idempotency_key=key, request=request, receipt=binding)

    def view(self, external_run_id, *, owner_id, now=None):
        self._owner(owner_id)
        for event in self.store.list(ExecutionComment, limit=500):
            if event.kind != CommentKind.HANDOFF:
                continue
            try:
                binding = json.loads(event.body)["workshop_binding"]
            except (ValueError, KeyError, TypeError):
                continue
            if binding["external_run_id"] != external_run_id:
                continue
            task = next((t for t in self.tasks.snapshot().tasks if t.task_id == binding["native_task_id"]), None)
            if task is None:
                raise ValueError("Native task missing; do not display orphan result")
            result = project_status(binding["source"], now=now)
            result.update(native_task_id=task.task_id, native_task_status=task.status.value,
                          native_run_id=None, project_id=binding["project_id"],
                          history=[e.model_dump(mode="json") for e in self.tasks.history(task.task_id)],
                          stop_supported=False,
                          stop_reason="Внешний запуск уже завершён; native run не зарегистрирован")
            return result
        raise LookupError("Result unavailable; do not infer running state")

LABELS = {"partial": "Частичный результат", "queued": "В очереди", "preparing": "В работе", "running": "В работе", "needs_review": "На проверке", "review": "На проверке", "succeeded": "На проверке", "needs_owner": "Нужен ответ владельца", "blocked": "Заблокировано", "failed": "Ошибка", "error": "Ошибка", "cancelled": "Остановлено", "paused": "Остановлено", "interrupted": "Остановлено"}


def _stamp(value):
    stamp = datetime.fromisoformat(value) if isinstance(value, str) else datetime.fromtimestamp(value, UTC)
    if stamp.tzinfo is None:
        raise ValueError("Timezone required")
    return stamp.astimezone(UTC)


def project_status(source, *, now=None, stale_seconds=120):
    """Render one evidence object identically for CRM and Telegram; 0 != accepted."""
    now = now or datetime.now(UTC)
    stamp = _stamp(source["updated_at"])
    semantic = source["status"]
    stale = semantic in {"running", "preparing", "queued"} and (now-stamp).total_seconds() > stale_seconds
    return {"task_id": source["task_id"], "run_id": source["run_id"],
            "status": "Нет свежих данных" if stale else LABELS.get(semantic, "Заблокировано"),
            "semantic_status": semantic, "updated_at": stamp.isoformat(),
            "accepted": False, "published": False, "snapshot_only": True,
            "stale": stale, "changed_files": source.get("changed_files", []),
            "checks": source.get("checks", []), "preview": None,
            "limitation": "Статический снимок; не подтверждает живое исполнение"}
