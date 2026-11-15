"""Run one predefined synthetic task through native CRM records; no general approvals."""
import asyncio,json,sys,hashlib
from datetime import UTC,datetime,timedelta
from pathlib import Path
from raytsystem.tasking import TaskService
from raytsystem.contracts import TaskStatus,derive_id
from raytsystem.contracts.execution import ExecutionApproval,TaskAssignment,TaskWorkspace
from raytsystem.execution.service import ExecutionService
ROOT=Path('/data/workspace');role=sys.argv[1]
TASKS={
'orchestrator':('План учебного продукта','Только прочитай products/training/greeting.py. Подготовь короткое ТЗ Builder: вернуть Hello, ANODHAS и проверить результат. Не изменяй файлы. Scope products/training. task_owner owner.'),
'builder':('Учебный продукт: приветствие','Измени только products/training/greeting.py: greeting() возвращает ровно Hello, ANODHAS. Проверь функцию через python без создания pycache. Рабочая копия /workspace. task_owner owner. Не меняй остальные файлы.'),
'reviewer':('Независимая проверка учебного продукта','Только прочитай products/training/greeting.py и проверь, что greeting() возвращает ровно Hello, ANODHAS. Если можно, выполни python с PYTHONDONTWRITEBYTECODE=1. Файлы не меняй. Верни pass/fail/inconclusive с доказательством и ограничениями. task_owner owner.'),
'librarian':('Описание результата учебного продукта','Прочитай только products/training/greeting.py и предложи короткую заметку о текущем результате. Не записывай в Obsidian и не меняй файлы. task_owner owner.'),
'researcher':('Анализ учебного продукта','Прочитай только products/training/greeting.py и укажи два полезных теста функции. Не используй интернет, не меняй файлы. task_owner owner.')}
if role not in TASKS:raise ValueError('Unknown predefined role')
title,description=TASKS[role];key='roles-v5-training-'+role
# The only payload is a fixed, artificial greeting and installed project instructions.
tasks=TaskService(ROOT);created=tasks.create_task(title=title,description=description,actor='user:owner',idempotency_key=key)
p=tasks.transition_task(created.task.task_id,TaskStatus.PLANNED,actor='user:owner',idempotency_key=key+'-plan',expected_generation_id=created.generation_id)
r=tasks.transition_task(created.task.task_id,TaskStatus.READY,actor='user:owner',idempotency_key=key+'-ready',expected_generation_id=p.generation_id)
with ExecutionService(ROOT) as s:
 e=next(x for x in s.employees.load().employees if x.name.lower()==role)
 a=s.assign_task(task_id=r.task.task_id,employee_id=e.employee_id,expected_generation_id=r.generation_id,idempotency_key=key+'-assign').assignment
 waiting=asyncio.run(s.manual_heartbeat(assignment_id=a.assignment_id,idempotency_key=key+'-run',fixture_mode=False))
 policy=waiting.policy_decision
 if not waiting.approval_required or policy is None:
  print(json.dumps(waiting.receipt()));sys.exit(2)
 if policy.destination!='provider:openai':raise ValueError('Unexpected destination')
 ws=s.store.list(TaskWorkspace,task_id=r.task.task_id)
 if len(ws)!=1:raise ValueError('Ambiguous workspace')
 now=datetime.now(UTC)-timedelta(seconds=1)
 seed=ExecutionApproval(approval_id='xapr_pending',action='execute_runtime',payload_sha256=policy.payload_sha256,employee_id=e.employee_id,task_id=r.task.task_id,run_id=waiting.run_id,workspace_id=ws[0].workspace_id,destination=policy.destination,scope=policy.required_approval_scope,approved_by='user:owner_fixed_training',approved_at=now,expires_at=now+timedelta(minutes=5))
 approval=seed.model_copy(update={'approval_id':derive_id('xapr',seed.identity_payload())});s.store.put(approval,expected_revision=None)
 result=asyncio.run(s.manual_heartbeat(assignment_id=a.assignment_id,idempotency_key=key+'-run',approval_id=approval.approval_id,fixture_mode=False))
 print(json.dumps({'role':role,'task_id':r.task.task_id,'workspace':ws[0].repo_path,**result.receipt()}))
