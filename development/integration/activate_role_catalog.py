"""Activate existing five roles; preserve ledgers and unrelated catalog entries."""
import json, shutil, subprocess, datetime
from pathlib import Path
import yaml
from raytsystem.catalog import CatalogService
from raytsystem.execution.config import load_execution_config
from raytsystem.execution.employees import project_employee_catalog
from raytsystem.codegraph.projection import CodeGraphProjection

ROOT=Path('/data/workspace')
ROLES=Path('/app/integration/roles')
NAMES=('orchestrator','builder','reviewer','librarian','researcher')
backup=ROOT/'.raytsystem'/'config-backups'/datetime.datetime.now(datetime.UTC).strftime('%Y%m%dT%H%M%SZ')
backup.mkdir(parents=True,mode=0o700)
files=[ROOT/'config/raytsystem.toml',ROOT/'config/runtime-adapters.yaml']+[ROOT/f'packs/starter/agents/agent_{r}.yaml' for r in NAMES]
for file in files:
 dest=backup/file.relative_to(ROOT);dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(file,dest)
registry=files[1]; payload=yaml.safe_load(registry.read_text())
payload['adapters']=[x for x in payload['adapters'] if x['adapter_id']!='adapter_hermes_bridge']
payload['adapters'].append(dict(adapter_id='adapter_hermes_bridge',name='Hermes · ANODHAS',version='1.1.0',state='available',isolation_mode='managed_worktree',capabilities=['read_workspace','staged_write']))
registry.write_text(yaml.safe_dump(payload,allow_unicode=True,sort_keys=False))
for role in NAMES:
 skill=ROOT/'skills'/f'anodhas-{role}';skill.mkdir(exist_ok=True)
 (skill/'SKILL.md').write_text(f'---\nname: anodhas-{role}\ndescription: "Роль {role} ANODHAS: системные границы, рабочий процесс и отчёт."\nversion: "1.1.0"\n---\n'+(ROLES/f'{role}.md').read_text())
 file=ROOT/f'packs/starter/agents/agent_{role}.yaml';data=yaml.safe_load(file.read_text())
 data.update(runtime_adapter_id='adapter_hermes_bridge',enabled=True,version='1.1.0',context_paths=[f'skills/anodhas-{role}/SKILL.md'],skill_ids=[f'anodhas-{role}'],requested_filesystem_mode='staging_only' if role=='builder' else 'read_only')
 file.write_text(yaml.safe_dump(data,allow_unicode=True,sort_keys=False))
config=files[0];text=config.read_text().replace('runtime_execution_enabled = false','runtime_execution_enabled = true').replace('max_concurrent_runs = 2','max_concurrent_runs = 1')
if 'hermes_bridge_enabled' not in text:text=text.replace('[features]','[features]\nhermes_bridge_enabled = true')
config.write_text(text)
catalog=CatalogService(ROOT).load();settings=load_execution_config(ROOT)
employees=project_employee_catalog(catalog,flags=settings.features)
# Track only bounded development inputs, never databases, vaults, transcripts or credentials.
def git(*args):return subprocess.check_output(['git','-c','core.hooksPath=/dev/null','-C',str(ROOT),*args],text=True).strip()
if not (ROOT/'.git').exists():
 git('init','-b','main');git('config','user.name','ANODHAS Development');git('config','user.email','contact004@example.invalid')
# Isolated training product is the first development input.
product=ROOT/'products'/'training';product.mkdir(parents=True,exist_ok=True)
if not (product/'greeting.py').exists():(product/'greeting.py').write_text("def greeting():\n    return 'Hello'\n")
git('add','--','products/training', 'config/raytsystem.toml','config/runtime-adapters.yaml','packs/starter')
for role in NAMES:git('add','--',f'skills/anodhas-{role}')
if git('diff','--cached','--name-only'):git('commit','-m','Configure bounded Hermes roles and training product')
graph=CodeGraphProjection(ROOT);graph.rebuild()
print(json.dumps({'backup':str(backup),'catalog':catalog.catalog_sha256,'agents':[(e.name,e.status.value,e.runtime_adapter_id) for e in employees.employees],'graph':graph.status(verify_hashes=True).state.value,'commit':git('rev-parse','HEAD')},ensure_ascii=False))
