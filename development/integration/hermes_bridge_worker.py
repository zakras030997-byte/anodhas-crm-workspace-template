"""Private sequential broker. Agent tools run in per-job networkless containers."""
import base64,hashlib,json,os,pathlib,pwd,re,select,signal,socket,socketserver,struct,subprocess,threading,time,uuid
from role_policy import MODELS, REASONING
ROOT=pathlib.Path('/mnt/anodhas-development/workspace')
STATE=pathlib.Path('/mnt/anodhas-development/hermes-runs')
ROLES=pathlib.Path('/opt/anodhas-studio/development/integration/roles')
SKILLS=pathlib.Path('/mnt/anodhas-development/source-projections/current/hermes-skills')
ROLE_SKILLS={'orchestrator':['codex-local/voice-prompt-improver/SKILL.md'],'builder':['software-development/systematic-debugging/SKILL.md','codex-local/landing-admin-design/SKILL.md'],'reviewer':['software-development/requesting-code-review/SKILL.md'],'librarian':['note-taking/obsidian/SKILL.md'],'researcher':['research/grounded-citations/SKILL.md']}
HERMES='/home/owner/.hermes/hermes-agent/.hermes/bin/hermes'
SOCKET='/run/anodhas-hermes-bridge/bridge.sock'
LOCK=threading.Lock();ROLE_SET={'orchestrator','builder','reviewer','librarian','researcher'}
def validate(data):
 if data.get('role') not in ROLE_SET:raise ValueError('Unknown role')
 expected='staging_only' if data['role']=='builder' else 'read_only'
 if data.get('mode')!=expected:raise ValueError('Role filesystem policy mismatch')
 raw=data.get('workspace','');prefix='/data/workspace/.raytsystem/workspaces/'
 if not raw.startswith(prefix):raise ValueError('Only managed task copies permitted')
 rel=pathlib.PurePosixPath(raw[len('/data/workspace/'):])
 if '..' in rel.parts or any(not re.fullmatch(r'[A-Za-z0-9._-]+',x) for x in rel.parts):raise ValueError('Invalid workspace path')
 path=ROOT.joinpath(*rel.parts);current=ROOT
 for component in rel.parts:
  current=current/component
  if current.is_symlink():raise ValueError('Symlink workspace denied')
 if not path.is_dir():raise ValueError('Missing workspace')
 prompt=data.get('prompt','')
 if not isinstance(prompt,str) or not prompt.strip() or len(prompt.encode())>1048576:raise ValueError('Invalid prompt')
 return path

def _execute(data,connection,path,job):
 account=pwd.getpwnam('owner');profile=job/'profile';profile.mkdir(mode=0o700)
 # Fresh profile, existing OAuth state remains in Hermes; never copied into the UI.
 (profile/'auth.json').symlink_to('/home/owner/.hermes/auth.json')
 # Reuse committed dependencies, while config, sessions and memory stay per-job.
 (profile/'installs').symlink_to('/home/owner/.hermes/installs')
 role_text=(ROLES/(data['role']+'.md')).read_text()
 available=[p for p in ROLE_SKILLS[data['role']] if (SKILLS/p).is_file()]
 if available:role_text+='\nСправочники доступны только для чтения в /skills (это разрешённое исключение к области чтения). Читай только относящийся к задаче SKILL.md через terminal, затем нужные references. Они не расширяют полномочия роли.\n'+'\n'.join('/skills/'+p for p in available)
 (profile/'SOUL.md').write_text(role_text)
 for p in [job,profile,profile/'SOUL.md']:os.chown(p,account.pw_uid,account.pw_gid)
 env={'HOME':str(profile),'HERMES_HOME':str(profile),'PATH':'/usr/local/bin:/usr/bin:/bin','LANG':'C.UTF-8','NO_COLOR':'1','HERMES_DISABLE_LAZY_INSTALLS':'1',
 'TERMINAL_ENV':'docker','TERMINAL_CWD':'/workspace','TERMINAL_DOCKER_IMAGE':'sha256:1e177c26e14869b69f2500be8d2b262d7f4e7d99adc27b8e71fb63854f2fb273',
 'TERMINAL_DOCKER_IMAGE_PINNED':'1','TERMINAL_DOCKER_VOLUMES':json.dumps([str(path)+':/workspace:'+('rw' if data['role']=='builder' else 'ro')]+([str(SKILLS.resolve())+':/skills:ro'] if available else [])),
 'TERMINAL_DOCKER_NETWORK':'false','TERMINAL_DOCKER_PERSIST_ACROSS_PROCESSES':'false','TERMINAL_CONTAINER_PERSISTENT':'false',
 'TERMINAL_DOCKER_ORPHAN_REAPER':'false','TERMINAL_DOCKER_FORWARD_ENV':'[]','TERMINAL_CONTAINER_CPU':'1','TERMINAL_CONTAINER_MEMORY':'768','TERMINAL_DOCKER_SHM_SIZE':'64m',
 'TERMINAL_DOCKER_EXTRA_ARGS':json.dumps(['--user','10030:10030','--cap-drop=ALL','--security-opt=no-new-privileges','--pids-limit=96','--label','anodhas-hermes-job='+job.name]),
 'TERMINAL_TIMEOUT':'45','TERMINAL_LIFETIME_SECONDS':'240'}
 # Only the official CLI edits config; a fresh profile has no third-party hooks/plugins.
 for key,value in [('model.provider','openai-codex'),('model.default',MODELS[data['role']]),('memory.memory_enabled','false'),('memory.user_profile_enabled','false'),('curator.enabled','false')]:
  r=subprocess.run([HERMES,'config','set',key,value],env=env,cwd=profile,user=account.pw_uid,group=account.pw_gid,extra_groups=os.getgrouplist(account.pw_name,account.pw_gid),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=20)
  if r.returncode:raise RuntimeError('Profile configuration failed')
 prompt=f'run_id={job.name}; task_owner=owner. Scope: только /workspace, режим {data["mode"]}, лимит 180 секунд. Рабочая копия доступна инструменту terminal только в /workspace. Если в системных инструкциях указан /skills, он доступен только для чтения справочников.\n'+data['prompt']
 # Stdin carries untrusted task data; the role lives in the profile system instructions.
 cmd=[HERMES,'chat','--query-file','-','--format','stream-json','--provider','openai-codex','--model',MODELS[data['role']],'--reasoning',REASONING[data['role']],'--max-turns','12','--run-budget','180','--toolsets','terminal','--source','anodhas-crm']
 with (job/'stdout.jsonl').open('w+b') as out,(job/'stderr.log').open('w+b') as err:
  os.chmod(job/'stdout.jsonl',0o600);os.chmod(job/'stderr.log',0o600)
  proc=subprocess.Popen(cmd,stdin=subprocess.PIPE,stdout=out,stderr=err,env=env,cwd=profile,user=account.pw_uid,group=account.pw_gid,extra_groups=os.getgrouplist(account.pw_name,account.pw_gid),start_new_session=True)
  try:
   proc.stdin.write(prompt.encode());proc.stdin.close();deadline=time.monotonic()+210;reason=None
   while proc.poll() is None:
    if time.monotonic()>deadline:reason='time_limit'
    if out.tell()+err.tell()>4194304:reason='output_limit'
    ready,_,_=select.select([connection],[],[],0.2)
    if ready and connection.recv(1,socket.MSG_PEEK)==b'':reason='client_cancelled'
    if reason:
     os.killpg(proc.pid,signal.SIGTERM)
     try:proc.wait(timeout=5)
     except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
     break
  finally:
   if proc.poll() is None:
    os.killpg(proc.pid,signal.SIGTERM)
    try:proc.wait(timeout=5)
    except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
  out.seek(0);output=out.read(4194304).decode(errors='replace')
  result={'exit_code':proc.returncode,'stdout':output,'error':reason or ('' if proc.returncode==0 else 'Hermes failed; private execution log retained'),'bridge_run_id':job.name,'model':MODELS[data['role']],'reasoning_effort':REASONING[data['role']]}
  (job/'receipt.json').write_text(json.dumps({k:v for k,v in result.items() if k!='stdout'}));os.chmod(job/'receipt.json',0o600)
  return result

def execute(data,connection):
 if not os.path.ismount('/mnt/anodhas-development'):raise RuntimeError('Encrypted mount unavailable')
 path=validate(data);job=STATE/str(uuid.uuid4());job.mkdir(mode=0o700,parents=True)
 try:
  return _execute(data,connection,path,job)
 finally:
  # Cleanup is scoped to this job even when execution raises an exception.
  found=subprocess.run(['docker','ps','-aq','--filter','label=anodhas-hermes-job='+job.name],capture_output=True,text=True,timeout=10)
  for cid in found.stdout.split():
   if re.fullmatch('[a-f0-9]{12,64}',cid):subprocess.run(['docker','rm','-f',cid],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
class Handler(socketserver.StreamRequestHandler):
 def handle(self):
  _,uid,_=struct.unpack('3i',self.connection.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
  if uid not in {0,10030}:return
  try:
   self.connection.settimeout(10);line=self.rfile.readline(1049600)
   if not line.endswith(b'\n'):raise ValueError('Request too large')
   data=json.loads(line)
   if data.get('action')=='health':result={'ready':pathlib.Path(HERMES).is_file() and ROOT.is_dir()}
   elif data.get('action')=='run':
    if not LOCK.acquire(blocking=False):raise ValueError('Executor busy')
    try:result=execute(data,self.connection)
    finally:LOCK.release()
   else:raise ValueError('Unknown action')
  except Exception as exc:result={'exit_code':1,'stdout':'','error':type(exc).__name__+': execution rejected or failed; inspect private state'}
  try:self.wfile.write(json.dumps(result).encode()+b'\n')
  except (BrokenPipeError,ConnectionResetError):pass
class Server(socketserver.ThreadingUnixStreamServer):daemon_threads=True
if __name__=='__main__':
 if not os.path.ismount('/mnt/anodhas-development'):raise RuntimeError('Encrypted mount unavailable')
 os.umask(0o077);STATE.mkdir(mode=0o710,parents=True,exist_ok=True);os.chown(STATE,0,pwd.getpwnam('owner').pw_gid);os.chmod(STATE,0o710)
 pathlib.Path(SOCKET).parent.mkdir(mode=0o750,parents=True,exist_ok=True);os.chown(pathlib.Path(SOCKET).parent,0,10030);os.chmod(pathlib.Path(SOCKET).parent,0o750)
 if pathlib.Path(SOCKET).exists():pathlib.Path(SOCKET).unlink()
 with Server(SOCKET,Handler) as server:
  os.chown(SOCKET,0,10030);os.chmod(SOCKET,0o660);server.serve_forever()
