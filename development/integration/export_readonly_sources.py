"""Build a private Markdown projection; source files and permissions stay unchanged."""
import hashlib,json,os,pathlib,stat,time,uuid
SOURCES={'obsidian':pathlib.Path('/home/owner/Obsidian/Syncthing-Codex-Operational-Vault'),'hermes-skills':pathlib.Path('/home/owner/.hermes/skills')}
BASE=pathlib.Path('/mnt/anodhas-development/source-projections')
def main():
 if not os.path.ismount('/mnt/anodhas-development'):raise RuntimeError('Encrypted volume is not mounted')
 os.umask(0o077);BASE.mkdir(mode=0o700,exist_ok=True)
 generation=BASE/('snapshot-'+time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-'+uuid.uuid4().hex[:8]);generation.mkdir(mode=0o700)
 summary={}
 for name,source in SOURCES.items():
  target=generation/name;target.mkdir(mode=0o700)
  records=[]
  for directory,dirs,files in os.walk(source,followlinks=False):
   dirs[:]=[n for n in dirs if not n.startswith('.') and not (pathlib.Path(directory)/n).is_symlink()]
   for namepart in files:
    if namepart.startswith('.') or not namepart.endswith('.md'):continue
    path=pathlib.Path(directory)/namepart;rel=path.relative_to(source)
    if path.is_symlink():continue
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    try:
     before=os.fstat(fd)
     if not stat.S_ISREG(before.st_mode) or before.st_size>262144:continue
     data=os.read(fd,262145);after=os.fstat(fd)
     if (before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):raise RuntimeError('Source changed during projection')
    finally:os.close(fd)
    dest=target/rel;dest.parent.mkdir(parents=True,mode=0o700,exist_ok=True);dest.write_bytes(data);dest.chmod(0o400);os.utime(dest,ns=(before.st_atime_ns,before.st_mtime_ns))
    records.append({'path':str(rel),'sha256':hashlib.sha256(data).hexdigest(),'mtime_ns':before.st_mtime_ns,'bytes':len(data)})
  stamp={'source':name,'snapshot_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'mode':'readonly_snapshot','records':records}
  (target/'.source-manifest.json').write_text(json.dumps(stamp,ensure_ascii=False));summary[name]={'markdown_files':len(records),'skills':sum(pathlib.PurePosixPath(x['path']).name=='SKILL.md' for x in records),'snapshot_utc':stamp['snapshot_utc']}
 for directory,dirs,files in os.walk(generation):
  os.chown(directory,10030,10030);os.chmod(directory,0o500)
  for n in files:os.chown(pathlib.Path(directory)/n,10030,10030);os.chmod(pathlib.Path(directory)/n,0o400)
 print(json.dumps({'generation':str(generation),'sources':summary},ensure_ascii=False))
if __name__=='__main__':main()
