#!/usr/bin/env python3
"""Fixed private IPC client used only by the native execution adapter."""
import argparse,json,os,socket,sys
SOCKET='/run/hermes-bridge/bridge.sock'
def main():
 p=argparse.ArgumentParser();p.add_argument('--version',action='store_true');p.add_argument('--json',action='store_true');p.add_argument('--workspace');p.add_argument('--mode',choices=['read_only','staging_only']);p.add_argument('--role',choices=['orchestrator','builder','reviewer','librarian','researcher']);a=p.parse_args()
 request={'action':'health'} if a.version else {'action':'run','workspace':a.workspace,'mode':a.mode,'role':a.role,'prompt':sys.stdin.read(1048577)}
 if not a.version and (not all([a.json,a.workspace,a.mode,a.role]) or len(request['prompt'].encode())>1048576):p.error('Invalid bounded request')
 with socket.socket(socket.AF_UNIX) as s:
  s.settimeout(250);s.connect(SOCKET);s.sendall(json.dumps(request).encode()+b'\n');response=b''
  while b'\n' not in response:
   block=s.recv(65536)
   if not block:raise RuntimeError('Bridge disconnected; inspect run before retry')
   response+=block
   if len(response)>8388608:raise RuntimeError('Bridge output limit')
 result=json.loads(response)
 if a.version:
  if not result.get('ready'):raise RuntimeError('Bridge unavailable')
  print('anodhas-hermes-bridge 1.0');return
 print(result.get('stdout',''));print(result.get('error',''),file=sys.stderr);sys.exit(result.get('exit_code',1))
if __name__=='__main__':main()
