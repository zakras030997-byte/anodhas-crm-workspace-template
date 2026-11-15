"""Private, bounded Unix-socket RPC into the existing operator research instance.

No TCP listener, credentials, second collector, or automatic Telegram delivery.
The dedicated socket directory is mounted only into Radar and the CRM API.
"""
import asyncio
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import stat
from uuid import UUID
from urllib.parse import urlsplit, urlunsplit
from .catalog import DIRECTIONS, SOURCES
from .threads import QUERIES
from .service import ResearchBlocked

MAX_RESPONSE = 2 * 1024 * 1024


class BridgeConflict(ResearchBlocked):
    """Definitive rejection before starting work; safe to surface as HTTP 409."""
    def __init__(self, code):
        self.code = code
        super().__init__(code)

def candidate_id(card):
    p = urlsplit(card['url'])
    url = urlunsplit(('https', (p.hostname or '').lower(), p.path.rstrip('/'), '', ''))
    return hashlib.sha256(url.encode()).hexdigest()

def public_run(run):
    result = deepcopy(run)
    result['cards'] = [{**card, 'id': candidate_id(card)} for card in result.get('cards', [])[:10]]
    return result

class ResearchBridge:
    def __init__(self, research, path):
        self.research, self.path = research, Path(path)
        self.server = None
        self.tasks = set()
        self.connections = asyncio.Semaphore(8)

    async def start(self):
        if self.path.exists():
            if not stat.S_ISSOCK(self.path.lstat().st_mode):
                raise RuntimeError('Research bridge path is not a socket')
            self.path.unlink()
        self.server = await asyncio.start_unix_server(self.handle, path=str(self.path), limit=8192)
        os.chmod(self.path, 0o660)

    async def close(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for task in tuple(self.tasks): task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.server: self.path.unlink(missing_ok=True)

    async def handle(self, reader, writer):
        try:
            async with self.connections, asyncio.timeout(8):
                raw = await reader.readline()
                if len(raw) > 4096 or not raw.endswith(b'\n'): raise ValueError()
                command = json.loads(raw)
                result = await self.command(command)
                data = json.dumps({'ok': True, 'result': result}, ensure_ascii=False).encode()+b'\n'
                if len(data) > MAX_RESPONSE: raise ValueError()
                writer.write(data)
                await writer.drain()
        except BridgeConflict as exc:
            writer.write(json.dumps({'ok': False, 'error': exc.code, 'status': 409}).encode()+b'\n')
            await writer.drain()
        except (ResearchBlocked, FileNotFoundError, ValueError, KeyError, TypeError):
            writer.write(b'{"ok":false,"error":"research_request_rejected"}\n')
            await writer.drain()
        except Exception:
            writer.write(b'{"ok":false,"error":"research_unavailable"}\n')
        finally:
            writer.close()
            await writer.wait_closed()

    async def command(self, command):
        if not isinstance(command, dict): raise ValueError()
        action = command.get('action')
        if action == 'catalog' and set(command) == {'action'}:
            return {'directions': [{'id': d.id, 'label': d.label,
                'modes': ['tg', 'global'] + (['threads'] if d.id in QUERIES else []),
                'sources': [{'id': k, 'label': SOURCES[k][0], 'role': SOURCES[k][2]} for k in d.sources]}
                for d in DIRECTIONS.values()],
                'modes': [{'id':'tg','label':'Telegram','available':True},
                    {'id':'global','label':'Веб-поиск','available':self.research.backend is not None,'reason':'Нужен подключённый поисковый провайдер'},
                    {'id':'threads','label':'Threads','available':True}],
                'limits': {'days':7,'max_cards':10}}
        if action == 'list' and set(command) == {'action'}:
            paths = sorted(self.research.root.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)[:30]
            items=[]
            for path in paths:
                run=self.research.get(path.stem)
                items.append({**run,'cards':[], 'candidate_count':len(run.get('cards',[]))})
            return {'items':items}
        if action == 'get' and set(command) == {'action','run_id'}:
            return public_run(self.research.get(command['run_id']))
        if action == 'start' and set(command) == {'action','direction_id','mode','operation_key'}:
            op = str(UUID(command['operation_key']))
            if type(command['direction_id']) is not int: raise ValueError()
            request_id = 'crm:' + op
            run_id = hashlib.sha256(request_id.encode()).hexdigest()[:32]
            try:
                previous = self.research.get(run_id)
            except FileNotFoundError:
                previous = None
            if previous and (previous['direction_id'] != command['direction_id'] or previous['mode'] != command['mode']):
                raise BridgeConflict('operation_key_conflict')
            run, created = await self.research.start(command['direction_id'], command['mode'], request_id)
            # ResearchService can return somebody else's active run. That is
            # not acceptance of this key: no binding was persisted for it.
            if run['id'] != run_id:
                raise BridgeConflict('research_busy')
            if created:
                task=asyncio.create_task(self.research.execute(run['id']))
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)
            return {'run': public_run(run), 'created':created}
        if action == 'cancel' and set(command) == {'action','run_id'}:
            run = self.research.get(command['run_id'])
            current = self.research.latest()
            if current and current['id'] == run['id'] and run['status'] in {'queued','running'}:
                return public_run(await self.research.cancel())
            return public_run(run)
        raise ResearchBlocked('Unsupported bridge action')
