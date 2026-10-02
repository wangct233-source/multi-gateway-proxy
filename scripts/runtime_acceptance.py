from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx
import websockets


async def main():
    with tempfile.TemporaryDirectory(prefix='mgp-runtime-') as tmp:
        environment = dict(os.environ, DATABASE_PATH=str(Path(tmp)/'proxy.db'), SOFT_MEMORY_MB='1')
        process = subprocess.Popen([sys.executable,'-m','uvicorn','app.main:create_app','--factory','--host','127.0.0.1','--port','18091','--no-access-log'], env=environment,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            async with httpx.AsyncClient(base_url='http://127.0.0.1:18091',headers={'Authorization':'Bearer '+environment['ADMIN_TOKEN']},timeout=3) as client:
                for _ in range(60):
                    try:
                        response=await client.get('/healthz')
                        if response.status_code==200 and response.json()['memory_pressure']:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(.2)
                else:
                    raise AssertionError('memory_monitor_not_ready')
                response=await client.post('/gw/b/v1/chat/completions',json={'messages':[],'stream':True})
                assert response.status_code==429 and response.json()['error']['code']=='memory_pressure'
                gateways=(await client.get('/api/v1/gateways')).json()['gateways']
                assert all(g['effective_concurrency']==4 and g['concurrency']==8 for g in gateways)
                try:
                    async with websockets.connect('ws://127.0.0.1:18091/gw/c/v1/chat/completions',additional_headers={'Authorization':'Bearer '+environment['ADMIN_TOKEN']}):
                        raise AssertionError('WS wrongly accepted')
                except websockets.exceptions.InvalidStatus as exc:
                    assert exc.response.status_code==501
                response=await client.post('/api/v1/gateways/b/accounts',json={'id':'internal','provider_account_id':'internal','secret_ref':'env:ADMIN_TOKEN'})
                assert response.status_code==400
                print(json.dumps({'rss_watermark_admission429':True,'effective_concurrency_halved':True,
                                  'websocket_same_port_handshake501':True,'internal_token_as_account_rejected':True,
                                  'scope':'disposable fixture runtime; no provider requests'}))
        finally:
            process.terminate()
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill();process.wait()


if __name__=='__main__':
    asyncio.run(main())
