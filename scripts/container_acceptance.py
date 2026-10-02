from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time

import httpx

URL = 'http://127.0.0.1:8000'
HEADERS = {'Authorization': 'Bearer ' + os.environ['ADMIN_TOKEN']}


async def main():
    results = []
    async with httpx.AsyncClient(base_url=URL, headers=HEADERS, timeout=20, trust_env=False) as client:
        def check(name, condition):
            results.append({'test':name,'pass':bool(condition)})
            if not condition:
                raise AssertionError(name)
        response = await client.get('/api/v1/gateways')
        check('four_gateways_online', response.status_code==200 and len(response.json()['gateways'])==4)
        ids = ['a-cn','a-intl','b','c']
        streams = []
        async def stream(gid):
            async with client.stream('POST',f'/gw/{gid}/v1/chat/completions',json={'model':'mock-tool','messages':[{'role':'user','content':'test'}],'stream':True}) as response:
                content = await response.aread()
                return response.status_code==200 and '你好'.encode() in content and b'tool_calls' in content and b'[DONE]' in content and b'heartbeat' in content
        check('four_streams_utf8_tools_heartbeat_done', all(await asyncio.gather(*(stream(g) for g in ids))))
        check('auth_required', (await client.get('/api/v1/gateways',headers={'Authorization':'Bearer wrong'})).status_code==401)
        for gid in ids:
            response = await client.post(f'/api/v1/gateways/{gid}/tasks/claim/run',json={'dry_run':False})
            check(gid+'_claim_evidence501', response.status_code==501 and response.json()['error']['code']=='evidence_required')
            response = await client.post(f'/api/v1/gateways/{gid}/tasks/checkin/run',json={'dry_run':True})
            check(gid+'_dry_run_no_execution',response.status_code==200 and response.json()['allowed'] is False)
        check('task_kill_switch_on',(await client.get('/api/v1/tasks/kill-switch')).json()['enabled'])
        check('cors_allowed',(await client.options('/api/v1/gateways',headers={'Origin':'http://127.0.0.1:18081','Access-Control-Request-Method':'GET','Access-Control-Request-Headers':'authorization'})).status_code==200)
        check('cors_untrusted_denied',(await client.options('/api/v1/gateways',headers={'Origin':'https://evil.invalid','Access-Control-Request-Method':'GET','Access-Control-Request-Headers':'authorization'})).status_code==400)
        await client.patch('/api/v1/gateways/a-cn/settings',json={'concurrency':1,'queue_limit':0,'queue_timeout':.1})
        async def slow():
            async with client.stream('POST','/gw/a-cn/v1/chat/completions',json={'model':'mock-slow','messages':[{'role':'user','content':'test'}],'stream':True}) as response:
                await response.aread()
        task = asyncio.create_task(slow())
        await asyncio.sleep(.15)
        response = await client.post('/gw/a-cn/v1/chat/completions',json={'model':'mock-normal','messages':[],'stream':True})
        check('queue_full429',response.status_code==429)
        healthy = await client.post('/gw/b/v1/chat/completions',json={'model':'mock-normal','messages':[],'stream':False})
        check('gate_isolation_b_continues',healthy.status_code==200)
        await task
        await client.patch('/api/v1/gateways/a-cn/settings',json={'concurrency':8,'queue_limit':32,'queue_timeout':15})
        async with client.stream('POST','/gw/a-cn/v1/chat/completions',json={'model':'mock-slow','messages':[],'stream':True}) as response:
            async for data in response.aiter_raw():
                break
        await asyncio.sleep(.3)
        accounts = (await client.get('/api/v1/gateways/a-cn/accounts')).json()['accounts']
        check('client_disconnect_releases',all(a['in_flight']==0 for a in accounts))
        r = await client.patch('/api/v1/gateways/a-cn/accounts/test-0',json={'enabled':False})
        check('account_patch_scope',r.status_code==200 and next(a for a in r.json()['accounts'] if a['id']=='test-0')['enabled'] is False)
        c = (await client.get('/api/v1/gateways/c/accounts')).json()['accounts']
        check('account_patch_does_not_change_other_gateway',next(a for a in c if a['id']=='test-0')['enabled'] is True)
        await client.patch('/api/v1/gateways/a-cn/accounts/test-0',json={'enabled':True})
        check('webhook_disabled',(await client.post('/api/updates/webhook',json={})).status_code==503)
        check('responses_evidence501',(await client.post('/gw/c/v1/responses',json={})).status_code==501)
    connection = sqlite3.connect(os.environ['DATABASE_PATH'])
    check('sqlite_wal',connection.execute('PRAGMA journal_mode').fetchone()[0]=='wal')
    check('lease_foreign_key_is_gateway_scoped',len(connection.execute('PRAGMA foreign_key_list(leases)').fetchall())==2)
    connection.close()
    print(json.dumps({'tests':results,'passed':len(results),'scope':'local fixture in isolated Docker; no provider requests'}))


if __name__=='__main__':
    asyncio.run(main())
