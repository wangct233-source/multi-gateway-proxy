"""Disposable local Git update/rollback integration test; no remote credentials or repo used."""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx


def git(root, *args):
    return subprocess.check_output(['git','-C',str(root),*args],text=True).strip()


async def main():
    source=Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix='mgp-update-') as temporary:
        root=Path(temporary)
        for name in git(source,'ls-files').splitlines():
            path=root/name
            path.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(source/name,path)
        git(root,'init','-b','main')
        git(root,'config','user.name','Integration Fixture')
        git(root,'config','user.email','fixture@invalid.local')
        git(root,'add','.')
        git(root,'commit','-m','baseline')
        baseline=git(root,'rev-parse','HEAD')
        db=root/'data'/'proxy.db'
        db.parent.mkdir()
        marker=root/'.env'
        marker.write_text('DO_NOT_OVERWRITE=fixture\n')
        env=dict(os.environ,PORT='18090',DATABASE_PATH=str(db),UPDATES_ENABLED='true',UPDATES_GRACE_SECONDS='10',
                 UPDATES_AUTO_APPLY='false',UPDATES_REPO_SLUG='fixture/repo',UPDATES_POLL_SECONDS='300',
                 ADMIN_TOKEN='u'*32)
        # Self-contained loopback fixture: the in-flight SSE needs one openai-mode gateway.
        mock_port=18091
        mock=subprocess.Popen([sys.executable,str(source/'scripts'/'mock_upstream.py'),'--port',str(mock_port)],
                              stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        async def wait_mock():
            deadline=time.monotonic()+15
            while time.monotonic()<deadline:
                try:
                    async with httpx.AsyncClient(timeout=1) as client:
                        if (await client.get(f'http://127.0.0.1:{mock_port}/healthz')).status_code==200:
                            return
                except httpx.HTTPError:
                    await asyncio.sleep(.2)
            raise AssertionError('mock_not_ready')
        await wait_mock()
        env.update({'A_CN_UPSTREAM_URL':f'http://127.0.0.1:{mock_port}','A_CN_UPSTREAM_MODE':'openai',
                    'A_CN_EGRESS_PRIMARY':'direct://local','A_CN_EGRESS_CHECK_URL':f'http://127.0.0.1:{mock_port}/healthz',
                    'A_CN_ACCOUNTS_JSON':json.dumps([{'id':'acc1','provider_account_id':'provider-acc1',
                                                      'secret_ref':'env:A_CN_ACCOUNT_1_TOKEN','enabled':True,'metadata':{}}]),
                    'A_CN_ACCOUNT_1_TOKEN':'fixture-upstream-token'})
        log=open(root/'supervisor-test.log','w')
        worker=subprocess.Popen([sys.executable,'-m','app.supervisor'],cwd=root,env=env,stdout=log,stderr=log)
        base='http://127.0.0.1:18090'
        async def wait_state(value=None):
            deadline=time.monotonic()+45
            while time.monotonic()<deadline:
                if worker.poll() is not None:
                    log.flush()
                    raise AssertionError('supervisor_exited: '+ (root/'supervisor-test.log').read_text(errors='replace')[-6000:])
                try:
                    async with httpx.AsyncClient(timeout=1) as client:
                        response=await client.get(base+'/healthz')
                        if response.status_code==200:
                            state_file=db.parent/'update-state.json'
                            state=json.loads(state_file.read_text()) if state_file.exists() else {}
                            if value is None or state.get('state')==value:
                                return state
                except (httpx.HTTPError,ValueError,OSError):
                    pass
                await asyncio.sleep(.2)
            raise AssertionError('supervisor_not_ready')
        try:
            await wait_state()
            (root/'docs'/'update-test.md').write_text('benign fixture update\n')
            git(root,'add','docs/update-test.md')
            git(root,'commit','-m','benign candidate')
            candidate=git(root,'rev-parse','HEAD')
            git(root,'reset','--hard',baseline)
            opened=asyncio.Event()
            async def in_flight():
                async with httpx.AsyncClient(timeout=20) as client:
                    async with client.stream('POST',base+'/gw/a-cn/v1/chat/completions',headers={'Authorization':'Bearer '+env['ADMIN_TOKEN']},json={'model':'mock-slow','messages':[],'stream':True}) as response:
                        content=b''
                        async for chunk in response.aiter_raw():
                            opened.set()
                            content+=chunk
                        return response.status_code==200 and b'[DONE]' in content
            pending=asyncio.create_task(in_flight())
            await asyncio.wait_for(opened.wait(),5)
            state_file=db.parent/'update-state.json'
            state_file.write_text(json.dumps({'state':'pending','previous':baseline,'candidate':candidate}))
            completed=await pending
            applied=await wait_state('applied')
            assert completed and applied['commit']==candidate
            (root/'app'/'main.py').write_text('this is intentionally invalid python !!\n')
            git(root,'add','app/main.py')
            git(root,'commit','-m','invalid candidate')
            broken=git(root,'rev-parse','HEAD')
            git(root,'reset','--hard',candidate)
            state_file.write_text(json.dumps({'state':'pending','previous':candidate,'candidate':broken}))
            rolled=await wait_state('rolled_back')
            assert rolled['commit']==candidate and marker.read_text()=='DO_NOT_OVERWRITE=fixture\n'
            # Phase C: manual two-step flow. "available" must never arm the supervisor by itself,
            # and the apply endpoint must stay gated on admin auth plus trusted-repo validation.
            (root/'docs'/'update-test-2.md').write_text('manual candidate\n')
            git(root,'add','docs/update-test-2.md')
            git(root,'commit','-m','manual candidate')
            manual=git(root,'rev-parse','HEAD')
            git(root,'reset','--hard',candidate)
            state_file.write_text(json.dumps({'state':'available','previous':candidate,'candidate':manual}))
            await asyncio.sleep(3)
            still=json.loads(state_file.read_text())
            assert still['state']=='available', still
            async with httpx.AsyncClient(timeout=5) as client:
                assert (await client.get(base+'/healthz')).status_code==200
                denied=await client.post(base+'/api/updates/apply')
                assert denied.status_code==401, denied.status_code
                applied_attempt=await client.post(base+'/api/updates/apply',headers={'Authorization':'Bearer '+env['ADMIN_TOKEN']})
                assert applied_attempt.status_code==200, applied_attempt.text
                # No origin configured in the disposable repo: validation must fail closed,
                # never arming "pending" from an untrusted source.
                result=applied_attempt.json()
                assert result['state']=='failed' and result['last_error']=='fetch_or_validation_failed', result
            assert json.loads(state_file.read_text())['state']=='failed'
            print(json.dumps({'graceful_update_stream_completed':True,'same_supervisor_pid':worker.pid,
                              'candidate_applied':True,'invalid_candidate_rolled_back':True,'runtime_env_preserved':True,
                              'available_state_not_auto_applied':True,'apply_endpoint_admin_gated':True,
                              'untrusted_apply_fails_closed':True,
                              'scope':'disposable local Git repo; GitHub transport tested separately on cloud'}))
        finally:
            if os.name == 'nt':
                # terminate() only kills the supervisor on Windows; the uvicorn child
                # would keep proxy.db open and break TemporaryDirectory cleanup.
                subprocess.run(['taskkill','/T','/F','/PID',str(worker.pid)],capture_output=True)
            else:
                worker.send_signal(signal.SIGTERM)
            try: worker.wait(timeout=20)
            except subprocess.TimeoutExpired:
                worker.kill();worker.wait()
            mock.terminate()
            try: mock.wait(timeout=5)
            except subprocess.TimeoutExpired:
                mock.kill();mock.wait()
            log.close()


if __name__=='__main__':
    asyncio.run(main())
