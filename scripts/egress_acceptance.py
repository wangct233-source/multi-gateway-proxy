from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path

import httpx

from app.config import GatewayConfig
from app.egress.pool import EgressPool
from app.config import Config


async def main():
    global_config=Config.from_env()
    cfg=GatewayConfig('a-cn','A','A_CN',primary='http://127.0.0.1:18199',backup='http://127.0.0.1:18105',check_url='http://127.0.0.1:18080/healthz')
    failed=EgressPool(cfg,global_config)
    other=EgressPool(global_config.gateways['b'],global_config)
    absent=EgressPool(GatewayConfig('c','C','C',backup='http://127.0.0.1:18105',check_url='http://127.0.0.1:18080/healthz'),global_config)
    try:
        await asyncio.gather(failed.check(),other.check(),absent.check())
        selected=await failed.select()
        assert selected and selected.role=='backup'
        assert (await other.select()).role=='primary'
        assert await absent.select() is None
        await failed.fail(selected,'test_risk_hold')
        assert await failed.select() is None
        assert (await other.select()).healthy
        print(json.dumps({'primary_connect_failed_own_backup_used':True,'risk_hold_stops_own_gateway_without_borrowing':True,
                          'empty_primary_cannot_use_backup':True,'other_gateway_unaffected':True,
                          'scope':'separate loopback HTTP proxy endpoints, not distinct public IPs'}))
    finally:
        await asyncio.gather(failed.close(),other.close(),absent.close())


if __name__=='__main__':
    asyncio.run(main())
