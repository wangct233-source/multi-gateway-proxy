# 更新与回滚

默认关闭。/api/updates/webhook 校验原始body的sha256 HMAC（constant-time），只响应指定仓库refs/heads/main push；poll300秒。真实GitHub远端未配置，公网URL未提供，不制造callback。

**两步式更新（2026-10-02 起，默认）**：`UPDATES_AUTO_APPLY=false`（默认）时，`POST /api/updates/check`（webhook/轮询同路径）只完成 fetch+校验并把候选标记为 **available**，supervisor 明确忽略 available，不排空、不重启；只有管理员再调 `POST /api/updates/apply`（UI「立即更新」按钮）才把 available 转为 pending 并触发排空+切换+验证+回滚路径。apply 会重新 fetch 校验（防候选漂移），非 available 状态或校验失败一律不武装更新（fail closed）。`UPDATES_AUTO_APPLY=true` 保留旧的「检查即应用」行为。/api/updates/status 与 /api/v1/version 返回 available/pending/applied/rolled_back/failed 等状态、当前与候选 commit 及提交说明（candidate_subject）。

/app可写受信git仓库。Updater fetch main、不覆盖在途代码，验证ff-only祖先关系和clean工作树后写pending。Supervisor标准库小守护进程+一个uvicorn异步HTTP worker；不是4worker。新请求停止接受，普通请求/在途流按max(UPDATES_GRACE_SECONDS,STREAM_TOTAL_TIMEOUT)排空，再git reset trusted candidate。容器不重启，期间可能短暂连接拒绝，不宣称零停机。

compile/import及健康检查失败自动恢复previous commit。依赖文件变化拒绝在线安装，需重新构建镜像。迁移只可向后兼容，代码回滚不能撤销业务操作/数据库破坏性迁移。禁git clean，不允许候选跟踪.env/data/logs/runtime/keys/secret/db/key路径。

运行时只读deploy key位于仓库外runtime/keys/backend_deploy，只读挂/app/runtime/keys，trusted known_hosts配套。禁止GH_TOKEN/GITHUB_TOKEN或本机gh目录进镜像；公网UI匿名Release读取。

2026-10-02 实际交付：仓库已创建并完成首次push（见acceptance.md）；deploy key（ed25519，`multi-gateway-readonly`，read_only=true）已生成于仓库外runtime/keys并注册到后端私有仓；known_hosts取自GitHub官方meta API并与官方文档指纹三算法交叉核验一致（Ed25519/ECDSA/RSA），DSA弃用。容器内fetch已实测通过。

scripts/github_setup.py：先gh版本/auth/identity检查，再dry-run；--apply才创建私有backend/public UI并push已审核main提交，重名即停。**本轮初始建仓经由GitHub REST API + 用户本机凭据管理器令牌完成（身份wangct233-source，scopes含repo），gh CLI仍缺失，该脚本保留供日后复用；重名检查会按设计拒绝重复建仓。**key外置read_only true。需要PUBLIC_BACKEND_URL HTTPS与足长环境secret后才创建webhook，无值保持pending——**webhook本轮仍未配置（无公网HTTPS回调），线上更新依赖300s轮询实测通过**。

已在一次性Git副本测试：在途slow SSE保持DONE、候选成功应用、同supervisor进程、不合法Python候选自动回滚、.env内容保留。2026-10-02云端私有仓实测通过同链路（见acceptance.md），webhook公网交付仍未测。
