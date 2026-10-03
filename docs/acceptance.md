# 实测与未完成项 · 2026-10-02

## 范围

用户授权服务器容器下部署测试。独立目录 `/opt/multi-gateway-proxy-test-20261002`，Compose project `mgp-test`，端口仅127.0.0.1:8000，未修改面板/nginx/firewalld/Docker daemon。UI仍本机纯静态。服务器2CPU、约1.8GiB内存、Docker26.1.4/Compose2.27.1、vfs；Python容器3.12.15、UID10001。

四逻辑网关全以显式OpenAI-compatible fixture测试，各4个测试账号，各8槽、账号2槽，各队列32等待15s。四不同loopback HTTP代理端口一一绑定；G1另有本网关备用。**不是四个独立公网IP；没有真实厂商账号/调用/签到/奖励操作。** fixture和benchmark后续各用独立临时容器，只有app是正式部署服务；它们不是生产依赖。

## 正确性

- 核心最终10项unittest：本机3.14.5与云端3.12.15全部PASS；新增批次内错误写入不回滚无关项、内部Token不得引用为上游凭据；队列满/超时/取消、账号gateway复合隔离、幂等release、字节跨块UTF8、断连、慢客户端总期限、URL拼接/凭据替换、HMAC/config fail-closed。
- container_acceptance.py：23项PASS；4路同时流式中文/工具/心跳/DONE、管理鉴权、四路任务501与dryrun禁止执行、总杀开关、CORS精确预检、queue满429不影响其他网关、断连release、账号PATCH隔离、SQLite WAL/FK。
- egress_acceptance.py：主出口连接失败只选自身backup，backup risk hold后不借用B出口，空primary不能使用backup，其他gateway unaffected，PASS。
- runtime_acceptance.py：人为压低内存软目标后429准入拒绝/有效并发减半、同端口WS握手501、ADMIN_TOKEN上游引用拒绝，PASS。
- 重建测试夹具时曾绑定旧容器network ID无法启动，已仅重建命名fixture修复；应用首轮检测早于下一次出口health而503，等待正常探测后完整复验PASS。最终构建保留所有跟踪文件，工作树干净；不将这些环境失败隐藏。
- update_acceptance.py：一次性本地Git副本在途SSE完整结束，候选应用，同supervisor PID，非法候选回滚，.env保留，PASS。GitHub远端传输未测。

## GitHub 热更新实测（2026-10-02，新增）

前置：gh CLI 本机/服务器均缺失；经用户授权改用 GitHub REST API + 本机凭据管理器已存令牌（身份核验 wangct233-source，OAuth scopes: gist, repo, workflow）。令牌只在本机变量与请求头中使用，未打印、未入 Git/镜像/聊天。

- 建仓：`wangct233-source/multi-gateway-proxy`（private）与 `wangct233-source/multi-gateway-proxy-ui`（public），201；名称预检 404 后创建；本地 main 分别推送（fb08c67 / c5ed0c2），ls-remote 与本地一致。推送前审查 tracked 文件与全历史，未发现密钥/服务器 IP/密码模式。
- 只读 deploy key：ed25519（`multi-gateway-readonly`，指纹 KEKm1UjxsWkAuxk7/gZqNuGj2QrBquRpeVoaM05XVJI）生成于仓库外 `runtime/keys/`（.gitignore 已排除），API 注册 read_only=true（id=165158934）。SFTP 上传云端 `/opt/runtime/keys/`（600，属主10001），三文件 md5 双端一致。
- known_hosts：GitHub 主机公钥取自官方 `api.github.com/meta`，指纹与官方文档页三算法（Ed25519/ECDSA/RSA）交叉核验一致后写入；本机经 ssh.github.com:443 验证 key 被接受（GitHub 以仓库名身份应答）。本机 github.com:22 不通，云端 22/443/api 实测均通。
- 云端配置：容器与宿主机源码目录 origin 均指向 `git@github.com:wangct233-source/multi-gateway-proxy.git`；`.env` 增改 `UPDATES_ENABLED=true`、`UPDATES_REPO_SLUG=wangct233-source/multi-gateway-proxy`；容器重建（RestartCount 归零，资源限制/compose 不变）后重建共享网络命名空间的命名 fixture 容器。
- 预检：容器内以 updater 同款 GIT_SSH_COMMAND fetch 私有仓成功（FETCH_HEAD=fb08c67），工作树 clean，requirements 与运行版一致（ecb938d..fb08c67 仅 docs）。
- **更新实测**：POST /api/updates/check → pending(ecb938d→fb08c67) → **applied**；`/healthz` 200；容器 RestartCount=0（supervisor 仅重启 worker，容器未重建）；容器 HEAD=fb08c67。
- **坏候选回滚实测**：推送含语法错误的提交 0b72626 → pending → draining → **rolled_back**（last_error=candidate_failed）；容器代码自动恢复 fb08c67，healthz 200，零容器重启。
- **恢复更新实测**：revert 提交 214a45c → **applied**；healthz 200；更新后非流式 200 + 流式 SSE 至 `[DONE]` 业务冒烟通过。
- 边界：webhook 未配置（无公网 HTTPS 回调地址，PUBLIC_BACKEND_URL 空），线上更新依赖 300s 轮询，已实测；公网 webhook 交付仍未测。UI 公共仓匿名 `git ls-remote` 可读；GitHub Release 未发布（UI 启动匿名 Release 检查代码在，真实 Release 路径仍未测）。

## 两步式更新与服务器托管 UI（2026-10-02，新增）

**两步式热更新（后端 9a3dd20）**：`UPDATES_AUTO_APPLY` 默认 false。云端实测：push 提交 7ce76f8 → `POST /api/updates/check` 返回 state=**available**（含 previous/candidate/提交说明），等待 4 秒状态不变、worker HEAD 不变、healthz 200——supervisor 正确忽略 available；`GET /api/updates/apply` 405。本机 `scripts/update_acceptance.py` 扩展后全绿：在途 slow SSE 在排空中完整走完 `[DONE]`（Windows 端 supervisor 改用 CTRL_BREAK 触发 uvicorn 优雅停机，Linux SIGTERM 路径不变）、候选应用、坏候选回滚、.env 保留、**available 状态不自动应用**、apply 端点未带令牌 401、不受信来源 apply fail-closed（state=failed 而非 pending）。单测 11 项全过（新增 apply 状态机测试：available→apply→pending、pending 期间 check 不重拉、up_to_date 不武装、auto_apply=true 直达 pending、校验失败→failed）。

**服务器托管 UI（用户选定方案 A）**：静态 UI（4 个公开文件，无密钥）上传至 `/opt/multi-gateway-proxy-ui-static`；nginx 原装默认配置增加一行 vhost include + 独立 vhost `mgp-ui.conf`（监听 18443 ssl，自签证书 CN=IP，静态根 + `/api|/healthz` 同源反代 127.0.0.1:8000，`proxy_buffering off`、read/send timeout 700s）；`/gw` 数据面**未**对公网开放（管理令牌曾在聊天中出现，待轮换后再议）。防火墙仅新增 18443/tcp，既有端口未动；改动前 nginx.conf 与 vhost 列表备份至 `/root/_mgp_ui_nginx_backup_*`；首次因 CentOS7 openssl 无 `-addext` 生成证书失败，nginx -t 失败后脚本按设计自动撤下 vhost，修正后 nginx -t 通过并 reload。实测：本机 curl `https://127.0.0.1:18443/` UI=200、`/healthz`=200、无令牌 `/api`=401；公网 `https://<your-server-ip>:18443/` UI=200 且返回新版更新卡片。UI 同源模式：BaseURL 留空即用当前页面地址（app.js 同源回退）。浏览器端到端点击验收：机器人测试被用户接管——用户本人于公网打开 https://<your-server-ip>:18443/ 确认页面正常加载（"正常"）；连接后首次「立即更新」点击由用户完成，apply 链路已有 API 级与本机 E2E 证据，此处不代称已验收。

## 账号体系与 CodeBuddy 直连（2026-10-02，新增）

**G1/G2 真实上游直连（a99100a）**：适配器按官方桌面客户端协议补齐出站身份（Bearer + X-User-Id + X-Machine-ID/X-Session-ID 由 uid 稳定派生 + X-IDE-Type/Version/Product + X-Domain + UA/Origin/Referer/Accept-Language 分 realm；来源 wb_identity.py:76-105、wb_accounts.py:449-504、wb_fingerprint.py:11-24）。uid 优先从 access token JWT sub 提取。云端 .env 切换 A_CN→https://copilot.tencent.com、A_INTL→https://www.workbuddy.ai（mode=a、direct://local 出网）；实测请求真实到达腾讯 APISIX 网关返回 401（fixture 假 token 的预期响应）——链路通，待真实账号。工具配对修复前带 tools 请求仍 501（含 `tools:[]` 空数组的边界已修复为按键存在性判断）。

**账号登录链接 + 四格式导入（383a7c1）**：DB v2 幂等迁移（accounts 加 secret_inline 列，user_version 保持 1 保证回滚后旧代码可启动；实测老库自动升级）。`POST /api/v1/gateways/{id}/accounts/login-link` 真实调用腾讯 `/v2/plugin/auth/state` 成功返回 state+authUrl（实测 state=46649bfb…、10 分钟有效）；`GET …/login-status` 轮询 `/v2/plugin/auth/token`（code=11217 pending/0 取 accessToken 入库）。`POST …/accounts/import` 自动识别 A/A2（扁平 accessToken 与嵌套 auth/account）、B（token/user_id）、C（apiKey+secret 拼接、userId）及裸字符串四种格式，实测三格式各 1 项全部 imported；凭据跨网关复用仍被拒绝；secret_inline 永不出现在 API 响应（public_accounts 仅显示 source=imported/env）。单测 13 项全过。UI 账号页重做：删除手动 env 引用表单，改为「生成登录链接（仅 G1/G2）」+「导入 JSON 文件/粘贴」（多文件、数组/单对象、≤100 项），登录状态 3 秒自动轮询。本轮热更新实测：check→available→apply→applied→HEAD=383a7c1、healthz 200、schema 自动迁移，测试导入数据已清理。

## 镜像模式热更新与开源（2026-10-02，新增）

**开源**：仓库 wangct233-source/multi-gateway-proxy 已转公开（0569de0 起 Git 历史重写为单一干净提交，旧历史中服务器 IP 不再对外可见）；新增 MIT LICENSE、README 使用须知（上游服务条款风险提示，同 sub2api 做法）。UI 仓保持公开。

**镜像级自动更新（替代源码热更新）**：GitHub Actions 在每次 main push 后自动构建完整镜像并发布 ghcr.io（tags: sha-<full-commit> 与 latest），依赖全部打进镜像——requirements.txt 变化不再阻断更新。容器内 updater 改为镜像模式：check 经公开 GitHub API 比对远端 main；apply 向数据卷写 image-update-request.json；宿主机 cron 脚本（每分钟，flock 防重叠）拉取对应 sha 镜像 → 写 .env MGP_IMAGE → compose 重建（docker stop 620s 宽限内 supervisor 优雅排空在途流）→ 健康检查 → 成功写 image-update-status.json 并自动清理旧镜像（保留 2 个），失败自动回退上一个镜像。云端实测：CI 构建成功（run 37029891837）、ghcr 匿名拉取通过、容器切换至 ghcr 镜像 0569de0、check=up_to_date、fixture 与真实上游冒烟正常。单测 13 项全过（apply 死锁修复：锁内复检改用 _check_locked）。首次部署踩坑记录：宿主机源码目录停在 ecb938d 旧 compose（硬编码镜像名）导致切换无效，已从镜像内提取新 compose 修复——镜像模式下宿主机只需 compose 文件与 .env。

## G3/G4 原生协议移植（2026-10-03，新增）

**G3 b-remote（a2f9902，新 Python 实现，机制源自 Trae2api-cn 参考源码只读审阅）**：两步会话协议——`POST /chat_sessions`（flatten_query 拍平消息、agent_type/agent_id=solo_agent_remote、common_params 含 token+uid 稳定派生 device_id）→ `GET /chat_sessions/{id}/events?reply_to_message_id=` 读私有事件帧。网关侧把累积快照 message 事件计算为文本增量，heartbeat 转 SSE 注释帧，token_usage 映射 usage，done 缺失按不完整回合报错而非伪装成功。认证 `Cloud-IDE-JWT`，origin/referer 按 solo.trae.cn。

**G4 c-anthropic（同 commit）**：OpenAI↔Anthropic Messages 双向转换——system 抽取、tool_calls↔tool_use、tool↔tool_result、流式 content_block_delta(text_delta/input_json_delta)→chunk 增量、stop_reason 映射（end_turn→stop、tool_use→tool_calls、max_tokens→length）。双认证头 x-api-key+Bearer，anthropic-version 2023-06-01；签名路径按源码 fail-open 事实仅走免签 LLM 主路径。

**测试与部署**：单测 19 项全过（新增 6 项协议测试，含跨传输块 UTF-8 行解析——发现并修复逐块 decode 截断缺陷，改为字节级缓冲）；`scripts/native_acceptance.py` 本机四场景全绿（G3/G4 流式+非流式+工具调用，快照无重复累积）；mock_upstream 增加 chat_sessions/messages 模拟（并修复 keep-alive 下 POST body 未消费导致的连接错位）。部署链路随外部会话的 v1.0 镜像模式演进：push→GHA build-image→云端拉 GHCR 镜像 sha-a2f9902 重建 app 与 fixture（MGP_IMAGE 已固化进云端 .env；fixture 曾误用旧 local 镜像致新 mock 路由缺失，已换同版本镜像）。云端实测：四网关 ready；G3/G4 经 /v3 /v4 公网数据面流式+非流式返回正确 OpenAI 格式。**状态：mock 验证通过；真实 Trae/Zcode 上游未接（等待用户提供真实账号）**。B/C 出口改 direct://local（容器启动早于 fixture 的首轮检查失败会 60s 隔离后自愈）。

## 按网关隔离的风控体系（2026-10-03，97f4b36）

**设计原则（用户明确要求）**：A/B/C 三家上游风控体系互不通用（腾讯业务码 / 字节 9074 / 智谱 IP 级 3012），参数不可互换——`app/risk/` 骨架只提供分类/退避/封顶的通用机制，错误码语义、冷却时长、任务间隔全部写在各网关专属策略文件（codebuddy.py / trae.py / zcode.py），互不引用。

- **错误分类冷却**：proxy() 非 200 响应统一走 apply_risk——解析响应体业务码（兼容 CodeBuddy 业务码/OpenAI error/Anthropic error 三形态）→ 策略分类 → 落地（账号冷却内存+DB / 模型冷却 / 出口冷却 / 停用）。CodeBuddy：6004 只冷模型 300s 不冷账号、11102 负缓存 6h、11140 停用、余额类冷到次日 04:00、WAF 403 账号+出口抖动冷却；Trae：9074 指数退避 60s→1h 封顶（连续失败计数，当日封顶）；Zcode：3012 网关级静默（无独立出口 IP 的显式退化）10min→6h 封顶次日 00:00、3009 尊重 Retry-After 模型冷却、login_required 停用。
- **模型级冷却**：Runtime.model_cooldown 热状态；准入前检查，冷却中的模型返回 429+Retry-After，不影响账号与其他模型；管理 API public() 暴露 model_cooldowns。
- **token 提前刷新**（G1/G2）：accounts v3 迁移加 refresh_inline（幂等，回滚兼容）；导入自动提取 refreshToken；a 模式请求前检查 JWT exp（5 分钟缓冲），轮换锁串行双检，POST /v2/plugin/auth/token/refresh 成功后写回 DB 并热更新租约。B/C 无刷新协议证据，保持不实现。
- **一键批量签到**：POST /api/v1/tasks/batch-run 逐网关执行，各网关用各自的策略间隔错峰（CodeBuddy 45s / Trae 60s / Zcode 30s + 0~25% 抖动）；仍受杀开关/证据/窗口/每日限额约束（云端实测杀开关开启时 403）；UI 任务页新增批量按钮。claim/activity 仍 501 evidence_required。
- **测试**：单测 20 项全过（新增策略隔离+分类+退避递增+封顶+三形态错误提取）；native_acceptance 四场景回归全绿；云端部署 97f4b36 镜像实测四网关 ready、门禁 403、G1 真实链路 401（预期）、G3 mock 流式正常。

## 网关面板化（2026-10-03，b2cd598，学 A 原面板 dashboard.html 机制）

- **命名**：A-1 腾讯国内 / A-2 腾讯国际 / B TRAE CN / C Zcode（config.py GATEWAYS，全链路生效——云端实测 /api/v1/gateways 返回新名）。
- **用量统计**：`GET /api/v1/gateways/{id}/usage`（admin）——request_logs 聚合 24h 总数/成功/失败/流式/平均耗时/成功率 + 错误分类 Top10 + 最近 50 条 + 配置模型清单 + 模型冷却倒计时。云端实测 a-cn：2220 请求、成功率 99.55%（历史压测残留数据，符合预期）。
- **上游余额查询**：`GET …/credits`（A/B/C 各自协议，ae7e7ea）——A：POST {billing 域}/v2/billing/meter/get-user-resource（国内 billing=www.codebuddy.cn 与 chat 域分离），聚合套餐 remain/used/size；**B（强证据 trae_client.py:536-555,618-638,695-738）**：POST api.trae.cn/trae/api/v2/ug/checkin_credits/status（签到状态 checked_in/credits）+ POST …/pay/ide_user_ent_usage（entitlement 套餐 credits_limit/usage.credits_amount → total_limit/used/remaining），Cloud-IDE-JWT + x-device-id 稳定派生；**C（强证据 routes-quota.ts:219-258）**：GET zcode.z.ai/api/v1/zcode-plan/billing/balance → data.balances[] remaining_units/total_units——**仅 start-plan JWT 可查**，apiKey-only 账号如实 501 evidence_required（不冒充）。三家 5 分钟缓存+单账号租约。云端实测（ae7e7ea 镜像）：B 真实链路 401 透传（假令牌预期）、C 如实 501 提示需 JWT、A 同前。
- **UI**：新「模型与用量」页签（KPI 卡 + 模型表 + 最近请求表 + 模型冷却提示 + 余额查询按钮全网关显示，各网关显示各自协议说明）；任务页新增每网关「立即签到 / 领取奖励」按钮（真实执行，claim 缺证据如实 501）。UI 文件已同步服务器 18443。

## UI 重构（2026-10-03，ui b69199a/092098d，用户要求学 sub2api + 本地项目 C 的 UI 与交互）

- **触发**：用户明确不喜欢旧 UI，指定参考 sub2api（交互）与本地 Zcode 反代项目（视觉）。子代理分析两者得出模式清单：深色 CSS 变量主题、卡片网格、胶囊导航、左边框 3px 状态色条、进度条余额、5s 可见轮询、diff-key 复用 DOM（视觉抄 Zcode）；工具条搜索/筛选、批量操作、Toggle、危险操作 confirm、DataTable（交互抄 sub2api）。纯静态无框架不变。
- **布局重做**：左侧 240px 侧边栏 + 六胶囊导航（全局监控 / 四网关独立视图 / 全局设置与更新）；网关视图共用一个模板 section（JS 切换 data-gw），五页签：账号池 / 模型与用量 / 任务 / 设置 / 证据（capabilities 从设置页拆出独立「证据」页签，修复原 HTML 缺 pane-caps）。
- **账号池**：表格改卡片网格（.acc-grid），状态左边框色条 ok/cooldown/disabled/bad，工具条搜索（按 id/provider_account_id）+ 状态下拉筛选 + 行内启停按钮；「登录获取账号」仅 A 两网关显示（data-show=a）；导入 JSON 区 Toggle 折叠。
- **模型与用量**：余额改进度条（.bar，<20% 红 / <50% 金，四网关各自协议文本不变）；用量 KPI 卡；模型清单 chips 化。
- **监控**：四网关卡片网格（.gwcard，状态色条，整卡可点击跳转对应网关视图）+ 全局批量签到 + 杀开关徽章（阻断=红/关闭=绿）。
- **工程校验**：node --check 通过；脚本交叉校验 app.js 引用的 70 个 DOM id 全部存在于 index.html；本地 http.server + 浏览器代理实测七项（导航/页签/空态/深色主题/视图切换）全部通过后修复最后一项（accounts-grid 初始占位文案）。
- **部署**：UI 仓 push（b69199a + 092098d release.json 0.2.0）→ uipush.py SFTP 同步 4 个静态文件至 /opt/multi-gateway-proxy-ui-static → 公网 https 18443 实测返回新版 HTML/JS，healthz 200。后端镜像未动（后端仓无改动，UI 由 nginx 静态托管不进镜像）。styles.css 顺带清理 #1f3busy 笔误。

## 过期更新状态遮盖修复（2026-10-03，cc6adf4，云端实测踩坑）

- **现象**：push 文档提交 5e4f4a3 后 `POST /api/updates/check` 返回 state=applied 但容器自报 commit=d44e6ac、实际运行镜像却是 sha-ae7e7ea——三者互相矛盾。
- **根因**：`data/image-update-status.json` 是 01:30 cron 应用 d44e6ac 时写的；随后上个会话手动把 `.env` MGP_IMAGE 改回 sha-ae7e7ea 重建（04:56Z），未删状态文件。`Updater.status()` 无条件合并 host 状态 → 旧的 applied 永久遮盖真实状态；`apply()` 见非 available 只重新 check 永不武装——更新链路被卡死。
- **修复（cc6adf4）**：`status()` 只在 host 状态文件的 image 与运行镜像（MGP_IMAGE）一致时才采纳；不一致视为过期忽略（不带 image 字段的旧格式保持原行为）。新增单测 test_update_stale_host_status_is_ignored，21 项全过。
- **云端解锁与实测**：备份并删除过期状态文件（image-update-status.json.bak-stale-20261003）→ check=available（candidate=cc6adf4）→ apply=applying → 约 2 分钟后 applied，容器切至 sha-cc6adf4、healthz 200、四网关 ready、18443 返回重构后新 UI。本轮验证了「手动回滚后更新链路仍可用」，修复后此类操作不再需要手工清状态文件。
- **第二层修复（5951134）**：cc6adf4 上线后再次实测发现同类残留——cron 应用成功后留下的 applied 状态文件（image 与运行一致，不会被第一层 guard 过滤）在**下一次**上游有新版本时照样遮盖 available，apply 拒绝武装（73ed9b4 的 apply 被吞，容器停在 cc6adf4）。修复：`_check_locked` 发现 available 时删除已被取代的旧 applied 状态文件。云端全链路复验：5951134 部署后 push 文档提交，**未做任何手工清理**，check=available（旧 applied 自动清除）→ apply=applying → applied，容器切至 sha-5951134、healthz 200；再 check 报 applied/commit=5951134（此时 applied 记录与运行版本一致，语义正确）。
- **附注**：容器重建后 b/c 网关显示 paused——其 EGRESS_CHECK_URL=127.0.0.1:18080/healthz 指向 fixture 容器内的 mock 上游，app 容器自身 loopback 不可达，属如实上报（b/c 本就待真实账号/上游），与本轮改动无关；接入真实上游后自愈。

## 管理页密码登录（2026-10-03，88b07a8 / ui e92a6c7 v0.3.0，按用户要求取消 token 输入）

- **需求**：用户要求取消"管理 token 粘贴"，改为访问网页输密码，默认 `admin`，进去后可在设置页修改。
- **后端（88b07a8）**：新 `app/admin_auth.py`——密码哈希（每安装随机 salt + sha256）存新 `admin_auth` 单行表（SCHEMA 追加式，老代码/回滚兼容；曾试复用 settings 表被外键拒绝）。`admin()` 双钥匙：网页密码或 env `ADMIN_TOKEN`（保留为应急主钥匙，忘记密码可救援）。`POST /api/v1/auth/login`（公网暴露+默认弱密码的爆破防护：同 IP 连续错 5 次锁 60 秒，内存态）与 `POST /api/v1/auth/password`（需当前密码，新密码 6-128 位，落库即时生效）。数据面 DATA_TOKENS 不变。
- **前端（ui e92a6c7，v0.3.0）**：连接条改「管理密码」登录框（默认密码提示+锁定提示）；设置页新增「修改管理密码」卡片（当前/新/确认 + 默认密码黄色徽章，登录发现默认密码弹红色警示 toast）；修掉账号卡片渲染 `choice` 作用域错误（上轮引入，浏览器实测抓到）。
- **验证**：单测 22 项全过（新增 AdminAuth 播种/持久化/改密/锁定用例）。本地全栈（18083+18081）API 级全流程：admin 登录 200+default_password=true、错密码 401、改密后旧密码 401/新密码 200、5 连错后正确密码也 429、master 钥匙不受锁定影响；浏览器端到端七步全过（错误提示/登录/徽章/改密/新密码复登/旧密码拒绝）。顺带修正 local_test.py 四网关同值测试令牌触发跨网关凭据复用保护的问题（改为按网关区分），旧的 runtime/local/proxy.db 被幽灵句柄锁死删不掉，本地测试改用独立库文件。
- **云端**：check=available → apply → applied（全程无手工干预，验证了上轮自愈修复），容器切至 sha-88b07a8。实测：`admin` 登录 200（default_password=true）、错密码 401、密码可当 Bearer 用、master 钥匙仍可用、18443 返回 v0.3.0 登录界面。**云服务器当前就是默认密码 admin，等用户在设置页自行修改**。
- **追加修复（d2fc26a）**：a66d3b5（纯文档）更新重启后 `default_password` 变 false——is_default 标志只存内存，重启从表读回时一律当非默认。修复：is_default 不落库，每次从存储哈希反推（存的是 admin 的哈希即默认）。单测补断言，云端应用后终验 `admin` 登录 default_password=true。

## 补齐宿主机更新脚本入库（2026-10-03，c4b7f39，回答"别的服务器如何跟随仓库更新"）

- **背景**：用户问另一台服务器部署源码后如何更新。按代码核对：updater（镜像模式）只做 check（公开 GitHub API 比对 commit）与 apply（写 `data/image-update-request.json`），**消费请求文件的宿主机脚本不在仓库**——只存在于云端宿主机 `/opt/mgp-ops/image-update.sh`（/etc/cron.d 每分钟 flock 驱动），他人部署拿不到，docs/updates.md 还停留在废弃的源码模式描述。
- **入库（80860b5+c4b7f39）**：取回线上脚本做通用化（`MGP_DEPLOY_DIR`/`MGP_COMPOSE_PROJECT`/`MGP_IMAGE_REPO`/`MGP_HEALTH_URL` 环境变量配置，逻辑与线上逐行一致），落 `scripts/host-image-update.sh`（git 100755）；新增 `.gitattributes` 强制 `*.sh text eol=lf`；重写 docs/updates.md 为镜像模式并给出新服务器三条更新路径（手动 pull latest / 半自动 UI / 挂 cron 全自动）。排查插曲：PowerShell 管道会把 git 输出重编码成 CRLF，曾误判脚本为 CRLF 行尾——用 python 直接读 blob 核实为纯 LF（2919 字节）后才提交可执行位与 .gitattributes，未引入实际行尾改动。
- **云端**：check=available → apply → applied → healthz 200，容器切至 sha-c4b7f39（自愈链路再次全程无手工干预）。

## UI v0.4.0 静音控制台重构 + 总览页用量分析（2026-10-03，ui d805070）

- **触发**：用户给出完整视觉 brief（"安静的调度控制台"：深墨蓝底/发丝描边/暖橙单点缀/低饱和状态色/等宽数字/极轻动效/禁霓虹渐变玻璃），并要求总览页三块用量分析（模型分布、Token 使用趋势、最近使用 Top12），明确约束"不写后端、不建表、不做数据聚合"。
- **数据实情**：request_logs 无 model/token 列，`/usage` 仅有请求数/成功失败/最近50条/模型清单/模型冷却。三块分析按现有数据实现到最接近版本：用量趋势=各网关最近 50 条记录按小时聚合（成功/失败堆叠柱）；模型分布=各网关模型清单+冷却状态徽章；Top12=账号维度（请求数/成功率/最近活跃）。**真正的 Token 明细与按模型调用量需要后端加列记录，本轮遵守约束未做**。
- **实现**：styles.css 全量重写（双主题 CSS 变量体系，暖橙仅用于导航指示条/主操作/焦点；rise/toast 极轻动效；prefers-reduced-motion；细滚动条）；index.html 增线性图标（symbol+use，stroke 1.5 圆头）、侧栏主题开关、三块分析容器；app.js 增主题切换（localStorage 记忆）与 loadAnalysis（四路 /usage 并取、20 秒节流、仅监控视图、与 5 秒状态轮询解耦）。
- **修掉两个潜伏 bug**：① SVG 元素没有 hidden IDL 属性，主题图标切换失效（浏览器验收抓到，改用 class）；② 状态判断写成 `status === 'ok'`，而接口实际返回 `'ready'`——网关卡/账号卡自 v0.2.0 起一直错误显示红色"异常"。
- **验证**：node --check + DOM id/图标引用交叉校验；本地全栈（b/c 用 b-remote/c-anthropic mock 模式，四路共 60 条真实请求）浏览器端到端两轮：三块渲染（4 行×2 模型、"24h 合计 60 请求 · 成功率 100.0%"、Top12 十二行）、暗/亮双向切换含图标、五页签与设置页回归、控制台零报错。
- **部署**：UI 仓 d805070 推送（v0.4.0）→ uipush 同步 → 公网 18443 实测返回 v0.4.0 且 healthz 200；后端仓本次仅本文档变更。

## 性能（实测，非容量承诺）

| 负载 | 总请求 | 客户端在途峰值 | 总QPS | 总延迟p50/p95 ms | 失败率 |
|---|---:|---:|---:|---:|---:|
| 首轮SSE（所有测试进程同容器，污染资源统计） |800|32|74.283|178.850 / 1210.093|0%|
| 隔离fixture与压测器SSE |1600|32|54.735|322.916 / 1537.307|0%|
| 隔离短请求首轮 |1600|32|88.945|165.597 / 1183.620|0.0625%（1个transport_error，原因未定位）|
| 隔离短请求复测 |1600|32|73.368|224.189 / 1425.501|0%|
| 最终ecb938d镜像SSE复验 |1600|32|59.434|314.941 / 1436.126|0.0625%（1个ReadError）|

短请求首轮失败不被复测抹去；旧分类没有异常子类信息，已加类型统计。最终SSE也出现一次ReadError，HTTP状态尚未收到；app无对应异常、容器restart=0，无足够证据区分客户端keepalive竞态、测试代理或服务端问题，根因未解决。不改资源/daemon来猜修。所有数据受同一2CPU主机上fixture/压测器竞争影响，非真实上游QPS，未证明长期大量并发容量。原始JSON在[measurements](measurements/)。

隔离SSE逐网关（每路400请求）：

| 网关 | QPS | p50 ms | p95 ms | TTFT p95 ms | 失败 |
|---|---:|---:|---:|---:|---:|
|a-cn|13.684|330.863|1502.185|1451.262|0|
|a-intl|13.684|332.848|1484.175|1406.373|0|
|b|13.684|310.747|1493.063|1378.171|0|
|c|13.684|317.479|1863.615|1812.481|0|

## docker stats（真实抽样）

独立fixture后端容器：空闲58.3–64.1MiB，CPU0.33–0.83%；SSE压力59.3–75.55MiB，CPU53.91–114%；短请求压力64.25–64.77MiB，CPU47.86–94.10%。采样峰值不是瞬时绝对峰值。114%表示约1.14CPU，并非超出2CPU安全阀。

首轮将fixture/benchmark混进app：最高98.66MiB、197.24%CPU，已明确排除为纯后端资源指标。最终ecb938d镜像为227,331,421字节（227.33MB，216.80MiB），小于250MB；最终SSE后端压力66.97–82.35MiB，CPU50.43–108.03%，结束后72.23MiB、0.42%。cgroup核实reservation268435456、limit536870912、2CPU；UID10001，运行repo干净，restart=0。镜像内部.env/数据库/gh_auth/deploy私钥均不存在，GH_TOKEN/GITHUB_TOKEN env不存在。

服务器vfs使镜像层实际磁盘膨胀：根分区从3.2G涨至7.5G、剩余5.6G；未使用全局prune、未更改存储驱动。保留已验收镜像和源码回滚备份，后续部署需注意容量。

建议保留并发8起步，queue32、timeout15。256MB软预留/512MB阀在fixture下充足；CPU已接近单核，不建议盲目加并发。调到12/16前应补真实上游长流、慢消费者、30min soak和限流/风控统计。软水位针对HTTP worker RSS，非完整cgroup内存，SQLite pagecache/supervisor需安全阀兜底。

## 缺口（不是已完成）

1. G1/G2仅通用Chat适配；原Responses/完整工具修复/ACP/自动refresh、A2成本加权/完整任务中心尚未移植。
2. B Remote、C native签名/工具转换未接通；证据不足路径501。C完整许可、B第三方来源许可仍待补齐；不复制身份/验证码绕过代码。
3. 入站同端口识别WebSocket但正常代理未实现，握手前501，未宣传WS支持。
4. 四公网IP、HTTPS/SOCKS5实际出口及真实账号协议未测，只有loopback HTTP故障切换验证。
5. ~~GitHub repo/private deploy key/webhook未创建~~（2026-10-02 部分闭环）：两仓库已创建并推送、只读 deploy key 已注册并实测拉取、热更新/回滚云端实测通过；webhook 仍未配置（无公网 HTTPS 回调），gh CLI 仍缺失（本轮以 REST API + 本机凭据管理器令牌完成，未向用户索要聊天 token）。
6. 公网HTTPS回调地址未提供；HMAC webhook代码与300s轮询均已实测（轮询路径真实触发），webhook公网交付仍无虚构配置。UI 公共仓已建，GitHub Release 未发布。
7. 签到执行器A-CN/B仅源码参考且显式verified后可用，默认未启用；领取/国际活跃仍501；无真实业务验收。
8. alert目前结构化日志+API故障状态，无外部邮件/通知服务。

## 回滚

停止测试fixture仅停止命名容器mgp-test-fixture；主服务停止用`docker compose -p mgp-test stop app`，不删data。需要回滚源码使用已提交commit重建，先在线SQLite backup；不改服务器全局服务，不docker system prune。密钥/.env隔离、gh写凭据从未挂到镜像/容器。
