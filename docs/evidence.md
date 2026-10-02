# 证据矩阵与验收边界

## 口径

- **SOURCE**：指定原项目中可静态定位的代码/配置；不表示被新 Python 集成采用。
- **CONTRACT**：本次应用接口与部署约定；须与 `app/*` 实际行为和测试核对。
- **LOCAL-MOCK**：仅 loopback fixture 的 HTTP/SSE 工具验证，不能证明原厂授权、私有协议或公网出口。
- **BLOCKED / NOT-RUN**：环境缺工具或尚未执行，绝不写成 PASS。
- 报告中的“实测”是原报告作者的叙述，本轮没有复跑，不能继承成自己的验收证据。

初期本机无 Docker/gh。后续用户授权云服务器容器测试：Docker/Python 3.12 已实测，记录在 acceptance.md；GitHub 发布、SSH 私库读取和公网 webhook 仍 BLOCKED。原生厂商协议、真实账号、公网出口去重及任务收益尚未验收。旧工具自检记录保留为历史，不覆盖后续云端实测。

## 来源与机制矩阵

源码都位于仓库外，仅供只读参考，不打包整个旧代理。需遵守各来源许可证；下表是机制证据，不是原厂可用性证明。

| 对象 | 本地证据位置 / 版本 | 可确认事实 | 本期边界 |
| --- | --- | --- | --- |
| A 早期 Python / 国内 | `D:\下载\反代\codebuddy-反代\1`；HEAD `6c2a6637f27dcecb6c4956392dfd936fac687306`；`wb_accounts.py`、`wb_proxy.py`、`wb_tasks.py`、`wb_scheduler.py`；`analysis_A_domestic.md` | 同源码 runtime realm；CN chat 与 billing 域可不同；SSE、账号/出口槽、签到和任务存在原实现 | 仅作为早期协议与调度参考，**未全部移植**。`a` 仅通用 Chat 内容协议，无假 UA/身份模拟；真实上游未验收。 |
| A 早期 Python / 国际 | 同 HEAD；`A_diff.md`、`analysis_A_international.md`、`wb_webagent.py` | 同一份代码的 intl realm，不是独立国内/国际分支；国际日活/网页 ACP 与国内签到机制不同 | 不将 intl 活跃机制冒充 CN 签到，不声称 ACP/完整 Responses/所有工具转换已移植。 |
| A 原机制缺陷 | `analysis_A_domestic.md` 事实核查 E3/E4/E7；`wb_accounts.py`、`wb_scheduler.py` | `has_checkin` 定义不代表读取；签到失败亦写日期闸门；排程小时表并集与分项执行存在差异 | 不复制错误状态语义；原报告“可复用”只是建议，不是新实现验收。 |
| A2 Go | `D:\下载\反代\codebuddy-反代\2`；`analysis_A2_factcheck.md`；`internal/pool/*`、`internal/session/*`、`internal/upstream/*`、`internal/scheduler/*` | 成本分层/加权挑选、进程内 CAS 在途计数、SSE 重建/idle、任务动作等源码存在；transport 不因 env 自动独立出口 | **未来选择性移植**，本期不宣称全机制已转成 Python。Go CAS/内存 lease 不能当跨进程 DB lease；其 Redis 镜像不等于分布式锁。 |
| B / G3 | `D:\下载\反代\trae-反代`；HEAD `22ed0d85ee0245bc2cc3792cb96dc0bb6ef8b7fe`；`src/trae_remote_client.py`、`src/sse.py`、`src/main.py` | 原项目有 remote 会话/事件及工具路径，其 README 的原项目测试不是本期结果 | 新 Python 未完成 b-remote 协议移植/补证，默认 disabled；本地仅 openai fixture，不复制原厂登录、签到、身份变更。 |
| C / G4 | `D:\下载\反代\zcode-反代\ZcodeKnight-YU-v4.7.4\docker`；原 README/package.json 与该目录源码，非本轮 GitHub拉取 | 原项目描述 Anthropic/OpenAI/Responses 与领取机制；README 描述本身不证明新集成支持 | c-anthropic 协议/任务未补齐，默认 disabled，未实现返回 501；不将原验证码处理机制移植进本期。 |
| 新集成通用 OpenAI 模式 | `app/config.py` 契约以及 `/gw/{id}/v1/chat/completions` | IDs a-cn/a-intl/b/c，env 前缀 A_CN/A_INTL/B/C；httpx[socks]；流和非流接口由应用实现 | mock 四路均显式 openai，不能把 openai mock 当作 a/b-remote/c-anthropic 实现证据。 |
| Responses / 高级工具 | 原 A/A2/B/C 曾有部分能力，来源各异 | 模型/工具名称相同不能推导 wire protocol 相同 | 未移植部分应 501 evidence_required；本期 mock 的 tool_calls 仅测 OpenAI SSE，不代表高级工具翻译。 |
| HTTP / WS | 入站采用 HTTP；同数据路径 WS 握手前 denial | SSE 是 HTTP 流，不是 WebSocket | 501 应通过 ASGI websocket HTTP denial，不能先接受连接。不得造一个“WS代理成功”结果或新路由。 |
| G3/G4 任务 | `/api/v1/gateways/{id}/tasks/{type}/run` 契约 | 配置存在任务开关/总闸/窗口/每日限额 | 缺失协议/任务证据返回 501，不伪造签到/领取/积分；总闸默认 true、每路 tasks_enabled false。 |

### C 的硬边界

C 只接受用户已获授权的真实账号/上游权限。**禁止验证码绕过、自动求解以规避访问控制、假身份/假设备、伪造领取条件或伪造积分成功。** 无法在正常授权流程完成的步骤保持禁用并报告 evidence_required；没有本机凭据不扫描其他登录目录来填充样例。原项目存在相关代码不构成本期采用授权。

## 出口证据

配置隔离至少包含每网关独立的 primary/backup URL、独立 HTTP client/池以及路由绑定。仍须分别核验：

1. primary 为空时应 503，backup 配好也不能偷偷直连；显式 `direct://local` 标记 development。
2. 使用授权 HTTP/HTTPS/SOCKS5 代理访问受控回显端点，记录 gateway、role、时间、去敏代理标识、观测公网 IP、失败原因；日志不要写代理用户名/密码。
3. 同时对四路 primary 的公网 IP 去重；backup 单独核对，在故障切换后再验证，不能只对配置端口去重。
4. `https://example.com` 成功只说明可达，不能回显/证明公网 IP。不同代理端口可同 NAT；四容器/同宿主机默认路由/四个 direct 均可能同 IP。
5. 未完成以上授权公网验证前，表述只能是“四路配置隔离”，不能“四个独立公网出口已通过”。

## 初期验收矩阵（历史状态；后续云端结果见 acceptance.md）

| 验收项 | 测法 | 状态 |
| --- | --- | --- |
| Python 3.12 依赖与 app.supervisor | 在指定 3.12 环境安装现有 pinned requirements 后启动 | 本机工具测试环境不是容器 3.12；容器 NOT-RUN |
| Docker build / nonroot / 可写 /app git | committed main；build；检查 UID10001、/app 与 .git 所有权 | BLOCKED：Docker 无 |
| .env/keys/数据库不在镜像历史 | 审查 build context/.git 历史与镜像层；secret env 不作 build ARG/ENV | 静态配置可审查；镜像层检查 BLOCKED |
| Compose 单 app / 资源 / mount | 启动后核对 bind source、只读 keys、cgroup limits、health | BLOCKED：Docker 无 |
| GitHub / 私库更新 / webhook | 授权环境核对 git/SSH、签名、branch、串行更新、drain/回滚 | NOT-RUN；gh 无，不挂 gh auth |
| 本地标准库 mock | health、JSON、中文/工具/心跳、多 transport chunk、slow/error/close | 仅脚本自检结果可记 LOCAL-MOCK |
| benchmark 分布与失败 | 4 路同时起跑，HTTP/SSE 错误、DONE、TTFT、请求 QPS | 工具自检与集成测试分开；不作为原厂结果 |
| 管理 / 数据鉴权与 CORS | 缺/错 token、DATA_TOKENS 回落/独立值、null origin 拒绝 | 应用测试结果由应用验收记录提供 |
| 流式背压/断连 | 慢读取、客户端取消、idle/total 超时、在途槽释放、200 SSE 内错误 | 需应用层测试，不由工具存在推断 PASS |
| A 模式真实授权上游 | 经授权最小 Chat、401/429/流失败，不制造身份 | NOT-RUN |
| B/C 原协议、G3/G4任务 | 协议取证、映射、错误/幂等/权限测试 | 未移植/未补证：disabled / 501 |
| 四公网 IP | 四路授权代理经受控回显同时观测主备 | NOT-RUN，不宣称四 IP |
| 多副本 / 跨主机 | 真正事务 lease + fencing 的竞争/崩溃/恢复验证 | 本期不实现 Postgres；见 scaling |

## 工具自检记录

本轮实际 LOCAL-MOCK 自检环境为 Windows 本机现有 `.venv/Scripts/python.exe`，Python **3.14.5**、httpx **0.28.1**，不是容器 Python 3.12。使用临时 loopback 随机端口上的 ThreadingHTTPServer，进程结束即关闭，不写额外 tests/requirements/日志文件。

- 两个脚本 AST 语法检查通过，不生成 bytecode。
- mock `/healthz`、可选 `/fixture/v1` base、中文非流 JSON、`?mode=error` 已检查。
- 直连 fixture benchmark，`--path-template /fixture/v1/chat/completions --gateway all --clients 2 --requests 3`：每次四标签同时起跑、总计 12 请求。normal/tool/slow SSE 与 normal 非流成功退出 0；HTTP error、close 缺 DONE、200 SSE 内 error、idle 静默超时全部按预期退出 1，并分别统计 `http_status` / `missing_done` / `sse_error` / `idle_timeout` 各 12 次。
- 检查请求总数、每路在途不超过 2、总在途不超过 8、request QPS 按共享 wall time 计算、成功流 TTFT 样本和心跳/DONE计数；stdout/stderr 不含测试 token。
- 字节逐个喂入中文 UTF-8、CRLF、多行 data、注释心跳的 SSE parser 检查通过，角色/心跳不算 TTFT。
- 另用本机已有 Python **3.12.13** venv 复验 LF/CRLF/CR、BOM、中文逐字节 framing；normal/tool/error/close/sse-error/idle/total-timeout 共七组，各四标签总 8 请求。总超时与空闲超时独立分类，预期退出码/计数均通过。这是脚本的 3.12 兼容验证，不是 app.supervisor 或 Docker 实测。
- 同一 3.12 环境使用已有 PyYAML 解析 Compose 并断言仅 app 服务、UID10001、三 bind、keys readonly、资源变量、env_file 与入口；环境模板四组默认值/账号 JSON引用、两脚本 3.12 AST、Dockerfile 所有权/git/无 token 静态断言通过。**PyYAML 解析不替代 `docker compose config` 或实际构建。**

这只是脚本与直连 mock 自检；四个 benchmark 标签指向同一个 fixture，**不是四个真实 gateway、四个 IP 或原厂集成测试**。不将本机耗时填成生产性能。benchmark 输出中的 QPS、p50/p95、TTFT 都只适用于该次 fixture/机器/并发参数；保留原始 JSON 时去敏且不加入镜像。此段记录的是初期工具自检；Docker 后续已获云端验证，GitHub/真实上游/公网独立出口仍 BLOCKED/NOT-RUN，完整记录见 acceptance.md。
