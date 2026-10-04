# multi-gateway-proxy

把 CodeBuddy（国内/国际）、Trae、Zcode 四个 AI 服务统一封装成 OpenAI 兼容 API 的低资源反代网关。Python 3.12 + Starlette + SQLite，单容器运行，自带多账号池、并发隔离、流式透传、Web 管理台与镜像级自动更新。

## ⚠️ 使用须知（请先阅读）

- **服务条款风险**：本项目通过非官方接口访问上游 AI 服务，可能违反其服务条款（类似项目 sub2api 也作同样提示）。请自行评估风险，仅供学习研究，用者的账号封禁、积分清零等后果自负。
- **不做的事**：不提供验证码绕过、身份伪造、批量注册或奖励伪造；缺证据的能力明确返回 501，不伪造成功。
- **密钥安全**：上游账号凭据只存服务器环境变量或后端数据库，永远不进 Git、不进镜像、不进日志。

## 先读证据边界

本项目以 Python 3.12、Starlette、uvicorn、`httpx[socks]` 为运行栈，入口为 `python -m app.supervisor`，HTTP 端口为 8000。没有 UI、Node、Redis、MQ，也不启动其他代理容器。

| 范围 | 当前证据 / 边界 |
| --- | --- |
| A 国内 / 国际 | 早期 A Python 源码是参考；新适配器的 `a` 模式仅覆盖通用 Chat 内容协议，不表示 A 全机制已移植。A2 Go 机制是未来选择性移植来源。真实上游验收未跑。 |
| B / C（G3 / G4） | 原项目存在相应协议，不等于新 Python 集成已实现。`b-remote`、`c-anthropic` 默认 `disabled`；缺协议或任务证据的能力应明确 501 `evidence_required`，不编成功响应。 |
| 本地 mock | 四网关均显式 `openai`，只验证授权兼容接口形态、SSE 和错误处理，不能证明原厂协议或公网可用。 |
| Docker / GitHub | 本机无 Docker/`gh`；经用户授权在云服务器完成 Python 3.12 容器构建、健康、隔离/流式/回滚测试及 `docker stats`。GitHub 建仓与只读 deploy key 仍因本机 `gh` 缺失未执行。详见 [docs/acceptance.md](docs/acceptance.md)。 |
| 四出口 | 四套配置/客户端不等于四个公网 IP；同宿主机默认路由或 `direct://local` 不能当成独立出口证据。 |

来源许可与补证要求见 [docs/source-attribution.md](docs/source-attribution.md)；**新手部署教程见 [docs/deploy.md](docs/deploy.md)**；实测记录见 [docs/acceptance.md](docs/acceptance.md)；扩展边界见 [docs/scaling.md](docs/scaling.md)。当前是可运行的核心与受限适配版本，**不是四个真实厂商协议都已接通的成品**。所有云端压测用明确的 `openai` fixture 模式，没有真实账号/奖励调用。

## 路由与流式契约

| 网关 ID | 环境前缀 | HTTP 入站前缀 |
| --- | --- | --- |
| `a-cn` | `A_CN` | `/gw/a-cn` |
| `a-intl` | `A_INTL` | `/gw/a-intl` |
| `b` | `B` | `/gw/b` |
| `c` | `C` | `/gw/c` |

- `POST <前缀>/v1/chat/completions`：Chat HTTP 请求；`stream:true` 返回 SSE。
- `GET <前缀>/v1/models`：配置模型清单，不承诺实时向原厂 discover。应用支持可选 `*_MODELS` 逗号分隔清单；未配置可能返回空列表，不代表上游探测失败。
- Responses/高级工具转换尚未移植的路径应 501 `evidence_required`，不要从 A/A2 原仓库 README 推导集成支持范围。
- `GET /healthz` 是容器探针，不代表四上游健康或出口 IP 唯一。
- 管理面以 `/api/v1/gateways/{id}` 为前缀，包含 accounts、egress、settings、capabilities、tasks；任务手动入口为 `/api/v1/gateways/{id}/tasks/{type}/run`。
- `GET/POST /api/v1/tasks/kill-switch`、`GET /api/v1/version`；更新控制在 `/api/updates/status`、`/api/updates/check`、`/api/updates/webhook`。
- **HTTP ingress，不是 WebSocket 隧道。** 相同数据路由的 WS Upgrade 在握手前应返回 501 denial（ASGI `websocket.http.response` 扩展），不能先 accept 再假转发，也不虚构额外 WS 路由。缺该 denial 扩展的服务器行为须另外验收。

SSE 必须逐块转发并遵守背压；不能 `aread()`、`response.content` 或整包收集后再返回 SSE。客户端断开应关闭上游并释放网关/账号槽；已输出首帧后不能盲目切账号/重放请求。总超时与空闲超时是两条独立约束，心跳不能无限延长总期限。上游在 200 流内报错或 EOF 缺 `[DONE]` 不是成功。

## 配置与安全默认值

复制 `.env.example` 到本机 `.env`，设置随机 `ADMIN_TOKEN`（至少 24 个字符）；`DATA_TOKENS` 空则回落 admin，生产建议独立数据令牌。管理令牌不授予上游账号授权，不要混用两个层级的 token。

每网关使用 `*_UPSTREAM_URL` 和 `*_UPSTREAM_MODE=disabled|openai|a|b-remote|c-anthropic`。URL 是 base，不含用户名、密码、query、fragment；具体 suffix 由适配器拼接。不要把 token 放在 URL。B/C 保持 disabled，直到协议移植和授权验收完成；本地 mock 一律 openai。

- `*_EGRESS_PRIMARY` 空必须 503，不能隐式走直连；仅填 backup 也不能替代 primary 准入。
- 真实代理支持 `http://…`、`https://…`、`socks5://…`。容器访问宿主机代理使用 `host.docker.internal`，例如 `http://host.docker.internal:7890`。
- `direct://local` 仅本地开发测试；主备切换、代理 URL 不同或端口不同都不能证明四公网 IP。默认 `*_EGRESS_CHECK_URL=https://example.com` 只检测可达性；IP 验证需要受控回显和观测记录。
- `*_ACCOUNTS_JSON` 每项为 `{id, provider_account_id, secret_ref, enabled, metadata}`。`secret_ref` 是 `NAME` 或 `env:NAME`，统一规范化为 env_ref；真实值只在 `.env` 的 `A_CN_ACCOUNT_1_TOKEN` 等变量中。metadata 禁止 token、cookie、密码，勿将 token 明文入 SQLite；`provider_account_id` 必须为实际授权账号 ID，示例中的 local ID 仅用于 fixture。
- 默认每网关并发 8、每账号 2、队列 32、等待 15 秒、HTTP 最大连接 16 / keepalive 8。这不是原厂额度或性能保证，SSE 全生命周期占槽。
- 默认任务 `*_TASKS_ENABLED=false`、窗口 `00:00` 至 `23:59`、每日限制 1；启动总闸始终打开（即使 env 填 false），需管理 API 显式解除。窗口按服务器本地时区，支持跨午夜。`A_CN_CHECKIN_VERIFIED`/`B_CHECKIN_VERIFIED` 默认 false，确认合法协议后配置 `*_BILLING_URL` 才可开启已有签到执行器；B 需账号 metadata 中真实 device_id，不生成/轮换设备。code=0 仅接受，状态 `accepted_pending_verification` 不等于积分到账。所有领取/国际活跃/传统 C 签到保持 501；开关不能使缺失能力变为可用。
- 默认 CORS 精确列出 `http://127.0.0.1:<port>`，`ALLOW_NULL_ORIGIN=false`；不得用 `*` 或将 `null` 当普通可信源。
- `DATABASE_PATH=/app/data/proxy.db`，请求体上限 1048576 字节；connect 10 秒、stream idle 60 秒、stream total 600 秒。
- `SOFT_MEMORY_MB=256`、`MEMORY_WATERMARK=0.8` 是软准入配置；`MEM_RESERVATION=256m` 不是保证分配，硬上限 `MEM_LIMIT=512m`、`CPU_LIMIT=2.0` 需 Docker 主机实际支持并核验。

`.env` 由 Compose `env_file` 读取，**不是复制到镜像，也不是将文件挂到 /app**。POSIX `chmod 600 .env`；Windows 用 ACL 限制为部署账号可读写。不要打印 `docker compose config`（可能展开 secrets）、`docker inspect` 环境、完整 headers/body；env_ref 也不意味着容器 env 对宿主机管理员不可见。

## 部署（待有 Docker 的机器执行）

以下 shell 示例从此仓库根执行，PowerShell 用户按同义命令操作。两个本地仓库已经初始化 main 并完成首次提交；GitHub 远端尚未创建，不要在现有仓库重复初始化。

```bash
cp .env.example .env
# 编辑 .env：设置 ADMIN_TOKEN 等真实本机值。
# 同时把 DEPLOY_KEY_DIR 改为 ../runtime/keys，确保实际密钥挂载在 repo 外。
mkdir -p data logs ../runtime/keys
# Linux：bind mount 必须允许容器 UID/GID 10001 读写 data/logs。
sudo chown 10001:10001 data logs
chmod 700 data logs
chmod 600 .env
```

`DEPLOY_KEY_DIR=./runtime/keys` 仅保留为规范示例值；实际部署选择 `../runtime/keys` 或 repo 外绝对目录。Compose 仅把所选目录只读挂到 `/app/runtime/keys`。不启用私库更新时目录留空即可；该挂载是可选功能，可移除 compose 中此单个 bind 条目。不挂 `~/.config/gh`、gh auth、宿主机 home 或 Docker socket。私库 deploy key 必须只读仓库权限，`known_hosts` 通过可信渠道核对，不关闭 SSH host key 校验。

Dockerfile 非 root UID 10001，`/app` 连同 `.git` 属该用户且可写；整容器不设只读 rootfs，因为 supervisor 更新需要可写仓库。仅 data/logs 持久化，镜像内 git 更新在重建/换容器时会丢失，正式部署以构建镜像和提交为基线。

**build 前必须已经 `git init -b main` 并有提交**；源码和 requirements 应由应用工作流提供。缺 `.git`、非 main、无 HEAD 或缺 `app/supervisor.py` 时构建有意失败。已存在仓库先检查分支，不要覆盖现有历史。

```bash
# 只对尚未初始化的新仓库执行；这不是本轮已经执行的操作。
git init -b main
# 先审查秘密、许可、文件，再显式 stage（不要 git add .）。
git add app requirements.txt Dockerfile docker-compose.yml .dockerignore .env.example README.md docs scripts/benchmark.py scripts/mock_upstream.py
git diff --cached --check
git commit -m "Prepare multi-gateway deployment"
git branch --show-current
git rev-parse --verify HEAD
# 审查 git status、历史对象、remote URL，确保 .env、密钥、数据库未进入历史。
docker compose build
docker compose up -d
# 仅在受控环境检查状态，不输出展开的 secret 配置。
docker compose ps
curl --fail http://127.0.0.1:8000/healthz
```

requirements 由既有文件 pin 版本后 pip 安装；本轮不改依赖。`.dockerignore` **不忽略 .git**，因此其历史对象也入镜像：忽略 `.env` 并不能移除以前误提交的 token。发现历史 secret 应先轮换并清理历史/构建上下文，禁止带着泄露继续 build。git/openssh-client/CA 为 apt 最小安装并移除列表缓存，pip 禁用缓存；无前端构建。

Compose 只有 app 服务，绑定 `127.0.0.1:8000`，data/logs bind 可写、keys 只读；设置 restart、healthcheck、30 秒 start period 和 620 秒 stop grace。`UPDATES_GRACE_SECONDS` 或流总超时超过 600 时应同步增大 stop grace。健康状态本身不会让 Docker 自动重启 unhealthy 容器，restart 仅处理进程退出。对外服务需另设受控 TLS HTTP 入口；SSE 关闭响应缓冲，入口 idle/read timeout 不短于 stream 策略，勿声称公网测试已通过。

## 本地 mock 和 benchmark

mock 是标准库 `ThreadingHTTPServer`，默认只监听 loopback，无认证、无任何原厂网络调用。

```bash
python scripts/mock_upstream.py --port 18080
curl --fail http://127.0.0.1:18080/healthz
```

宿主机运行主代理时，四组配置分别设 `*_UPSTREAM_MODE=openai`、`*_UPSTREAM_URL=http://127.0.0.1:18080`、`*_EGRESS_PRIMARY=direct://local`；各账号 `enabled=true`、`secret_ref=env:对应_TOKEN变量`，对应变量填**本地 fixture 值**。可将 `*_EGRESS_CHECK_URL` 指向 mock `/healthz`，不依赖公网 example.com。本地直接启动需改 `DATABASE_PATH` 为本地可写路径（容器默认 /app/data 不适用于 Windows）。

容器访问宿主机 mock 时改 URL 为 `http://host.docker.internal:18080`；mock 必须显式 `--host 0.0.0.0` 且防火墙只许测试机器/容器访问。不要把无认证 mock 暴露到公网。支持 base 前缀，例如 `http://host.docker.internal:18080/fixture`，以及 `/v1` base 的拼接；不以容错 mock 证明生产 URL 拼接已正确。

模型 `mock-normal`、`mock-tool`、`mock-slow`、`mock-idle`、`mock-error`、`mock-close`、`mock-sse-error` 切模式；**直接访问 fixture** 可用 `?mode=slow` 等，主代理 base URL 不允许 query。SSE 包含中文、分块 UTF-8、分片工具参数和注释心跳；close 故意没有 `[DONE]`，error 是 HTTP 503，sse-error 是 200 内错误帧。idle 默认为静默 90 秒，可用 `--idle-seconds` 缩短测试。

在当前已有 venv（已装 pinned httpx）运行 benchmark，token 仅从 shell env 读取；脚本不会自动读取 .env、不会打印 token、URL、prompt 或响应内容。

```bash
# 在 shell 安全设置 ADMIN_TOKEN，值须与代理 .env 的管理 token 相同。
python scripts/benchmark.py --base-url http://127.0.0.1:8000 --token-env ADMIN_TOKEN --clients 4 --requests 20 --stream --gateway all
python scripts/benchmark.py --base-url http://127.0.0.1:8000 --token-env ADMIN_TOKEN --clients 2 --requests 8 --stream --gateway a-cn --model mock-tool --tools
```

`--clients` 和 `--requests` **均按每网关计**；all 时四网关共用起跑闸门，默认最多 16 个客户端并发、80 次请求。JSON 分别输出每网关/总体 req QPS、成功 req QPS、客户端峰值在途、TTFT、总延迟 p50/p95、失败率、状态/失败类别、原始 SSE 字节/事件/心跳计数。QPS 不是 token/s；角色帧和心跳不算 TTFT。总延迟包含排队并计至 EOF，成功 TTFT 单列；HTTP 200 内错误、缺 DONE、空流、idle/total timeout 均算失败，失败退出 1，参数/依赖失败 2。脚本不自动重试，也不整包缓冲流；非流式 JSON 仅有 1 MiB 有界解析。

仅直连 mock 的工具自检可用 `--path-template /v1/chat/completions`；默认路径是 `/gw/{gateway}/v1/chat/completions`，覆盖模板的结果不证明真实网关路由。原始帧计数不等于生成 token 数；`client_inflight_peak` 也不等于服务端实际执行或账号并发。

流式 curl 使用 env 中的 token，不把 secret 写成命令中的字面量，不用 `-v`：

```bash
curl --fail-with-body --no-buffer --max-time 610 \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data '{"model":"mock-normal","messages":[{"role":"user","content":"你好"}],"stream":true}' \
  http://127.0.0.1:8000/gw/a-cn/v1/chat/completions
```

shell 展开后的 header 仍可能被本机进程查看者看到；更高隔离场景用受保护客户端/凭据文件。`--fail-with-body` 不能识别 200 SSE 内错误，验收以 benchmark 的流终止/错误解析为准。

## 更新、备份、回滚

更新默认关闭：`UPDATES_ENABLED=false`、repo slug/webhook secret/public backend URL 均空，branch main、poll 300 秒、drain grace 600 秒。本机 `gh` 缺失不是 git 私库认证方案；更新镜像只装 git/SSH，不复制 gh 登录状态。只在审查可信仓库后设置 `owner/repo`，核对 supervisor 对 key/known_hosts、webhook HMAC、串行锁、drain 和失败回滚的实现并单独验收。更新源码不能自动安装新依赖，requirements 变化需要重建镜像。公网 URL/回调/私库拉取未实测。

备份前禁止新数据流量和任务，等待在途 SSE 结束；更新也暂停。**在线备份用 SQLite backup API**，不要直接 cp 活跃 db（尤其 WAL 模式）。以下命令由有权限的宿主机 Python 执行，生成带时间戳的独立备份：

```bash
python - <<'PY'
from pathlib import Path
import sqlite3, datetime
folder = Path('backups'); folder.mkdir(exist_ok=True)
source = sqlite3.connect(Path('data/proxy.db').resolve().as_uri() + '?mode=ro', uri=True)
target_path = folder / ('proxy-' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + '.db')
target = sqlite3.connect(target_path)
try:
    source.backup(target)
    assert target.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
finally:
    target.close()
    source.close()
print(target_path)
PY
```

`backups` 同样限制 ACL并离线加密。在线快照不自动涵盖 .env/日志/密钥，分别安全备份且不打包到镜像。需要冷备时 **停掉所有写入进程**，包括 supervisor/任务/其他副本，确认 SQLite 连接关闭；可做 `wal_checkpoint(TRUNCATE)` 后复制 db，若仍有 WAL 则在同一止写状态下将 db、`-wal`、`-shm` 成组保存，绝不能忽略/手删活跃 WAL。

回滚流程：先停入口/任务/更新并 drain；`docker compose stop app`；选已验收的旧镜像/提交重建并确认 requirements；检查 schema 是否向后兼容。需要恢复 DB 时停写后将验证过的 backup 放回 `data/proxy.db`、保留原 db/WAL 成组留底、设置 UID10001 权限；不要旧 db 搭新 WAL。重启后检查 `/healthz`、`/api/v1/version` 和本地 mock。env/代理配置另行回滚。镜像/文件回滚不能撤销已发生的上游计费或任务领取，不自动重放未知结果的请求。
