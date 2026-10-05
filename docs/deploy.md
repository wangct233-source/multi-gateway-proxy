# 从零部署教程（Linux 服务器）

面向第一次部署的人。全程只需 SSH 终端，不需要面板；用宝塔/1Panel/DPanel 也可以，但**编排文件请以下面第 2 步下载的正版为准**（面板自己保存的副本常是旧的/被改过的）。

## 0. 准备

- 一台 Linux 服务器（Ubuntu/Debian/CentOS 均可），能 SSH 登录
- Docker + Docker Compose v2。没装就一行：
  ```bash
  curl -fsSL https://get.docker.com | sh
  docker compose version   # 能打印版本号即可
  ```
- 本项目**不带任何 AI 账号**：CodeBuddy / Trae / Zcode 的账号凭据需自备（第 6 步）

## 1. 建目录，拿两个文件

```bash
mkdir -p /opt/mgp && cd /opt/mgp
curl -fsSLO https://raw.githubusercontent.com/wangct233-source/multi-gateway-proxy/main/docker-compose.yml
curl -fsSLo .env https://raw.githubusercontent.com/wangct233-source/multi-gateway-proxy/main/.env.example
```

只需要这两个文件，**不需要克隆整个源码**——程序本体在公开 Docker 镜像里（`ghcr.io/wangct233-source/multi-gateway-proxy`），启动时自动拉取。

## 2. 创建挂载目录（缺一个都会启动失败）

```bash
mkdir -p data logs ../runtime/keys
```

- `data/` 数据库等持久数据；`logs/` 日志
- `../runtime/keys` 是更新器预留的密钥目录，**空目录即可**，平时用不到

## 3. 编辑 .env（最小可启动配置）

```bash
nano .env     # 或 vi / 宝塔文件编辑器
```

必改一行：

```
ADMIN_TOKEN=换成一串至少24位的乱码
```

生成 ADMIN_TOKEN：`openssl rand -hex 24`。这是**应急主钥匙**（忘记网页密码时救援用），平时用不到，抄到笔记里保存即可。

镜像不用配：compose 为**纯拉取模式**（无 build 段），不设 `MGP_IMAGE` 时自动拉取公开的 `ghcr.io/wangct233-source/multi-gateway-proxy:latest`，永远不需要本地编译。（可选进阶：`MGP_IMAGE=ghcr.io/...:sha-<完整commit>` 可固定某个版本。）

先不用动其他行：四个网关默认全部 `disabled`，第 6 步再开通。`DATA_TOKENS` 不填时自动等于 `ADMIN_TOKEN`（就是绘画工具里填的 API Key）。

## 4. 启动与验证

```bash
docker compose up -d               # 镜像不存在会自动拉取（约 200MB）
docker compose ps                  # 状态应为 Up (healthy)
curl http://127.0.0.1:8000/healthz # 返回 {"status":"ok",...}
```

常见报错对照：

| 报错 | 原因与处理 |
|---|---|
| `open Dockerfile: no such file or directory` / `Image ... Building` | 你手里的 compose 还是**含 build 段的旧版**（多为面板保存的副本）→ 用第 1 步重新下载正版覆盖 |
| `ADMIN_TOKEN is required and must contain at least 24 characters` | `.env` 没建 / ADMIN_TOKEN 为空或太短 → 回第 3 步 |
| `invalid mount config ... create_host_path` | 第 2 步目录没建全 → 补建后 `docker compose up -d` |
| 面板里启动但行为怪异 | 面板保存的编排副本和仓库不一致 → 用第 1 步下载的正版内容替换面板里的编排 |
| 拉取镜像慢/失败 | 换国内镜像加速或配置代理后重试 `docker compose pull` |

## 5. 管理入口（三选一）

登录密码：**默认 `admin`**，登录后到「全局设置与更新 → 修改管理密码」立刻改掉。

**A. 桌面软件（最简单）**：向项目作者索取 `mgp-console.exe`（Windows 双击即用的独立窗口），首次指定后端：
```powershell
.\mgp-console.exe --backend http://服务器IP:8000
```
之后双击即用。要求后端能被你的电脑访问到（见下方“对外开放”）。

**B. 网页（推荐长期用）**：在服务器上装 nginx，托管管理界面 4 个静态文件（来自 https://github.com/wangct233-source/multi-gateway-proxy-ui ）并把 `/api`、`/healthz`、`/gw` 反代到 `127.0.0.1:8000`，页面里 baseURL **留空**即可（同源，零 CORS 配置）。

**对外开放（A/B 都需要）**：把 compose 里 `"127.0.0.1:8000:8000"` 改成 `"8000:8000"` 后 `docker compose up -d`，并放行防火墙端口。**建议防火墙只对你的 IP 放行 8000**。

## 6. 开通网关（填真实账号）

四个网关与 .env 前缀：`A_CN`（CodeBuddy 国内 /v1）、`A_INTL`（国际 /v2）、`B`（Trae CN /v3）、`C`（Zcode /v4）。每个网关三样：

```
A_CN_UPSTREAM_MODE=a          # disabled/openai/a/b-remote/c-anthropic
A_CN_UPSTREAM_URL=https://上游地址
A_CN_EGRESS_PRIMARY=direct://local   # 出口；direct://local=用服务器本机网络
```

账号**推荐在管理界面导入**：登录 → 对应网关 → 账号池 → 导入 JSON（自动识别各家格式：A 的 accessToken、B 的 token、C 的 apiKey/secret，支持数组批量），比手写 `*_ACCOUNTS_JSON` 省事。

改了 `.env` 后 `docker compose up -d` 重建生效。

**如实说明**：B/C 的原生协议代码已实现但**未对过真实上游**，缺验证证据的调用会返回 501 而不是伪装成功；签到/领奖默认全部关闭，需显式开 `*_CHECKIN_VERIFIED=true` 才生效。

## 7. 接入绘画/调用工具

- 接口地址：`http://服务器IP:8000/gw/a-cn/v1`（B 用 `/gw/b/v3`，C 用 `/gw/c/v4`）
- API Key：`DATA_TOKENS` 里配的值（没配就是 `ADMIN_TOKEN` 那串）
- OpenAI 兼容 `/chat/completions`，支持流式

## 8. 以后怎么更新

三选一（详见 [updates.md](updates.md)）：

```bash
# 手动（最简单）
docker pull ghcr.io/wangct233-source/multi-gateway-proxy:latest
MGP_IMAGE=ghcr.io/wangct233-source/multi-gateway-proxy:latest docker compose up -d
```

或在管理界面「全局设置与更新」点检查/立即更新（需 `.env` 加 `UPDATES_ENABLED=true`、`UPDATES_REPO_SLUG=wangct233-source/multi-gateway-proxy`，并按 updates.md 挂上宿主机脚本才会真正切换）。数据在 `data/` 卷里，更新不丢。

## 9. 安全与责任

- 管理密码别用默认的 `admin`；`ADMIN_TOKEN` 与 `DATA_TOKENS` 泄露请立即更换
- 不要把 8000 端口裸奔公网还搭配弱密码；推荐防火墙限源 IP 或套 TLS
- 用反代访问上游服务可能违反其服务条款，风险自担；本项目不提供也不支持验证码绕过、伪造身份等功能
