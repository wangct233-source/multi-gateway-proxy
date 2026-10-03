# 更新与回滚

更新为**镜像模式**（app/updater.py）：依赖全部烤进镜像，requirements.txt 变化不阻断更新；数据库/.env/data 卷不参与更新。GitHub Actions 在每次 main push 后构建镜像并发布 ghcr.io（tags: `sha-<完整commit>` 与 `latest`，见 .github/workflows/build.yml；GHA 层缓存使更新只拉薄的 COPY 层）。

## 流程（两步式，默认）

1. `UPDATES_ENABLED=true` + `UPDATES_REPO_SLUG=<owner/repo>` 时，worker 每 `UPDATES_POLL_SECONDS`（默认 300s）经**公开 GitHub API** 比对镜像内 commit 与远端 main（无需令牌）；不同则状态 `available`。也可 `POST /api/updates/check` 主动检查、配置 `UPDATES_WEBHOOK_SECRET` 走 HMAC webhook。
2. 管理员在 UI 点「立即更新」（`POST /api/updates/apply`）→ updater 向数据卷写 `data/image-update-request.json`（含 target commit / 候选镜像 / previous_image），状态 `applying`。容器自身不改代码。
3. **宿主机脚本**（[scripts/host-image-update.sh](../scripts/host-image-update.sh)，容器外运行）每分钟消费请求文件：拉取 `ghcr.io/<repo>:sha-<commit>` → 改写 .env 的 `MGP_IMAGE` → `docker compose up -d` → 轮询 healthz 3 分钟 → 成功写 `applied` 并清理旧镜像（保留 2 个）；失败写 `rolled_back` 并回退 previous_image。请求 30 分钟未被消费由 updater 判超时置 failed。
4. `UPDATES_AUTO_APPLY=true` 可跳过第 2 步（检查即应用，保留旧行为）；默认 false，fail-closed。

## 新服务器部署（别人的服务器跑你的源码）

仓库更新不会自动生效，三选一：

- **手动**：`docker pull ghcr.io/wangct233-source/multi-gateway-proxy:latest` 然后 `MGP_IMAGE=... docker compose up -d`（镜像公开可匿名拉；.env/data 为挂载卷，换镜像不丢数据；回滚=换回旧 sha tag）。
- **半自动**：设 `UPDATES_ENABLED=true` + `UPDATES_REPO_SLUG`，UI 能报告/确认新版本，但**必须**再装下面脚本才会真正切换，否则停在 applying。
- **全自动**：把脚本放到宿主机并挂 cron（每分钟，flock 防重叠）：

```bash
# /etc/cron.d/mgp-update
* * * * * root MGP_DEPLOY_DIR=/opt/your-deploy-dir flock -n /tmp/mgp-update.lock \
  /opt/your-deploy-dir/host-image-update.sh >> /var/log/mgp-update.log 2>&1
```

回滚：UI/API 层面健康检查失败自动回退上一镜像；手动回滚改 .env `MGP_IMAGE` 指回旧 sha 后 `docker compose up -d`（updater 会忽略与新运行镜像不匹配的过期状态记录）。

## 仍为旧设计的部分

`UPDATES_GRACE_SECONDS`/排空/容器内 git reset 描述对应早期源码模式，已被镜像模式替代；webhook 公网回调仍未配置（无公网 HTTPS 回调地址，线上靠轮询/手动 check）。历史验收细节见 acceptance.md。
