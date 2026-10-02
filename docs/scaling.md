# 扩展边界与租约设计（仅文档）

本期是单 app 服务、单宿主机本地 SQLite。本文是容量规划与未来扩展设计，**不实现 Postgres、Redis、MQ，不宣称分布式调度已经验收**。进程内 semaphore、A2 的 Go CAS、异步 audit/request 日志都不是跨进程锁。

## 先调单副本，不直接加 workers

- 四网关各 `CONCURRENCY=8`，理论路由槽总和 32；单账号默认 2，所以仅一个 enabled 账号的网关实际账号瓶颈是 2，而不是 8。任务若共用账号需占同一许可，不能旁路。
- `QUEUE_LIMIT=32` / `QUEUE_TIMEOUT=15` 都按网关，四队列最多 128 个等待项；应存轻量上下文且请求体不超过 1 MiB。这个算术不等于内存实测。
- 每网关连接上限 16 / keepalive 8，应覆盖路由并发并预留受控探针/任务预算。四 client 的连接池不证明出口 IP 独立。
- SSE 从准入直到 EOF/断连都占槽；排队时间不是生成时间。先查看每网关等待、在途、账号/出口冷却、TTFT、总时长、失败率和实际 RSS，再降低/增加限额。
- `SOFT_MEMORY_MB=256`、watermark 0.8 配合硬上限 512m；软准入应留管理/health/日志预算，不缓存完整 SSE。mem reservation 不是预分配。CPU 2 是约束参数，不是吞吐承诺。
- 不直接 `uvicorn --workers N` 或 `docker compose --scale app=N`：内存队列/槽/亲和/冷却会复制，固定 host 8000 也冲突。仅扩大进程数会把同账号/同上游压力放大 N 倍。

benchmark 使用四网关共享起跑闸门；clients/requests 按每网关，输出客户端 QPS 与峰值在途。客户端在途包括服务端排队，不等于账号 lease 数。比较同一 fixture、同一并发/请求数量；少量样本的 p95 不可信，失败应与成功延迟分开看。真实上游 benchmark 必须获授权、设预算和限速，不从 mock 数值推导公网额度。

## 跨进程许可必须是原子条件 claim

SQLite 本地主机若未来多进程共享，需将关键状态放在同一事务边界：

- gateway active slots、account slots（跨网关使用同一 `provider_account_id` 时按真实 provider 身份协调），必要的 egress budget；
- account/model cooldown、quota、会话亲和绑定的 CAS；
- task 幂等键 `(provider, provider_account_id, task_type, provider_business_date)`、attempt/result 状态；
- update/scheduler leader lease。

**可用设计是每个资源预置有限 slot 行 + lease claim，不是异步审计流水。** 概念字段：`resource_key, slot_index, lease_id, owner, expires_at, fencing_token`；`UNIQUE(resource_key, slot_index)`。不能先查 free、离开事务、再异步写 claim。

下面是未来设计的 SQL 示意，**不是本期新建 schema 或可直接对现有 DB 执行的迁移**：

```sql
BEGIN IMMEDIATE;
-- :now 取数据库时钟，预先绑定，不用竞争者任意指定过期时间。
UPDATE leases
SET owner = :owner, lease_id = :new_lease_id,
    expires_at = :expires, fencing_token = fencing_token + 1
WHERE resource_key = :resource AND slot_index = (
    SELECT slot_index FROM leases
    WHERE resource_key = :resource AND expires_at <= :now
    ORDER BY slot_index LIMIT 1
)
AND expires_at <= :now
RETURNING slot_index, lease_id, fencing_token;
-- 无返回行：容量已占满；不要写审计行冒充取得锁。
COMMIT;
```

如果需要同时 claim gateway 与 account/egress，按固定资源顺序在一个事务内分别条件更新，并在任一失败时 ROLLBACK，全成功后 COMMIT。事务内不可包含 HTTP、睡眠或慢审计写入；SQLite 单 writer，配置 busy timeout、有界重试+抖动和失败指标。并发上限只能通过有效 slot 数约束，`COUNT(*)` 后异步插入存在 TOCTOU，不是可用方案。

### fencing 和生命周期

1. 每次重获同一资源产生**单调递增 fencing_token** 与随机唯一 lease_id；过期只允许新持有者 claim，不让旧 owner 续自己的过期租约。
2. 心跳 renew 是条件更新：`WHERE owner=:owner AND lease_id=:id AND fencing_token=:fence AND expires_at>:now`。返回 0 表示已经失去许可，停止读取/发送并关闭上游，不能继续完成任务或覆写新状态。
3. release、结果写入、cooldown/亲和更新都验证相同 owner/id/fence；旧请求 `finally` 不得释放新持有者的 slot。资源 epoch/token 计数不得随删除/重建归零；备份恢复/灾难重建需提升 epoch 防旧 token 回流。
4. lease 期限应覆盖/持续续租整个 SSE（最大 total 600 秒），heartbeat 周期明显小于 TTL；lease 丢失 fail closed。单机 GC 暂停或 DB不可达不能悄悄放行。
5. **本地 fencing 不能令不认识 token 的外部原厂 API撤销已在途请求。** 必须在每次新外部调用前确认许可，失去许可停止后续动作；claim/计费等副作用依赖原厂幂等键或结果对账。仅靠 TTL 不能宣称 exactly-once。
6. 审计可异步记录 claim/result 供追踪，但必须在取得真实事务锁之后；异步 audit/event 表不参与同步准入，不能被展示为跨进程锁。

## 任务与更新幂等

任务区分 reserved/running/succeeded/failed/unknown，不能无条件写“今天成功”。恢复租约不意味着安全重放：原厂可能已执行、进程在提交结果前崩溃。unknown 状态先查询/对账，缺证据能力继续 501。日期依据 provider business timezone，不能靠不同容器本地日期去重；跨午夜窗口、每日上限/总闸应在事务判定并再次在调用前核对。

updater 只有一位 leader，先串行验证 commit/branch/可信来源，再禁新准入、drain 在途、执行更新并验证健康/版本。滚动切换时不得让两个任务 scheduler 同时工作；代码/requirements/schema 回滚协调见 README。副本扩展也不能改掉 C 禁止验证码绕过和假身份的边界。

## SQLite 的主机边界

- SQLite + WAL 适合本地文件系统、单宿主机小规模。多进程共享也需事务 lease 与 fence，不能靠 Python async 或 executor 就证明正确。
- **不要把 SQLite/WAL 放 NFS、SMB、分布式网络卷来跨 host。** 文件锁、shared-memory、缓存一致性、崩溃恢复语义可能不满足 WAL 要求；共享目录不是数据库服务。
- 不将同一 DB 复制给每个副本当共享真相；状态会分叉。SQLite online backup 得到的是快照，不是复制协议。
- 在线 backup/冷备必须考虑 WAL 和所有 writer；恢复期间停止全部副本、任务与 supervisor。具体命令见 README。

## 未来跨主机方案（本期不实现 Postgres）

获得需求和证据后才做数据库迁移：Postgres 作为集中状态、行级原子条件 UPDATE/事务或 `FOR UPDATE SKIP LOCKED` 选 slot、唯一任务幂等键、数据库时钟和 fencing；不要把现在异步审计表换个连接字符串就称为分布式锁。设计网关/账号共享 identity、容量上限、lease TTL、schema migration/rollback、跨节点 leader、连接池和审计隔离。没有 Postgres 服务、驱动、迁移或 compose 条目在本期交付中。

验收必须注入双进程同时 claim、worker crash、长暂停超过 TTL、旧 release、旧结果写入、数据库超时、滚动更新、客户端断连、重复任务以及故障恢复；证明 active permits 不超过 slot 数、旧 fence 无写权限、unknown 副作用不被重放。通过这些之前只运行单副本，不宣称线性扩展或跨主机安全。
