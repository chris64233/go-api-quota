# go-api-quota

组织（org）、用户（user）、访问密钥（key）三层联动配额服务。

开发环境：Go 1.23.0，仅依赖标准库。

## 核心语义

### 层级与窗口

- 配额按 `org → user → key` 三级配置，user 必须挂在已配置的 org 下，key 必须挂在已配置的 user 下。
- 每层独立设置限额与窗口长度；窗口按长度对齐，采用**左闭右开**区间 `[start, start+window)`。

### 消费（Consume）

- 只有当**当前窗口内三个层级都还有额度**时消费才成功；任一不足则整体失败，不产生任何部分扣减。
- 三层扣减在同一事务中提交。额度记录携带版本号，写入必须满足版本条件（乐观并发控制），
  并发请求既不会超额，也不会丢失更新；版本冲突由服务层有限重试。
- 外部请求号 `request_id` 保证幂等：**同号同内容**返回第一次的结果（不重复扣减），
  **同号异内容**返回 `IDEMPOTENCY_CONFLICT`。

### 退还（Refund）

- 消费成功后在限定时间内（默认 24h，可用 `WithRefundTTL` 调整）允许部分或全部退还，
  可多次退，但**累计退还不得超过原消费**，否则返回 `STATE_CONFLICT`。
- 退还请求同样按 `request_id` 幂等：重复调用沿用首次结果，异内容报幂等冲突。
- 退还的额度**一律回退到原消费所属窗口**的用量记录。即使窗口已经切换，
  也绝不会补充新窗口的额度。

### 持久化

业务数据（配额配置、窗口用量、消费记录、幂等记录）以 JSON 文件原子落盘
（临时文件 + rename），重启后状态完整恢复；测试可使用纯内存存储。

## 错误类别

所有业务错误携带可判别的 `code`（HTTP 响应体 `error.code`，Go 侧用 `CodeOf(err)` 提取）：

| code | 含义 | HTTP |
|---|---|---|
| `QUOTA_EXHAUSTED` | 某层额度不足（`level` 字段指示层级） | 409 |
| `WINDOW_EXPIRED` | 退还超出限定时间 | 409 |
| `VERSION_CONFLICT` | 额度记录版本条件不满足（重试耗尽后） | 409 |
| `STATE_CONFLICT` | 状态冲突，如累计退还超过原消费 | 409 |
| `IDEMPOTENCY_CONFLICT` | 同一请求号携带不同内容 | 409 |
| `NOT_FOUND` | 配额、消费记录不存在 | 404 |
| `INVALID_ARGUMENT` | 参数非法 | 400 |

## HTTP API

```
PUT  /v1/quotas/{level}/{entityID}     配置配额  {"parentId":"org1","limit":1000,"windowSeconds":3600}
GET  /v1/quotas/{level}/{entityID}     查询配额配置
POST /v1/consumptions                  消费      {"requestId":"r1","keyId":"key1","amount":30}
POST /v1/consumptions/{id}/refunds     退还      {"requestId":"rf1","amount":10}
GET  /v1/balances?keyId=key1           查询三层余额（当前窗口 limit/used/remaining）
```

## 运行

```sh
go run ./cmd/server -addr :8080 -db quota-store.json
```

示例：

```sh
curl -X PUT localhost:8080/v1/quotas/org/org1  -d '{"limit":1000,"windowSeconds":3600}'
curl -X PUT localhost:8080/v1/quotas/user/user1 -d '{"parentId":"org1","limit":500,"windowSeconds":3600}'
curl -X PUT localhost:8080/v1/quotas/key/key1  -d '{"parentId":"user1","limit":100,"windowSeconds":3600}'
curl -X POST localhost:8080/v1/consumptions -d '{"requestId":"r1","keyId":"key1","amount":30}'
curl 'localhost:8080/v1/balances?keyId=key1'
curl -X POST localhost:8080/v1/consumptions/<id>/refunds -d '{"requestId":"rf1","amount":10}'
```

## 代码结构

- `types.go` — 层级、配额配置、用量记录、消费/退还等领域模型与窗口计算
- `store.go` — 带版本条件的事务化存储（JSON 文件 / 内存）
- `service.go` — 消费、退还、余额查询、配额配置的核心逻辑
- `errors.go` — 可判别的错误类别
- `httpapi.go` — HTTP API
- `cmd/server` — 服务入口

## 测试

```sh
go test ./...        # 全部测试
go test -race ./...  # 含并发竞态检测
```

覆盖：三层联动扣减与整体失败、并发不超额不丢失更新、消费/退还幂等与冲突、
多次退还与累计上限、退还超时、窗口左闭右开、跨窗口退还只回原窗口、
版本条件、持久化重启恢复、HTTP 错误码映射。
