# go-api-quota

组织（org）、用户（user）、访问密钥（key）三层联动配额服务。

开发环境：Go 1.23.0，仅依赖标准库。

## 核心语义

- **联动扣减**：消费只有在当前窗口内三个层级都有足够额度时才成功；三层扣减在一个事务里整体提交，任一层不足则全部不扣。
- **并发安全**：每条额度记录（配置 / 窗口用量 / 消费记录）都带版本号，写入必须满足版本条件，冲突时自动重试；并发请求不会超额，也不会丢失更新。
- **幂等**：消费与退还都以调用方提供的 `request_id` 为幂等键。同号同内容返回首次结果（`duplicate: true`），同号异内容返回 `idempotency_conflict`。
- **退还**：成功消费在退还时限内（默认 24h，可用 `-refund-ttl` 调整）允许部分或全部退还，可多次退，累计不超过原消费金额；退还请求同样幂等。
- **窗口**：左闭右开区间 `[start, start+window_seconds)`，按 Unix 纪元对齐。用量按窗口起点独立记录，因此即使窗口切换与消费、退还并发发生，退还也只回补原消费所属窗口，绝不补充新窗口额度。
- **持久化**：业务数据（配置、各窗口用量、消费与退还记录）以 JSON 文件原子落盘（临时文件 + rename），重启后状态完整恢复。

## 运行

    go run ./cmd/server -addr :8080 -data quota-store.json [-refund-ttl 24h]

## API

所有错误都返回 `{"error": {"code", "message"}}`，错误码彼此可判别：

| code | HTTP | 含义 |
|---|---|---|
| `validation` | 400 | 参数不合法 |
| `quota_not_configured` | 404 | 该层级未配置配额 |
| `consume_not_found` | 404 | 退还的原消费不存在 |
| `insufficient_quota` | 409 | 当前窗口某层额度不足 |
| `idempotency_conflict` | 409 | 同请求号不同内容 |
| `version_conflict` | 409 | 版本条件不满足 |
| `refund_window_expired` | 409 | 超出退还时限 |
| `refund_exceeds` | 409 | 累计退还超过原消费 |

### 配置配额

    PUT /v1/quotas
    {"level": "org", "subject_id": "org-1", "limit": 1000, "window_seconds": 60, "expected_version": 0}

`expected_version` 为 0 表示新建；更新时传入当前版本作为并发条件。

    GET /v1/quotas?level=org&subject_id=org-1

### 消费（三层联动扣减）

    POST /v1/consume
    {"request_id": "c-1", "org_id": "org-1", "user_id": "u-1", "key_id": "k-1", "amount": 10}

首次成功返回 `201`，幂等重放返回 `200` 且 `duplicate: true`。

### 退还

    POST /v1/refund
    {"request_id": "r-1", "consume_request_id": "c-1", "amount": 5}

### 三层余额查询

    GET /v1/balances?org_id=org-1&user_id=u-1&key_id=k-1

返回三个层级当前窗口的 `limit` / `used` / `remaining` 及窗口起止时间。

## 代码结构

    cmd/server/          服务入口
    internal/quota/      领域模型、事务化存储（含版本条件与文件持久化）、业务逻辑
    internal/httpapi/    REST 接口与错误码映射

## 测试

    go test ./...        # 单元与接口测试
    go test -race ./...  # 含并发超额 / 丢失更新场景
