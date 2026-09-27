# go-api-quota

组织（org）、用户（user）、访问密钥（key）三层联动配额服务。

开发环境：Go 1.23.0，仅依赖标准库。

## 核心语义

- **联动扣减**：消费只有在当前窗口内三个层级都有足够额度时才成功；三层扣减在一个事务里整体提交，任一层不足则全部不扣。
- **并发安全**：每条额度记录（配置 / 窗口用量 / 消费记录）都带版本号，写入必须满足版本条件，冲突时自动重试；并发请求不会超额，也不会丢失更新。
- **幂等**：消费与退还都以调用方提供的 `request_id` 为幂等键。同号同内容返回首次结果（`duplicate: true`），同号异内容返回 `idempotency_conflict`。
- **退还**：成功消费在退还时限内（默认 24h，可用 `-refund-ttl` 调整）允许部分或全部退还，可多次退，累计不超过原消费金额；退还请求同样幂等。
- **预占（reservation）**：先同时保留三层可用额度而不立即消费，之后再确认、取消或由系统在到期时自动回收。
  - 创建预占在一个事务里原子锁定组织、用户、密钥三层额度，并记录三层的原窗口、数量与过期时间；任一层不足整笔失败。可用额度 = `limit - used - reserved`，预占与直接消费共享同一额度池。
  - **确认**把三层原窗口里的 `reserved` 转为正式 `used`，并生成一条正式消费记录（请求号为 `<预占请求号>/consume`），之后可对其走正常退还流程；**取消**与**到期回收**只释放原窗口的 `reserved`。窗口已切换时，确认、取消、到期都只作用于预占保存的原窗口，绝不补到新窗口。
  - 预占状态机为 `reserved → confirmed / cancelled / expired`，后三者均为终态；确认、取消与到期回收并发时只会形成一个终态，失败方收到 `reservation_state_conflict`。
  - 预占创建以 `request_id` 为幂等键（TTL 秒数也属于请求内容）；确认、取消使用各自独立的操作请求号做幂等，同一操作号不能跨操作或跨预占复用。到期回收在预占、消费等写入口惰性执行，也可调用服务方法主动扫描；只读余额查询同样不会把已到期预占计入可用额。
- **窗口**：左闭右开区间 `[start, start+window_seconds)`，按 Unix 纪元对齐。用量按窗口起点独立记录，因此即使窗口切换与消费、退还、预占操作并发发生，退还与预占的确认/取消/到期也只作用于记录中的原窗口，绝不补充新窗口额度。
- **持久化**：业务数据（配置、各窗口用量、消费、退还、预占与预占操作记录）以 JSON 文件原子落盘（临时文件 + rename），重启后状态完整恢复。

## 运行

    go run ./cmd/server -addr :8080 -data quota-store.json [-refund-ttl 24h] [-reservation-ttl 1m]

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
| `reservation_not_found` | 404 | 预占不存在 |
| `reservation_state_conflict` | 409 | 预占已进入终态（确认/取消/到期并发落败） |

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

返回三个层级当前窗口的 `limit` / `used` / `reserved` / `remaining` 及窗口起止时间；`remaining = limit - used - reserved`，已到期但尚未惰性回收的预占不计入 `reserved`。

### 预占（先锁额度，后确认/取消）

创建预占（`ttl_seconds` 可省略，省略时使用服务缺省时长）：

    POST /v1/reservations
    {"request_id": "rsv-1", "org_id": "org-1", "user_id": "u-1", "key_id": "k-1", "amount": 10, "ttl_seconds": 120}

返回三层各增加 `reserved` 后的余额，记录中含三层原窗口 `windows`、`expires_at` 与状态 `reserved`。同号同内容重放返回 `200` 且 `duplicate: true`（含预占当前状态），同号异内容返回 `409 idempotency_conflict`。

确认（转为正式消费，返回生成的消费记录 `consume`）：

    POST /v1/reservations/confirm
    {"request_id": "op-1", "reservation_id": "rsv-1"}

取消（额度退回原窗口）：

    POST /v1/reservations/cancel
    {"request_id": "op-2", "reservation_id": "rsv-1"}

确认/取消的 `request_id` 是操作自身的幂等键，与预占请求号独立；同号换操作或换预占返回 `idempotency_conflict`。对已终态预占再操作返回 `409 reservation_state_conflict`。

查询预占状态（查询时若已过期会自动回收）：

    GET /v1/reservations?request_id=rsv-1

## 代码结构

    cmd/server/          服务入口
    internal/quota/      领域模型、事务化存储（含版本条件与文件持久化）、业务逻辑
    internal/httpapi/    REST 接口与错误码映射

## 测试

    go test ./...        # 单元与接口测试
    go test -race ./...  # 含并发超额 / 丢失更新场景
