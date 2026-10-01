# go-api-quota

组织（org）、用户（user）、访问密钥（key）三层联动配额服务。

开发环境：Go 1.23.0，仅依赖标准库。

## 核心语义

- **联动扣减**：消费只有在当前窗口内三个层级都有足够额度时才成功；三层扣减在一个事务里整体提交，任一层不足则全部不扣。
- **并发安全**：每条额度记录（配置 / 窗口用量 / 消费记录）都带版本号，写入必须满足版本条件，冲突时自动重试；并发请求不会超额，也不会丢失更新。
- **幂等**：消费与退还都以调用方提供的 `request_id` 为幂等键。同号同内容返回首次结果（`duplicate: true`），同号异内容返回 `idempotency_conflict`。
- **退还**：成功消费在退还时限内（默认 24h，可用 `-refund-ttl` 调整）允许部分或全部退还，可多次退，累计不超过原消费金额；退还请求同样幂等。
- **预占（reservation）**：预占不立即消费，而是在一个事务内同时保留组织、用户、访问密钥三层当前窗口的可用额度（`reserved`），任一层不足整笔失败。业务结束后可**确认**（reserved 转为正式 used）或**取消**（释放 reserved）；超过有效期（默认 5m，可用 `-reservation-ttl` 调整，也可在请求中用 `ttl_seconds` 指定）未确认则**自动到期回收**。确认、取消、到期只能作用于预占时记录的**原窗口**，窗口切换后绝不补充新窗口。预占状态机为 `reserved → confirmed / cancelled / expired`，后三者为终态，并发操作只形成一个终态。预占同样以 `request_id` 幂等；预占中的额度与正式消费一样占用窗口余额（`remaining = limit - used - reserved`）。
- **重平衡（rebalance，管理员调整层级额度）**：管理员通过一张**重平衡单**在组织、用户、密钥三层之间转移额度。三层目标额度的增减之和必须为 0（零和转移），因此同一份额度不可能被两个层级同时算作可用。调整**只改变各层 `limit`，绝不改动任何窗口的 `used`/`reserved`**，已经消费的数量与在途预占都不受影响；每层新额度都必须容纳当前窗口 `used + reserved`，任一层不足时整笔调整不落地，三层配置都不修改。重平衡与确认消费、取消预占在同一事务、同一版本机制上计算新余额：重平衡读取并触达三层窗口用量记录，并发确认/取消先提交时会触发版本重试，旧调整不可能覆盖后来确认的消费；某一层配额耗尽只会拒绝受影响的调整，已成功的消费不会被回滚。重平衡单以 `request_id` 幂等：同号同内容返回首次结果，同号异内容返回 `idempotency_conflict`；同号同内容但**窗口已滚动**或**目标额度已被后来的调整改变**返回 `rebalance_conflict`。单据为每层记录调整时的窗口、已消费、已预占、调整前后额度与增减量（`delta`）、配置/用量版本依据，以及同窗口仍在途的预占单号（`in_flight_reservations`，非空即表示该层调整与在途消费并存，可据此判断调整是否涉及在途消费）。
- **窗口**：左闭右开区间 `[start, start+window_seconds)`，按 Unix 纪元对齐。用量按窗口起点独立记录，因此即使窗口切换与消费、退还、预占结算并发发生，退还与预占结算也只回补原窗口，绝不补充新窗口额度。
- **持久化**：业务数据（配置、各窗口用量、消费、退还、预占与重平衡记录）以 JSON 文件原子落盘（临时文件 + rename），重启后状态完整恢复，启动时自动回收重启期间到期的预占。

## 运行

    go run ./cmd/server -addr :8080 -data quota-store.json \
        [-refund-ttl 24h] [-reservation-ttl 5m] [-expiry-interval 1s]

## API

所有错误都返回 `{"error": {"code", "message"}}`，错误码彼此可判别：

| code | HTTP | 含义 |
|---|---|---|
| `validation` | 400 | 参数不合法 |
| `quota_not_configured` | 404 | 该层级未配置配额 |
| `consume_not_found` | 404 | 退还的原消费不存在 |
| `reservation_not_found` | 404 | 预占不存在 |
| `insufficient_quota` | 409 | 当前窗口某层额度不足（消费或预占） |
| `idempotency_conflict` | 409 | 同请求号不同内容 |
| `version_conflict` | 409 | 版本条件不满足 |
| `refund_window_expired` | 409 | 超出退还时限 |
| `refund_exceeds` | 409 | 累计退还超过原消费 |
| `reservation_state_conflict` | 409 | 预占已处于终态，操作不被允许（如确认已取消/已到期的预占） |
| `rebalance_conflict` | 409 | 同号重平衡重放时窗口已滚动，或目标额度已被后来的调整改变 |
| `rebalance_not_found` | 404 | 重平衡单不存在 |

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

### 预占

    POST /v1/reservations
    {"request_id": "rsv-1", "org_id": "org-1", "user_id": "u-1", "key_id": "k-1",
     "amount": 10, "ttl_seconds": 300}

三层当前窗口可用额度都足够时返回 `201`，余额视图中三层均为 `used=0, reserved=10, remaining=limit-used-reserved`。
同号同内容重放返回 `200` 且 `duplicate: true`；同号异内容返回 `409 idempotency_conflict`。
`ttl_seconds` 可省略，省略时使用服务端 `-reservation-ttl`（默认 5 分钟）。

确认（转为正式消费，生成请求号为 `reservation:<预占请求号>` 的消费记录，之后可按普通消费退还）：

    POST /v1/reservations/confirm
    {"request_id": "rsv-1"}

取消（额度退回原窗口）：

    POST /v1/reservations/cancel
    {"request_id": "rsv-1"}

确认/取消重复调用返回首次结果（`duplicate: true`）；对已处于其他终态的预占操作返回
`409 reservation_state_conflict`。已过期的预占会在确认/取消/查询时被惰性回收，
也可由后台定时任务或手动触发批量扫描：

    POST /v1/reservations/expire        # 返回 {"expired": ["rsv-1", ...]}

查询预占状态：

    GET /v1/reservations?request_id=rsv-1

> 即使预占期间发生窗口切换，确认/取消/到期也只调整预占时记录的原窗口用量，
> 不会把额度补到新窗口。

### 三层余额查询

    GET /v1/balances?org_id=org-1&user_id=u-1&key_id=k-1

返回三个层级当前窗口的 `limit` / `used` / `reserved` / `remaining` 及窗口起止时间；
`config_version` 是配置版本，`last_rebalance_request_id` 是最近一次改变该层额度的
重平衡单号，余额变化可凭它回到具体重平衡单（未被重平衡调整过时该字段省略）。

### 重平衡（管理员调整层级额度）

    POST /v1/rebalances
    {"request_id": "rb-1", "org_id": "org-1", "user_id": "u-1", "key_id": "k-1",
     "org_limit": 80, "user_limit": 120, "key_limit": 100}

三层目标额度必须全部给出、均为正数，且增减之和为 0；三层当前窗口
`used + reserved` 都不超过目标额度时才整体提交（上例：组织 −20、用户 +20、密钥不变）。
成功返回 `201`，响应包含：

- `rebalance.levels[]`：每层一条调整依据——`window_start`、`used`、`reserved`、
  `limit_before` / `limit_after` / `delta`（本层增加或减少了多少额度）、
  `config_version_before` / `config_version_after` / `usage_version`（版本依据）、
  `in_flight_reservations`（同窗口仍在途的预占单号；非空表示该层调整与在途消费并存，
  调整不改动其预留额度，这些预占确认/取消时仍按原窗口结算）。
- `balances[]`：调整后三层余额，`used`/`reserved` 不变，只反映新的 `limit` 与 `remaining`。

失败语义：

- 非零和、缺目标额度、目标非正、三层零变更 → `400 validation`；
- 任一层目标额度低于该层当前窗口 `used + reserved` → `409 insufficient_quota`，
  整笔调整不落地，已成功的消费/预占不受影响，更不会被回滚；
- 同号异内容 → `409 idempotency_conflict`；
- 同号同内容重放但窗口已滚动，或当前额度已不再是原单目标额度（被后来的调整改变）
  → `409 rebalance_conflict`，旧结果不会覆盖新状态；
- 同号同内容重放且状态未变 → `200` 且 `duplicate: true`。

### 历史与追溯查询

查询单笔重平衡单：

    GET /v1/rebalances?request_id=rb-1

按主体过滤重平衡历史（参数均可选，全空返回全部，按时间从旧到新排序）：

    GET /v1/rebalances?org_id=org-1&user_id=u-1&key_id=k-1

消费历史（含预占确认生成、请求号为 `reservation:<预占请求号>` 的正式消费）：

    GET /v1/consume-history?org_id=org-1&user_id=u-1&key_id=k-1

预占历史（返回各预占的当前状态，同样支持三层主体过滤）：

    GET /v1/reservation-history?org_id=org-1&user_id=u-1&key_id=k-1

> 管理员调整后，对照重平衡历史中每层的 `delta` 即可看出组织、用户、密钥各自
> 增加或减少了多少额度；`used`/`reserved` 与 `in_flight_reservations` 说明调整时
> 是否有在途消费；余额上的 `last_rebalance_request_id` 把每一层当前额度追溯回
> 具体重平衡单，而不只是显示当前剩余数字。

## 代码结构

    cmd/server/          服务入口
    internal/quota/      领域模型、事务化存储（含版本条件与文件持久化）、业务逻辑
    internal/httpapi/    REST 接口与错误码映射

## 测试

    go test ./...        # 单元与接口测试
    go test -race ./...  # 含并发超额 / 丢失更新场景
