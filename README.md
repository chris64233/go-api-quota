# go-api-quota

组织（org）、用户（user）、访问密钥（key）三层联动配额服务。

开发环境：Go 1.23.0，仅依赖标准库。

## 核心语义

- **联动扣减**：消费只有在当前窗口内三个层级都有足够额度时才成功；三层扣减在一个事务里整体提交，任一层不足则全部不扣。
- **并发安全**：每条额度记录（配置 / 窗口用量 / 消费记录）都带版本号，写入必须满足版本条件，冲突时自动重试；并发请求不会超额，也不会丢失更新。
- **幂等**：消费与退还都以调用方提供的 `request_id` 为幂等键。同号同内容返回首次结果（`duplicate: true`），同号异内容返回 `idempotency_conflict`。
- **退还**：成功消费在退还时限内（默认 24h，可用 `-refund-ttl` 调整）允许部分或全部退还，可多次退，累计不超过原消费金额；退还请求同样幂等。
- **预占（reservation）**：预占不立即消费，而是在一个事务内同时保留组织、用户、访问密钥三层当前窗口的可用额度（`reserved`），任一层不足整笔失败。业务结束后可**确认**（reserved 转为正式 used）或**取消**（释放 reserved）；超过有效期（默认 5m，可用 `-reservation-ttl` 调整，也可在请求中用 `ttl_seconds` 指定）未确认则**自动到期回收**。确认、取消、到期只能作用于预占时记录的**原窗口**，窗口切换后绝不补充新窗口。预占状态机为 `reserved → confirmed / cancelled / expired`，后三者为终态，并发操作只形成一个终态。预占同样以 `request_id` 幂等；预占中的额度与正式消费一样占用窗口余额（`remaining = limit - used - reserved`）。
- **窗口**：左闭右开区间 `[start, start+window_seconds)`，按 Unix 纪元对齐。用量按窗口起点独立记录，因此即使窗口切换与消费、退还、预占结算并发发生，退还与预占结算也只回补原窗口，绝不补充新窗口额度。
- **窗口封存**：管理员可把已结束的窗口封存为不可变快照（limit/used/reserved + 未完成预占清单）。封存后该窗口不再接受新的消费、预占与额度调整；属于该窗口的确认、取消、到期与退还仍按原请求号结算到封存窗口，绝不补充新窗口。封存以 `request_id` 幂等，并发封存只形成一个结算版本。
- **持久化**：业务数据（配置、各窗口用量、消费、退还与预占记录）以 JSON 文件原子落盘（临时文件 + rename），重启后状态完整恢复，启动时自动回收重启期间到期的预占。

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
| `window_not_ended` | 409 | 封存的窗口尚未结束 |
| `window_seal_blocked` | 409 | 窗口内仍有已到期未回收的预占，需先触发到期回收 |
| `window_sealed` | 409 | 窗口已封存：拒绝新的消费、预占或重复封存 |
| `seal_not_found` | 404 | 指定窗口没有封存快照 |

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

返回三个层级当前窗口的 `limit` / `used` / `reserved` / `remaining` 及窗口起止时间。

### 窗口封存（管理员）

    POST /v1/windows/seal
    {"request_id": "seal-1", "level": "org", "subject_id": "org-1", "window_start": 1780358400,
     "expected_usage_version": 3}

把一个**已经结束**的窗口的最终账目固化为不可变快照：该层级该主体在窗口结束时的
`limit`、`used`、`reserved` 以及仍未完成（`reserved` 状态）的预占清单。
`expected_usage_version` 可省略；携带时作为版本条件，与窗口用量记录当前版本不一致
返回 `409 version_conflict`，避免基于过期视图封存。

封存被拒绝且不留下任何半成品的情况：

- 窗口尚未结束（`window_not_ended`）或起点未按窗口对齐（`validation`）；
- 窗口内仍有**已到期但未回收**的预占（`window_seal_blocked`，先调用
  `POST /v1/reservations/expire` 完成回收再封存）；
- 窗口已被其他请求号封存（`window_sealed`）。

封存以 `request_id` 幂等：同号同内容重放返回 `200` 且 `duplicate: true`，
同号异内容返回 `409 idempotency_conflict`。并发封存同一窗口时只有一个请求成功，
其余得到 `window_sealed`，只会形成一个结算版本；封存快照随业务数据一起原子落盘，
重启后完整恢复。

**封存后各类操作的允许范围：**

| 操作 | 是否允许 | 说明 |
|---|---|---|
| 新消费 / 新预占落入已封存窗口 | 拒绝（`window_sealed`） | 正常流程下消费只落入当前窗口；时钟回拨等异常也被拦截 |
| 属于该窗口预占的确认 / 取消 / 到期回收 | 允许 | 只更新封存窗口的结算账目（used/reserved），不补充新窗口，也不改写快照 |
| 属于该窗口消费的退还 | 允许 | 按原请求号处理，只回补封存窗口 |
| 重复封存同一窗口 | 拒绝 / 幂等 | 同请求号返回首次结果，异请求号返回 `window_sealed` |
| 配额配置调整（`PUT /v1/quotas`） | 允许 | 只影响未来窗口；已封存窗口的 `limit` 以快照为准，永远不变 |

查询封存快照与对账单：

    GET /v1/windows/seals?level=org&subject_id=org-1&window_start=1780358400
    GET /v1/windows/statement?level=org&subject_id=org-1&window_start=1780358400

`statement` 同时返回该窗口的当前账目、封存快照（若已封存）以及形成差异的
来源记录清单（`consume` / `refund` / `reservation_reserve` / `reservation_settle`，
金额带符号），可逐笔对出快照与当前账目之间的差异由哪笔消费、退还或预占形成。

## 代码结构

    cmd/server/          服务入口
    internal/quota/      领域模型、事务化存储（含版本条件与文件持久化）、业务逻辑
    internal/httpapi/    REST 接口与错误码映射

## 测试

    go test ./...        # 单元与接口测试
    go test -race ./...  # 含并发超额 / 丢失更新场景
