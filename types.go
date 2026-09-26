package apiquota

import "time"

// Level 表示配额层级：组织、用户、访问密钥。
type Level string

const (
	LevelOrg  Level = "org"
	LevelUser Level = "user"
	LevelKey  Level = "key"
)

// levelOrder 自顶向下的层级顺序，消费与退还均按此顺序联动。
var levelOrder = []Level{LevelOrg, LevelUser, LevelKey}

// QuotaConfig 是某一层某个实体的配额配置。
type QuotaConfig struct {
	Level    Level         `json:"level"`
	EntityID string        `json:"entity_id"`
	ParentID string        `json:"parent_id,omitempty"` // user→org，key→user
	Limit    int64         `json:"limit"`               // 每个窗口允许的最大用量
	Window   time.Duration `json:"window"`              // 窗口长度，左闭右开
	Version  int64         `json:"version"`
}

// UsageRecord 记录某实体在某个窗口内已使用的额度。
// Version 用于乐观并发控制：写入必须携带读取时的版本，防止丢失更新。
type UsageRecord struct {
	Level       Level     `json:"level"`
	EntityID    string    `json:"entity_id"`
	WindowStart time.Time `json:"window_start"`
	Used        int64     `json:"used"`
	Version     int64     `json:"version"`
}

// Consumption 是一笔成功消费的持久化记录。
// Windows 记录消费发生时三个层级各自所属的窗口起点，
// 退还只能回退到这些原窗口，绝不补充新窗口。
type Consumption struct {
	ID        string              `json:"id"`
	RequestID string              `json:"request_id"`
	OrgID     string              `json:"org_id"`
	UserID    string              `json:"user_id"`
	KeyID     string              `json:"key_id"`
	Amount    int64               `json:"amount"`
	Refunded  int64               `json:"refunded"`
	CreatedAt time.Time           `json:"created_at"`
	Windows   map[Level]time.Time `json:"windows"`
	Version   int64               `json:"version"`
}

// IdempotencyRecord 保证外部请求号的幂等性：
// 同号同内容返回首次结果，同号异内容报幂等冲突。
type IdempotencyRecord struct {
	RequestID   string    `json:"request_id"`
	Fingerprint string    `json:"fingerprint"`
	Response    []byte    `json:"response"`
	CreatedAt   time.Time `json:"created_at"`
}

// ConsumeRequest 是消费请求。
type ConsumeRequest struct {
	RequestID string `json:"request_id"`
	KeyID     string `json:"key_id"`
	Amount    int64  `json:"amount"`
}

// RefundRequest 是退还请求。
type RefundRequest struct {
	RequestID     string `json:"request_id"`
	ConsumptionID string `json:"consumption_id"`
	Amount        int64  `json:"amount"`
}

// RefundResult 是一次退还的结果。
type RefundResult struct {
	ConsumptionID       string    `json:"consumption_id"`
	Amount              int64     `json:"amount"`
	TotalRefunded       int64     `json:"total_refunded"`
	RemainingRefundable int64     `json:"remaining_refundable"`
	RefundedAt          time.Time `json:"refunded_at"`
}

// Balance 是某一层当前窗口的余额视图。
type Balance struct {
	Level       Level     `json:"level"`
	EntityID    string    `json:"entity_id"`
	Limit       int64     `json:"limit"`
	Used        int64     `json:"used"`
	Remaining   int64     `json:"remaining"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
}

// Balances 是三层余额的汇总。
type Balances struct {
	Org  Balance `json:"org"`
	User Balance `json:"user"`
	Key  Balance `json:"key"`
}

// windowStart 返回 t 所属窗口的起点。窗口按 window 长度对齐，
// 采用左闭右开区间：[start, start+window)。
func windowStart(t time.Time, window time.Duration) time.Time {
	return t.Truncate(window)
}
