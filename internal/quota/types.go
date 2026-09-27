package quota

import (
	"errors"
	"fmt"
	"time"
)

// Level 表示配额层级：组织、用户、访问密钥。
type Level string

const (
	LevelOrg  Level = "org"
	LevelUser Level = "user"
	LevelKey  Level = "key"
)

// Levels 按层级从高到低排列。
var Levels = []Level{LevelOrg, LevelUser, LevelKey}

// Valid 报告层级是否合法。
func (l Level) Valid() bool {
	switch l {
	case LevelOrg, LevelUser, LevelKey:
		return true
	}
	return false
}

// 可判别错误：调用方可以用 errors.Is 区分额度不足、窗口、版本、
// 状态与幂等冲突等不同失败原因。
var (
	// ErrValidation 请求参数不合法。
	ErrValidation = errors.New("validation error")
	// ErrQuotaNotConfigured 对应层级尚未配置配额。
	ErrQuotaNotConfigured = errors.New("quota not configured")
	// ErrInsufficientQuota 当前窗口内某一层级额度不足。
	ErrInsufficientQuota = errors.New("insufficient quota")
	// ErrIdempotencyConflict 相同请求号携带了不同的请求内容。
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	// ErrVersionConflict 额度记录的版本条件不满足（并发更新）。
	ErrVersionConflict = errors.New("version conflict")
	// ErrConsumeNotFound 退还原消费记录不存在。
	ErrConsumeNotFound = errors.New("consume record not found")
	// ErrRefundWindowExpired 退还超出了允许的时间范围。
	ErrRefundWindowExpired = errors.New("refund window expired")
	// ErrRefundExceeds 累计退还超过原消费金额。
	ErrRefundExceeds = errors.New("refund exceeds consumed amount")
	// ErrReservationNotFound 预占记录不存在。
	ErrReservationNotFound = errors.New("reservation not found")
	// ErrReservationStateConflict 预占不在允许该操作的状态（已终态）。
	ErrReservationStateConflict = errors.New("reservation state conflict")
)

// QuotaConfig 是某一层级某个主体的配额配置。
type QuotaConfig struct {
	Level         Level     `json:"level"`
	SubjectID     string    `json:"subject_id"`
	Limit         int64     `json:"limit"`
	WindowSeconds int64     `json:"window_seconds"`
	Version       int64     `json:"version"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// WindowStart 返回 t 所属窗口的起点（左闭右开 [start, start+WindowSeconds)），
// 窗口按 Unix 纪元对齐。
func (c QuotaConfig) WindowStart(t time.Time) int64 {
	sec := t.Unix()
	return sec - sec%c.WindowSeconds
}

// Contains 报告 t 是否落在以 start 为起点的窗口内。
func (c QuotaConfig) Contains(start int64, t time.Time) bool {
	return c.WindowStart(t) == start
}

// UsageRecord 记录某主体在某个窗口内已使用的额度。
// 以窗口起点为键的一部分，因此窗口切换后旧窗口的用量仍然保留，
// 退还才能精确回到原消费所属窗口。
//
// Used 是已正式消费的额度；Reserved 是已预占但尚未确认的额度。
// 二者都会占用可用额度：remaining = limit - used - reserved。
// 预占确认时金额从 Reserved 转为 Used；取消或到期只释放 Reserved，
// 且只操作预占记录保存的原窗口。
type UsageRecord struct {
	Level       Level  `json:"level"`
	SubjectID   string `json:"subject_id"`
	WindowStart int64  `json:"window_start"`
	Used        int64  `json:"used"`
	Reserved    int64  `json:"reserved"`
	Version     int64  `json:"version"`
}

// ConsumeRecord 是一次成功消费的持久化记录，同时充当消费幂等键。
type ConsumeRecord struct {
	RequestID string `json:"request_id"`
	OrgID     string `json:"org_id"`
	UserID    string `json:"user_id"`
	KeyID     string `json:"key_id"`
	Amount    int64  `json:"amount"`
	// Windows 记录消费发生时各层级所属窗口的起点，
	// 退还只能回补这些窗口，绝不补充新窗口。
	Windows   map[Level]int64 `json:"windows"`
	Refunded  int64           `json:"refunded"`
	CreatedAt time.Time       `json:"created_at"`
	Version   int64           `json:"version"`
}

// sameContent 判断两次同号消费请求的内容是否一致。
func (r ConsumeRecord) sameContent(orgID, userID, keyID string, amount int64) bool {
	return r.OrgID == orgID && r.UserID == userID && r.KeyID == keyID && r.Amount == amount
}

// Refundable 返回当前还可退还的金额。
func (r ConsumeRecord) Refundable() int64 {
	return r.Amount - r.Refunded
}

// RefundRecord 是一次成功退还的持久化记录，同时充当退还幂等键。
type RefundRecord struct {
	RequestID        string    `json:"request_id"`
	ConsumeRequestID string    `json:"consume_request_id"`
	Amount           int64     `json:"amount"`
	CreatedAt        time.Time `json:"created_at"`
}

// ReservationStatus 是预占的生命周期状态。
type ReservationStatus string

const (
	// StatusReserved 预占生效中：三层额度已保留，等待确认或取消。
	StatusReserved ReservationStatus = "reserved"
	// StatusConfirmed 已确认：预占转为正式消费（终态）。
	StatusConfirmed ReservationStatus = "confirmed"
	// StatusCancelled 已取消：额度退回原窗口（终态）。
	StatusCancelled ReservationStatus = "cancelled"
	// StatusExpired 已到期：系统自动回收，额度退回原窗口（终态）。
	StatusExpired ReservationStatus = "expired"
)

// Terminal 报告状态是否为终态；终态之间不可再迁移。
func (s ReservationStatus) Terminal() bool {
	return s == StatusConfirmed || s == StatusCancelled || s == StatusExpired
}

// ReservationRecord 是一次配额预占。
// Windows 保存预占发生时三个层级各自的窗口起点；确认只把这些窗口里的
// Reserved 转为 Used，取消或到期只释放这些窗口里的 Reserved，
// 即使窗口已经切换也绝不触碰新窗口。
type ReservationRecord struct {
	RequestID string `json:"request_id"`
	OrgID     string `json:"org_id"`
	UserID    string `json:"user_id"`
	KeyID     string `json:"key_id"`
	Amount    int64  `json:"amount"`
	// TTLSeconds 是预占请求的有效时长（秒），按请求语义做幂等比较；
	// ExpiresAt = CreatedAt + TTLSeconds。
	TTLSeconds int64 `json:"ttl_seconds"`
	// Windows 记录预占发生时各层级所属窗口的起点。
	Windows   map[Level]int64   `json:"windows"`
	Status    ReservationStatus `json:"status"`
	ExpiresAt time.Time         `json:"expires_at"`
	CreatedAt time.Time         `json:"created_at"`
	// DecidedAt 是进入终态的时间，reserved 状态下为零值。
	DecidedAt time.Time `json:"decided_at"`
	// ConsumeRequestID 确认后生成的正式消费记录请求号。
	ConsumeRequestID string `json:"consume_request_id,omitempty"`
	Version          int64  `json:"version"`
}

// subjects 返回预占涉及的三层主体。
func (r ReservationRecord) subjects() map[Level]string {
	return map[Level]string{LevelOrg: r.OrgID, LevelUser: r.UserID, LevelKey: r.KeyID}
}

// sameContent 判断两次同号预占请求的内容是否一致（TTL 秒数也必须一致）。
func (r ReservationRecord) sameContent(orgID, userID, keyID string, amount, ttlSeconds int64) bool {
	return r.OrgID == orgID && r.UserID == userID && r.KeyID == keyID &&
		r.Amount == amount && r.TTLSeconds == ttlSeconds
}

// ReservationOp 区分确认与取消操作，用于操作幂等键，
// 防止同一个请求号先用于确认、后又用于取消。
type ReservationOp string

const (
	opConfirm ReservationOp = "confirm"
	opCancel  ReservationOp = "cancel"
)

// ReservationOpRecord 记录确认/取消操作的幂等键，
// 同时保证同一请求号不能跨操作或跨预占复用。
type ReservationOpRecord struct {
	RequestID     string        `json:"request_id"`
	Op            ReservationOp `json:"op"`
	ReservationID string        `json:"reservation_id"`
	CreatedAt     time.Time     `json:"created_at"`
}

// LevelBalance 是某一层级的余额视图。
type LevelBalance struct {
	Level         Level  `json:"level"`
	SubjectID     string `json:"subject_id"`
	Limit         int64  `json:"limit"`
	WindowStart   int64  `json:"window_start"`
	WindowEnd     int64  `json:"window_end"`
	Used          int64  `json:"used"`
	Reserved      int64  `json:"reserved"`
	Remaining     int64  `json:"remaining"`
	ConfigVersion int64  `json:"config_version"`
}

// ConsumeResult 是消费接口的返回结果。
type ConsumeResult struct {
	Record    ConsumeRecord  `json:"record"`
	Balances  []LevelBalance `json:"balances"`
	Duplicate bool           `json:"duplicate"`
}

// RefundResult 是退还接口的返回结果。
type RefundResult struct {
	Refund    RefundRecord  `json:"refund"`
	Consume   ConsumeRecord `json:"consume"`
	Duplicate bool          `json:"duplicate"`
}

// ReserveResult 是预占接口的返回结果。
type ReserveResult struct {
	Reservation ReservationRecord `json:"reservation"`
	Balances    []LevelBalance    `json:"balances"`
	Duplicate   bool              `json:"duplicate"`
}

// ReservationDecisionResult 是确认/取消接口的返回结果。
type ReservationDecisionResult struct {
	Reservation ReservationRecord `json:"reservation"`
	// Consume 仅确认时返回生成的正式消费记录。
	Consume   *ConsumeRecord `json:"consume,omitempty"`
	Balances  []LevelBalance `json:"balances"`
	Duplicate bool           `json:"duplicate"`
}

// insufficientError 携带具体层级信息，便于调用方定位是哪一层超额。
type insufficientError struct {
	level     Level
	subjectID string
	remaining int64
	amount    int64
}

func (e *insufficientError) Error() string {
	return fmt.Sprintf("%s: level=%s subject=%s remaining=%d requested=%d",
		ErrInsufficientQuota, e.level, e.subjectID, e.remaining, e.amount)
}

func (e *insufficientError) Unwrap() error { return ErrInsufficientQuota }
