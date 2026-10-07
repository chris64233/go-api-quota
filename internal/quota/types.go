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
	// ErrReservationState 预占当前状态不允许该操作（如对已取消的预占再确认）。
	ErrReservationState = errors.New("reservation state conflict")
	// ErrWindowSealed 窗口已封存：拒绝新的消费、预占、额度调整或重复封存。
	ErrWindowSealed = errors.New("window sealed")
	// ErrWindowNotEnded 窗口尚未结束，不允许封存。
	ErrWindowNotEnded = errors.New("window not ended")
	// ErrWindowNotSealable 窗口内存在无法结算的预占（账目与预占不一致），不允许封存。
	ErrWindowNotSealable = errors.New("window has unsettleable reservations")
	// ErrSealNotFound 指定窗口没有封存快照。
	ErrSealNotFound = errors.New("seal not found")
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
// Used 是已正式消费的额度；Reserved 是预占中、尚未形成终态的额度。
// 两者都占用窗口额度，窗口内的可用额度为 Limit - Used - Reserved。
type UsageRecord struct {
	Level       Level  `json:"level"`
	SubjectID   string `json:"subject_id"`
	WindowStart int64  `json:"window_start"`
	Used        int64  `json:"used"`
	Reserved    int64  `json:"reserved"`
	Version     int64  `json:"version"`
}

// SealSnapshot 是窗口封存时固定下来的最终账目快照。
// 封存后该窗口不再接受新的消费、预占与额度调整；属于该窗口的
// 确认、取消、到期与退还只依据本快照更新结算结果（Used/Reserved），
// 每次结算推进 Version，旧版本的迟到写入会因版本条件失败，
// 不会覆盖已经发布的快照。
type SealSnapshot struct {
	// RequestID 是首次封存请求的请求号，充当封存幂等键。
	RequestID   string `json:"request_id"`
	Level       Level  `json:"level"`
	SubjectID   string `json:"subject_id"`
	WindowStart int64  `json:"window_start"`
	WindowEnd   int64  `json:"window_end"`
	Limit       int64  `json:"limit"`
	Used        int64  `json:"used"`
	Reserved    int64  `json:"reserved"`
	// OpenReservations 是封存时仍处于 reserved 状态、
	// 尚未结算的预占请求号清单。
	OpenReservations []string  `json:"open_reservations"`
	SealedAt         time.Time `json:"sealed_at"`
	Version          int64     `json:"version"`
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

// ReservationState 是预占的生命周期状态。
type ReservationState string

const (
	// ReservationReserved 预占中：三层额度已保留，等待业务结果。
	ReservationReserved ReservationState = "reserved"
	// ReservationConfirmed 已确认：预占转为正式消费（终态）。
	ReservationConfirmed ReservationState = "confirmed"
	// ReservationCancelled 已取消：额度退回原窗口（终态）。
	ReservationCancelled ReservationState = "cancelled"
	// ReservationExpired 已到期：被系统自动回收，额度退回原窗口（终态）。
	ReservationExpired ReservationState = "expired"
)

// Terminal 报告该状态是否为终态。
func (s ReservationState) Terminal() bool {
	switch s {
	case ReservationConfirmed, ReservationCancelled, ReservationExpired:
		return true
	}
	return false
}

// ReservationRecord 是一次配额预占的持久化记录，同时充当预占幂等键。
type ReservationRecord struct {
	RequestID string `json:"request_id"`
	OrgID     string `json:"org_id"`
	UserID    string `json:"user_id"`
	KeyID     string `json:"key_id"`
	Amount    int64  `json:"amount"`
	// Windows 记录预占发生时各层级所属窗口的起点。确认、取消、到期回收
	// 都只能作用于这些窗口，绝不补充已经切换到的新窗口。
	Windows   map[Level]int64  `json:"windows"`
	State     ReservationState `json:"state"`
	ExpiresAt time.Time        `json:"expires_at"`
	CreatedAt time.Time        `json:"created_at"`
	// SettledAt 是进入终态（确认/取消/到期）的时间。
	SettledAt time.Time `json:"settled_at"`
	// ConsumeRequestID 是确认后生成的正式消费记录的请求号，
	// 与预占请求号一一对应。
	ConsumeRequestID string `json:"consume_request_id,omitempty"`
	Version          int64  `json:"version"`
}

// subjects 返回预占涉及的三层主体。
func (r ReservationRecord) subjects() map[Level]string {
	return map[Level]string{LevelOrg: r.OrgID, LevelUser: r.UserID, LevelKey: r.KeyID}
}

// sameContent 判断两次同号预占请求的内容是否一致；
// 过期时长由服务端统一配置，不属于请求内容。
func (r ReservationRecord) sameContent(orgID, userID, keyID string, amount int64) bool {
	return r.OrgID == orgID && r.UserID == userID && r.KeyID == keyID && r.Amount == amount
}

// LevelBalance 是某一层级的余额视图。
// Remaining = Limit - Used - Reserved，即扣掉正式消费与预占中额度后的可用额度。
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

// ReservationResult 是预占接口的返回结果。
type ReservationResult struct {
	Reservation ReservationRecord `json:"reservation"`
	Balances    []LevelBalance    `json:"balances"`
	Duplicate   bool              `json:"duplicate"`
}

// ConfirmResult 是确认接口的返回结果。
type ConfirmResult struct {
	Reservation ReservationRecord `json:"reservation"`
	// Consume 是确认后生成（或重放时首次生成）的正式消费记录。
	Consume   ConsumeRecord  `json:"consume"`
	Balances  []LevelBalance `json:"balances"`
	Duplicate bool           `json:"duplicate"`
}

// CancelResult 是取消接口的返回结果。
type CancelResult struct {
	Reservation ReservationRecord `json:"reservation"`
	Balances    []LevelBalance    `json:"balances"`
	Duplicate   bool              `json:"duplicate"`
}

// ExpireResult 描述一次到期回收的结果。
type ExpireResult struct {
	// Expired 是本次实际回收的预占请求号。
	Expired []string `json:"expired"`
}

// SealResult 是窗口封存接口的返回结果。
type SealResult struct {
	Snapshot  SealSnapshot `json:"snapshot"`
	Duplicate bool         `json:"duplicate"`
}

// WindowEntry 描述形成某个窗口账目差异的一笔记录：
// 一笔消费、一笔退还或一笔预占。
type WindowEntry struct {
	// Kind 为 consume / refund / reservation。
	Kind      string `json:"kind"`
	RequestID string `json:"request_id"`
	// Amount 是该笔记录影响窗口账目的金额：
	// 消费与确认预占为正（增加 used），退还与取消/到期预占为负。
	Amount int64 `json:"amount"`
	// State 仅预占使用，为预占当前状态。
	State string `json:"state,omitempty"`
}

// WindowReport 是某个主体在某个窗口的完整账目视图：
// 当前用量、封存快照（若已封存）以及形成差异的全部记录。
type WindowReport struct {
	Level       Level  `json:"level"`
	SubjectID   string `json:"subject_id"`
	WindowStart int64  `json:"window_start"`
	WindowEnd   int64  `json:"window_end"`
	Limit       int64  `json:"limit"`
	Used        int64  `json:"used"`
	Reserved    int64  `json:"reserved"`
	Sealed      bool   `json:"sealed"`
	// Snapshot 仅在窗口已封存时存在。
	Snapshot *SealSnapshot `json:"snapshot,omitempty"`
	Entries  []WindowEntry `json:"entries"`
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
