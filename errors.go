package apiquota

import "errors"

// ErrorCode 标识可判别的错误类别，调用方可据此区分
// 额度不足、窗口、版本、状态与幂等冲突等场景。
type ErrorCode string

const (
	CodeQuotaExhausted      ErrorCode = "QUOTA_EXHAUSTED"      // 某一层额度不足
	CodeWindowExpired       ErrorCode = "WINDOW_EXPIRED"       // 退还超出限定时间窗口
	CodeVersionConflict     ErrorCode = "VERSION_CONFLICT"     // 额度记录版本条件不满足
	CodeStateConflict       ErrorCode = "STATE_CONFLICT"       // 状态冲突，如累计退还超过原消费
	CodeIdempotencyConflict ErrorCode = "IDEMPOTENCY_CONFLICT" // 同一请求号携带不同内容
	CodeNotFound            ErrorCode = "NOT_FOUND"
	CodeInvalidArgument     ErrorCode = "INVALID_ARGUMENT"
)

// Error 是服务对外返回的业务错误。
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Level   Level     `json:"level,omitempty"` // 额度不足时指示发生在哪一层
}

func (e *Error) Error() string {
	if e.Level != "" {
		return string(e.Code) + " (" + string(e.Level) + "): " + e.Message
	}
	return string(e.Code) + ": " + e.Message
}

// ErrVersionConflict 是版本条件不满足时由存储层返回的哨兵错误，
// 服务层会对其进行有限重试。
var ErrVersionConflict = &Error{Code: CodeVersionConflict, Message: "usage record version conflict"}

// CodeOf 提取错误的类别；非业务错误返回空串。
func CodeOf(err error) ErrorCode {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

func invalidArg(msg string) *Error { return &Error{Code: CodeInvalidArgument, Message: msg} }
func notFound(msg string) *Error   { return &Error{Code: CodeNotFound, Message: msg} }
