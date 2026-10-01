package quota

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// maxVersionRetries 是版本冲突时的最大重试次数。
// 正常负载下冲突极少，重试即可收敛；超过上限说明竞争异常激烈。
const maxVersionRetries = 8

// Service 实现三层配额的配置、消费、退还、预占与查询。
type Service struct {
	store Store
	// now 可注入，便于测试窗口切换。
	now func() time.Time
	// refundTTL 是消费成功后允许退还的时间范围。
	refundTTL time.Duration
	// reservationTTL 是预占的默认有效时长。
	reservationTTL time.Duration
}

// Option 定制 Service 行为。
type Option func(*Service)

// WithClock 注入时钟（测试用）。
func WithClock(now func() time.Time) Option {
	return func(s *Service) { s.now = now }
}

// WithRefundTTL 设置退还的有效时长。
func WithRefundTTL(d time.Duration) Option {
	return func(s *Service) { s.refundTTL = d }
}

// NewService 创建配额服务。
func NewService(store Store, opts ...Option) *Service {
	s := &Service{
		store:          store,
		now:            time.Now,
		refundTTL:      24 * time.Hour,
		reservationTTL: defaultReservationTTL,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// SetConfig 配置某层级某主体的配额。expectedVersion 为 0 表示新建；
// 非 0 时作为版本条件，与存储版本不一致返回 ErrVersionConflict。
func (s *Service) SetConfig(ctx context.Context, level Level, subjectID string, limit, windowSeconds, expectedVersion int64) (QuotaConfig, error) {
	if !level.Valid() {
		return QuotaConfig{}, fmt.Errorf("%w: invalid level %q", ErrValidation, level)
	}
	if subjectID == "" {
		return QuotaConfig{}, fmt.Errorf("%w: subject id is required", ErrValidation)
	}
	if limit <= 0 {
		return QuotaConfig{}, fmt.Errorf("%w: limit must be positive", ErrValidation)
	}
	if windowSeconds <= 0 {
		return QuotaConfig{}, fmt.Errorf("%w: window seconds must be positive", ErrValidation)
	}
	if expectedVersion < 0 {
		return QuotaConfig{}, fmt.Errorf("%w: expected version must not be negative", ErrValidation)
	}
	var out QuotaConfig
	err := s.store.Update(ctx, func(tx *Tx) error {
		cfg := QuotaConfig{
			Level:         level,
			SubjectID:     subjectID,
			Limit:         limit,
			WindowSeconds: windowSeconds,
			UpdatedAt:     s.now(),
		}
		if err := tx.PutConfig(cfg, expectedVersion); err != nil {
			return err
		}
		cfg.Version = expectedVersion + 1
		out = cfg
		return nil
	})
	return out, err
}

// GetConfig 读取某层级某主体的配额配置。
func (s *Service) GetConfig(ctx context.Context, level Level, subjectID string) (QuotaConfig, error) {
	var out QuotaConfig
	err := s.store.View(ctx, func(tx *Tx) error {
		cfg, ok := tx.GetConfig(level, subjectID)
		if !ok {
			return fmt.Errorf("%w: level=%s subject=%s", ErrQuotaNotConfigured, level, subjectID)
		}
		out = cfg
		return nil
	})
	return out, err
}

// ConsumeRequest 是一次消费请求。
type ConsumeRequest struct {
	RequestID string `json:"request_id"`
	OrgID     string `json:"org_id"`
	UserID    string `json:"user_id"`
	KeyID     string `json:"key_id"`
	Amount    int64  `json:"amount"`
}

// subjects 返回请求涉及的三层主体。
func (r ConsumeRequest) subjects() map[Level]string {
	return map[Level]string{LevelOrg: r.OrgID, LevelUser: r.UserID, LevelKey: r.KeyID}
}

// Consume 在三个层级联动扣减额度：只有当前窗口内三层都有足够额度才成功，
// 扣减作为一个整体提交。同一 RequestID 重试返回首次结果；
// 同号不同内容返回 ErrIdempotencyConflict。
func (s *Service) Consume(ctx context.Context, req ConsumeRequest) (ConsumeResult, error) {
	if req.RequestID == "" || req.OrgID == "" || req.UserID == "" || req.KeyID == "" {
		return ConsumeResult{}, fmt.Errorf("%w: request id, org id, user id and key id are required", ErrValidation)
	}
	if req.Amount <= 0 {
		return ConsumeResult{}, fmt.Errorf("%w: amount must be positive", ErrValidation)
	}
	var result ConsumeResult
	err := s.withRetry(ctx, func(tx *Tx) error {
		now := s.now()
		// 幂等：同号同内容直接返回首次结果，同号异内容报冲突。
		if prev, ok := tx.GetConsume(req.RequestID); ok {
			if !prev.sameContent(req.OrgID, req.UserID, req.KeyID, req.Amount) {
				return fmt.Errorf("%w: request %s already used with different content",
					ErrIdempotencyConflict, req.RequestID)
			}
			balances, err := loadBalances(tx, s.now, req.subjects())
			if err != nil {
				return err
			}
			result = ConsumeResult{Record: prev, Balances: balances, Duplicate: true}
			return nil
		}

		subjects := req.subjects()
		configs := map[Level]QuotaConfig{}
		windows := map[Level]int64{}
		usages := map[Level]UsageRecord{}
		versions := map[Level]int64{}
		for _, level := range Levels {
			cfg, ok := tx.GetConfig(level, subjects[level])
			if !ok {
				return fmt.Errorf("%w: level=%s subject=%s", ErrQuotaNotConfigured, level, subjects[level])
			}
			configs[level] = cfg
			start := cfg.WindowStart(now)
			windows[level] = start
			usage, ok := tx.GetUsage(level, subjects[level], start)
			if !ok {
				usage = UsageRecord{Level: level, SubjectID: subjects[level], WindowStart: start}
			}
			if usage.Used+usage.Reserved+req.Amount > cfg.Limit {
				return &insufficientError{
					level:     level,
					subjectID: subjects[level],
					remaining: cfg.Limit - usage.Used - usage.Reserved,
					amount:    req.Amount,
				}
			}
			usages[level] = usage
			versions[level] = usage.Version
		}

		// 三层都够才提交：任一层版本变化都会让事务失败并重试。
		for _, level := range Levels {
			usage := usages[level]
			usage.Used += req.Amount
			if err := tx.PutUsage(usage, versions[level]); err != nil {
				return err
			}
		}
		rec := ConsumeRecord{
			RequestID: req.RequestID,
			OrgID:     req.OrgID,
			UserID:    req.UserID,
			KeyID:     req.KeyID,
			Amount:    req.Amount,
			Windows:   windows,
			CreatedAt: now,
		}
		if err := tx.PutConsume(rec, 0); err != nil {
			return err
		}
		rec.Version = 1
		balances, err := loadBalances(tx, s.now, subjects)
		if err != nil {
			return err
		}
		result = ConsumeResult{Record: rec, Balances: balances}
		return nil
	})
	return result, err
}

// RefundRequest 是一次退还请求。
type RefundRequest struct {
	RequestID        string `json:"request_id"`
	ConsumeRequestID string `json:"consume_request_id"`
	Amount           int64  `json:"amount"`
}

// Refund 对一次成功消费做部分或全部退还，可多次退，累计不超过原消费。
// 退还只回补原消费所属窗口的用量，即使窗口已经切换也不会补充新窗口。
// 同一 RequestID 重试返回首次结果，同号异内容返回 ErrIdempotencyConflict。
func (s *Service) Refund(ctx context.Context, req RefundRequest) (RefundResult, error) {
	if req.RequestID == "" || req.ConsumeRequestID == "" {
		return RefundResult{}, fmt.Errorf("%w: request id and consume request id are required", ErrValidation)
	}
	if req.Amount <= 0 {
		return RefundResult{}, fmt.Errorf("%w: amount must be positive", ErrValidation)
	}
	var result RefundResult
	err := s.withRetry(ctx, func(tx *Tx) error {
		now := s.now()
		// 退还幂等。
		if prev, ok := tx.GetRefund(req.RequestID); ok {
			if prev.ConsumeRequestID != req.ConsumeRequestID || prev.Amount != req.Amount {
				return fmt.Errorf("%w: request %s already used with different content",
					ErrIdempotencyConflict, req.RequestID)
			}
			consume, ok := tx.GetConsume(prev.ConsumeRequestID)
			if !ok {
				return fmt.Errorf("%w: %s", ErrConsumeNotFound, prev.ConsumeRequestID)
			}
			result = RefundResult{Refund: prev, Consume: consume, Duplicate: true}
			return nil
		}

		consume, ok := tx.GetConsume(req.ConsumeRequestID)
		if !ok {
			return fmt.Errorf("%w: %s", ErrConsumeNotFound, req.ConsumeRequestID)
		}
		if now.After(consume.CreatedAt.Add(s.refundTTL)) {
			return fmt.Errorf("%w: consume %s created at %s, ttl %s",
				ErrRefundWindowExpired, consume.RequestID, consume.CreatedAt, s.refundTTL)
		}
		if consume.Refunded+req.Amount > consume.Amount {
			return fmt.Errorf("%w: consume %s refundable=%d requested=%d",
				ErrRefundExceeds, consume.RequestID, consume.Refundable(), req.Amount)
		}

		// 回补到原消费记录的窗口，与当前所属窗口无关。
		subjects := map[Level]string{LevelOrg: consume.OrgID, LevelUser: consume.UserID, LevelKey: consume.KeyID}
		for _, level := range Levels {
			start := consume.Windows[level]
			usage, ok := tx.GetUsage(level, subjects[level], start)
			if !ok {
				return fmt.Errorf("%w: usage record missing for level=%s subject=%s window=%d",
					ErrVersionConflict, level, subjects[level], start)
			}
			usage.Used -= req.Amount
			if usage.Used < 0 {
				usage.Used = 0
			}
			if err := tx.PutUsage(usage, usage.Version); err != nil {
				return err
			}
		}
		consume.Refunded += req.Amount
		if err := tx.PutConsume(consume, consume.Version); err != nil {
			return err
		}
		refund := RefundRecord{
			RequestID:        req.RequestID,
			ConsumeRequestID: req.ConsumeRequestID,
			Amount:           req.Amount,
			CreatedAt:        now,
		}
		if err := tx.PutRefund(refund); err != nil {
			return err
		}
		consume.Version++
		result = RefundResult{Refund: refund, Consume: consume}
		return nil
	})
	return result, err
}

// Balances 返回三个层级当前窗口的余额视图。
func (s *Service) Balances(ctx context.Context, orgID, userID, keyID string) ([]LevelBalance, error) {
	subjects := map[Level]string{LevelOrg: orgID, LevelUser: userID, LevelKey: keyID}
	var out []LevelBalance
	err := s.store.View(ctx, func(tx *Tx) error {
		balances, err := loadBalances(tx, s.now, subjects)
		if err != nil {
			return err
		}
		out = balances
		return nil
	})
	return out, err
}

// loadBalances 在事务内读取三层余额；未配置的层级返回 ErrQuotaNotConfigured。
func loadBalances(tx *Tx, now func() time.Time, subjects map[Level]string) ([]LevelBalance, error) {
	balances := make([]LevelBalance, 0, len(Levels))
	for _, level := range Levels {
		subjectID := subjects[level]
		cfg, ok := tx.GetConfig(level, subjectID)
		if !ok {
			return nil, fmt.Errorf("%w: level=%s subject=%s", ErrQuotaNotConfigured, level, subjectID)
		}
		start := cfg.WindowStart(now())
		var used, reserved int64
		if usage, ok := tx.GetUsage(level, subjectID, start); ok {
			used = usage.Used
			reserved = usage.Reserved
		}
		balances = append(balances, LevelBalance{
			Level:                  level,
			SubjectID:              subjectID,
			Limit:                  cfg.Limit,
			WindowStart:            start,
			WindowEnd:              start + cfg.WindowSeconds,
			Used:                   used,
			Reserved:               reserved,
			Remaining:              cfg.Limit - used - reserved,
			ConfigVersion:          cfg.Version,
			LastRebalanceRequestID: cfg.LastRebalanceRequestID,
		})
	}
	return balances, nil
}

// withRetry 在版本冲突时重试事务；其他错误直接返回。
func (s *Service) withRetry(ctx context.Context, fn func(tx *Tx) error) error {
	var err error
	for i := 0; i < maxVersionRetries; i++ {
		err = s.store.Update(ctx, fn)
		if !errors.Is(err, ErrVersionConflict) {
			return err
		}
	}
	return err
}
