package apiquota

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Service 提供组织、用户、访问密钥三层联动配额的核心逻辑。
type Service struct {
	store     *Store
	now       func() time.Time
	refundTTL time.Duration
}

// Option 定制 Service 行为。
type Option func(*Service)

// WithClock 注入时钟，便于测试窗口切换。
func WithClock(now func() time.Time) Option {
	return func(s *Service) { s.now = now }
}

// WithRefundTTL 设置消费成功后允许退还的限定时间，默认 24 小时。
func WithRefundTTL(d time.Duration) Option {
	return func(s *Service) { s.refundTTL = d }
}

// NewService 创建配额服务。
func NewService(store *Store, opts ...Option) *Service {
	s := &Service{store: store, now: time.Now, refundTTL: 24 * time.Hour}
	for _, o := range opts {
		o(s)
	}
	return s
}

// transact 执行事务；遇到版本冲突时有限重试，重试耗尽后返回版本冲突错误。
func (s *Service) transact(fn func(*Tx) error) error {
	var err error
	for i := 0; i < 8; i++ {
		err = s.store.Transact(fn)
		if !errors.Is(err, ErrVersionConflict) {
			return err
		}
	}
	return err
}

// ConfigureQuota 创建或更新某层实体的配额配置。
// 用户必须挂在已配置的组织下，密钥必须挂在已配置的用户下。
func (s *Service) ConfigureQuota(ctx context.Context, cfg QuotaConfig) (*QuotaConfig, error) {
	switch cfg.Level {
	case LevelOrg, LevelUser, LevelKey:
	default:
		return nil, invalidArg("level must be one of org|user|key")
	}
	if cfg.EntityID == "" {
		return nil, invalidArg("entity_id is required")
	}
	if cfg.Limit <= 0 {
		return nil, invalidArg("limit must be positive")
	}
	if cfg.Window <= 0 {
		return nil, invalidArg("window must be positive")
	}
	if cfg.Level != LevelOrg && cfg.ParentID == "" {
		return nil, invalidArg("parent_id is required for user and key quotas")
	}
	err := s.transact(func(tx *Tx) error {
		if cfg.Level != LevelOrg {
			parentLevel := LevelOrg
			if cfg.Level == LevelKey {
				parentLevel = LevelUser
			}
			if _, ok := tx.GetQuota(parentLevel, cfg.ParentID); !ok {
				return notFound(fmt.Sprintf("parent %s quota %q not configured", parentLevel, cfg.ParentID))
			}
		}
		var expected int64
		if existing, ok := tx.GetQuota(cfg.Level, cfg.EntityID); ok {
			expected = existing.Version
		}
		return tx.PutQuota(cfg, expected)
	})
	if err != nil {
		return nil, err
	}
	out, _ := s.GetQuota(ctx, cfg.Level, cfg.EntityID)
	return out, nil
}

// GetQuota 读取某层实体的配额配置。
func (s *Service) GetQuota(ctx context.Context, level Level, entityID string) (*QuotaConfig, error) {
	var out QuotaConfig
	err := s.store.Transact(func(tx *Tx) error {
		cfg, ok := tx.GetQuota(level, entityID)
		if !ok {
			return notFound(fmt.Sprintf("%s quota %q not configured", level, entityID))
		}
		out = cfg
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// chain 解析 key→user→org 的完整配额链，任一环节未配置都会失败。
func resolveChain(tx *Tx, keyID string) ([3]QuotaConfig, error) {
	var chain [3]QuotaConfig
	key, ok := tx.GetQuota(LevelKey, keyID)
	if !ok {
		return chain, notFound(fmt.Sprintf("key quota %q not configured", keyID))
	}
	user, ok := tx.GetQuota(LevelUser, key.ParentID)
	if !ok {
		return chain, notFound(fmt.Sprintf("user quota %q not configured", key.ParentID))
	}
	org, ok := tx.GetQuota(LevelOrg, user.ParentID)
	if !ok {
		return chain, notFound(fmt.Sprintf("org quota %q not configured", user.ParentID))
	}
	chain[0], chain[1], chain[2] = org, user, key
	return chain, nil
}

// Consume 在当前窗口内对三层配额联动扣减。
// 三层都有足够额度才成功，扣减作为一个整体提交；
// 同一 request_id 重复调用返回首次结果，携带不同内容则报幂等冲突。
func (s *Service) Consume(ctx context.Context, req ConsumeRequest) (*Consumption, error) {
	if req.RequestID == "" {
		return nil, invalidArg("request_id is required")
	}
	if req.KeyID == "" {
		return nil, invalidArg("key_id is required")
	}
	if req.Amount <= 0 {
		return nil, invalidArg("amount must be positive")
	}
	fp := fingerprint("consume", req)
	var out Consumption
	err := s.transact(func(tx *Tx) error {
		if rec, ok := tx.GetIdempotency(req.RequestID); ok {
			if rec.Fingerprint != fp {
				return &Error{Code: CodeIdempotencyConflict, Message: "request_id reused with a different payload"}
			}
			return json.Unmarshal(rec.Response, &out)
		}
		chain, err := resolveChain(tx, req.KeyID)
		if err != nil {
			return err
		}
		now := s.now()
		type step struct {
			cfg   QuotaConfig
			usage UsageRecord
		}
		steps := make([]step, 0, 3)
		for _, cfg := range chain {
			ws := windowStart(now, cfg.Window)
			u, _ := tx.GetUsage(cfg.Level, cfg.EntityID, ws.Unix())
			u.Level, u.EntityID, u.WindowStart = cfg.Level, cfg.EntityID, ws
			if cfg.Limit-u.Used < req.Amount {
				return &Error{
					Code:  CodeQuotaExhausted,
					Level: cfg.Level,
					Message: fmt.Sprintf("%s quota exhausted: limit=%d used=%d requested=%d",
						cfg.Level, cfg.Limit, u.Used, req.Amount),
				}
			}
			steps = append(steps, step{cfg: cfg, usage: u})
		}
		c := Consumption{
			ID:        newID(),
			RequestID: req.RequestID,
			OrgID:     chain[0].EntityID,
			UserID:    chain[1].EntityID,
			KeyID:     chain[2].EntityID,
			Amount:    req.Amount,
			CreatedAt: now,
			Windows:   make(map[Level]time.Time, 3),
		}
		for _, st := range steps {
			u := st.usage
			expected := u.Version
			u.Used += req.Amount
			if err := tx.PutUsage(u, expected); err != nil {
				return err
			}
			c.Windows[st.cfg.Level] = u.WindowStart
		}
		if err := tx.PutConsumption(c, 0); err != nil {
			return err
		}
		resp, err := json.Marshal(c)
		if err != nil {
			return err
		}
		if err := tx.PutIdempotency(IdempotencyRecord{
			RequestID: req.RequestID, Fingerprint: fp, Response: resp, CreatedAt: now,
		}); err != nil {
			return err
		}
		out = c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// Refund 对一笔成功消费做部分或全部退还。
// 退还必须发生在消费后的限定时间内，可多次退，累计不得超过原消费；
// 额度一律回退到原消费所属窗口，绝不补充新窗口。
// 同一 request_id 重复调用返回首次结果，携带不同内容则报幂等冲突。
func (s *Service) Refund(ctx context.Context, req RefundRequest) (*RefundResult, error) {
	if req.RequestID == "" {
		return nil, invalidArg("request_id is required")
	}
	if req.ConsumptionID == "" {
		return nil, invalidArg("consumption_id is required")
	}
	if req.Amount <= 0 {
		return nil, invalidArg("amount must be positive")
	}
	fp := fingerprint("refund", req)
	var out RefundResult
	err := s.transact(func(tx *Tx) error {
		if rec, ok := tx.GetIdempotency(req.RequestID); ok {
			if rec.Fingerprint != fp {
				return &Error{Code: CodeIdempotencyConflict, Message: "request_id reused with a different payload"}
			}
			return json.Unmarshal(rec.Response, &out)
		}
		c, ok := tx.GetConsumption(req.ConsumptionID)
		if !ok {
			return notFound(fmt.Sprintf("consumption %q not found", req.ConsumptionID))
		}
		now := s.now()
		if now.After(c.CreatedAt.Add(s.refundTTL)) {
			return &Error{
				Code:    CodeWindowExpired,
				Message: fmt.Sprintf("refund window expired: consumed at %s, ttl %s", c.CreatedAt.Format(time.RFC3339), s.refundTTL),
			}
		}
		if c.Refunded+req.Amount > c.Amount {
			return &Error{
				Code:    CodeStateConflict,
				Message: fmt.Sprintf("cumulative refund would exceed original consumption: refunded=%d amount=%d requested=%d", c.Refunded, c.Amount, req.Amount),
			}
		}
		// 回退到原消费所属窗口的用量记录，即使该窗口已经关闭。
		for _, lvl := range levelOrder {
			entityID := c.OrgID
			if lvl == LevelUser {
				entityID = c.UserID
			} else if lvl == LevelKey {
				entityID = c.KeyID
			}
			ws := c.Windows[lvl]
			u, ok := tx.GetUsage(lvl, entityID, ws.Unix())
			if !ok || u.Used < req.Amount {
				return &Error{Code: CodeStateConflict, Message: fmt.Sprintf("usage record inconsistent for %s %q", lvl, entityID)}
			}
			expected := u.Version
			u.Used -= req.Amount
			if err := tx.PutUsage(u, expected); err != nil {
				return err
			}
		}
		expected := c.Version
		c.Refunded += req.Amount
		if err := tx.PutConsumption(c, expected); err != nil {
			return err
		}
		out = RefundResult{
			ConsumptionID:       c.ID,
			Amount:              req.Amount,
			TotalRefunded:       c.Refunded,
			RemainingRefundable: c.Amount - c.Refunded,
			RefundedAt:          now,
		}
		resp, err := json.Marshal(out)
		if err != nil {
			return err
		}
		if err := tx.PutIdempotency(IdempotencyRecord{
			RequestID: req.RequestID, Fingerprint: fp, Response: resp, CreatedAt: now,
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// Balances 返回 key 所属链路上三层在当前窗口的余额。
func (s *Service) Balances(ctx context.Context, keyID string) (*Balances, error) {
	if keyID == "" {
		return nil, invalidArg("key_id is required")
	}
	var out Balances
	err := s.store.Transact(func(tx *Tx) error {
		chain, err := resolveChain(tx, keyID)
		if err != nil {
			return err
		}
		now := s.now()
		dst := []*Balance{&out.Org, &out.User, &out.Key}
		for i, cfg := range chain {
			ws := windowStart(now, cfg.Window)
			u, _ := tx.GetUsage(cfg.Level, cfg.EntityID, ws.Unix())
			*dst[i] = Balance{
				Level:       cfg.Level,
				EntityID:    cfg.EntityID,
				Limit:       cfg.Limit,
				Used:        u.Used,
				Remaining:   cfg.Limit - u.Used,
				WindowStart: ws,
				WindowEnd:   ws.Add(cfg.Window),
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// fingerprint 计算请求内容的指纹，用于幂等判定。
func fingerprint(op string, v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return op + ":" + hex.EncodeToString(sum[:])
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
