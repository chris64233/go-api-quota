package quota

import (
	"context"
	"fmt"
	"time"
)

// 默认预占有效期与到期回收扫描间隔。
const (
	defaultReservationTTL = 5 * time.Minute
	// consumeIDPrefix 让确认生成的消费记录请求号与预占请求号一一对应，
	// 同时避免与普通消费的请求号撞键。
	consumeIDPrefix = "reservation:"
)

// WithReservationTTL 设置预占的默认有效时长；请求显式给出 ttl_seconds 时以请求为准。
func WithReservationTTL(d time.Duration) Option {
	return func(s *Service) { s.reservationTTL = d }
}

// ReserveRequest 是一次预占请求。
type ReserveRequest struct {
	RequestID string `json:"request_id"`
	OrgID     string `json:"org_id"`
	UserID    string `json:"user_id"`
	KeyID     string `json:"key_id"`
	Amount    int64  `json:"amount"`
	// TTLSeconds 为 0 时使用服务默认有效期。
	TTLSeconds int64 `json:"ttl_seconds"`
}

func (r ReserveRequest) subjects() map[Level]string {
	return map[Level]string{LevelOrg: r.OrgID, LevelUser: r.UserID, LevelKey: r.KeyID}
}

func reservationConsumeID(requestID string) string { return consumeIDPrefix + requestID }

// Reserve 原子锁定组织、用户、访问密钥三层额度：三层当前窗口的
// 可用额度（limit - used - reserved）都足够时才整体预占成功，
// 任一层不足整笔失败、不保留任何额度。预占记录原窗口、数量与过期时间。
//
// 同一 RequestID 重试：同内容返回首次结果（Duplicate=true），
// 不同内容返回 ErrIdempotencyConflict。
func (s *Service) Reserve(ctx context.Context, req ReserveRequest) (ReservationResult, error) {
	if req.RequestID == "" || req.OrgID == "" || req.UserID == "" || req.KeyID == "" {
		return ReservationResult{}, fmt.Errorf("%w: request id, org id, user id and key id are required", ErrValidation)
	}
	if req.Amount <= 0 {
		return ReservationResult{}, fmt.Errorf("%w: amount must be positive", ErrValidation)
	}
	if req.TTLSeconds < 0 {
		return ReservationResult{}, fmt.Errorf("%w: ttl seconds must not be negative", ErrValidation)
	}
	ttl := s.reservationTTL
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}

	var result ReservationResult
	err := s.withRetry(ctx, func(tx *Tx) error {
		now := s.now()
		// 幂等：同号同内容返回首次结果，同号异内容报冲突。
		if prev, ok := tx.GetReservation(req.RequestID); ok {
			if !prev.sameContent(req.OrgID, req.UserID, req.KeyID, req.Amount) {
				return fmt.Errorf("%w: request %s already used with different content",
					ErrIdempotencyConflict, req.RequestID)
			}
			balances, err := loadBalances(tx, s.now, req.subjects())
			if err != nil {
				return err
			}
			result = ReservationResult{Reservation: prev, Balances: balances, Duplicate: true}
			return nil
		}

		subjects := req.subjects()
		windows := map[Level]int64{}
		usages := map[Level]UsageRecord{}
		versions := map[Level]int64{}
		for _, level := range Levels {
			cfg, ok := tx.GetConfig(level, subjects[level])
			if !ok {
				return fmt.Errorf("%w: level=%s subject=%s", ErrQuotaNotConfigured, level, subjects[level])
			}
			start := cfg.WindowStart(now)
			windows[level] = start
			if err := ensureWindowOpen(tx, level, subjects[level], start); err != nil {
				return err
			}
			usage, ok := tx.GetUsage(level, subjects[level], start)
			if !ok {
				usage = UsageRecord{Level: level, SubjectID: subjects[level], WindowStart: start}
			}
			// 预占与正式消费一样占用窗口额度。
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

		// 三层都够才提交：把额度从可用转为预占中。
		for _, level := range Levels {
			usage := usages[level]
			usage.Reserved += req.Amount
			if err := tx.PutUsage(usage, versions[level]); err != nil {
				return err
			}
		}
		rec := ReservationRecord{
			RequestID: req.RequestID,
			OrgID:     req.OrgID,
			UserID:    req.UserID,
			KeyID:     req.KeyID,
			Amount:    req.Amount,
			Windows:   windows,
			State:     ReservationReserved,
			ExpiresAt: now.Add(ttl),
			CreatedAt: now,
		}
		if err := tx.PutReservation(rec, 0); err != nil {
			return err
		}
		rec.Version = 1
		balances, err := loadBalances(tx, s.now, subjects)
		if err != nil {
			return err
		}
		result = ReservationResult{Reservation: rec, Balances: balances}
		return nil
	})
	return result, err
}

// ConfirmReservation 把预占转为正式消费：只在预占记录的原窗口内把额度
// 从 reserved 转为 used，不触碰当前新窗口。仅 reserved 状态可确认，
// 重复确认返回首次结果（Duplicate=true）。
func (s *Service) ConfirmReservation(ctx context.Context, requestID string) (ConfirmResult, error) {
	if requestID == "" {
		return ConfirmResult{}, fmt.Errorf("%w: request id is required", ErrValidation)
	}
	// 若已到期，先在独立事务里完成回收，再让后续判断看到终态。
	if err := s.expireIfDue(ctx, requestID); err != nil {
		return ConfirmResult{}, err
	}

	var result ConfirmResult
	var expiredInTx bool
	err := s.withRetry(ctx, func(tx *Tx) error {
		now := s.now()
		expiredInTx = false // 重试时清除上一次尝试的残留标记。
		rec, ok := tx.GetReservation(requestID)
		if !ok {
			return fmt.Errorf("%w: %s", ErrReservationNotFound, requestID)
		}
		subjects := rec.subjects()
		switch {
		case rec.State == ReservationConfirmed:
			consume, ok := tx.GetConsume(rec.ConsumeRequestID)
			if !ok {
				return fmt.Errorf("%w: consume %s for reservation %s",
					ErrConsumeNotFound, rec.ConsumeRequestID, requestID)
			}
			balances, err := loadBalances(tx, s.now, subjects)
			if err != nil {
				return err
			}
			result = ConfirmResult{Reservation: rec, Consume: consume, Balances: balances, Duplicate: true}
			return nil
		case rec.State != ReservationReserved:
			return fmt.Errorf("%w: reservation %s is %s", ErrReservationState, requestID, rec.State)
		}
		// 状态迁移事务内再次判定到期：到期则在本事务内直接回收并提交，
		// 消除“惰性回收与确认之间跨过到期点”的竞态。
		if !now.Before(rec.ExpiresAt) {
			if _, err := settleReservation(tx, rec, ReservationExpired, now); err != nil {
				return err
			}
			rec.State = ReservationExpired
			rec.SettledAt = now
			if err := tx.PutReservation(rec, rec.Version); err != nil {
				return err
			}
			expiredInTx = true
			return nil
		}

		// 在原窗口内把预占转为正式消费。
		consume, err := settleReservation(tx, rec, ReservationConfirmed, now)
		if err != nil {
			return err
		}
		rec.State = ReservationConfirmed
		rec.SettledAt = now
		rec.ConsumeRequestID = consume.RequestID
		if err := tx.PutReservation(rec, rec.Version); err != nil {
			return err
		}
		if err := tx.PutConsume(consume, 0); err != nil {
			return err
		}
		rec.Version++
		consume.Version = 1
		balances, err := loadBalances(tx, s.now, subjects)
		if err != nil {
			return err
		}
		result = ConfirmResult{Reservation: rec, Consume: consume, Balances: balances}
		return nil
	})
	if err != nil {
		return ConfirmResult{}, err
	}
	if expiredInTx {
		return ConfirmResult{}, fmt.Errorf("%w: reservation %s is %s",
			ErrReservationState, requestID, ReservationExpired)
	}
	return result, nil
}

// CancelReservation 取消预占：只把额度释放回预占记录的原窗口。
// 仅 reserved 状态可取消；重复取消返回首次结果（Duplicate=true），
// 对已确认/已到期的预占取消返回 ErrReservationState。
func (s *Service) CancelReservation(ctx context.Context, requestID string) (CancelResult, error) {
	if requestID == "" {
		return CancelResult{}, fmt.Errorf("%w: request id is required", ErrValidation)
	}
	if err := s.expireIfDue(ctx, requestID); err != nil {
		return CancelResult{}, err
	}

	var result CancelResult
	var expiredInTx bool
	err := s.withRetry(ctx, func(tx *Tx) error {
		now := s.now()
		expiredInTx = false // 重试时清除上一次尝试的残留标记。
		rec, ok := tx.GetReservation(requestID)
		if !ok {
			return fmt.Errorf("%w: %s", ErrReservationNotFound, requestID)
		}
		subjects := rec.subjects()
		switch {
		case rec.State == ReservationCancelled:
			balances, err := loadBalances(tx, s.now, subjects)
			if err != nil {
				return err
			}
			result = CancelResult{Reservation: rec, Balances: balances, Duplicate: true}
			return nil
		case rec.State != ReservationReserved:
			return fmt.Errorf("%w: reservation %s is %s", ErrReservationState, requestID, rec.State)
		}
		// 事务内再次判定到期：跨过到期点时在本事务内直接回收。
		if !now.Before(rec.ExpiresAt) {
			if _, err := settleReservation(tx, rec, ReservationExpired, now); err != nil {
				return err
			}
			rec.State = ReservationExpired
			rec.SettledAt = now
			if err := tx.PutReservation(rec, rec.Version); err != nil {
				return err
			}
			expiredInTx = true
			return nil
		}

		if _, err := settleReservation(tx, rec, ReservationCancelled, now); err != nil {
			return err
		}
		rec.State = ReservationCancelled
		rec.SettledAt = now
		if err := tx.PutReservation(rec, rec.Version); err != nil {
			return err
		}
		rec.Version++
		balances, err := loadBalances(tx, s.now, subjects)
		if err != nil {
			return err
		}
		result = CancelResult{Reservation: rec, Balances: balances}
		return nil
	})
	if err != nil {
		return CancelResult{}, err
	}
	if expiredInTx {
		return CancelResult{}, fmt.Errorf("%w: reservation %s is %s",
			ErrReservationState, requestID, ReservationExpired)
	}
	return result, nil
}

// settleReservation 在事务内按终态调整原窗口用量：
// confirmed 把 reserved 转为 used；cancelled/expired 仅释放 reserved。
// 所有调整都只作用于预占记录保存的原窗口，与当前窗口无关。
// 窗口已封存时改为依据封存快照更新结算结果并推进快照版本。
func settleReservation(tx *Tx, rec ReservationRecord, state ReservationState, now time.Time) (ConsumeRecord, error) {
	subjects := rec.subjects()
	for _, level := range Levels {
		var usedDelta int64
		if state == ReservationConfirmed {
			usedDelta = rec.Amount
		}
		if err := adjustWindowUsage(tx, level, subjects[level], rec.Windows[level], usedDelta, -rec.Amount); err != nil {
			return ConsumeRecord{}, err
		}
	}
	if state != ReservationConfirmed {
		return ConsumeRecord{}, nil
	}
	return ConsumeRecord{
		RequestID: reservationConsumeID(rec.RequestID),
		OrgID:     rec.OrgID,
		UserID:    rec.UserID,
		KeyID:     rec.KeyID,
		Amount:    rec.Amount,
		Windows:   rec.Windows,
		CreatedAt: now,
	}, nil
}

// GetReservation 查询预占状态。已到期但尚未被扫描回收的预占会先完成回收，
// 因此查询看到的状态不会晚于实际到期时间。
func (s *Service) GetReservation(ctx context.Context, requestID string) (ReservationRecord, error) {
	if requestID == "" {
		return ReservationRecord{}, fmt.Errorf("%w: request id is required", ErrValidation)
	}
	if err := s.expireIfDue(ctx, requestID); err != nil {
		return ReservationRecord{}, err
	}
	var out ReservationRecord
	err := s.store.View(ctx, func(tx *Tx) error {
		rec, ok := tx.GetReservation(requestID)
		if !ok {
			return fmt.Errorf("%w: %s", ErrReservationNotFound, requestID)
		}
		out = rec
		return nil
	})
	return out, err
}

// expireIfDue 在独立事务里回收单笔已到期预占。未到期或已终态时为空操作。
// 独立事务保证回收本身被提交，不会随业务事务回滚。
func (s *Service) expireIfDue(ctx context.Context, requestID string) error {
	return s.store.Update(ctx, func(tx *Tx) error {
		rec, ok := tx.GetReservation(requestID)
		if !ok || rec.State != ReservationReserved {
			return nil
		}
		now := s.now()
		if now.Before(rec.ExpiresAt) {
			return nil
		}
		if _, err := settleReservation(tx, rec, ReservationExpired, now); err != nil {
			return err
		}
		rec.State = ReservationExpired
		rec.SettledAt = now
		return tx.PutReservation(rec, rec.Version)
	})
}

// ExpireDue 扫描并回收所有已到期仍处于 reserved 状态的预占，
// 额度一律退回各预占的原窗口，返回本次实际回收的请求号。
func (s *Service) ExpireDue(ctx context.Context) (ExpireResult, error) {
	out := ExpireResult{Expired: []string{}}
	// withRetry：扫描期间某笔被并发的惰性回收先行迁移时会产生版本冲突，
	// 重试后该笔已是终态、自动跳过，不影响其余到期预占回收。
	err := s.withRetry(ctx, func(tx *Tx) error {
		out.Expired = out.Expired[:0]
		now := s.now()
		for _, rec := range tx.ListReservations() {
			if rec.State != ReservationReserved || now.Before(rec.ExpiresAt) {
				continue
			}
			if _, err := settleReservation(tx, rec, ReservationExpired, now); err != nil {
				return err
			}
			rec.State = ReservationExpired
			rec.SettledAt = now
			if err := tx.PutReservation(rec, rec.Version); err != nil {
				return err
			}
			out.Expired = append(out.Expired, rec.RequestID)
		}
		return nil
	})
	return out, err
}

// RunExpiryLoop 按固定间隔执行到期回收，直到 ctx 取消。
// 供服务进程后台调用；到期判定以注入时钟为准。
func (s *Service) RunExpiryLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.ExpireDue(ctx)
		}
	}
}
