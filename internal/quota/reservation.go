package quota

import (
	"context"
	"fmt"
	"time"
)

// consumeIDSuffix 追加在预占请求号之后，构成确认所生成消费记录的请求号。
const consumeIDSuffix = "/consume"

// ReserveRequest 是一次预占请求。TTLSeconds 为 0 时使用服务缺省时长。
type ReserveRequest struct {
	RequestID  string `json:"request_id"`
	OrgID      string `json:"org_id"`
	UserID     string `json:"user_id"`
	KeyID      string `json:"key_id"`
	Amount     int64  `json:"amount"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

func (r ReserveRequest) subjects() map[Level]string {
	return map[Level]string{LevelOrg: r.OrgID, LevelUser: r.UserID, LevelKey: r.KeyID}
}

// Reserve 在三个层级原子预占额度：三层当前窗口的可用额度（扣除已用与已预占）
// 都足够时才成功，任一层不足整笔失败。预占记录原窗口、数量与过期时间。
// 同一 RequestID 重试返回首次结果；同号异内容返回 ErrIdempotencyConflict。
func (s *Service) Reserve(ctx context.Context, req ReserveRequest) (ReserveResult, error) {
	if req.RequestID == "" || req.OrgID == "" || req.UserID == "" || req.KeyID == "" {
		return ReserveResult{}, fmt.Errorf("%w: request id, org id, user id and key id are required", ErrValidation)
	}
	if req.Amount <= 0 {
		return ReserveResult{}, fmt.Errorf("%w: amount must be positive", ErrValidation)
	}
	if req.TTLSeconds < 0 {
		return ReserveResult{}, fmt.Errorf("%w: ttl seconds must not be negative", ErrValidation)
	}
	ttl := s.reservationTTL
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}
	ttlSeconds := int64(ttl / time.Second)
	if ttlSeconds <= 0 {
		return ReserveResult{}, fmt.Errorf("%w: effective ttl %s must be at least one second", ErrValidation, ttl)
	}
	var result ReserveResult
	err := s.withRetry(ctx, func(tx *Tx) error {
		now := s.now()
		expiresAt := now.Add(ttl)

		// 幂等：同号同内容返回首次结果（含其当前状态），同号异内容报冲突。
		if prev, ok := tx.GetReservation(req.RequestID); ok {
			if !prev.sameContent(req.OrgID, req.UserID, req.KeyID, req.Amount, ttlSeconds) {
				return fmt.Errorf("%w: request %s already used with different content",
					ErrIdempotencyConflict, req.RequestID)
			}
			if err := sweepExpiredReservations(tx, now); err != nil {
				return err
			}
			// sweep 可能已把该预占置为过期，重新读取以返回最新状态。
			if cur, ok := tx.GetReservation(req.RequestID); ok {
				prev = cur
			}
			balances, err := loadBalances(tx, s.now, req.subjects())
			if err != nil {
				return err
			}
			result = ReserveResult{Reservation: prev, Balances: balances, Duplicate: true}
			return nil
		}

		// 先自动回收已到期预占，释放出来的额度可用于本次预占。
		if err := sweepExpiredReservations(tx, now); err != nil {
			return err
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
			usage, ok := tx.GetUsage(level, subjects[level], start)
			if !ok {
				usage = UsageRecord{Level: level, SubjectID: subjects[level], WindowStart: start}
			}
			// 已用与已预占共同占用可用额度。
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

		// 三层都够才提交：同时锁定三个层级的可用额度。
		for _, level := range Levels {
			usage := usages[level]
			usage.Reserved += req.Amount
			if err := tx.PutUsage(usage, versions[level]); err != nil {
				return err
			}
		}
		rec := ReservationRecord{
			RequestID:  req.RequestID,
			OrgID:      req.OrgID,
			UserID:     req.UserID,
			KeyID:      req.KeyID,
			Amount:     req.Amount,
			TTLSeconds: ttlSeconds,
			Windows:    windows,
			Status:     StatusReserved,
			ExpiresAt:  expiresAt,
			CreatedAt:  now,
		}
		if err := tx.PutReservation(rec, 0); err != nil {
			return err
		}
		rec.Version = 1
		balances, err := loadBalances(tx, s.now, subjects)
		if err != nil {
			return err
		}
		result = ReserveResult{Reservation: rec, Balances: balances}
		return nil
	})
	return result, err
}

// ReservationDecisionRequest 是确认或取消预占的请求。
// RequestID 是本次操作自身的幂等键，与预占的请求号相互独立。
type ReservationDecisionRequest struct {
	RequestID     string `json:"request_id"`
	ReservationID string `json:"reservation_id"`
}

// ConfirmReservation 确认预占：三层原窗口里的 Reserved 转为正式 Used，
// 并生成一条正式消费记录（可走既有退还流程）。仅 reserved 状态可确认。
func (s *Service) ConfirmReservation(ctx context.Context, req ReservationDecisionRequest) (ReservationDecisionResult, error) {
	return s.decideReservation(ctx, opConfirm, req)
}

// CancelReservation 取消预占：三层原窗口里的 Reserved 被释放。
// 仅 reserved 状态可取消。
func (s *Service) CancelReservation(ctx context.Context, req ReservationDecisionRequest) (ReservationDecisionResult, error) {
	return s.decideReservation(ctx, opCancel, req)
}

func (s *Service) decideReservation(ctx context.Context, op ReservationOp, req ReservationDecisionRequest) (ReservationDecisionResult, error) {
	if req.RequestID == "" || req.ReservationID == "" {
		return ReservationDecisionResult{}, fmt.Errorf("%w: request id and reservation id are required", ErrValidation)
	}
	var result ReservationDecisionResult
	err := s.withRetry(ctx, func(tx *Tx) error {
		now := s.now()
		// 过期回收与确认/取消并发时，先到期者在本事务内形成终态。
		if err := sweepExpiredReservations(tx, now); err != nil {
			return err
		}

		// 操作幂等：同号同操作同预占返回首次结果；复用方式不一致报冲突。
		if prev, ok := tx.GetReservationOp(req.RequestID); ok {
			if prev.Op != op || prev.ReservationID != req.ReservationID {
				return fmt.Errorf("%w: request %s already used for %s of reservation %s",
					ErrIdempotencyConflict, req.RequestID, prev.Op, prev.ReservationID)
			}
			return s.buildDecisionResult(tx, req.ReservationID, true, &result)
		}

		rec, ok := tx.GetReservation(req.ReservationID)
		if !ok {
			return fmt.Errorf("%w: %s", ErrReservationNotFound, req.ReservationID)
		}
		if rec.Status != StatusReserved {
			// 已被取消、确认或到期回收：并发操作只能形成一个终态，失败者报状态冲突。
			return fmt.Errorf("%w: reservation %s is %s", ErrReservationStateConflict, req.ReservationID, rec.Status)
		}

		subjects := rec.subjects()
		if op == opConfirm {
			// 原窗口内 Reserved -> Used，窗口切换后也只作用于原窗口。
			for _, level := range Levels {
				usage, ok := tx.GetUsage(level, subjects[level], rec.Windows[level])
				if !ok {
					return fmt.Errorf("%w: usage record missing for level=%s subject=%s window=%d",
						ErrVersionConflict, level, subjects[level], rec.Windows[level])
				}
				usage.Reserved -= rec.Amount
				usage.Used += rec.Amount
				if err := tx.PutUsage(usage, usage.Version); err != nil {
					return err
				}
			}
			consume := ConsumeRecord{
				RequestID: rec.RequestID + consumeIDSuffix,
				OrgID:     rec.OrgID,
				UserID:    rec.UserID,
				KeyID:     rec.KeyID,
				Amount:    rec.Amount,
				Windows:   rec.Windows,
				CreatedAt: now,
			}
			if err := tx.PutConsume(consume, 0); err != nil {
				return err
			}
			rec.ConsumeRequestID = consume.RequestID
		} else {
			// 取消：只释放原窗口的 Reserved。
			for _, level := range Levels {
				usage, ok := tx.GetUsage(level, subjects[level], rec.Windows[level])
				if !ok {
					return fmt.Errorf("%w: usage record missing for level=%s subject=%s window=%d",
						ErrVersionConflict, level, subjects[level], rec.Windows[level])
				}
				usage.Reserved -= rec.Amount
				if usage.Reserved < 0 {
					usage.Reserved = 0
				}
				if err := tx.PutUsage(usage, usage.Version); err != nil {
					return err
				}
			}
		}

		if op == opConfirm {
			rec.Status = StatusConfirmed
		} else {
			rec.Status = StatusCancelled
		}
		rec.DecidedAt = now
		if err := tx.PutReservation(rec, rec.Version); err != nil {
			return err
		}
		if err := tx.PutReservationOp(ReservationOpRecord{
			RequestID:     req.RequestID,
			Op:            op,
			ReservationID: req.ReservationID,
			CreatedAt:     now,
		}); err != nil {
			return err
		}
		return s.buildDecisionResult(tx, req.ReservationID, false, &result)
	})
	return result, err
}

// buildDecisionResult 在事务内组装确认/取消返回值。
func (s *Service) buildDecisionResult(tx *Tx, reservationID string, duplicate bool, result *ReservationDecisionResult) error {
	rec, ok := tx.GetReservation(reservationID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrReservationNotFound, reservationID)
	}
	balances, err := loadBalances(tx, s.now, rec.subjects())
	if err != nil {
		return err
	}
	out := ReservationDecisionResult{Reservation: rec, Balances: balances, Duplicate: duplicate}
	if rec.Status == StatusConfirmed && rec.ConsumeRequestID != "" {
		consume, ok := tx.GetConsume(rec.ConsumeRequestID)
		if !ok {
			return fmt.Errorf("%w: consume %s generated by reservation %s",
				ErrConsumeNotFound, rec.ConsumeRequestID, reservationID)
		}
		out.Consume = &consume
	}
	*result = out
	return nil
}

// GetReservation 查询预占状态；查询时顺带回收已到期预占，保证状态准确。
func (s *Service) GetReservation(ctx context.Context, requestID string) (ReservationRecord, error) {
	if requestID == "" {
		return ReservationRecord{}, fmt.Errorf("%w: request id is required", ErrValidation)
	}
	var out ReservationRecord
	err := s.withRetry(ctx, func(tx *Tx) error {
		if err := sweepExpiredReservations(tx, s.now()); err != nil {
			return err
		}
		rec, ok := tx.GetReservation(requestID)
		if !ok {
			return fmt.Errorf("%w: %s", ErrReservationNotFound, requestID)
		}
		out = rec
		return nil
	})
	return out, err
}

// ExpireDue 主动回收所有已到期但仍处于 reserved 状态的预占，
// 返回被回收的记录数。各业务入口也会惰性执行同样的回收。
func (s *Service) ExpireDue(ctx context.Context) (int, error) {
	expired := 0
	err := s.withRetry(ctx, func(tx *Tx) error {
		n, err := expireDueInTx(tx, s.now())
		expired = n
		return err
	})
	return expired, err
}

// sweepExpiredReservations 在事务内回收全部到期预占：
// 只释放预占记录保存的原窗口里的 Reserved。
func sweepExpiredReservations(tx *Tx, now time.Time) error {
	_, err := expireDueInTx(tx, now)
	return err
}

func expireDueInTx(tx *Tx, now time.Time) (int, error) {
	due := tx.ReservationsByStatus(StatusReserved)
	n := 0
	for _, rec := range due {
		if rec.ExpiresAt.After(now) {
			continue
		}
		subjects := rec.subjects()
		for _, level := range Levels {
			start := rec.Windows[level]
			usage, ok := tx.GetUsage(level, subjects[level], start)
			if !ok {
				return 0, fmt.Errorf("%w: usage record missing for level=%s subject=%s window=%d",
					ErrVersionConflict, level, subjects[level], start)
			}
			usage.Reserved -= rec.Amount
			if usage.Reserved < 0 {
				usage.Reserved = 0
			}
			if err := tx.PutUsage(usage, usage.Version); err != nil {
				return 0, err
			}
		}
		rec.Status = StatusExpired
		rec.DecidedAt = now
		if err := tx.PutReservation(rec, rec.Version); err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}
