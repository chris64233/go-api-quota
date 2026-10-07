package quota

import (
	"context"
	"fmt"
	"sort"
)

// SealRequest 是一次窗口封存请求（管理员操作）。
type SealRequest struct {
	RequestID   string `json:"request_id"`
	Level       Level  `json:"level"`
	SubjectID   string `json:"subject_id"`
	WindowStart int64  `json:"window_start"`
	// ExpectedUsageVersion 非空时作为版本条件：与窗口用量记录当前版本
	// 不一致则返回 ErrVersionConflict，避免基于过期视图封存。
	ExpectedUsageVersion *int64 `json:"expected_usage_version,omitempty"`
}

// SealWindow 封存一个已经结束的配额窗口：把该层级该主体在窗口结束时的
// limit、used、reserved 与未完成预占清单固化为不可变快照。
//
// 拒绝条件（均不留下任何半成品，整个校验与写入在一个事务内完成）：
//   - 窗口尚未结束（ErrWindowNotEnded）；
//   - 窗口内仍有已到期但未回收的预占（ErrWindowSealBlocked，需先触发到期回收）；
//   - 窗口已被其他请求封存（ErrWindowSealed）；
//   - 携带的 ExpectedUsageVersion 与当前版本不一致（ErrVersionConflict）。
//
// 未到期、仍处于 reserved 状态的预占允许存在，会记入快照的 Outstanding 清单，
// 封存后仍可按原请求号确认、取消或到期回收，只更新该窗口的结算账目。
//
// 同一 RequestID 重试：同内容返回首次结果（Duplicate=true），
// 不同内容返回 ErrIdempotencyConflict。
func (s *Service) SealWindow(ctx context.Context, req SealRequest) (SealResult, error) {
	if req.RequestID == "" || req.SubjectID == "" {
		return SealResult{}, fmt.Errorf("%w: request id and subject id are required", ErrValidation)
	}
	if !req.Level.Valid() {
		return SealResult{}, fmt.Errorf("%w: invalid level %q", ErrValidation, req.Level)
	}
	if req.WindowStart < 0 {
		return SealResult{}, fmt.Errorf("%w: window start must not be negative", ErrValidation)
	}
	if req.ExpectedUsageVersion != nil && *req.ExpectedUsageVersion < 0 {
		return SealResult{}, fmt.Errorf("%w: expected usage version must not be negative", ErrValidation)
	}

	var result SealResult
	err := s.withRetry(ctx, func(tx *Tx) error {
		now := s.now()
		// 幂等：同号同内容返回首次结果，同号异内容报冲突。
		if prev, ok := tx.GetSealByRequest(req.RequestID); ok {
			if !prev.sameContent(req.Level, req.SubjectID, req.WindowStart) {
				return fmt.Errorf("%w: request %s already used with different content",
					ErrIdempotencyConflict, req.RequestID)
			}
			result = SealResult{Seal: prev, Duplicate: true}
			return nil
		}
		// 窗口已被其他请求封存：形成不了第二个结算版本。
		if _, ok := tx.GetSeal(req.Level, req.SubjectID, req.WindowStart); ok {
			return fmt.Errorf("%w: level=%s subject=%s window=%d",
				ErrWindowSealed, req.Level, req.SubjectID, req.WindowStart)
		}
		cfg, ok := tx.GetConfig(req.Level, req.SubjectID)
		if !ok {
			return fmt.Errorf("%w: level=%s subject=%s", ErrQuotaNotConfigured, req.Level, req.SubjectID)
		}
		if req.WindowStart%cfg.WindowSeconds != 0 {
			return fmt.Errorf("%w: window start %d is not aligned to window seconds %d",
				ErrValidation, req.WindowStart, cfg.WindowSeconds)
		}
		windowEnd := req.WindowStart + cfg.WindowSeconds
		if now.Unix() < windowEnd {
			return fmt.Errorf("%w: window [%d, %d) still active at %d",
				ErrWindowNotEnded, req.WindowStart, windowEnd, now.Unix())
		}

		usage, ok := tx.GetUsage(req.Level, req.SubjectID, req.WindowStart)
		if !ok {
			usage = UsageRecord{Level: req.Level, SubjectID: req.SubjectID, WindowStart: req.WindowStart}
		}
		if req.ExpectedUsageVersion != nil && *req.ExpectedUsageVersion != usage.Version {
			return fmt.Errorf("%w: usage %s/%s/%d stored=%d expected=%d",
				ErrVersionConflict, req.Level, req.SubjectID, req.WindowStart,
				usage.Version, *req.ExpectedUsageVersion)
		}

		// 已到期但未回收的预占不允许结算：拒绝封存，调用方需先触发到期回收。
		// 未到期的预占记入未完成清单，封存后仍可结算。
		outstanding := []OutstandingReservation{}
		for _, rec := range tx.ListReservationsForWindow(req.Level, req.SubjectID, req.WindowStart) {
			if rec.State != ReservationReserved {
				continue
			}
			if !now.Before(rec.ExpiresAt) {
				return fmt.Errorf("%w: reservation %s expired at %s but not settled",
					ErrWindowSealBlocked, rec.RequestID, rec.ExpiresAt)
			}
			outstanding = append(outstanding, OutstandingReservation{
				RequestID: rec.RequestID,
				Amount:    rec.Amount,
				ExpiresAt: rec.ExpiresAt,
			})
		}
		sort.Slice(outstanding, func(i, j int) bool { return outstanding[i].RequestID < outstanding[j].RequestID })

		seal := SealRecord{
			RequestID:    req.RequestID,
			Level:        req.Level,
			SubjectID:    req.SubjectID,
			WindowStart:  req.WindowStart,
			WindowEnd:    windowEnd,
			Limit:        cfg.Limit,
			Used:         usage.Used,
			Reserved:     usage.Reserved,
			UsageVersion: usage.Version,
			Outstanding:  outstanding,
			SealedAt:     now,
		}
		if err := tx.PutSeal(seal); err != nil {
			return err
		}
		seal.Version = 1
		result = SealResult{Seal: seal}
		return nil
	})
	return result, err
}

// GetSeal 查询某层级某主体在指定窗口的封存快照。
func (s *Service) GetSeal(ctx context.Context, level Level, subjectID string, windowStart int64) (SealRecord, error) {
	var out SealRecord
	err := s.store.View(ctx, func(tx *Tx) error {
		seal, ok := tx.GetSeal(level, subjectID, windowStart)
		if !ok {
			return fmt.Errorf("%w: level=%s subject=%s window=%d",
				ErrSealNotFound, level, subjectID, windowStart)
		}
		out = seal
		return nil
	})
	return out, err
}

// WindowStatement 返回某层级某主体在指定窗口的对账视图：
// 当前账目、封存快照（若已封存）以及形成差异的消费、退还与预占记录。
func (s *Service) WindowStatement(ctx context.Context, level Level, subjectID string, windowStart int64) (WindowStatement, error) {
	if !level.Valid() {
		return WindowStatement{}, fmt.Errorf("%w: invalid level %q", ErrValidation, level)
	}
	if subjectID == "" {
		return WindowStatement{}, fmt.Errorf("%w: subject id is required", ErrValidation)
	}
	var out WindowStatement
	err := s.store.View(ctx, func(tx *Tx) error {
		cfg, ok := tx.GetConfig(level, subjectID)
		if !ok {
			return fmt.Errorf("%w: level=%s subject=%s", ErrQuotaNotConfigured, level, subjectID)
		}
		out = WindowStatement{
			Level:       level,
			SubjectID:   subjectID,
			WindowStart: windowStart,
			WindowEnd:   windowStart + cfg.WindowSeconds,
			Limit:       cfg.Limit,
			Entries:     []WindowEntry{},
		}
		if usage, ok := tx.GetUsage(level, subjectID, windowStart); ok {
			out.Used = usage.Used
			out.Reserved = usage.Reserved
		}
		if seal, ok := tx.GetSeal(level, subjectID, windowStart); ok {
			out.Sealed = true
			s := seal
			out.Seal = &s
		}
		out.Entries = append(out.Entries, windowEntries(tx, level, subjectID, windowStart)...)
		sort.Slice(out.Entries, func(i, j int) bool {
			if out.Entries[i].CreatedAt.Equal(out.Entries[j].CreatedAt) {
				return out.Entries[i].RequestID < out.Entries[j].RequestID
			}
			return out.Entries[i].CreatedAt.Before(out.Entries[j].CreatedAt)
		})
		return nil
	})
	return out, err
}

// windowEntries 汇总形成指定窗口账目的全部来源记录。
func windowEntries(tx *Tx, level Level, subjectID string, windowStart int64) []WindowEntry {
	entries := []WindowEntry{}
	consumeInWindow := map[string]bool{}
	for _, rec := range tx.state.Consumes {
		if consumeSubject(rec, level) != subjectID || rec.Windows[level] != windowStart {
			continue
		}
		consumeInWindow[rec.RequestID] = true
		entries = append(entries, WindowEntry{
			Kind:      "consume",
			RequestID: rec.RequestID,
			Amount:    rec.Amount,
			CreatedAt: rec.CreatedAt,
		})
	}
	for _, rec := range tx.state.Refunds {
		if !consumeInWindow[rec.ConsumeRequestID] {
			continue
		}
		entries = append(entries, WindowEntry{
			Kind:      "refund",
			RequestID: rec.RequestID,
			Amount:    -rec.Amount,
			CreatedAt: rec.CreatedAt,
		})
	}
	for _, rec := range tx.ListReservationsForWindow(level, subjectID, windowStart) {
		entries = append(entries, WindowEntry{
			Kind:      "reservation_reserve",
			RequestID: rec.RequestID,
			Amount:    rec.Amount,
			State:     string(rec.State),
			CreatedAt: rec.CreatedAt,
		})
		if rec.State.Terminal() {
			// 结算对窗口占用的影响：确认保持占用（reserved→used），取消/到期释放。
			settle := rec.Amount
			if rec.State == ReservationCancelled || rec.State == ReservationExpired {
				settle = -rec.Amount
			}
			entries = append(entries, WindowEntry{
				Kind:      "reservation_settle",
				RequestID: rec.RequestID,
				Amount:    settle,
				State:     string(rec.State),
				CreatedAt: rec.SettledAt,
			})
		}
	}
	return entries
}

// consumeSubject 返回消费记录在指定层级上的主体标识。
func consumeSubject(rec ConsumeRecord, level Level) string {
	switch level {
	case LevelOrg:
		return rec.OrgID
	case LevelUser:
		return rec.UserID
	default:
		return rec.KeyID
	}
}
