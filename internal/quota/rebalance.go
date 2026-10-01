package quota

import (
	"context"
	"fmt"
	"sort"
)

// RebalanceRequest 是一次管理员层级额度重平衡请求。
// 三层目标额度必须全部给出，且三层增减之和为 0（零和转移），
// 因此同一份额度不可能被两个层级同时算作可用。
type RebalanceRequest struct {
	RequestID    string          `json:"request_id"`
	OrgID        string          `json:"org_id"`
	UserID       string          `json:"user_id"`
	KeyID        string          `json:"key_id"`
	TargetLimits map[Level]int64 `json:"target_limits"`
}

func (r RebalanceRequest) subjects() map[Level]string {
	return map[Level]string{LevelOrg: r.OrgID, LevelUser: r.UserID, LevelKey: r.KeyID}
}

// Rebalance 在组织、用户、密钥三层之间零和转移额度。
//
// 调整只改变各层配置的 Limit，绝不改动任何窗口的 used/reserved，
// 因此已经消费的数量与在途预占都不受影响。每个层级的目标额度都必须
// 容纳当前窗口已消费 + 已预占（target >= used + reserved），任一层不足
// 时整笔调整不落地，三层配置都不修改。
//
// 重平衡与确认消费、取消预占在同一事务与同一版本机制上计算新余额：
// 重平衡读取并触达三层窗口用量记录，并发的确认/取消已先行提交时，
// 用量版本不匹配会触发重试，旧调整不可能覆盖后来确认的消费。
// 某一层额度被耗尽只会导致调整失败，已成功的消费不会被回滚。
//
// 同一 RequestID 重复提交：同内容返回首次结果（Duplicate=true）；
// 同号异内容返回 ErrIdempotencyConflict；同号同内容但窗口已滚动、
// 或当前额度已不再是原单目标额度（被后来的调整改变）返回
// ErrRebalanceConflict。
func (s *Service) Rebalance(ctx context.Context, req RebalanceRequest) (RebalanceResult, error) {
	if req.RequestID == "" || req.OrgID == "" || req.UserID == "" || req.KeyID == "" {
		return RebalanceResult{}, fmt.Errorf("%w: request id, org id, user id and key id are required", ErrValidation)
	}
	if len(req.TargetLimits) != len(Levels) {
		return RebalanceResult{}, fmt.Errorf("%w: target limits for all three levels are required", ErrValidation)
	}
	subjects := req.subjects()
	var sum int64
	for _, level := range Levels {
		target := req.TargetLimits[level]
		if target <= 0 {
			return RebalanceResult{}, fmt.Errorf("%w: target limit for level %s must be positive", ErrValidation, level)
		}
		sum += target
	}

	var result RebalanceResult
	err := s.withRetry(ctx, func(tx *Tx) error {
		now := s.now()

		// 幂等处理。
		if prev, ok := tx.GetRebalance(req.RequestID); ok {
			if !prev.sameContent(req.OrgID, req.UserID, req.KeyID, req.TargetLimits) {
				return fmt.Errorf("%w: request %s already used with different content",
					ErrIdempotencyConflict, req.RequestID)
			}
			// 同内容重放：原单必须仍能描述当前状态。
			// 1) 原单依据的窗口若已滚动，旧结果不再适用于当前窗口；
			// 2) 原单的目标额度若已被后来的配置/重平衡改变，返回原结果会掩盖该变化。
			for _, level := range Levels {
				subjectID := subjects[level]
				cfg, exists := tx.GetConfig(level, subjectID)
				entry, hasEntry := prev.levelEntry(level)
				if !exists || !hasEntry {
					return fmt.Errorf("%w: rebalance %s level=%s baseline missing",
						ErrRebalanceConflict, req.RequestID, level)
				}
				if cfg.WindowStart(now) != entry.WindowStart {
					return fmt.Errorf("%w: rebalance %s level=%s window moved: record=%d now=%d",
						ErrRebalanceConflict, req.RequestID, level, entry.WindowStart, cfg.WindowStart(now))
				}
				if cfg.Limit != entry.LimitAfter {
					return fmt.Errorf("%w: rebalance %s level=%s target %d no longer current, limit=%d",
						ErrRebalanceConflict, req.RequestID, level, entry.LimitAfter, cfg.Limit)
				}
			}
			balances, err := loadBalances(tx, s.now, subjects)
			if err != nil {
				return err
			}
			result = RebalanceResult{Record: prev, Balances: balances, Duplicate: true}
			return nil
		}

		// 首次提交：读取三层配置与当前窗口用量。
		type levelInput struct {
			cfg    QuotaConfig
			usage  UsageRecord
			exists bool
			target int64
		}
		inputs := map[Level]*levelInput{}
		for _, level := range Levels {
			cfg, ok := tx.GetConfig(level, subjects[level])
			if !ok {
				return fmt.Errorf("%w: level=%s subject=%s", ErrQuotaNotConfigured, level, subjects[level])
			}
			usage, exists := tx.GetUsage(level, subjects[level], cfg.WindowStart(now))
			if !exists {
				usage = UsageRecord{Level: level, SubjectID: subjects[level], WindowStart: cfg.WindowStart(now)}
			}
			inputs[level] = &levelInput{cfg: cfg, usage: usage, exists: exists, target: req.TargetLimits[level]}
		}

		// 零和校验：三层增减之和必须为 0。
		var deltaSum int64
		anyChange := false
		for _, level := range Levels {
			in := inputs[level]
			delta := in.target - in.cfg.Limit
			deltaSum += delta
			if delta != 0 {
				anyChange = true
			}
		}
		if deltaSum != 0 {
			return fmt.Errorf("%w: target limits must be zero-sum across levels, delta sum=%d",
				ErrValidation, deltaSum)
		}
		if !anyChange {
			return fmt.Errorf("%w: target limits equal current limits on every level, nothing to rebalance",
				ErrValidation)
		}

		// 下限校验：任一层新额度无法容纳当前窗口已消费 + 已预占时整笔失败。
		// 只拒绝调整本身，不触碰任何消费与预占。
		for _, level := range Levels {
			in := inputs[level]
			committed := in.usage.Used + in.usage.Reserved
			if in.target < committed {
				return &insufficientError{
					level:     level,
					subjectID: subjects[level],
					remaining: in.cfg.Limit - committed,
					amount:    committed - in.target,
				}
			}
		}

		// 收集各层当前窗口仍在途的预占，作为“是否影响在途消费”的依据。
		inFlight := map[Level][]string{}
		for _, rec := range tx.ListReservations() {
			if rec.State != ReservationReserved {
				continue
			}
			recSubjects := rec.subjects()
			for _, level := range Levels {
				in := inputs[level]
				if recSubjects[level] == in.cfg.SubjectID && rec.Windows[level] == in.usage.WindowStart {
					inFlight[level] = append(inFlight[level], rec.RequestID)
				}
			}
		}
		for _, ids := range inFlight {
			sort.Strings(ids)
		}

		// 先触达三层窗口用量：版本条件保证重平衡与确认/取消基于同一版本计算。
		for _, level := range Levels {
			in := inputs[level]
			if err := tx.PutUsage(in.usage, in.usage.Version); err != nil {
				return err
			}
		}

		// 再以版本条件更新三层配置额度；used/reserved 不变。
		entries := make([]LevelRebalance, 0, len(Levels))
		for _, level := range Levels {
			in := inputs[level]
			before := in.cfg
			updated := before
			updated.Limit = in.target
			updated.UpdatedAt = now
			updated.LastRebalanceRequestID = req.RequestID
			if err := tx.PutConfig(updated, before.Version); err != nil {
				return err
			}
			entries = append(entries, LevelRebalance{
				Level:                level,
				SubjectID:            in.cfg.SubjectID,
				WindowStart:          in.usage.WindowStart,
				LimitBefore:          before.Limit,
				LimitAfter:           in.target,
				Delta:                in.target - before.Limit,
				Used:                 in.usage.Used,
				Reserved:             in.usage.Reserved,
				ConfigVersionBefore:  before.Version,
				ConfigVersionAfter:   before.Version + 1,
				UsageVersion:         in.usage.Version + 1,
				InFlightReservations: inFlight[level],
			})
		}

		rec := RebalanceRecord{
			RequestID:    req.RequestID,
			OrgID:        req.OrgID,
			UserID:       req.UserID,
			KeyID:        req.KeyID,
			TargetLimits: req.TargetLimits,
			Levels:       entries,
			CreatedAt:    now,
		}
		if err := tx.PutRebalance(rec, 0); err != nil {
			return err
		}
		rec.Version = 1
		balances, err := loadBalances(tx, s.now, subjects)
		if err != nil {
			return err
		}
		result = RebalanceResult{Record: rec, Balances: balances}
		return nil
	})
	return result, err
}

// GetRebalance 按请求号查询单笔重平衡单。
func (s *Service) GetRebalance(ctx context.Context, requestID string) (RebalanceRecord, error) {
	if requestID == "" {
		return RebalanceRecord{}, fmt.Errorf("%w: request id is required", ErrValidation)
	}
	var out RebalanceRecord
	err := s.store.View(ctx, func(tx *Tx) error {
		rec, ok := tx.GetRebalance(requestID)
		if !ok {
			return fmt.Errorf("%w: %s", ErrRebalanceNotFound, requestID)
		}
		out = rec
		return nil
	})
	return out, err
}

// HistoryFilter 按三层主体过滤历史记录，空字段不参与过滤。
type HistoryFilter struct {
	OrgID  string
	UserID string
	KeyID  string
}

func (f HistoryFilter) matches(orgID, userID, keyID string) bool {
	if f.OrgID != "" && f.OrgID != orgID {
		return false
	}
	if f.UserID != "" && f.UserID != userID {
		return false
	}
	if f.KeyID != "" && f.KeyID != keyID {
		return false
	}
	return true
}

// RebalanceHistory 查询重平衡历史。不带任何过滤条件时返回全部重平衡单；
// 给定 org_id/user_id/key_id 时只返回涉及该主体的单据。
// 结果按创建时间从旧到新排序，便于回放管理员对每层额度的连续调整。
func (s *Service) RebalanceHistory(ctx context.Context, filter HistoryFilter) ([]RebalanceRecord, error) {
	var out []RebalanceRecord
	err := s.store.View(ctx, func(tx *Tx) error {
		for _, rec := range tx.ListRebalances() {
			if filter.matches(rec.OrgID, rec.UserID, rec.KeyID) {
				out = append(out, rec)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].RequestID < out[j].RequestID
	})
	return out, nil
}

// ConsumeHistory 查询消费历史（含预占确认生成的正式消费）。
// 过滤与排序规则同 RebalanceHistory。
func (s *Service) ConsumeHistory(ctx context.Context, filter HistoryFilter) ([]ConsumeRecord, error) {
	var out []ConsumeRecord
	err := s.store.View(ctx, func(tx *Tx) error {
		for _, rec := range tx.ListConsumes() {
			if filter.matches(rec.OrgID, rec.UserID, rec.KeyID) {
				out = append(out, rec)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].RequestID < out[j].RequestID
	})
	return out, nil
}

// ReservationHistory 查询预占历史，过滤与排序规则同 RebalanceHistory。
func (s *Service) ReservationHistory(ctx context.Context, filter HistoryFilter) ([]ReservationRecord, error) {
	var out []ReservationRecord
	err := s.store.View(ctx, func(tx *Tx) error {
		for _, rec := range tx.ListReservations() {
			if filter.matches(rec.OrgID, rec.UserID, rec.KeyID) {
				out = append(out, rec)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].RequestID < out[j].RequestID
	})
	return out, nil
}
