package quota

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// SealRequest 是一次窗口封存请求。
type SealRequest struct {
	RequestID   string `json:"request_id"`
	Level       Level  `json:"level"`
	SubjectID   string `json:"subject_id"`
	WindowStart int64  `json:"window_start"`
}

// SealWindow 封存一个已经结束的配额窗口：把该主体在该窗口的
// limit / used / reserved 以及未完成预占清单固定为快照。
//
// 窗口未结束返回 ErrWindowNotEnded；窗口内仍存在与账目不一致、
// 无法结算的预占返回 ErrWindowNotSealable；两种情况下都不写入任何数据。
// 窗口已被其他请求封存返回 ErrWindowSealed。
//
// 封存以 RequestID 幂等：同号同内容重放返回首次快照（Duplicate=true），
// 同号不同内容返回 ErrIdempotencyConflict。
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
	var result SealResult
	err := s.withRetry(ctx, func(tx *Tx) error {
		now := s.now()
		// 幂等：同号同内容返回首次快照，同号异内容报冲突。
		if prev, ok := tx.GetSealByRequest(req.RequestID); ok {
			if prev.Level != req.Level || prev.SubjectID != req.SubjectID || prev.WindowStart != req.WindowStart {
				return fmt.Errorf("%w: request %s already used with different content",
					ErrIdempotencyConflict, req.RequestID)
			}
			result = SealResult{Snapshot: prev, Duplicate: true}
			return nil
		}

		cfg, ok := tx.GetConfig(req.Level, req.SubjectID)
		if !ok {
			return fmt.Errorf("%w: level=%s subject=%s", ErrQuotaNotConfigured, req.Level, req.SubjectID)
		}
		if cfg.WindowStart(time.Unix(req.WindowStart, 0)) != req.WindowStart {
			return fmt.Errorf("%w: window start %d is not aligned to window_seconds=%d",
				ErrValidation, req.WindowStart, cfg.WindowSeconds)
		}
		windowEnd := req.WindowStart + cfg.WindowSeconds
		if now.Unix() < windowEnd {
			return fmt.Errorf("%w: window [%d, %d) still open at %d",
				ErrWindowNotEnded, req.WindowStart, windowEnd, now.Unix())
		}
		if _, ok := tx.GetSeal(req.Level, req.SubjectID, req.WindowStart); ok {
			return fmt.Errorf("%w: level=%s subject=%s window=%d already sealed",
				ErrWindowSealed, req.Level, req.SubjectID, req.WindowStart)
		}

		usage, _ := tx.GetUsage(req.Level, req.SubjectID, req.WindowStart)
		// 收集该窗口仍未结算的预占，并校验其与窗口账目一致：
		// 所有未完成预占的总额必须恰好等于窗口的 reserved，
		// 否则存在无法结算的预占，拒绝封存且不留半成品。
		var open []string
		var openSum int64
		for _, rec := range tx.ListReservations() {
			if rec.State != ReservationReserved {
				continue
			}
			if rec.subjects()[req.Level] != req.SubjectID || rec.Windows[req.Level] != req.WindowStart {
				continue
			}
			open = append(open, rec.RequestID)
			openSum += rec.Amount
		}
		if openSum != usage.Reserved {
			return fmt.Errorf("%w: level=%s subject=%s window=%d reserved=%d open_reservations=%d",
				ErrWindowNotSealable, req.Level, req.SubjectID, req.WindowStart, usage.Reserved, openSum)
		}
		sort.Strings(open)

		snap := SealSnapshot{
			RequestID:        req.RequestID,
			Level:            req.Level,
			SubjectID:        req.SubjectID,
			WindowStart:      req.WindowStart,
			WindowEnd:        windowEnd,
			Limit:            cfg.Limit,
			Used:             usage.Used,
			Reserved:         usage.Reserved,
			OpenReservations: open,
			SealedAt:         now,
		}
		if err := tx.PutSeal(snap, 0); err != nil {
			return err
		}
		snap.Version = 1
		result = SealResult{Snapshot: snap}
		return nil
	})
	return result, err
}

// GetSeal 读取某主体在指定窗口的封存快照。
func (s *Service) GetSeal(ctx context.Context, level Level, subjectID string, windowStart int64) (SealSnapshot, error) {
	var out SealSnapshot
	err := s.store.View(ctx, func(tx *Tx) error {
		snap, ok := tx.GetSeal(level, subjectID, windowStart)
		if !ok {
			return fmt.Errorf("%w: level=%s subject=%s window=%d", ErrSealNotFound, level, subjectID, windowStart)
		}
		out = snap
		return nil
	})
	return out, err
}

// ListSeals 列出封存快照；level 与 subjectID 为空字符串表示不过滤。
func (s *Service) ListSeals(ctx context.Context, level Level, subjectID string) ([]SealSnapshot, error) {
	var out []SealSnapshot
	err := s.store.View(ctx, func(tx *Tx) error {
		for _, snap := range tx.ListSeals() {
			if level != "" && snap.Level != level {
				continue
			}
			if subjectID != "" && snap.SubjectID != subjectID {
				continue
			}
			out = append(out, snap)
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].WindowStart != out[j].WindowStart {
			return out[i].WindowStart < out[j].WindowStart
		}
		if out[i].Level != out[j].Level {
			return out[i].Level < out[j].Level
		}
		return out[i].SubjectID < out[j].SubjectID
	})
	return out, err
}

// WindowReport 返回某主体在某个窗口的完整账目：当前用量、
// 封存快照（若已封存，以快照账目为准）以及形成差异的
// 全部消费、退还与预占记录。
func (s *Service) WindowReport(ctx context.Context, level Level, subjectID string, windowStart int64) (WindowReport, error) {
	if !level.Valid() {
		return WindowReport{}, fmt.Errorf("%w: invalid level %q", ErrValidation, level)
	}
	if subjectID == "" {
		return WindowReport{}, fmt.Errorf("%w: subject id is required", ErrValidation)
	}
	report := WindowReport{
		Level:       level,
		SubjectID:   subjectID,
		WindowStart: windowStart,
		Entries:     []WindowEntry{},
	}
	err := s.store.View(ctx, func(tx *Tx) error {
		cfg, ok := tx.GetConfig(level, subjectID)
		if !ok {
			return fmt.Errorf("%w: level=%s subject=%s", ErrQuotaNotConfigured, level, subjectID)
		}
		report.WindowEnd = windowStart + cfg.WindowSeconds
		report.Limit = cfg.Limit
		if usage, ok := tx.GetUsage(level, subjectID, windowStart); ok {
			report.Used = usage.Used
			report.Reserved = usage.Reserved
		}
		if snap, ok := tx.GetSeal(level, subjectID, windowStart); ok {
			// 封存后以快照账目为结算结果。
			report.Sealed = true
			report.Limit = snap.Limit
			report.Used = snap.Used
			report.Reserved = snap.Reserved
			s := snap
			report.Snapshot = &s
		}
		matches := func(windows map[Level]int64, subjects map[Level]string) bool {
			return windows[level] == windowStart && subjects[level] == subjectID
		}
		for _, rec := range tx.listConsumes() {
			if matches(rec.Windows, map[Level]string{LevelOrg: rec.OrgID, LevelUser: rec.UserID, LevelKey: rec.KeyID}) {
				report.Entries = append(report.Entries, WindowEntry{
					Kind: "consume", RequestID: rec.RequestID, Amount: rec.Amount,
				})
			}
		}
		for _, rec := range tx.listRefunds() {
			consume, ok := tx.GetConsume(rec.ConsumeRequestID)
			if !ok {
				continue
			}
			if matches(consume.Windows, map[Level]string{LevelOrg: consume.OrgID, LevelUser: consume.UserID, LevelKey: consume.KeyID}) {
				report.Entries = append(report.Entries, WindowEntry{
					Kind: "refund", RequestID: rec.RequestID, Amount: -rec.Amount,
				})
			}
		}
		for _, rec := range tx.ListReservations() {
			if matches(rec.Windows, rec.subjects()) {
				report.Entries = append(report.Entries, WindowEntry{
					Kind: "reservation", RequestID: rec.RequestID,
					Amount: rec.Amount, State: string(rec.State),
				})
			}
		}
		return nil
	})
	if err != nil {
		return WindowReport{}, err
	}
	sort.Slice(report.Entries, func(i, j int) bool {
		if report.Entries[i].RequestID != report.Entries[j].RequestID {
			return report.Entries[i].RequestID < report.Entries[j].RequestID
		}
		return report.Entries[i].Kind < report.Entries[j].Kind
	})
	return report, nil
}

// adjustWindowUsage 在事务内调整某主体某窗口的 used / reserved。
// 窗口已封存时只依据封存快照更新结算结果并推进快照版本，
// 不改动已冻结的窗口用量记录；未封存时更新窗口用量记录。
// reserved 不允许变负（说明迟到操作与已发布账目冲突）。
func adjustWindowUsage(tx *Tx, level Level, subjectID string, windowStart int64, usedDelta, reservedDelta int64) error {
	if snap, ok := tx.GetSeal(level, subjectID, windowStart); ok {
		if snap.Reserved+reservedDelta < 0 {
			return fmt.Errorf("%w: sealed window %d level=%s subject=%s reserved=%d delta=%d",
				ErrVersionConflict, windowStart, level, subjectID, snap.Reserved, reservedDelta)
		}
		snap.Used += usedDelta
		if snap.Used < 0 {
			snap.Used = 0
		}
		snap.Reserved += reservedDelta
		return tx.PutSeal(snap, snap.Version)
	}
	usage, ok := tx.GetUsage(level, subjectID, windowStart)
	if !ok {
		return fmt.Errorf("%w: usage record missing for level=%s subject=%s window=%d",
			ErrVersionConflict, level, subjectID, windowStart)
	}
	if usage.Reserved+reservedDelta < 0 {
		return fmt.Errorf("%w: level=%s subject=%s window=%d reserved=%d delta=%d",
			ErrVersionConflict, level, subjectID, windowStart, usage.Reserved, reservedDelta)
	}
	usage.Used += usedDelta
	if usage.Used < 0 {
		usage.Used = 0
	}
	usage.Reserved += reservedDelta
	return tx.PutUsage(usage, usage.Version)
}

// ensureWindowOpen 在事务内校验窗口未封存；已封存返回 ErrWindowSealed。
func ensureWindowOpen(tx *Tx, level Level, subjectID string, windowStart int64) error {
	if _, ok := tx.GetSeal(level, subjectID, windowStart); ok {
		return fmt.Errorf("%w: level=%s subject=%s window=%d",
			ErrWindowSealed, level, subjectID, windowStart)
	}
	return nil
}
