package quota

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// window0 是 setup 时钟起点所属的窗口起点。
func window0() int64 { return testEpoch.Unix() }

func sealReq(id string, level Level, windowStart int64) SealRequest {
	return SealRequest{
		RequestID:   id,
		Level:       level,
		SubjectID:   string(level) + "-1",
		WindowStart: windowStart,
	}
}

func usageOf(t *testing.T, svc *Service, level Level, windowStart int64) UsageRecord {
	t.Helper()
	var rec UsageRecord
	err := svc.store.View(context.Background(), func(tx *Tx) error {
		rec, _ = tx.GetUsage(level, string(level)+"-1", windowStart)
		return nil
	})
	if err != nil {
		t.Fatalf("view usage: %v", err)
	}
	return rec
}

func TestSealRejectsWindowNotEnded(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	_, err := svc.SealWindow(ctx, sealReq("seal-1", LevelOrg, window0()))
	if !errors.Is(err, ErrWindowNotEnded) {
		t.Fatalf("expected ErrWindowNotEnded, got %v", err)
	}
	// 拒绝后不能留下半成品。
	if _, err := svc.GetSeal(ctx, LevelOrg, "org-1", window0()); !errors.Is(err, ErrSealNotFound) {
		t.Fatalf("expected no seal record, got %v", err)
	}
}

func TestSealRejectsUnalignedWindow(t *testing.T) {
	svc, clk := setup(t)
	clk.Advance(2 * time.Minute)
	_, err := svc.SealWindow(context.Background(), sealReq("seal-1", LevelOrg, window0()+7))
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestSealBlockedByExpiredUnsettledReservation(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	_, err := svc.Reserve(ctx, ReserveRequest{
		RequestID: "r1", OrgID: "org-1", UserID: "user-1", KeyID: "key-1",
		Amount: 10, TTLSeconds: 30,
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 预占到期但尚未回收，且窗口也已结束：封存必须拒绝且不留半成品。
	clk.Advance(2 * time.Minute)
	if _, err := svc.SealWindow(ctx, sealReq("seal-1", LevelOrg, window0())); !errors.Is(err, ErrWindowSealBlocked) {
		t.Fatalf("expected ErrWindowSealBlocked, got %v", err)
	}
	if _, err := svc.GetSeal(ctx, LevelOrg, "org-1", window0()); !errors.Is(err, ErrSealNotFound) {
		t.Fatalf("expected no seal record, got %v", err)
	}
	// 触发到期回收后封存成功，reserved 已归零。
	if _, err := svc.ExpireDue(ctx); err != nil {
		t.Fatalf("expire: %v", err)
	}
	res, err := svc.SealWindow(ctx, sealReq("seal-1", LevelOrg, window0()))
	if err != nil {
		t.Fatalf("seal after expiry: %v", err)
	}
	if res.Seal.Reserved != 0 || len(res.Seal.Outstanding) != 0 {
		t.Fatalf("expected settled snapshot, got %+v", res.Seal)
	}
}

func TestSealSnapshotWithOutstandingReservation(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Consume(ctx, consumeReq("c1", 40)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := svc.Reserve(ctx, ReserveRequest{
		RequestID: "r1", OrgID: "org-1", UserID: "user-1", KeyID: "key-1", Amount: 15,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	clk.Advance(time.Minute) // 进入下一窗口，原窗口结束

	res, err := svc.SealWindow(ctx, sealReq("seal-org", LevelOrg, window0()))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if res.Duplicate {
		t.Fatal("first seal should not be duplicate")
	}
	if res.Seal.Limit != 100 || res.Seal.Used != 40 || res.Seal.Reserved != 15 {
		t.Fatalf("unexpected snapshot: %+v", res.Seal)
	}
	if len(res.Seal.Outstanding) != 1 || res.Seal.Outstanding[0].RequestID != "r1" {
		t.Fatalf("unexpected outstanding: %+v", res.Seal.Outstanding)
	}

	// 封存后确认未完成预占：只更新封存窗口的结算账目，不触碰新窗口。
	if _, err := svc.ConfirmReservation(ctx, "r1"); err != nil {
		t.Fatalf("confirm after seal: %v", err)
	}
	oldUsage := usageOf(t, svc, LevelOrg, window0())
	if oldUsage.Used != 55 || oldUsage.Reserved != 0 {
		t.Fatalf("sealed window usage = %+v, want used=55 reserved=0", oldUsage)
	}
	newUsage := usageOf(t, svc, LevelOrg, window0()+60)
	if newUsage.Used != 0 || newUsage.Reserved != 0 {
		t.Fatalf("new window must stay untouched, got %+v", newUsage)
	}
	// 快照本身不被结算操作改写。
	seal, err := svc.GetSeal(ctx, LevelOrg, "org-1", window0())
	if err != nil {
		t.Fatalf("get seal: %v", err)
	}
	if seal.Used != 40 || seal.Reserved != 15 {
		t.Fatalf("snapshot must be immutable, got %+v", seal)
	}
}

func TestRefundAfterSealSettlesIntoSealedWindow(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Consume(ctx, consumeReq("c1", 50)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	clk.Advance(time.Minute)
	if _, err := svc.SealWindow(ctx, sealReq("seal-1", LevelOrg, window0())); err != nil {
		t.Fatalf("seal: %v", err)
	}
	// 窗口已封存，退还仍按原请求号处理，只回补封存窗口。
	if _, err := svc.Refund(ctx, RefundRequest{RequestID: "rf1", ConsumeRequestID: "c1", Amount: 20}); err != nil {
		t.Fatalf("refund after seal: %v", err)
	}
	if got := usageOf(t, svc, LevelOrg, window0()).Used; got != 30 {
		t.Fatalf("sealed window used = %d, want 30", got)
	}
	if got := usageOf(t, svc, LevelOrg, window0()+60).Used; got != 0 {
		t.Fatalf("new window used = %d, want 0", got)
	}
}

func TestSealedWindowRejectsNewWrites(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	clk.Advance(time.Minute)
	if _, err := svc.SealWindow(ctx, sealReq("seal-1", LevelOrg, window0())); err != nil {
		t.Fatalf("seal: %v", err)
	}
	// 时钟回到已封存窗口：新的消费与预占都必须被拒绝。
	clk.Advance(-time.Minute)
	if _, err := svc.Consume(ctx, consumeReq("c-late", 10)); !errors.Is(err, ErrWindowSealed) {
		t.Fatalf("expected ErrWindowSealed for consume, got %v", err)
	}
	if _, err := svc.Reserve(ctx, ReserveRequest{
		RequestID: "r-late", OrgID: "org-1", UserID: "user-1", KeyID: "key-1", Amount: 10,
	}); !errors.Is(err, ErrWindowSealed) {
		t.Fatalf("expected ErrWindowSealed for reserve, got %v", err)
	}
}

func TestSealIdempotency(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()
	clk.Advance(time.Minute)

	res, err := svc.SealWindow(ctx, sealReq("seal-1", LevelOrg, window0()))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	// 同号同内容：返回首次结果。
	dup, err := svc.SealWindow(ctx, sealReq("seal-1", LevelOrg, window0()))
	if err != nil {
		t.Fatalf("duplicate seal: %v", err)
	}
	if !dup.Duplicate || !dup.Seal.SealedAt.Equal(res.Seal.SealedAt) {
		t.Fatalf("expected first result, got %+v", dup)
	}
	// 同号异内容：幂等冲突。
	if _, err := svc.SealWindow(ctx, sealReq("seal-1", LevelUser, window0())); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
	// 异号同窗口：窗口已封存冲突。
	if _, err := svc.SealWindow(ctx, sealReq("seal-2", LevelOrg, window0())); !errors.Is(err, ErrWindowSealed) {
		t.Fatalf("expected ErrWindowSealed, got %v", err)
	}
}

func TestSealExpectedUsageVersion(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Consume(ctx, consumeReq("c1", 10)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	clk.Advance(time.Minute)
	stale := int64(0) // 消费前读取到的版本
	req := sealReq("seal-1", LevelOrg, window0())
	req.ExpectedUsageVersion = &stale
	if _, err := svc.SealWindow(ctx, req); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict, got %v", err)
	}
	cur := int64(1)
	req.ExpectedUsageVersion = &cur
	if _, err := svc.SealWindow(ctx, req); err != nil {
		t.Fatalf("seal with current version: %v", err)
	}
}

func TestConcurrentSealSingleVersion(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()
	if _, err := svc.Consume(ctx, consumeReq("c1", 25)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	clk.Advance(time.Minute)

	// 并发封存：只能形成一个封存版本。
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = svc.SealWindow(ctx, sealReq(fmt.Sprintf("seal-%d", i), LevelOrg, window0()))
		}(i)
	}
	wg.Wait()
	var succeeded, sealedConflict int
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrWindowSealed):
			sealedConflict++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 || sealedConflict != len(errs)-1 {
		t.Fatalf("succeeded=%d sealedConflict=%d, want 1 and %d", succeeded, sealedConflict, len(errs)-1)
	}
	seal, err := svc.GetSeal(ctx, LevelOrg, "org-1", window0())
	if err != nil {
		t.Fatalf("get seal: %v", err)
	}
	if seal.Used != 25 || seal.Version != 1 {
		t.Fatalf("unexpected final snapshot: %+v", seal)
	}
}

func TestSealSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	clk := newClock(testEpoch)
	ctx := context.Background()

	newSvc := func() *Service {
		store, err := OpenFileStore(path)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		return NewService(store, WithClock(clk.Now), WithRefundTTL(time.Hour))
	}
	svc := newSvc()
	for _, level := range Levels {
		if _, err := svc.SetConfig(ctx, level, string(level)+"-1", 100, 60, 0); err != nil {
			t.Fatalf("set config: %v", err)
		}
	}
	if _, err := svc.Consume(ctx, consumeReq("c1", 30)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	clk.Advance(time.Minute)
	if _, err := svc.SealWindow(ctx, sealReq("seal-1", LevelOrg, window0())); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// 重启后：快照完整恢复，重复封存仍按幂等/冲突处理，封存窗口仍拒绝写入。
	svc = newSvc()
	seal, err := svc.GetSeal(ctx, LevelOrg, "org-1", window0())
	if err != nil {
		t.Fatalf("get seal after restart: %v", err)
	}
	if seal.Used != 30 || seal.RequestID != "seal-1" {
		t.Fatalf("unexpected snapshot after restart: %+v", seal)
	}
	if _, err := svc.SealWindow(ctx, sealReq("seal-2", LevelOrg, window0())); !errors.Is(err, ErrWindowSealed) {
		t.Fatalf("expected ErrWindowSealed after restart, got %v", err)
	}
	clk.Advance(-time.Minute)
	if _, err := svc.Consume(ctx, consumeReq("c-late", 5)); !errors.Is(err, ErrWindowSealed) {
		t.Fatalf("expected ErrWindowSealed for consume after restart, got %v", err)
	}
}

func TestWindowStatementAttributesEntries(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Consume(ctx, consumeReq("c1", 40)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := svc.Reserve(ctx, ReserveRequest{
		RequestID: "r1", OrgID: "org-1", UserID: "user-1", KeyID: "key-1", Amount: 10,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := svc.ConfirmReservation(ctx, "r1"); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if _, err := svc.Refund(ctx, RefundRequest{RequestID: "rf1", ConsumeRequestID: "c1", Amount: 5}); err != nil {
		t.Fatalf("refund: %v", err)
	}
	clk.Advance(time.Minute)
	if _, err := svc.SealWindow(ctx, sealReq("seal-1", LevelOrg, window0())); err != nil {
		t.Fatalf("seal: %v", err)
	}

	st, err := svc.WindowStatement(ctx, LevelOrg, "org-1", window0())
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if !st.Sealed || st.Seal == nil || st.Seal.Used != 45 {
		t.Fatalf("unexpected statement seal: %+v", st.Seal)
	}
	if st.Used != 45 {
		t.Fatalf("statement used = %d, want 45", st.Used)
	}
	kinds := map[string]int64{}
	for _, e := range st.Entries {
		kinds[e.Kind] += e.Amount
	}
	if kinds["consume"] != 50 || kinds["refund"] != -5 ||
		kinds["reservation_reserve"] != 10 || kinds["reservation_settle"] != 10 {
		t.Fatalf("unexpected entries: %+v", st.Entries)
	}
}
