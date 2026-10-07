package quota

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// window0 是 setup 时钟起点所在的窗口起点。
func window0() int64 {
	sec := testEpoch.Unix()
	return sec - sec%60
}

func sealReq(id string, level Level, windowStart int64) SealRequest {
	return SealRequest{
		RequestID:   id,
		Level:       level,
		SubjectID:   string(level) + "-1",
		WindowStart: windowStart,
	}
}

// sealAllLevels 封存三层主体在指定窗口的账目。
func sealAllLevels(t *testing.T, svc *Service, idPrefix string, windowStart int64) {
	t.Helper()
	for _, level := range Levels {
		if _, err := svc.SealWindow(context.Background(), sealReq(idPrefix+"-"+string(level), level, windowStart)); err != nil {
			t.Fatalf("seal %s: %v", level, err)
		}
	}
}

func TestSealWindowBoundary(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()
	ws := window0()

	// 窗口未结束：拒绝封存。
	if _, err := svc.SealWindow(ctx, sealReq("s1", LevelOrg, ws)); !errors.Is(err, ErrWindowNotEnded) {
		t.Fatalf("seal open window: %v", err)
	}
	// 未对齐的窗口起点：参数错误。
	clk.Advance(61 * time.Second)
	if _, err := svc.SealWindow(ctx, sealReq("s2", LevelOrg, ws+1)); !errors.Is(err, ErrValidation) {
		t.Fatalf("seal unaligned window: %v", err)
	}
	// 窗口结束后：封存成功，快照记录账目。
	res, err := svc.SealWindow(ctx, sealReq("s3", LevelOrg, ws))
	if err != nil {
		t.Fatalf("seal ended window: %v", err)
	}
	if res.Duplicate || res.Snapshot.Limit != 100 || res.Snapshot.WindowEnd != ws+60 {
		t.Fatalf("unexpected snapshot: %+v", res.Snapshot)
	}
	// 未结束的当前窗口仍不可封存。
	if _, err := svc.SealWindow(ctx, sealReq("s4", LevelOrg, ws+60)); !errors.Is(err, ErrWindowNotEnded) {
		t.Fatalf("seal current window: %v", err)
	}
}

func TestSealIdempotency(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()
	ws := window0()
	clk.Advance(61 * time.Second)

	first, err := svc.SealWindow(ctx, sealReq("s1", LevelOrg, ws))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	// 同号同内容：返回首次结果。
	again, err := svc.SealWindow(ctx, sealReq("s1", LevelOrg, ws))
	if err != nil {
		t.Fatalf("replay seal: %v", err)
	}
	if !again.Duplicate || again.Snapshot.RequestID != first.Snapshot.RequestID ||
		again.Snapshot.Version != first.Snapshot.Version || again.Snapshot.SealedAt != first.Snapshot.SealedAt {
		t.Fatalf("replay mismatch: %+v vs %+v", again.Snapshot, first.Snapshot)
	}
	// 同号不同内容：幂等冲突。
	if _, err := svc.SealWindow(ctx, sealReq("s1", LevelUser, ws)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("same id different content: %v", err)
	}
	// 不同请求号封存同一窗口：窗口已封存冲突。
	if _, err := svc.SealWindow(ctx, sealReq("s2", LevelOrg, ws)); !errors.Is(err, ErrWindowSealed) {
		t.Fatalf("reseal same window: %v", err)
	}
}

func TestSealedWindowRejectsNewOperations(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()
	ws := window0()
	clk.Advance(61 * time.Second)
	sealAllLevels(t, svc, "s", ws)

	// 时钟回到已封存窗口：新消费与新预占都被拒绝。
	clk.Advance(-61 * time.Second)
	if _, err := svc.Consume(ctx, consumeReq("c1", 10)); !errors.Is(err, ErrWindowSealed) {
		t.Fatalf("consume in sealed window: %v", err)
	}
	if _, err := svc.Reserve(ctx, ReserveRequest{
		RequestID: "rsv1", OrgID: "org-1", UserID: "user-1", KeyID: "key-1", Amount: 10,
	}); !errors.Is(err, ErrWindowSealed) {
		t.Fatalf("reserve in sealed window: %v", err)
	}
	// 额度调整被拒绝；纯版本检查式的不变更新仍允许。
	if _, err := svc.SetConfig(ctx, LevelOrg, "org-1", 200, 60, 1); !errors.Is(err, ErrWindowSealed) {
		t.Fatalf("adjust limit after seal: %v", err)
	}
	if _, err := svc.SetConfig(ctx, LevelOrg, "org-1", 100, 120, 1); !errors.Is(err, ErrWindowSealed) {
		t.Fatalf("adjust window after seal: %v", err)
	}
	if _, err := svc.SetConfig(ctx, LevelOrg, "org-1", 100, 60, 1); err != nil {
		t.Fatalf("unchanged config update: %v", err)
	}
}

func TestLateSettlementUsesSnapshot(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()
	ws := window0()

	if _, err := svc.Consume(ctx, consumeReq("c1", 30)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := svc.Reserve(ctx, ReserveRequest{
		RequestID: "rsv1", OrgID: "org-1", UserID: "user-1", KeyID: "key-1", Amount: 20,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	// 窗口结束后封存三层。
	clk.Advance(61 * time.Second)
	sealAllLevels(t, svc, "s", ws)
	snap, err := svc.GetSeal(ctx, LevelOrg, "org-1", ws)
	if err != nil {
		t.Fatalf("get seal: %v", err)
	}
	if snap.Used != 30 || snap.Reserved != 20 || len(snap.OpenReservations) != 1 || snap.OpenReservations[0] != "rsv1" {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	// 迟到的确认：只依据快照更新结算，不触碰新窗口。
	if _, err := svc.ConfirmReservation(ctx, "rsv1"); err != nil {
		t.Fatalf("late confirm: %v", err)
	}
	snap, _ = svc.GetSeal(ctx, LevelOrg, "org-1", ws)
	if snap.Used != 50 || snap.Reserved != 0 || snap.Version != 2 {
		t.Fatalf("snapshot after confirm: %+v", snap)
	}
	// 迟到的退还：同样只更新快照。
	if _, err := svc.Refund(ctx, RefundRequest{RequestID: "r1", ConsumeRequestID: "c1", Amount: 10}); err != nil {
		t.Fatalf("late refund: %v", err)
	}
	snap, _ = svc.GetSeal(ctx, LevelOrg, "org-1", ws)
	if snap.Used != 40 || snap.Version != 3 {
		t.Fatalf("snapshot after refund: %+v", snap)
	}
	// 新窗口余额不受迟到结算影响。
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, b := range balances {
		if b.Used != 0 || b.Reserved != 0 || b.Remaining != 100 {
			t.Fatalf("new window touched: %+v", b)
		}
	}
}

func TestConcurrentSealAndSettle(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()
	ws := window0()
	if _, err := svc.Reserve(ctx, ReserveRequest{
		RequestID: "rsv1", OrgID: "org-1", UserID: "user-1", KeyID: "key-1", Amount: 20,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	clk.Advance(61 * time.Second)

	// 封存与确认并发：只形成一个结算版本。
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errs[0] = svc.SealWindow(ctx, sealReq("s-org", LevelOrg, ws))
	}()
	go func() {
		defer wg.Done()
		_, errs[1] = svc.ConfirmReservation(ctx, "rsv1")
	}()
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent seal/confirm: %v", err)
		}
	}
	rec, err := svc.GetReservation(ctx, "rsv1")
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}
	if rec.State != ReservationConfirmed {
		t.Fatalf("reservation state: %s", rec.State)
	}
	snap, err := svc.GetSeal(ctx, LevelOrg, "org-1", ws)
	if err != nil {
		t.Fatalf("get seal: %v", err)
	}
	// 无论先后顺序，最终账目都是 used=20, reserved=0。
	if snap.Used != 20 || snap.Reserved != 0 {
		t.Fatalf("final snapshot: %+v", snap)
	}

	// 并发封存同一窗口：只有一个成功。
	var okCount, sealedCount int
	var mu sync.Mutex
	wg.Add(8)
	for i := 0; i < 8; i++ {
		go func(i int) {
			defer wg.Done()
			_, err := svc.SealWindow(ctx, sealReq(string(rune('a'+i)), LevelUser, ws))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				okCount++
			case errors.Is(err, ErrWindowSealed):
				sealedCount++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if okCount != 1 || sealedCount != 7 {
		t.Fatalf("concurrent seals: ok=%d sealed=%d", okCount, sealedCount)
	}
}

func TestSealRestartRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	clk := newClock(testEpoch)
	ws := window0()
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
	clk.Advance(61 * time.Second)
	if _, err := svc.SealWindow(ctx, sealReq("s1", LevelOrg, ws)); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// 重启后：快照完整恢复，重复封存返回首次结果，迟到退还仍走快照。
	svc = newSvc()
	res, err := svc.SealWindow(ctx, sealReq("s1", LevelOrg, ws))
	if err != nil || !res.Duplicate {
		t.Fatalf("replay after restart: %+v %v", res, err)
	}
	if _, err := svc.Refund(ctx, RefundRequest{RequestID: "r1", ConsumeRequestID: "c1", Amount: 5}); err != nil {
		t.Fatalf("refund after restart: %v", err)
	}
	snap, err := svc.GetSeal(ctx, LevelOrg, "org-1", ws)
	if err != nil {
		t.Fatalf("get seal after restart: %v", err)
	}
	if snap.Used != 25 || snap.Version != 2 {
		t.Fatalf("snapshot after restart: %+v", snap)
	}
}

func TestWindowReport(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()
	ws := window0()

	if _, err := svc.Consume(ctx, consumeReq("c1", 30)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := svc.Reserve(ctx, ReserveRequest{
		RequestID: "rsv1", OrgID: "org-1", UserID: "user-1", KeyID: "key-1", Amount: 20,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := svc.Refund(ctx, RefundRequest{RequestID: "r1", ConsumeRequestID: "c1", Amount: 5}); err != nil {
		t.Fatalf("refund: %v", err)
	}

	report, err := svc.WindowReport(ctx, LevelOrg, "org-1", ws)
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if report.Sealed || report.Used != 25 || report.Reserved != 20 {
		t.Fatalf("report before seal: %+v", report)
	}
	if len(report.Entries) != 3 {
		t.Fatalf("entries: %+v", report.Entries)
	}
	byKind := map[string]WindowEntry{}
	for _, e := range report.Entries {
		byKind[e.Kind] = e
	}
	if byKind["consume"].Amount != 30 || byKind["refund"].Amount != -5 ||
		byKind["reservation"].Amount != 20 || byKind["reservation"].State != "reserved" {
		t.Fatalf("entries: %+v", report.Entries)
	}

	clk.Advance(61 * time.Second)
	sealAllLevels(t, svc, "s", ws)
	report, err = svc.WindowReport(ctx, LevelOrg, "org-1", ws)
	if err != nil {
		t.Fatalf("report after seal: %v", err)
	}
	if !report.Sealed || report.Snapshot == nil || report.Snapshot.RequestID != "s-org" {
		t.Fatalf("report after seal: %+v", report)
	}
	seals, err := svc.ListSeals(ctx, LevelOrg, "org-1")
	if err != nil || len(seals) != 1 {
		t.Fatalf("list seals: %+v %v", seals, err)
	}
}
