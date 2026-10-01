package quota

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func rebalanceReq(id string, org, user, key int64) RebalanceRequest {
	return RebalanceRequest{
		RequestID: id,
		OrgID:     "org-1",
		UserID:    "user-1",
		KeyID:     "key-1",
		TargetLimits: map[Level]int64{
			LevelOrg: org, LevelUser: user, LevelKey: key,
		},
	}
}

func levelEntryOf(t *testing.T, rec RebalanceRecord, level Level) LevelRebalance {
	t.Helper()
	entry, ok := rec.levelEntry(level)
	if !ok {
		t.Fatalf("rebalance %s has no entry for level %s", rec.RequestID, level)
	}
	return entry
}

func TestRebalanceZeroSumTransfer(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 组织 -20，用户 +20，密钥不变：同一份额度在层间转移。
	res, err := svc.Rebalance(ctx, rebalanceReq("rb1", 80, 120, 100))
	if err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	if res.Duplicate {
		t.Fatal("first rebalance should not be marked duplicate")
	}
	want := map[Level][2]int64{
		LevelOrg:  {80, -20},
		LevelUser: {120, 20},
		LevelKey:  {100, 0},
	}
	var deltaSum int64
	for _, level := range Levels {
		entry := levelEntryOf(t, res.Record, level)
		deltaSum += entry.Delta
		if entry.LimitAfter != want[level][0] || entry.Delta != want[level][1] {
			t.Errorf("level %s after=%d delta=%d, want %d/%d",
				level, entry.LimitAfter, entry.Delta, want[level][0], want[level][1])
		}
		if entry.LimitBefore != 100 {
			t.Errorf("level %s before=%d, want 100", level, entry.LimitBefore)
		}
		if entry.Used != 0 || entry.Reserved != 0 {
			t.Errorf("level %s used=%d reserved=%d, want 0/0 baseline", level, entry.Used, entry.Reserved)
		}
		b := balanceOf(t, res.Balances, level)
		if b.Limit != want[level][0] || b.Remaining != want[level][0] {
			t.Errorf("level %s limit=%d remaining=%d", level, b.Limit, b.Remaining)
		}
		if b.LastRebalanceRequestID != "rb1" {
			t.Errorf("level %s last rebalance=%q, want rb1", level, b.LastRebalanceRequestID)
		}
		if b.ConfigVersion != entry.ConfigVersionAfter {
			t.Errorf("level %s balance config version=%d, entry after=%d",
				level, b.ConfigVersion, entry.ConfigVersionAfter)
		}
	}
	if deltaSum != 0 {
		t.Errorf("delta sum=%d, want 0 (zero-sum)", deltaSum)
	}
}

func TestRebalanceRejectsNonZeroSum(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 80+110+100=290，凭空多出 10。
	_, err := svc.Rebalance(ctx, rebalanceReq("rb1", 80, 110, 100))
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("want ErrValidation, got %v", err)
	}
	// 三层额度均不得改变。
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.Limit != 100 || b.LastRebalanceRequestID != "" {
			t.Errorf("level %s limit=%d last=%q, want 100/empty",
				level, b.Limit, b.LastRebalanceRequestID)
		}
	}
	// 缺失目标额度 / 非正目标同样被拒。
	if _, err := svc.Rebalance(ctx, RebalanceRequest{
		RequestID: "rb2", OrgID: "org-1", UserID: "user-1", KeyID: "key-1",
		TargetLimits: map[Level]int64{LevelOrg: 80, LevelUser: 120},
	}); !errors.Is(err, ErrValidation) {
		t.Fatalf("missing target: want ErrValidation, got %v", err)
	}
	if _, err := svc.Rebalance(ctx, rebalanceReq("rb3", 0, 200, 100)); !errors.Is(err, ErrValidation) {
		t.Fatalf("non-positive target: want ErrValidation, got %v", err)
	}
}

func TestRebalanceInsufficientIsAtomic(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 三层均已消费 90。
	if _, err := svc.Consume(ctx, consumeReq("c1", 90)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	// 组织调减到 80 < 已消费 90：整笔调整不落地。
	_, err := svc.Rebalance(ctx, rebalanceReq("rb1", 80, 120, 100))
	if !errors.Is(err, ErrInsufficientQuota) {
		t.Fatalf("want ErrInsufficientQuota, got %v", err)
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Limit != 100 || b.Used != 90 || b.Remaining != 10 {
			t.Errorf("level %s limit=%d used=%d remaining=%d, want 100/90/10 (no partial rebalance)",
				level, b.Limit, b.Used, b.Remaining)
		}
	}
	// 被拒的调整单号不得出现在历史里。
	history, err := svc.RebalanceHistory(ctx, HistoryFilter{})
	if err != nil || len(history) != 0 {
		t.Fatalf("history len=%d err=%v, want 0", len(history), err)
	}
}

func TestRebalanceFloorIncludesReserved(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 预占 70 后，任何一层目标额度低于 70 都必须拒绝。
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 70)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 用户调减到 60 < reserved 70。
	_, err := svc.Rebalance(ctx, rebalanceReq("rb1", 130, 60, 110))
	if !errors.Is(err, ErrInsufficientQuota) {
		t.Fatalf("want ErrInsufficientQuota, got %v", err)
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.Limit != 100 || b.Reserved != 70 {
			t.Errorf("level %s limit=%d reserved=%d, want 100/70", level, b.Limit, b.Reserved)
		}
	}
	// 目标恰好等于 used+reserved（70）允许通过。
	res, err := svc.Rebalance(ctx, rebalanceReq("rb2", 130, 70, 100))
	if err != nil {
		t.Fatalf("rebalance to floor: %v", err)
	}
	for _, level := range Levels {
		entry := levelEntryOf(t, res.Record, level)
		if entry.Used != 0 || entry.Reserved != 70 {
			t.Errorf("level %s basis used=%d reserved=%d, want 0/70",
				level, entry.Used, entry.Reserved)
		}
	}
}

func TestRebalanceDoesNotTouchInFlightReservation(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	// 预占 30，然后把组织额度调减 40（转 20 给用户、20 给密钥）。
	if _, err := svc.Reserve(ctx, reserveReqWithTTL("rsv1", 30, 10*time.Minute)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	res, err := svc.Rebalance(ctx, rebalanceReq("rb1", 60, 120, 120))
	if err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	// 单据记录了在途预占，作为“调整与在途消费并存”的依据。
	for _, level := range Levels {
		entry := levelEntryOf(t, res.Record, level)
		if len(entry.InFlightReservations) != 1 || entry.InFlightReservations[0] != "rsv1" {
			t.Errorf("level %s in-flight=%v, want [rsv1]", level, entry.InFlightReservations)
		}
	}
	// used 仍为 0、reserved 仍为 30：调整不改动在途预占。
	for _, level := range Levels {
		b := balanceOf(t, res.Balances, level)
		if b.Used != 0 || b.Reserved != 30 {
			t.Errorf("level %s used=%d reserved=%d, want 0/30", level, b.Used, b.Reserved)
		}
	}
	// 跨过窗口后确认：消费进入原窗口，新窗口额度不受影响，
	// 调整后的新额度同样不被这笔旧窗口消费占用。
	clk.Advance(61 * time.Second)
	confirm, err := svc.ConfirmReservation(ctx, "rsv1")
	if err != nil {
		t.Fatalf("confirm after rebalance: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, confirm.Balances, level)
		if b.Used != 0 || b.Reserved != 0 {
			t.Errorf("level %s new window used=%d reserved=%d, want 0/0",
				level, b.Used, b.Reserved)
		}
	}
	// 原窗口：used=30；配置额度保持调整后的值。
	if err := svc.store.View(ctx, func(tx *Tx) error {
		for _, level := range Levels {
			usage, ok := tx.GetUsage(level, string(level)+"-1", testEpoch.Unix())
			if !ok {
				t.Fatalf("level %s old window usage missing", level)
			}
			if usage.Used != 30 {
				t.Errorf("level %s old window used=%d, want 30", level, usage.Used)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestRebalanceIdempotency(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	req := rebalanceReq("rb1", 80, 120, 100)
	first, err := svc.Rebalance(ctx, req)
	if err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	dup, err := svc.Rebalance(ctx, req)
	if err != nil {
		t.Fatalf("duplicate rebalance: %v", err)
	}
	if !dup.Duplicate || dup.Record.Version != first.Record.Version {
		t.Fatalf("replay should return first result, got %+v", dup)
	}
	// 同号异内容：幂等冲突。
	if _, err := svc.Rebalance(ctx, rebalanceReq("rb1", 90, 110, 100)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different content: want ErrIdempotencyConflict, got %v", err)
	}
	other := rebalanceReq("rb1", 80, 120, 100)
	other.OrgID = "org-other"
	if _, err := svc.Rebalance(ctx, other); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different org: want ErrIdempotencyConflict, got %v", err)
	}
	// 额度只能变化一次。
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	want := map[Level]int64{LevelOrg: 80, LevelUser: 120, LevelKey: 100}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.Limit != want[level] {
			t.Errorf("level %s limit=%d, want %d", level, b.Limit, want[level])
		}
	}
}

func TestRebalanceConflictOnWindowRotation(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	req := rebalanceReq("rb1", 80, 120, 100)
	if _, err := svc.Rebalance(ctx, req); err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	// 窗口滚动后同号同内容重放：原单依据的窗口已失效，报冲突。
	clk.Advance(61 * time.Second)
	if _, err := svc.Rebalance(ctx, req); !errors.Is(err, ErrRebalanceConflict) {
		t.Fatalf("want ErrRebalanceConflict after window rotation, got %v", err)
	}
	// 已落地的调整不受影响。
	cfg, err := svc.GetConfig(ctx, LevelOrg, "org-1")
	if err != nil {
		t.Fatalf("get config: %v", err)
	}
	if cfg.Limit != 80 {
		t.Errorf("org limit=%d, want 80", cfg.Limit)
	}
}

func TestRebalanceConflictWhenTargetDrifted(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	first := rebalanceReq("rb1", 80, 120, 100)
	if _, err := svc.Rebalance(ctx, first); err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	// 后来的重平衡改变了三层额度。
	if _, err := svc.Rebalance(ctx, rebalanceReq("rb2", 70, 120, 110)); err != nil {
		t.Fatalf("second rebalance: %v", err)
	}
	// 用 rb1 原内容重放：其目标额度已不是当前值，旧调整不能覆盖新状态。
	if _, err := svc.Rebalance(ctx, first); !errors.Is(err, ErrRebalanceConflict) {
		t.Fatalf("want ErrRebalanceConflict when target drifted, got %v", err)
	}
}

// TestRebalanceConcurrentWithConfirm 重平衡与确认消费并发到达时，
// 两者都基于同一版本计算：确认掉的消费不丢失，调整只拒绝受影响的那一笔，
// 绝不能回滚已成功的消费。
func TestRebalanceConcurrentWithConfirm(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// rsv1 预占 60、rsv2 预占 10，当前窗口三层 used=0 reserved=70 remaining=30。
	if _, err := svc.Reserve(ctx, reserveReqWithTTL("rsv1", 60, 10*time.Minute)); err != nil {
		t.Fatalf("reserve rsv1: %v", err)
	}
	if _, err := svc.Reserve(ctx, reserveReqWithTTL("rsv2", 10, 10*time.Minute)); err != nil {
		t.Fatalf("reserve rsv2: %v", err)
	}

	// tight：三层调到 50/150/100，低于已预占总额 70，任何顺序下都必须被拒。
	// loose：三层调到 90/110/100，任何顺序下都满足下限（90 >= used+reserved=70）。
	// 确认把 reserved 转为 used，占用总量不变，因此调整看到的下限与确认先后无关；
	// 版本机制保证两者在同一用量版本上串行，确认掉的消费不会被旧调整覆盖。
	start := make(chan struct{})
	var wg sync.WaitGroup
	var confirmErr, tightErr, looseErr error
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		_, confirmErr = svc.ConfirmReservation(ctx, "rsv1")
	}()
	go func() {
		defer wg.Done()
		<-start
		_, tightErr = svc.Rebalance(ctx, rebalanceReq("rb-tight", 50, 150, 100))
	}()
	go func() {
		defer wg.Done()
		<-start
		_, looseErr = svc.Rebalance(ctx, rebalanceReq("rb-loose", 90, 110, 100))
	}()
	close(start)
	wg.Wait()

	if confirmErr != nil {
		t.Fatalf("confirm must succeed regardless of rebalance: %v", confirmErr)
	}
	if !errors.Is(tightErr, ErrInsufficientQuota) && tightErr != nil {
		t.Fatalf("unexpected tight rebalance error: %v", tightErr)
	}
	if looseErr != nil {
		t.Fatalf("loose rebalance error: %v", looseErr)
	}
	if tightErr == nil {
		t.Fatal("tight rebalance to 50 must be rejected: reserved 70 (or confirmed 60) exceeds target")
	}

	// 最终不变量：已确认的消费一定保留；被拒绝的调整不影响任何用量。
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Used != 60 {
			t.Errorf("level %s used=%d, want 60 (confirmed consume never rolled back)", level, b.Used)
		}
		if b.Reserved != 10 {
			t.Errorf("level %s reserved=%d, want 10 (rsv2 untouched)", level, b.Reserved)
		}
		if b.Limit < b.Used+b.Reserved {
			t.Errorf("level %s limit=%d below used+reserved=%d", level, b.Limit, b.Used+b.Reserved)
		}
	}
	// rsv2 仍可确认：其在途额度没有被任何调整破坏。
	if _, err := svc.ConfirmReservation(ctx, "rsv2"); err != nil {
		t.Fatalf("rsv2 confirm after rebalance race: %v", err)
	}
}

func TestRebalanceHistoryAndFilters(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 先配置第二组主体。
	for _, level := range Levels {
		if _, err := svc.SetConfig(ctx, level, string(level)+"-2", 100, 60, 0); err != nil {
			t.Fatalf("set config %s-2: %v", level, err)
		}
	}
	mkReq := func(id, org, user, key string, orgL int64) RebalanceRequest {
		return RebalanceRequest{
			RequestID: id, OrgID: org, UserID: user, KeyID: key,
			TargetLimits: map[Level]int64{LevelOrg: orgL, LevelUser: 200 - orgL, LevelKey: 100},
		}
	}
	if _, err := svc.Rebalance(ctx, mkReq("rb1", "org-1", "user-1", "key-1", 80)); err != nil {
		t.Fatalf("rb1: %v", err)
	}
	if _, err := svc.Rebalance(ctx, mkReq("rb2", "org-2", "user-2", "key-2", 70)); err != nil {
		t.Fatalf("rb2: %v", err)
	}

	all, err := svc.RebalanceHistory(ctx, HistoryFilter{})
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(all) != 2 || all[0].RequestID != "rb1" || all[1].RequestID != "rb2" {
		t.Fatalf("all history order=%v, want [rb1 rb2]", []string{all[0].RequestID, all[1].RequestID})
	}
	org1, err := svc.RebalanceHistory(ctx, HistoryFilter{OrgID: "org-1"})
	if err != nil {
		t.Fatalf("filtered history: %v", err)
	}
	if len(org1) != 1 || org1[0].RequestID != "rb1" {
		t.Fatalf("org-1 history=%v, want [rb1]", org1)
	}
	user2, err := svc.RebalanceHistory(ctx, HistoryFilter{UserID: "user-2"})
	if err != nil {
		t.Fatalf("user-2 history: %v", err)
	}
	if len(user2) != 1 || user2[0].RequestID != "rb2" {
		t.Fatalf("user-2 history=%v, want [rb2]", user2)
	}

	// 单笔查询。
	rec, err := svc.GetRebalance(ctx, "rb2")
	if err != nil {
		t.Fatalf("get rb2: %v", err)
	}
	if org := levelEntryOf(t, rec, LevelOrg); org.LimitBefore != 100 || org.LimitAfter != 70 || org.Delta != -30 {
		t.Errorf("org-2 basis=%+v, want 100->70 delta -30", org)
	}
	if _, err := svc.GetRebalance(ctx, "ghost"); !errors.Is(err, ErrRebalanceNotFound) {
		t.Fatalf("want ErrRebalanceNotFound, got %v", err)
	}
}

func TestConsumeAndReservationHistory(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Consume(ctx, consumeReq("c1", 10)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 20)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := svc.ConfirmReservation(ctx, "rsv1"); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	consumes, err := svc.ConsumeHistory(ctx, HistoryFilter{})
	if err != nil {
		t.Fatalf("consume history: %v", err)
	}
	// 普通消费 + 预占确认生成的消费都应出现。
	if len(consumes) != 2 {
		var ids []string
		for _, c := range consumes {
			ids = append(ids, c.RequestID)
		}
		t.Fatalf("consumes=%v, want c1 and reservation:rsv1", ids)
	}
	byKey, err := svc.ConsumeHistory(ctx, HistoryFilter{KeyID: "key-1"})
	if err != nil || len(byKey) != 2 {
		t.Fatalf("key-1 consumes=%d err=%v, want 2", len(byKey), err)
	}
	byOther, err := svc.ConsumeHistory(ctx, HistoryFilter{KeyID: "key-other"})
	if err != nil || len(byOther) != 0 {
		t.Fatalf("other key consumes=%d err=%v, want 0", len(byOther), err)
	}

	reservations, err := svc.ReservationHistory(ctx, HistoryFilter{})
	if err != nil {
		t.Fatalf("reservation history: %v", err)
	}
	if len(reservations) != 1 || reservations[0].RequestID != "rsv1" ||
		reservations[0].State != ReservationConfirmed {
		t.Fatalf("reservations=%+v, want confirmed rsv1", reservations)
	}
}

// TestBalanceTraceableToRebalance 验证每层余额变化都能回到具体重平衡单：
// 连续两次调整后，余额上的 last_rebalance_request_id 指向最近一次单据，
// 沿单据链可以还原每层额度的增减过程。
func TestBalanceTraceableToRebalance(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Rebalance(ctx, rebalanceReq("rb1", 80, 120, 100)); err != nil {
		t.Fatalf("rb1: %v", err)
	}
	if _, err := svc.Consume(ctx, consumeReq("c1", 30)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := svc.Rebalance(ctx, rebalanceReq("rb2", 90, 110, 100)); err != nil {
		t.Fatalf("rb2: %v", err)
	}

	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.LastRebalanceRequestID != "rb2" {
			t.Errorf("level %s last rebalance=%q, want rb2", level, b.LastRebalanceRequestID)
		}
	}

	// 沿单据链回溯每层调整依据：100 -> 80 -> 90（组织），100 -> 120 -> 110（用户）。
	rb2, err := svc.GetRebalance(ctx, "rb2")
	if err != nil {
		t.Fatalf("get rb2: %v", err)
	}
	rb1, err := svc.GetRebalance(ctx, "rb1")
	if err != nil {
		t.Fatalf("get rb1: %v", err)
	}
	org2 := levelEntryOf(t, rb2, LevelOrg)
	org1 := levelEntryOf(t, rb1, LevelOrg)
	if org1.LimitBefore != 100 || org1.LimitAfter != 80 || org2.LimitBefore != 80 || org2.LimitAfter != 90 {
		t.Errorf("org chain: %d->%d then %d->%d, want 100->80 then 80->90",
			org1.LimitBefore, org1.LimitAfter, org2.LimitBefore, org2.LimitAfter)
	}
	// rb2 依据里必须保留已消费 30 的事实。
	if org2.Used != 30 || org2.Reserved != 0 {
		t.Errorf("rb2 org basis used=%d reserved=%d, want 30/0", org2.Used, org2.Reserved)
	}
	user2 := levelEntryOf(t, rb2, LevelUser)
	user1 := levelEntryOf(t, rb1, LevelUser)
	if user1.LimitAfter != 120 || user2.LimitBefore != 120 || user2.LimitAfter != 110 {
		t.Errorf("user chain: %d->%d then %d->%d, want 100->120 then 120->110",
			user1.LimitBefore, user1.LimitAfter, user2.LimitBefore, user2.LimitAfter)
	}
	// 窗口与版本依据完整。
	for _, level := range Levels {
		e := levelEntryOf(t, rb2, level)
		if e.WindowStart != testEpoch.Unix() {
			t.Errorf("level %s window=%d, want %d", level, e.WindowStart, testEpoch.Unix())
		}
		if e.ConfigVersionBefore <= 0 || e.ConfigVersionAfter != e.ConfigVersionBefore+1 {
			t.Errorf("level %s versions before=%d after=%d", level, e.ConfigVersionBefore, e.ConfigVersionAfter)
		}
	}
}

func TestRebalanceFileStorePersistence(t *testing.T) {
	path := t.TempDir() + "/store.json"
	clk := newClock(testEpoch)

	store, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	svc := NewService(store, WithClock(clk.Now))
	ctx := context.Background()
	for _, level := range Levels {
		if _, err := svc.SetConfig(ctx, level, string(level)+"-1", 100, 60, 0); err != nil {
			t.Fatalf("set config %s: %v", level, err)
		}
	}
	req := rebalanceReq("rb1", 70, 130, 100)
	if _, err := svc.Rebalance(ctx, req); err != nil {
		t.Fatalf("rebalance: %v", err)
	}

	// 重启后：重平衡单可查，幂等重放仍返回首次结果，额度保持调整后的值。
	reopened, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	svc2 := NewService(reopened, WithClock(clk.Now))
	dup, err := svc2.Rebalance(ctx, req)
	if err != nil {
		t.Fatalf("replay after reload: %v", err)
	}
	if !dup.Duplicate {
		t.Fatal("rebalance should be a duplicate after reload")
	}
	balances, err := svc2.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	if b := balanceOf(t, balances, LevelOrg); b.Limit != 70 {
		t.Errorf("org limit=%d after reload, want 70", b.Limit)
	}
	history, err := svc2.RebalanceHistory(ctx, HistoryFilter{})
	if err != nil || len(history) != 1 || history[0].RequestID != "rb1" {
		t.Fatalf("history after reload=%v err=%v", history, err)
	}
}

func TestRebalanceRequiresConfiguredLevels(t *testing.T) {
	svc := NewService(NewMemoryStore())
	_, err := svc.Rebalance(context.Background(), rebalanceReq("rb1", 80, 120, 100))
	if !errors.Is(err, ErrQuotaNotConfigured) {
		t.Fatalf("want ErrQuotaNotConfigured, got %v", err)
	}
}

func TestRebalanceRejectsNoOpAndValidation(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()
	// 三层 delta 全为 0 不是一次调整。
	_, err := svc.Rebalance(ctx, rebalanceReq("rb1", 100, 100, 100))
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("no-op rebalance: want ErrValidation, got %v", err)
	}
	if _, err := svc.Rebalance(ctx, RebalanceRequest{
		OrgID: "org-1", UserID: "user-1", KeyID: "key-1",
		TargetLimits: map[Level]int64{LevelOrg: 80, LevelUser: 120, LevelKey: 100},
	}); !errors.Is(err, ErrValidation) {
		t.Fatalf("missing request id: want ErrValidation, got %v", err)
	}
	if _, err := svc.GetRebalance(ctx, ""); !errors.Is(err, ErrValidation) {
		t.Fatalf("empty id: want ErrValidation, got %v", err)
	}
}

// TestRebalanceConcurrentCancellation 取消预占与重平衡并发：
// 调整若先看到 reserved=90 则因下限不足被拒（取消照常成功）；
// 取消若先提交则释放占用，调整成功落地。两种顺序下额度状态都必须自洽。
func TestRebalanceConcurrentCancellation(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReqWithTTL("rsv1", 90, 10*time.Minute)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 取消后 reserved 归零，调到 50 才可行；不取消时 50 < 90 必须拒绝。
	start := make(chan struct{})
	var wg sync.WaitGroup
	var cancelErr, rbErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, cancelErr = svc.CancelReservation(ctx, "rsv1")
	}()
	go func() {
		defer wg.Done()
		<-start
		_, rbErr = svc.Rebalance(ctx, rebalanceReq("rb1", 50, 150, 100))
	}()
	close(start)
	wg.Wait()

	if cancelErr != nil {
		t.Fatalf("cancel: %v", cancelErr)
	}
	if rbErr != nil && !errors.Is(rbErr, ErrInsufficientQuota) {
		t.Fatalf("rebalance race: unexpected error %v", rbErr)
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Used != 0 || b.Reserved != 0 {
			t.Errorf("level %s used=%d reserved=%d, want 0/0 after cancel", level, b.Used, b.Reserved)
		}
		// 调整成功时三层应为 50/150/100；被拒时维持 100。
		wantLimit := int64(100)
		if rbErr == nil {
			wantLimit = map[Level]int64{LevelOrg: 50, LevelUser: 150, LevelKey: 100}[level]
		}
		if b.Limit != wantLimit {
			t.Errorf("level %s limit=%d, want %d (rebalance accepted=%v)",
				level, b.Limit, wantLimit, rbErr == nil)
		}
	}
}

// TestRebalanceExhaustedLevelOnlyRejectsAdjustment 某层配额耗尽后，
// 只拒绝受影响的调整，其他消费/预占与已成功消费都不受牵连。
func TestRebalanceExhaustedLevelOnlyRejectsAdjustment(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 第一层组合把三层都耗尽。
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 100)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	_, err := svc.Rebalance(ctx, rebalanceReq("rb1", 99, 101, 100))
	if !errors.Is(err, ErrInsufficientQuota) {
		t.Fatalf("want ErrInsufficientQuota when exhausted, got %v", err)
	}
	// 已成功的预占仍然完好。
	rec, err := svc.GetReservation(ctx, "rsv1")
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}
	if rec.State != ReservationReserved {
		t.Errorf("reservation state=%s, want reserved (not rolled back)", rec.State)
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.Reserved != 100 || b.Limit != 100 {
			t.Errorf("level %s limit=%d reserved=%d, want 100/100", level, b.Limit, b.Reserved)
		}
	}

	// 释放额度后，另一笔不触及下限的调整可以成功，且确认消费照常。
	if _, err := svc.CancelReservation(ctx, "rsv1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := svc.Consume(ctx, consumeReq("c1", 20)); err != nil {
		t.Fatalf("consume after rejected rebalance: %v", err)
	}
	if _, err := svc.Rebalance(ctx, rebalanceReq("rb2", 50, 130, 120)); err != nil {
		t.Fatalf("rebalance after freeing capacity: %v", err)
	}
}

func TestRebalanceRecordBasisVersions(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Consume(ctx, consumeReq("c1", 25)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	res, err := svc.Rebalance(ctx, rebalanceReq("rb1", 75, 110, 115))
	if err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	for _, level := range Levels {
		entry := levelEntryOf(t, res.Record, level)
		if entry.Used != 25 {
			t.Errorf("level %s used basis=%d, want 25", level, entry.Used)
		}
		if entry.WindowStart != testEpoch.Unix() {
			t.Errorf("level %s window=%d, want %d", level, entry.WindowStart, testEpoch.Unix())
		}
		if entry.UsageVersion <= 0 {
			t.Errorf("level %s usage version=%d, want touched positive version", level, entry.UsageVersion)
		}
		if entry.ConfigVersionBefore != 1 || entry.ConfigVersionAfter != 2 {
			t.Errorf("level %s config versions before=%d after=%d, want 1/2",
				level, entry.ConfigVersionBefore, entry.ConfigVersionAfter)
		}
		if len(entry.InFlightReservations) != 0 {
			t.Errorf("level %s in-flight=%v, want empty", level, entry.InFlightReservations)
		}
	}
}
