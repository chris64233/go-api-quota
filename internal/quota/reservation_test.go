package quota

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func reserveReq(id string, amount int64) ReserveRequest {
	return ReserveRequest{
		RequestID: id,
		OrgID:     "org-1",
		UserID:    "user-1",
		KeyID:     "key-1",
		Amount:    amount,
	}
}

// reserveReqWithTTL 用显式有效期构造预占请求。
func reserveReqWithTTL(id string, amount int64, ttl time.Duration) ReserveRequest {
	req := reserveReq(id, amount)
	req.TTLSeconds = int64(ttl.Seconds())
	return req
}

func TestReserveLocksAllThreeLevels(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	res, err := svc.Reserve(ctx, reserveReq("rsv1", 30))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res.Duplicate {
		t.Fatal("first reserve should not be marked duplicate")
	}
	if res.Reservation.State != ReservationReserved {
		t.Fatalf("state=%s, want reserved", res.Reservation.State)
	}
	if !res.Reservation.ExpiresAt.After(res.Reservation.CreatedAt) {
		t.Fatal("expires_at should be after created_at")
	}
	for _, level := range Levels {
		b := balanceOf(t, res.Balances, level)
		// 预占不形成消费：used 不变，reserved 增加，可用额度相应减少。
		if b.Used != 0 || b.Reserved != 30 || b.Remaining != 70 {
			t.Errorf("level %s: used=%d reserved=%d remaining=%d, want 0/30/70",
				level, b.Used, b.Reserved, b.Remaining)
		}
		if res.Reservation.Windows[level] != b.WindowStart {
			t.Errorf("level %s: recorded window=%d, want current %d",
				level, res.Reservation.Windows[level], b.WindowStart)
		}
	}
}

func TestReserveInsufficientIsAtomic(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 先把密钥层的可用额度压到只剩 10：正式消费 60 + 预占 30。
	if _, err := svc.Consume(ctx, consumeReq("c1", 60)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := svc.Reserve(ctx, reserveReq("rsv0", 30)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 再预占 20：密钥层只剩 10，整笔必须失败，任何一层都不能被预占。
	_, err := svc.Reserve(ctx, reserveReq("rsv1", 20))
	if !errors.Is(err, ErrInsufficientQuota) {
		t.Fatalf("want ErrInsufficientQuota, got %v", err)
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Used != 60 || b.Reserved != 30 || b.Remaining != 10 {
			t.Errorf("level %s: used=%d reserved=%d remaining=%d, want 60/30/10 (no partial reserve)",
				level, b.Used, b.Reserved, b.Remaining)
		}
	}
}

func TestConsumeRespectsReservation(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 预占 80 后，普通消费只能使用剩余的 20。
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 80)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := svc.Consume(ctx, consumeReq("c1", 20)); err != nil {
		t.Fatalf("consume 20 within remaining: %v", err)
	}
	_, err := svc.Consume(ctx, consumeReq("c2", 1))
	if !errors.Is(err, ErrInsufficientQuota) {
		t.Fatalf("want ErrInsufficientQuota, got %v", err)
	}
}

func TestConfirmReservationTurnsIntoConsume(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 40)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	res, err := svc.ConfirmReservation(ctx, "rsv1")
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if res.Reservation.State != ReservationConfirmed || res.Consume.Amount != 40 {
		t.Fatalf("state=%s consume amount=%d", res.Reservation.State, res.Consume.Amount)
	}
	if res.Consume.RequestID != reservationConsumeID("rsv1") {
		t.Errorf("consume request id=%s", res.Consume.RequestID)
	}
	for _, level := range Levels {
		b := balanceOf(t, res.Balances, level)
		if b.Used != 40 || b.Reserved != 0 || b.Remaining != 60 {
			t.Errorf("level %s: used=%d reserved=%d remaining=%d, want 40/0/60",
				level, b.Used, b.Reserved, b.Remaining)
		}
	}
	// 确认产生的正式消费可以按普通消费退还。
	refund, err := svc.Refund(ctx, RefundRequest{
		RequestID: "rf1", ConsumeRequestID: reservationConsumeID("rsv1"), Amount: 40,
	})
	if err != nil {
		t.Fatalf("refund confirmed consume: %v", err)
	}
	if refund.Consume.Refunded != 40 {
		t.Errorf("refunded=%d, want 40", refund.Consume.Refunded)
	}
}

func TestCancelReservationReleasesOriginalWindow(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 40)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 跨过窗口边界后再取消：额度只能退回旧窗口。
	clk.Advance(61 * time.Second)
	res, err := svc.CancelReservation(ctx, "rsv1")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if res.Reservation.State != ReservationCancelled {
		t.Fatalf("state=%s, want cancelled", res.Reservation.State)
	}
	// 新窗口额度保持完整，未被退回动作补充。
	for _, level := range Levels {
		b := balanceOf(t, res.Balances, level)
		if b.Used != 0 || b.Reserved != 0 || b.Remaining != 100 {
			t.Errorf("level %s new window: used=%d reserved=%d remaining=%d, want 0/0/100",
				level, b.Used, b.Reserved, b.Remaining)
		}
	}
	// 旧窗口的预占被释放。
	if err := svc.store.View(ctx, func(tx *Tx) error {
		usage, ok := tx.GetUsage(LevelOrg, "org-1", testEpoch.Unix())
		if !ok {
			t.Fatal("old window usage record missing")
		}
		if usage.Reserved != 0 || usage.Used != 0 {
			t.Errorf("old window used=%d reserved=%d, want 0/0", usage.Used, usage.Reserved)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestConfirmAfterWindowRotationConsumesOriginalWindow(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 40)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	clk.Advance(61 * time.Second) // 注意：TTL 默认 5 分钟，预占尚未到期
	if _, err := svc.ConfirmReservation(ctx, "rsv1"); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	// 新窗口余额保持 0/100，消费计入旧窗口。
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.Used != 0 || b.Reserved != 0 {
			t.Errorf("level %s new window used=%d reserved=%d, want 0/0", level, b.Used, b.Reserved)
		}
	}
	if err := svc.store.View(ctx, func(tx *Tx) error {
		usage, ok := tx.GetUsage(LevelKey, "key-1", testEpoch.Unix())
		if !ok {
			t.Fatal("old window usage record missing")
		}
		if usage.Used != 40 || usage.Reserved != 0 {
			t.Errorf("old window used=%d reserved=%d, want 40/0", usage.Used, usage.Reserved)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestReservationTerminalStateConflicts(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 10)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 重复确认返回首次结果。
	first, err := svc.ConfirmReservation(ctx, "rsv1")
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	dup, err := svc.ConfirmReservation(ctx, "rsv1")
	if err != nil {
		t.Fatalf("duplicate confirm: %v", err)
	}
	if !dup.Duplicate || dup.Consume.RequestID != first.Consume.RequestID {
		t.Fatalf("duplicate confirm should return first result, got %+v", dup)
	}
	// 已确认后再取消：状态冲突，且额度不能被退回。
	if _, err := svc.CancelReservation(ctx, "rsv1"); !errors.Is(err, ErrReservationState) {
		t.Fatalf("want ErrReservationState, got %v", err)
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.Used != 10 || b.Reserved != 0 {
			t.Errorf("level %s used=%d reserved=%d, want 10/0 after rejected cancel", level, b.Used, b.Reserved)
		}
	}

	// 另一个预占：先取消，再确认同样冲突。
	if _, err := svc.Reserve(ctx, reserveReq("rsv2", 10)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := svc.CancelReservation(ctx, "rsv2"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := svc.ConfirmReservation(ctx, "rsv2"); !errors.Is(err, ErrReservationState) {
		t.Fatalf("want ErrReservationState, got %v", err)
	}
	// 重复取消返回首次结果。
	cdup, err := svc.CancelReservation(ctx, "rsv2")
	if err != nil {
		t.Fatalf("duplicate cancel: %v", err)
	}
	if !cdup.Duplicate {
		t.Fatal("duplicate cancel should be marked duplicate")
	}
}

func TestReservationNotFound(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()
	for _, fn := range []func() error{
		func() error { _, err := svc.ConfirmReservation(ctx, "nope"); return err },
		func() error { _, err := svc.CancelReservation(ctx, "nope"); return err },
		func() error { _, err := svc.GetReservation(ctx, "nope"); return err },
	} {
		if err := fn(); !errors.Is(err, ErrReservationNotFound) {
			t.Fatalf("want ErrReservationNotFound, got %v", err)
		}
	}
}

func TestReserveIdempotency(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	first, err := svc.Reserve(ctx, reserveReq("rsv1", 40))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	dup, err := svc.Reserve(ctx, reserveReq("rsv1", 40))
	if err != nil {
		t.Fatalf("duplicate reserve: %v", err)
	}
	if !dup.Duplicate {
		t.Fatal("replay should be marked duplicate")
	}
	if dup.Reservation.Version != first.Reservation.Version {
		t.Errorf("replay version=%d, want %d", dup.Reservation.Version, first.Reservation.Version)
	}
	b := balanceOf(t, dup.Balances, LevelOrg)
	if b.Reserved != 40 {
		t.Errorf("org reserved=%d, want 40 (no double reservation)", b.Reserved)
	}
	// 同号异内容。
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 41)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("want ErrIdempotencyConflict, got %v", err)
	}
	req := reserveReq("rsv1", 40)
	req.OrgID = "org-other"
	if _, err := svc.Reserve(ctx, req); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("want ErrIdempotencyConflict for different org, got %v", err)
	}
}

func TestReservationLazyExpiry(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReqWithTTL("rsv1", 40, time.Minute)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	clk.Advance(61 * time.Second) // 跨过 TTL，同时跨过窗口边界

	// 查询触发惰性回收。
	rec, err := svc.GetReservation(ctx, "rsv1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.State != ReservationExpired {
		t.Fatalf("state=%s, want expired", rec.State)
	}
	// 到期后确认、取消都被拒绝。
	if _, err := svc.ConfirmReservation(ctx, "rsv1"); !errors.Is(err, ErrReservationState) {
		t.Fatalf("confirm expired: want ErrReservationState, got %v", err)
	}
	if _, err := svc.CancelReservation(ctx, "rsv1"); !errors.Is(err, ErrReservationState) {
		t.Fatalf("cancel expired: want ErrReservationState, got %v", err)
	}
	// 额度只退回旧窗口；新窗口保持完整。
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.Reserved != 0 || b.Used != 0 || b.Remaining != 100 {
			t.Errorf("level %s new window: %+v", level, b)
		}
	}
	if err := svc.store.View(ctx, func(tx *Tx) error {
		usage, ok := tx.GetUsage(LevelOrg, "org-1", testEpoch.Unix())
		if !ok || usage.Reserved != 0 {
			t.Errorf("old window reserved released incorrectly: ok=%v usage=%+v", ok, usage)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestExpireDueSweep(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	// rsv1、rsv2 一分钟后到期，rsv3 十分钟后到期。
	for _, id := range []string{"rsv1", "rsv2"} {
		if _, err := svc.Reserve(ctx, reserveReqWithTTL(id, 10, time.Minute)); err != nil {
			t.Fatalf("reserve %s: %v", id, err)
		}
	}
	if _, err := svc.Reserve(ctx, reserveReqWithTTL("rsv3", 10, 10*time.Minute)); err != nil {
		t.Fatalf("reserve rsv3: %v", err)
	}
	clk.Advance(61 * time.Second)

	out, err := svc.ExpireDue(ctx)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if len(out.Expired) != 2 {
		t.Fatalf("expired=%v, want 2 reservations", out.Expired)
	}
	// 再次扫描为空操作。
	out2, err := svc.ExpireDue(ctx)
	if err != nil {
		t.Fatalf("expire again: %v", err)
	}
	if len(out2.Expired) != 0 {
		t.Fatalf("second sweep expired=%v, want none", out2.Expired)
	}
	for _, id := range []string{"rsv1", "rsv2"} {
		rec, err := svc.GetReservation(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if rec.State != ReservationExpired {
			t.Errorf("%s state=%s, want expired", id, rec.State)
		}
	}
	rec3, err := svc.GetReservation(ctx, "rsv3")
	if err != nil {
		t.Fatalf("get rsv3: %v", err)
	}
	if rec3.State != ReservationReserved {
		t.Errorf("rsv3 state=%s, want still reserved", rec3.State)
	}
	// 旧窗口：两笔到期释放 reserved，未到期的 rsv3 仍保留 10。
	if err := svc.store.View(ctx, func(tx *Tx) error {
		usage, ok := tx.GetUsage(LevelOrg, "org-1", testEpoch.Unix())
		if !ok {
			t.Fatal("old window usage missing")
		}
		if usage.Reserved != 10 {
			t.Errorf("old window reserved=%d, want 10 (only rsv3)", usage.Reserved)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// runReservationRace 在预占上并发发起多组操作，收集真正完成状态迁移的赢家。
func runReservationRace(t *testing.T, svc *Service, requestID string, confirms, cancels, expires int) []string {
	t.Helper()
	ctx := context.Background()
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var winners []string

	launch := func(n int, fn func() (string, bool)) {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				winner, ok := fn()
				if ok {
					mu.Lock()
					winners = append(winners, winner)
					mu.Unlock()
				}
			}()
		}
	}
	launch(confirms, func() (string, bool) {
		res, err := svc.ConfirmReservation(ctx, requestID)
		switch {
		case err == nil && !res.Duplicate:
			return "confirmed", true
		case err == nil, errors.Is(err, ErrReservationState):
			return "", false
		default:
			t.Errorf("unexpected confirm error: %v", err)
			return "", false
		}
	})
	launch(cancels, func() (string, bool) {
		res, err := svc.CancelReservation(ctx, requestID)
		switch {
		case err == nil && !res.Duplicate:
			return "cancelled", true
		case err == nil, errors.Is(err, ErrReservationState):
			return "", false
		default:
			t.Errorf("unexpected cancel error: %v", err)
			return "", false
		}
	})
	launch(expires, func() (string, bool) {
		out, err := svc.ExpireDue(ctx)
		if err != nil {
			t.Errorf("unexpected expire error: %v", err)
		}
		for _, id := range out.Expired {
			if id == requestID {
				return "expired", true
			}
		}
		return "", false
	})
	close(start)
	wg.Wait()
	return winners
}

// assertSingleTerminal 验证预占只有一个终态，且原窗口 reserved 恰好释放一次。
func assertSingleTerminal(t *testing.T, svc *Service, requestID string, want string) {
	t.Helper()
	ctx := context.Background()
	rec, err := svc.GetReservation(ctx, requestID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !rec.State.Terminal() {
		t.Fatalf("final state=%s, want terminal", rec.State)
	}
	if string(rec.State) != want {
		t.Errorf("stored state=%s, winner=%s", rec.State, want)
	}
	if err := svc.store.View(ctx, func(tx *Tx) error {
		for _, level := range Levels {
			usage, ok := tx.GetUsage(level, rec.subjects()[level], rec.Windows[level])
			if !ok {
				t.Fatalf("level %s usage missing", level)
			}
			if usage.Reserved != 0 {
				t.Errorf("level %s reserved=%d, want 0 (released exactly once)", level, usage.Reserved)
			}
			wantUsed := int64(0)
			if rec.State == ReservationConfirmed {
				wantUsed = 10
			}
			if usage.Used != wantUsed {
				t.Errorf("level %s used=%d, want %d", level, usage.Used, wantUsed)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestReservationRaceConfirmVsCancel 未到期时并发确认与取消，只能形成一个终态。
func TestReservationRaceConfirmVsCancel(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReqWithTTL("rsv1", 10, 10*time.Minute)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	winners := runReservationRace(t, svc, "rsv1", 12, 12, 0)
	if len(winners) != 1 {
		t.Fatalf("terminal transitions=%v, want exactly 1", winners)
	}
	assertSingleTerminal(t, svc, "rsv1", winners[0])
}

// TestReservationRaceConfirmCancelExpire 已到期时并发确认、取消与回收，
// 最终只能是 expired 一个终态（可能由任一接口的惰性回收完成），额度恰好退回一次。
func TestReservationRaceConfirmCancelExpire(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReqWithTTL("rsv1", 10, time.Minute)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	clk.Advance(61 * time.Second) // 已到期且已跨过窗口边界
	_ = runReservationRace(t, svc, "rsv1", 8, 8, 8)
	assertSingleTerminal(t, svc, "rsv1", "expired")
}

// TestReserveConcurrentNoOvercommit 并发预占不能超额预占。
func TestReserveConcurrentNoOvercommit(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	const workers = 20
	const amount = 10 // 恰好允许 10 笔
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Reserve(ctx, reserveReq(fmt.Sprintf("rsv%d", i), amount))
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)

	okCount := 0
	for err := range errs {
		if err == nil {
			okCount++
		} else if !errors.Is(err, ErrInsufficientQuota) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if okCount != 10 {
		t.Fatalf("successful reserves=%d, want exactly 10", okCount)
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Reserved != 100 || b.Remaining != 0 {
			t.Errorf("level %s reserved=%d remaining=%d, want 100/0", level, b.Reserved, b.Remaining)
		}
	}
}

func TestReserveValidation(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 0)); !errors.Is(err, ErrValidation) {
		t.Fatalf("amount 0: want ErrValidation, got %v", err)
	}
	req := reserveReq("rsv2", 1)
	req.TTLSeconds = -1
	if _, err := svc.Reserve(ctx, req); !errors.Is(err, ErrValidation) {
		t.Fatalf("negative ttl: want ErrValidation, got %v", err)
	}
}
