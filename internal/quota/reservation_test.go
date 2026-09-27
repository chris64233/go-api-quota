package quota

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func reserveReq(id string, amount int64, ttlSeconds int64) ReserveRequest {
	return ReserveRequest{
		RequestID:  id,
		OrgID:      "org-1",
		UserID:     "user-1",
		KeyID:      "key-1",
		Amount:     amount,
		TTLSeconds: ttlSeconds,
	}
}

func decisionReq(id, reservationID string) ReservationDecisionRequest {
	return ReservationDecisionRequest{RequestID: id, ReservationID: reservationID}
}

func TestReserveLocksAllThreeLevels(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	res, err := svc.Reserve(ctx, reserveReq("rsv1", 30, 120))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res.Duplicate {
		t.Fatal("first reserve should not be marked duplicate")
	}
	if res.Reservation.Status != StatusReserved {
		t.Fatalf("status=%s, want reserved", res.Reservation.Status)
	}
	// 记录原窗口、数量与过期时间。
	wantExpiry := clk.Now().Add(120 * time.Second)
	if !res.Reservation.ExpiresAt.Equal(wantExpiry) {
		t.Errorf("expires at %v, want %v", res.Reservation.ExpiresAt, wantExpiry)
	}
	for _, level := range Levels {
		if res.Reservation.Windows[level] != testEpoch.Unix() {
			t.Errorf("level %s window=%d, want %d", level, res.Reservation.Windows[level], testEpoch.Unix())
		}
	}
	// 三层同时锁定：used 不动，reserved 增加，可用额相应减少。
	for _, level := range Levels {
		b := balanceOf(t, res.Balances, level)
		if b.Used != 0 || b.Reserved != 30 || b.Remaining != 70 {
			t.Errorf("level %s used=%d reserved=%d remaining=%d, want 0/30/70",
				level, b.Used, b.Reserved, b.Remaining)
		}
	}
}

func TestReserveInsufficientIsAtomic(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 先预占 90，只剩 10 可用。
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 90, 120)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 任一层不足（三层配置相同，均不足）整笔失败，任何一层都不能被锁定。
	_, err := svc.Reserve(ctx, reserveReq("rsv2", 20, 120))
	if !errors.Is(err, ErrInsufficientQuota) {
		t.Fatalf("want ErrInsufficientQuota, got %v", err)
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Reserved != 90 || b.Remaining != 10 {
			t.Errorf("level %s reserved=%d remaining=%d, want 90/10 (no partial lock)",
				level, b.Reserved, b.Remaining)
		}
	}
}

func TestReserveOneLevelShortFailsWhole(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()
	// 把用户层配额调到 5。
	if _, err := svc.SetConfig(ctx, LevelUser, "user-1", 5, 60, 1); err != nil {
		t.Fatalf("set config: %v", err)
	}
	_, err := svc.Reserve(ctx, reserveReq("rsv1", 10, 120))
	if !errors.Is(err, ErrInsufficientQuota) {
		t.Fatalf("want ErrInsufficientQuota, got %v", err)
	}
	// 三层都不能被锁定。
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.Reserved != 0 {
			t.Errorf("level %s reserved=%d, want 0", level, b.Reserved)
		}
	}
}

func TestReserveConsumesShareAvailablePool(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 60, 120)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 直接消费也要与预占共享额度池：60 预占 + 50 消费 > 100。
	_, err := svc.Consume(ctx, consumeReq("c1", 50))
	if !errors.Is(err, ErrInsufficientQuota) {
		t.Fatalf("want ErrInsufficientQuota, got %v", err)
	}
	// 40 恰好放得下。
	if _, err := svc.Consume(ctx, consumeReq("c1", 40)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	balances, _ := svc.Balances(ctx, "org-1", "user-1", "key-1")
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Used != 40 || b.Reserved != 60 || b.Remaining != 0 {
			t.Errorf("level %s used=%d reserved=%d remaining=%d, want 40/60/0",
				level, b.Used, b.Reserved, b.Remaining)
		}
	}
}

func TestReserveIdempotency(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	first, err := svc.Reserve(ctx, reserveReq("rsv1", 40, 120))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 同号同内容：返回首次结果，不重复锁定。
	dup, err := svc.Reserve(ctx, reserveReq("rsv1", 40, 120))
	if err != nil {
		t.Fatalf("duplicate reserve: %v", err)
	}
	if !dup.Duplicate {
		t.Fatal("replay should be marked duplicate")
	}
	if dup.Reservation.Version != first.Reservation.Version {
		t.Errorf("replay version=%d, want first version %d", dup.Reservation.Version, first.Reservation.Version)
	}
	for _, level := range Levels {
		b := balanceOf(t, dup.Balances, level)
		if b.Reserved != 40 {
			t.Errorf("level %s reserved=%d, want 40 (no double lock)", level, b.Reserved)
		}
	}
	// 同号不同数量：冲突。
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 41, 120)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different amount: want ErrIdempotencyConflict, got %v", err)
	}
	// 同号不同过期时间：也算不同内容，冲突。
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 40, 121)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different ttl: want ErrIdempotencyConflict, got %v", err)
	}
	// 同号不同主体：冲突。
	req := reserveReq("rsv1", 40, 120)
	req.OrgID = "org-2"
	if _, err := svc.Reserve(ctx, req); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different org: want ErrIdempotencyConflict, got %v", err)
	}
}

func TestConfirmReservation(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 40, 120)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	res, err := svc.ConfirmReservation(ctx, decisionReq("d1", "rsv1"))
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if res.Reservation.Status != StatusConfirmed || !res.Reservation.Status.Terminal() {
		t.Fatalf("status=%s, want confirmed terminal", res.Reservation.Status)
	}
	if res.Consume == nil || res.Consume.Amount != 40 {
		t.Fatalf("confirmed consume=%v, want amount 40", res.Consume)
	}
	for _, level := range Levels {
		b := balanceOf(t, res.Balances, level)
		if b.Used != 40 || b.Reserved != 0 || b.Remaining != 60 {
			t.Errorf("level %s used=%d reserved=%d remaining=%d, want 40/0/60",
				level, b.Used, b.Reserved, b.Remaining)
		}
	}
	// 确认生成的正式消费可走既有退还流程。
	refund, err := svc.Refund(ctx, RefundRequest{
		RequestID: "r1", ConsumeRequestID: res.Consume.RequestID, Amount: 40,
	})
	if err != nil {
		t.Fatalf("refund confirmed consume: %v", err)
	}
	if refund.Consume.Refundable() != 0 {
		t.Errorf("refundable=%d, want 0", refund.Consume.Refundable())
	}
}

func TestCancelReservation(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 40, 120)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	res, err := svc.CancelReservation(ctx, decisionReq("d1", "rsv1"))
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if res.Reservation.Status != StatusCancelled || !res.Reservation.Status.Terminal() {
		t.Fatalf("status=%s, want cancelled terminal", res.Reservation.Status)
	}
	if res.Consume != nil {
		t.Fatal("cancel must not produce a consume record")
	}
	for _, level := range Levels {
		b := balanceOf(t, res.Balances, level)
		if b.Used != 0 || b.Reserved != 0 || b.Remaining != 100 {
			t.Errorf("level %s used=%d reserved=%d remaining=%d, want 0/0/100",
				level, b.Used, b.Reserved, b.Remaining)
		}
	}
}

func TestDecisionIdempotencyAndReuseConflict(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 10, 120)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	first, err := svc.ConfirmReservation(ctx, decisionReq("d1", "rsv1"))
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	// 同号同操作同预占：返回首次结果。
	dup, err := svc.ConfirmReservation(ctx, decisionReq("d1", "rsv1"))
	if err != nil {
		t.Fatalf("duplicate confirm: %v", err)
	}
	if !dup.Duplicate || dup.Reservation.Version != first.Reservation.Version {
		t.Fatalf("dup=%+v, want duplicate with first version", dup)
	}
	// 同一请求号改用于取消：冲突。
	if _, err := svc.CancelReservation(ctx, decisionReq("d1", "rsv1")); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("reuse as cancel: want ErrIdempotencyConflict, got %v", err)
	}
	// 同一请求号指向别的预占：冲突。
	if _, err := svc.Reserve(ctx, reserveReq("rsv2", 10, 120)); err != nil {
		t.Fatalf("reserve rsv2: %v", err)
	}
	if _, err := svc.ConfirmReservation(ctx, decisionReq("d1", "rsv2")); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("reuse for other reservation: want ErrIdempotencyConflict, got %v", err)
	}
}

func TestDecisionAfterTerminalStateConflict(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 10, 120)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := svc.CancelReservation(ctx, decisionReq("d1", "rsv1")); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// 已取消的预占不能再确认。
	_, err := svc.ConfirmReservation(ctx, decisionReq("d2", "rsv1"))
	if !errors.Is(err, ErrReservationStateConflict) {
		t.Fatalf("confirm after cancel: want ErrReservationStateConflict, got %v", err)
	}

	if _, err := svc.Reserve(ctx, reserveReq("rsv2", 10, 120)); err != nil {
		t.Fatalf("reserve rsv2: %v", err)
	}
	if _, err := svc.ConfirmReservation(ctx, decisionReq("d3", "rsv2")); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	// 已确认的预占不能再取消。
	_, err = svc.CancelReservation(ctx, decisionReq("d4", "rsv2"))
	if !errors.Is(err, ErrReservationStateConflict) {
		t.Fatalf("cancel after confirm: want ErrReservationStateConflict, got %v", err)
	}
	// 未知预占。
	_, err = svc.ConfirmReservation(ctx, decisionReq("d5", "nope"))
	if !errors.Is(err, ErrReservationNotFound) {
		t.Fatalf("unknown: want ErrReservationNotFound, got %v", err)
	}
}

func TestReservationExpiryReleasesAndTerminal(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 40, 60)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	clk.Advance(60 * time.Second) // now == ExpiresAt，左闭右开，算到期
	n, err := svc.ExpireDue(ctx)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if n != 1 {
		t.Fatalf("expired count=%d, want 1", n)
	}
	rec, err := svc.GetReservation(ctx, "rsv1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.Status != StatusExpired || !rec.Status.Terminal() {
		t.Fatalf("status=%s, want expired terminal", rec.Status)
	}
	balances, _ := svc.Balances(ctx, "org-1", "user-1", "key-1")
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Used != 0 || b.Reserved != 0 || b.Remaining != 100 {
			t.Errorf("level %s used=%d reserved=%d remaining=%d, want 0/0/100",
				level, b.Used, b.Reserved, b.Remaining)
		}
	}
	// 再次回收是幂等的，无记录可回收。
	if n, err := svc.ExpireDue(ctx); err != nil || n != 0 {
		t.Fatalf("second expire n=%d err=%v, want 0/nil", n, err)
	}
	// 已到期的预占不能确认或取消。
	if _, err := svc.ConfirmReservation(ctx, decisionReq("d1", "rsv1")); !errors.Is(err, ErrReservationStateConflict) {
		t.Fatalf("confirm after expiry: want ErrReservationStateConflict, got %v", err)
	}
	if _, err := svc.CancelReservation(ctx, decisionReq("d2", "rsv1")); !errors.Is(err, ErrReservationStateConflict) {
		t.Fatalf("cancel after expiry: want ErrReservationStateConflict, got %v", err)
	}
}

func TestExpiryLazyReclaimOnReserveAndConsume(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	// 预占 100 把额度占满。
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 100, 60)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	clk.Advance(61 * time.Second)
	// 未显式回收：直接预占应惰性回收旧预占后成功。
	res, err := svc.Reserve(ctx, reserveReq("rsv2", 30, 120))
	if err != nil {
		t.Fatalf("reserve after expiry: %v", err)
	}
	if b := balanceOf(t, res.Balances, LevelOrg); b.Reserved != 30 || b.Remaining != 70 {
		t.Errorf("after lazy sweep reserved=%d remaining=%d, want 30/70", b.Reserved, b.Remaining)
	}
	// 查询旧预占应显示已过期。
	rec, err := svc.GetReservation(ctx, "rsv1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.Status != StatusExpired {
		t.Fatalf("status=%s, want expired", rec.Status)
	}
}

func TestExpiredReservationDoesNotOccupyReadOnlyBalances(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 40, 60)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	clk.Advance(61 * time.Second)
	// 不触发任何写操作，只读余额视图就应把到期预占视为已释放。
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Reserved != 0 || b.Remaining != 100 {
			t.Errorf("level %s reserved=%d remaining=%d, want 0/100 after expiry",
				level, b.Reserved, b.Remaining)
		}
	}
}

func TestConfirmCreditsOriginalWindowOnly(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 60, 120)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 跨过窗口边界后确认：消费只能落在预占的原窗口，新窗口保持干净。
	clk.Advance(61 * time.Second)
	res, err := svc.ConfirmReservation(ctx, decisionReq("d1", "rsv1"))
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, res.Balances, level)
		if b.Used != 0 || b.Reserved != 0 || b.Remaining != 100 {
			t.Errorf("level %s new window used=%d reserved=%d remaining=%d, want 0/0/100",
				level, b.Used, b.Reserved, b.Remaining)
		}
	}
	// 原窗口用量应为 60。
	err = svc.store.View(ctx, func(tx *Tx) error {
		for _, level := range Levels {
			usage, ok := tx.GetUsage(level, string(level)+"-1", testEpoch.Unix())
			if !ok {
				t.Fatalf("old window usage missing for level %s", level)
			}
			if usage.Used != 60 || usage.Reserved != 0 {
				t.Errorf("level %s old window used=%d reserved=%d, want 60/0",
					level, usage.Used, usage.Reserved)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestCancelReleasesOriginalWindowOnly(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 60, 300)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	clk.Advance(61 * time.Second) // 窗口切换，但预占尚未到期
	if _, err := svc.CancelReservation(ctx, decisionReq("d1", "rsv1")); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// 旧窗口的预占被释放，新窗口从未被触碰。
	err := svc.store.View(ctx, func(tx *Tx) error {
		for _, level := range Levels {
			oldUsage, ok := tx.GetUsage(level, string(level)+"-1", testEpoch.Unix())
			if !ok {
				t.Fatalf("old window usage missing for level %s", level)
			}
			if oldUsage.Reserved != 0 {
				t.Errorf("level %s old window reserved=%d, want 0", level, oldUsage.Reserved)
			}
			if _, ok := tx.GetUsage(level, string(level)+"-1", testEpoch.Unix()+60); ok {
				t.Errorf("level %s new window should never have been created", level)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestDecisionConcurrentSingleTerminal 并发确认、取消与过期回收，
// 预占只能进入一个终态，额度只能转换/释放一次。
func TestDecisionConcurrentSingleTerminal(t *testing.T) {
	for _, expireFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("expireFirst=%v", expireFirst), func(t *testing.T) {
			svc, clk := setup(t)
			ctx := context.Background()
			if _, err := svc.Reserve(ctx, reserveReq("rsv1", 40, 60)); err != nil {
				t.Fatalf("reserve: %v", err)
			}
			if expireFirst {
				clk.Advance(61 * time.Second)
			}

			const workers = 24
			var wg sync.WaitGroup
			type outcome struct {
				err     error
				changed bool // 是否真正产生了终态转换
			}
			outcomes := make(chan outcome, workers)
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					switch {
					case i%3 == 0:
						_, err := svc.ConfirmReservation(ctx, decisionReq(fmt.Sprintf("d-c-%d", i), "rsv1"))
						outcomes <- outcome{err: err, changed: err == nil}
					case i%3 == 1:
						_, err := svc.CancelReservation(ctx, decisionReq(fmt.Sprintf("d-x-%d", i), "rsv1"))
						outcomes <- outcome{err: err, changed: err == nil}
					default:
						n, err := svc.ExpireDue(ctx)
						outcomes <- outcome{err: err, changed: n == 1}
					}
				}(i)
			}
			wg.Wait()
			close(outcomes)

			successes := 0
			for o := range outcomes {
				err := o.err
				switch {
				case err == nil:
					if o.changed {
						successes++
					}
				case errors.Is(err, ErrReservationStateConflict),
					errors.Is(err, ErrIdempotencyConflict):
					// 失败的竞争操作。
				default:
					t.Fatalf("unexpected error: %v", err)
				}
			}
			if successes != 1 {
				t.Fatalf("successful terminal transitions=%d, want exactly 1", successes)
			}

			rec, err := svc.GetReservation(ctx, "rsv1")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if !rec.Status.Terminal() {
				t.Fatalf("status=%s, want terminal", rec.Status)
			}
			// 无论谁胜出，旧窗口三层账目必须自洽且只转换一次。
			err = svc.store.View(ctx, func(tx *Tx) error {
				for _, level := range Levels {
					usage, ok := tx.GetUsage(level, string(level)+"-1", testEpoch.Unix())
					if !ok {
						t.Fatalf("usage missing for level %s", level)
					}
					switch rec.Status {
					case StatusConfirmed:
						if usage.Used != 40 || usage.Reserved != 0 {
							t.Errorf("confirmed: level %s used=%d reserved=%d, want 40/0",
								level, usage.Used, usage.Reserved)
						}
					case StatusCancelled, StatusExpired:
						if usage.Used != 0 || usage.Reserved != 0 {
							t.Errorf("%s: level %s used=%d reserved=%d, want 0/0",
								rec.Status, level, usage.Used, usage.Reserved)
						}
					}
				}
				return nil
			})
			if err != nil {
				t.Fatalf("view: %v", err)
			}
		})
	}
}

func TestGetReservationUnknown(t *testing.T) {
	svc, _ := setup(t)
	_, err := svc.GetReservation(context.Background(), "nope")
	if !errors.Is(err, ErrReservationNotFound) {
		t.Fatalf("want ErrReservationNotFound, got %v", err)
	}
}

func TestReservationPersistedAcrossRestart(t *testing.T) {
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
	if _, err := svc.Reserve(ctx, reserveReq("rsv1", 30, 600)); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	reopened, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	svc2 := NewService(reopened, WithClock(clk.Now))
	// 预占记录与三层锁定的额度都要恢复：重放幂等且不重复锁定。
	res, err := svc2.Reserve(ctx, reserveReq("rsv1", 30, 600))
	if err != nil {
		t.Fatalf("replay reserve: %v", err)
	}
	if !res.Duplicate {
		t.Fatal("reserve replay should be a duplicate after reload")
	}
	if b := balanceOf(t, res.Balances, LevelKey); b.Reserved != 30 || b.Remaining != 70 {
		t.Errorf("after reload key reserved=%d remaining=%d, want 30/70", b.Reserved, b.Remaining)
	}
	// 恢复后仍可确认。
	dec, err := svc2.ConfirmReservation(ctx, decisionReq("d1", "rsv1"))
	if err != nil {
		t.Fatalf("confirm after reload: %v", err)
	}
	if dec.Reservation.Status != StatusConfirmed {
		t.Fatalf("status=%s, want confirmed", dec.Reservation.Status)
	}
}
