package apiquota

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestService 建立 org1(1000) → user1(500) → key1(100) 的三层配额，
// 窗口均为 1 小时，退还限定时间为 24 小时。
func newTestService(t *testing.T) (*Service, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	svc := NewService(NewMemoryStore(), WithClock(clock.Now))
	ctx := context.Background()
	for _, cfg := range []QuotaConfig{
		{Level: LevelOrg, EntityID: "org1", Limit: 1000, Window: time.Hour},
		{Level: LevelUser, EntityID: "user1", ParentID: "org1", Limit: 500, Window: time.Hour},
		{Level: LevelKey, EntityID: "key1", ParentID: "user1", Limit: 100, Window: time.Hour},
	} {
		if _, err := svc.ConfigureQuota(ctx, cfg); err != nil {
			t.Fatalf("configure %s/%s: %v", cfg.Level, cfg.EntityID, err)
		}
	}
	return svc, clock
}

func mustConsume(t *testing.T, svc *Service, reqID string, amount int64) *Consumption {
	t.Helper()
	c, err := svc.Consume(context.Background(), ConsumeRequest{RequestID: reqID, KeyID: "key1", Amount: amount})
	if err != nil {
		t.Fatalf("consume %s: %v", reqID, err)
	}
	return c
}

func TestConsumeDeductsAllThreeLevels(t *testing.T) {
	svc, _ := newTestService(t)
	mustConsume(t, svc, "r1", 30)

	b, err := svc.Balances(context.Background(), "key1")
	if err != nil {
		t.Fatal(err)
	}
	for _, lvl := range []Balance{b.Org, b.User, b.Key} {
		if lvl.Used != 30 {
			t.Errorf("%s used = %d, want 30", lvl.Level, lvl.Used)
		}
	}
	if b.Key.Remaining != 70 || b.User.Remaining != 470 || b.Org.Remaining != 970 {
		t.Errorf("unexpected remainings: %+v", b)
	}
}

func TestConsumeFailsAtomicallyWhenAnyLevelInsufficient(t *testing.T) {
	cases := []struct {
		name  string
		limit struct{ org, user, key int64 }
		want  Level
	}{
		{"org", struct{ org, user, key int64 }{50, 500, 100}, LevelOrg},
		{"user", struct{ org, user, key int64 }{1000, 50, 100}, LevelUser},
		{"key", struct{ org, user, key int64 }{1000, 500, 50}, LevelKey},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			svc := NewService(NewMemoryStore(), WithClock(clock.Now))
			ctx := context.Background()
			for _, cfg := range []QuotaConfig{
				{Level: LevelOrg, EntityID: "org1", Limit: tc.limit.org, Window: time.Hour},
				{Level: LevelUser, EntityID: "user1", ParentID: "org1", Limit: tc.limit.user, Window: time.Hour},
				{Level: LevelKey, EntityID: "key1", ParentID: "user1", Limit: tc.limit.key, Window: time.Hour},
			} {
				if _, err := svc.ConfigureQuota(ctx, cfg); err != nil {
					t.Fatal(err)
				}
			}
			_, err := svc.Consume(ctx, ConsumeRequest{RequestID: "r1", KeyID: "key1", Amount: 60})
			var e *Error
			if !errors.As(err, &e) || e.Code != CodeQuotaExhausted {
				t.Fatalf("err = %v, want QUOTA_EXHAUSTED", err)
			}
			if e.Level != tc.want {
				t.Errorf("level = %s, want %s", e.Level, tc.want)
			}
			// 任何一层都不应被扣减
			b, err := svc.Balances(ctx, "key1")
			if err != nil {
				t.Fatal(err)
			}
			if b.Org.Used != 0 || b.User.Used != 0 || b.Key.Used != 0 {
				t.Errorf("partial deduction happened: %+v", b)
			}
		})
	}
}

func TestConsumeIdempotency(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	first := mustConsume(t, svc, "r1", 30)

	// 同号同内容：返回首次结果，不重复扣减
	second, err := svc.Consume(ctx, ConsumeRequest{RequestID: "r1", KeyID: "key1", Amount: 30})
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Errorf("replayed consumption ID = %s, want %s", second.ID, first.ID)
	}
	b, _ := svc.Balances(ctx, "key1")
	if b.Key.Used != 30 {
		t.Errorf("key used = %d, want 30 (no double deduction)", b.Key.Used)
	}

	// 同号异内容：幂等冲突
	_, err = svc.Consume(ctx, ConsumeRequest{RequestID: "r1", KeyID: "key1", Amount: 31})
	if CodeOf(err) != CodeIdempotencyConflict {
		t.Errorf("err = %v, want IDEMPOTENCY_CONFLICT", err)
	}
}

func TestConcurrentConsumeNeverExceeds(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	const goroutines = 50 // 每个请求扣 10，key 限额 100 → 恰好 10 个成功
	var wg sync.WaitGroup
	var okCount, exhaustedCount, otherCount int64
	var mu sync.Mutex
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Consume(ctx, ConsumeRequest{
				RequestID: fmt.Sprintf("r%d", i),
				KeyID:     "key1",
				Amount:    10,
			})
			mu.Lock()
			defer mu.Unlock()
			switch CodeOf(err) {
			case "":
				okCount++
			case CodeQuotaExhausted:
				exhaustedCount++
			default:
				otherCount++
			}
		}(i)
	}
	wg.Wait()

	if otherCount != 0 {
		t.Fatalf("unexpected errors: %d", otherCount)
	}
	if okCount != 10 || exhaustedCount != 40 {
		t.Fatalf("ok=%d exhausted=%d, want 10/40", okCount, exhaustedCount)
	}
	b, _ := svc.Balances(ctx, "key1")
	if b.Key.Used != 100 {
		t.Errorf("key used = %d, want exactly 100 (no oversell)", b.Key.Used)
	}
}

func TestRefundPartialMultipleAndCap(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	c := mustConsume(t, svc, "c1", 100)

	r1, err := svc.Refund(ctx, RefundRequest{RequestID: "rf1", ConsumptionID: c.ID, Amount: 30})
	if err != nil {
		t.Fatal(err)
	}
	if r1.TotalRefunded != 30 || r1.RemainingRefundable != 70 {
		t.Errorf("after first refund: %+v", r1)
	}
	r2, err := svc.Refund(ctx, RefundRequest{RequestID: "rf2", ConsumptionID: c.ID, Amount: 70})
	if err != nil {
		t.Fatal(err)
	}
	if r2.TotalRefunded != 100 || r2.RemainingRefundable != 0 {
		t.Errorf("after second refund: %+v", r2)
	}
	b, _ := svc.Balances(ctx, "key1")
	if b.Key.Used != 0 || b.User.Used != 0 || b.Org.Used != 0 {
		t.Errorf("balances not fully restored: %+v", b)
	}

	// 累计退还超过原消费：状态冲突
	_, err = svc.Refund(ctx, RefundRequest{RequestID: "rf3", ConsumptionID: c.ID, Amount: 1})
	if CodeOf(err) != CodeStateConflict {
		t.Errorf("err = %v, want STATE_CONFLICT", err)
	}
}

func TestRefundIdempotency(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	c := mustConsume(t, svc, "c1", 50)

	first, err := svc.Refund(ctx, RefundRequest{RequestID: "rf1", ConsumptionID: c.ID, Amount: 20})
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Refund(ctx, RefundRequest{RequestID: "rf1", ConsumptionID: c.ID, Amount: 20})
	if err != nil {
		t.Fatal(err)
	}
	if *first != *second {
		t.Errorf("replayed refund differs: %+v vs %+v", first, second)
	}
	b, _ := svc.Balances(ctx, "key1")
	if b.Key.Used != 30 {
		t.Errorf("key used = %d, want 30 (refund applied once)", b.Key.Used)
	}

	_, err = svc.Refund(ctx, RefundRequest{RequestID: "rf1", ConsumptionID: c.ID, Amount: 21})
	if CodeOf(err) != CodeIdempotencyConflict {
		t.Errorf("err = %v, want IDEMPOTENCY_CONFLICT", err)
	}
}

func TestRefundWindowExpired(t *testing.T) {
	clock := newFakeClock()
	svc := NewService(NewMemoryStore(), WithClock(clock.Now), WithRefundTTL(time.Hour))
	ctx := context.Background()
	for _, cfg := range []QuotaConfig{
		{Level: LevelOrg, EntityID: "org1", Limit: 1000, Window: 24 * time.Hour},
		{Level: LevelUser, EntityID: "user1", ParentID: "org1", Limit: 500, Window: 24 * time.Hour},
		{Level: LevelKey, EntityID: "key1", ParentID: "user1", Limit: 100, Window: 24 * time.Hour},
	} {
		if _, err := svc.ConfigureQuota(ctx, cfg); err != nil {
			t.Fatal(err)
		}
	}
	c := mustConsume(t, svc, "c1", 50)
	clock.Advance(time.Hour + time.Second)

	_, err := svc.Refund(ctx, RefundRequest{RequestID: "rf1", ConsumptionID: c.ID, Amount: 10})
	if CodeOf(err) != CodeWindowExpired {
		t.Errorf("err = %v, want WINDOW_EXPIRED", err)
	}
}

func TestRefundReturnsToOriginalWindow(t *testing.T) {
	svc, clock := newTestService(t)
	ctx := context.Background()

	c := mustConsume(t, svc, "c1", 40)
	origWindow := c.Windows[LevelKey]

	// 窗口切换后再退还（退还限定时间 24h，窗口 1h）
	clock.Advance(2 * time.Hour)

	if _, err := svc.Refund(ctx, RefundRequest{RequestID: "rf1", ConsumptionID: c.ID, Amount: 15}); err != nil {
		t.Fatal(err)
	}

	// 新窗口的余额不受退还影响
	b, _ := svc.Balances(ctx, "key1")
	if b.Key.Used != 0 || b.Key.Remaining != 100 {
		t.Errorf("new window balance = %+v, want untouched", b.Key)
	}
	if b.Key.WindowStart.Equal(origWindow) {
		t.Fatalf("expected window to have rolled over")
	}

	// 原窗口的用量记录被回退
	var oldUsed int64
	err := svc.store.Transact(func(tx *Tx) error {
		u, ok := tx.GetUsage(LevelKey, "key1", origWindow.Unix())
		if !ok {
			return fmt.Errorf("original window usage record missing")
		}
		oldUsed = u.Used
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if oldUsed != 25 {
		t.Errorf("original window used = %d, want 25 (40-15)", oldUsed)
	}
}

func TestWindowBoundariesAreLeftClosedRightOpen(t *testing.T) {
	svc, clock := newTestService(t)
	ctx := context.Background()

	mustConsume(t, svc, "c1", 100) // 打满当前窗口
	b1, _ := svc.Balances(ctx, "key1")
	if b1.Key.Remaining != 0 {
		t.Fatalf("remaining = %d, want 0", b1.Key.Remaining)
	}

	// 恰好推进到窗口右边界：属于新窗口
	clock.Advance(b1.Key.WindowEnd.Sub(clock.Now()))
	mustConsume(t, svc, "c2", 100)
	b2, _ := svc.Balances(ctx, "key1")
	if !b2.Key.WindowStart.Equal(b1.Key.WindowEnd) {
		t.Errorf("new window start = %s, want %s", b2.Key.WindowStart, b1.Key.WindowEnd)
	}
}

func TestVersionConditioning(t *testing.T) {
	store := NewMemoryStore()
	ws := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)

	// 创建记录，版本变为 1
	err := store.Transact(func(tx *Tx) error {
		return tx.PutUsage(UsageRecord{Level: LevelKey, EntityID: "k", WindowStart: ws, Used: 1}, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	// 携带过期版本写入：版本冲突
	err = store.Transact(func(tx *Tx) error {
		return tx.PutUsage(UsageRecord{Level: LevelKey, EntityID: "k", WindowStart: ws, Used: 2}, 0)
	})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
	// 携带当前版本写入：成功，版本递增
	err = store.Transact(func(tx *Tx) error {
		u, _ := tx.GetUsage(LevelKey, "k", ws.Unix())
		if u.Version != 1 {
			return fmt.Errorf("version = %d, want 1", u.Version)
		}
		return tx.PutUsage(UsageRecord{Level: LevelKey, EntityID: "k", WindowStart: ws, Used: 2}, u.Version)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPersistenceAcrossReload(t *testing.T) {
	path := t.TempDir() + "/store.json"
	clock := newFakeClock()
	ctx := context.Background()

	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(store, WithClock(clock.Now))
	for _, cfg := range []QuotaConfig{
		{Level: LevelOrg, EntityID: "org1", Limit: 1000, Window: time.Hour},
		{Level: LevelUser, EntityID: "user1", ParentID: "org1", Limit: 500, Window: time.Hour},
		{Level: LevelKey, EntityID: "key1", ParentID: "user1", Limit: 100, Window: time.Hour},
	} {
		if _, err := svc.ConfigureQuota(ctx, cfg); err != nil {
			t.Fatal(err)
		}
	}
	c := mustConsume(t, svc, "c1", 40)

	// 重新打开存储，状态应当完整恢复
	store2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	svc2 := NewService(store2, WithClock(clock.Now))
	b, err := svc2.Balances(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if b.Key.Used != 40 || b.User.Used != 40 || b.Org.Used != 40 {
		t.Errorf("restored balances = %+v, want used=40 on all levels", b)
	}
	// 恢复后退还仍然可用
	if _, err := svc2.Refund(ctx, RefundRequest{RequestID: "rf1", ConsumptionID: c.ID, Amount: 40}); err != nil {
		t.Fatal(err)
	}
	// 幂等记录在重启后依然生效：同号重放不重复扣减
	replayed, err := svc2.Consume(ctx, ConsumeRequest{RequestID: "c1", KeyID: "key1", Amount: 40})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ID != c.ID {
		t.Errorf("replayed ID = %s, want %s", replayed.ID, c.ID)
	}
}

func TestConsumeUnconfiguredKey(t *testing.T) {
	svc, _ := newTestService(t)
	_, err := svc.Consume(context.Background(), ConsumeRequest{RequestID: "r1", KeyID: "ghost", Amount: 1})
	if CodeOf(err) != CodeNotFound {
		t.Errorf("err = %v, want NOT_FOUND", err)
	}
}
