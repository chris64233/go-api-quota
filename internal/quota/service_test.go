package quota

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// clock 是可手动推进的时钟，用于测试窗口切换与退还时限。
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock(t time.Time) *clock { return &clock{now: t} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

var testEpoch = time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)

// setup 创建配置了相同三层配额（limit=100，窗口 60s）的服务与可控时钟。
func setup(t *testing.T) (*Service, *clock) {
	t.Helper()
	clk := newClock(testEpoch)
	svc := NewService(NewMemoryStore(), WithClock(clk.Now), WithRefundTTL(time.Hour))
	ctx := context.Background()
	for _, level := range Levels {
		if _, err := svc.SetConfig(ctx, level, string(level)+"-1", 100, 60, 0); err != nil {
			t.Fatalf("set config %s: %v", level, err)
		}
	}
	return svc, clk
}

func consumeReq(id string, amount int64) ConsumeRequest {
	return ConsumeRequest{
		RequestID: id,
		OrgID:     "org-1",
		UserID:    "user-1",
		KeyID:     "key-1",
		Amount:    amount,
	}
}

func balanceOf(t *testing.T, balances []LevelBalance, level Level) LevelBalance {
	t.Helper()
	for _, b := range balances {
		if b.Level == level {
			return b
		}
	}
	t.Fatalf("balance for level %s not found", level)
	return LevelBalance{}
}

func TestConsumeDeductsAllThreeLevels(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	res, err := svc.Consume(ctx, consumeReq("c1", 30))
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if res.Duplicate {
		t.Fatal("first consume should not be marked duplicate")
	}
	for _, level := range Levels {
		b := balanceOf(t, res.Balances, level)
		if b.Used != 30 || b.Remaining != 70 {
			t.Errorf("level %s: used=%d remaining=%d, want 30/70", level, b.Used, b.Remaining)
		}
	}
}

func TestConsumeInsufficientIsAtomic(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 先把密钥层额度用到只剩 10。
	if _, err := svc.Consume(ctx, consumeReq("c1", 90)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	// 再消费 20：密钥层不足，整体必须失败，且任何一层都不能被扣。
	_, err := svc.Consume(ctx, consumeReq("c2", 20))
	if !errors.Is(err, ErrInsufficientQuota) {
		t.Fatalf("want ErrInsufficientQuota, got %v", err)
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Used != 90 {
			t.Errorf("level %s used=%d, want 90 (no partial deduction)", level, b.Used)
		}
	}
}

func TestConsumeConcurrentNoOverspend(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	const workers = 20
	const amount = 10 // 总额度 100，恰好允许 10 笔成功
	var wg sync.WaitGroup
	successes := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Consume(ctx, consumeReq(fmt.Sprintf("c%d", i), amount))
			successes <- err
		}(i)
	}
	wg.Wait()
	close(successes)

	okCount := 0
	for err := range successes {
		if err == nil {
			okCount++
		} else if !errors.Is(err, ErrInsufficientQuota) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if okCount != 10 {
		t.Fatalf("successful consumes=%d, want exactly 10", okCount)
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		b := balanceOf(t, balances, level)
		if b.Used != 100 || b.Remaining != 0 {
			t.Errorf("level %s used=%d remaining=%d, want 100/0", level, b.Used, b.Remaining)
		}
	}
}

func TestConsumeIdempotency(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	first, err := svc.Consume(ctx, consumeReq("c1", 40))
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	// 同号同内容：返回首次结果，不重复扣减。
	dup, err := svc.Consume(ctx, consumeReq("c1", 40))
	if err != nil {
		t.Fatalf("duplicate consume: %v", err)
	}
	if !dup.Duplicate {
		t.Fatal("replay should be marked duplicate")
	}
	if dup.Record.Version != first.Record.Version {
		t.Errorf("replay version=%d, want first version %d", dup.Record.Version, first.Record.Version)
	}
	b := balanceOf(t, dup.Balances, LevelOrg)
	if b.Used != 40 {
		t.Errorf("org used=%d, want 40 (no double deduction)", b.Used)
	}
	// 同号异内容：幂等冲突。
	req := consumeReq("c1", 41)
	if _, err := svc.Consume(ctx, req); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("want ErrIdempotencyConflict, got %v", err)
	}
}

func TestRefundPartialMultipleAndCap(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Consume(ctx, consumeReq("c1", 50)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	// 分两次退还 20 + 30，累计等于原消费。
	for i, amount := range []int64{20, 30} {
		res, err := svc.Refund(ctx, RefundRequest{
			RequestID:        fmt.Sprintf("r%d", i),
			ConsumeRequestID: "c1",
			Amount:           amount,
		})
		if err != nil {
			t.Fatalf("refund %d: %v", i, err)
		}
		if res.Consume.Refunded != 20+int64(i)*30 {
			t.Errorf("refunded=%d after refund %d", res.Consume.Refunded, i)
		}
	}
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.Used != 0 {
			t.Errorf("level %s used=%d, want 0 after full refund", level, b.Used)
		}
	}
	// 再退就超过原消费。
	_, err = svc.Refund(ctx, RefundRequest{RequestID: "r2", ConsumeRequestID: "c1", Amount: 1})
	if !errors.Is(err, ErrRefundExceeds) {
		t.Fatalf("want ErrRefundExceeds, got %v", err)
	}
}

func TestRefundIdempotency(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	if _, err := svc.Consume(ctx, consumeReq("c1", 50)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	first, err := svc.Refund(ctx, RefundRequest{RequestID: "r1", ConsumeRequestID: "c1", Amount: 20})
	if err != nil {
		t.Fatalf("refund: %v", err)
	}
	dup, err := svc.Refund(ctx, RefundRequest{RequestID: "r1", ConsumeRequestID: "c1", Amount: 20})
	if err != nil {
		t.Fatalf("duplicate refund: %v", err)
	}
	if !dup.Duplicate {
		t.Fatal("replay should be marked duplicate")
	}
	if dup.Consume.Refunded != first.Consume.Refunded {
		t.Errorf("refunded=%d, want %d (no double refund)", dup.Consume.Refunded, first.Consume.Refunded)
	}
	// 同号异内容。
	_, err = svc.Refund(ctx, RefundRequest{RequestID: "r1", ConsumeRequestID: "c1", Amount: 21})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("want ErrIdempotencyConflict, got %v", err)
	}
}

func TestRefundWindowExpiry(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Consume(ctx, consumeReq("c1", 10)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	clk.Advance(2 * time.Hour) // 超过 1 小时退还时限
	_, err := svc.Refund(ctx, RefundRequest{RequestID: "r1", ConsumeRequestID: "c1", Amount: 10})
	if !errors.Is(err, ErrRefundWindowExpired) {
		t.Fatalf("want ErrRefundWindowExpired, got %v", err)
	}
}

func TestRefundCreditsOriginalWindowOnly(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	if _, err := svc.Consume(ctx, consumeReq("c1", 60)); err != nil {
		t.Fatalf("consume: %v", err)
	}
	// 跨过窗口边界（窗口 60s，左闭右开），新窗口额度重新可用。
	clk.Advance(61 * time.Second)
	balances, err := svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	if b := balanceOf(t, balances, LevelOrg); b.Used != 0 || b.Remaining != 100 {
		t.Fatalf("new window used=%d remaining=%d, want 0/100", b.Used, b.Remaining)
	}
	// 在新窗口退还旧消费：只能回补旧窗口，新窗口额度绝不能被补充。
	if _, err := svc.Refund(ctx, RefundRequest{RequestID: "r1", ConsumeRequestID: "c1", Amount: 60}); err != nil {
		t.Fatalf("refund: %v", err)
	}
	balances, err = svc.Balances(ctx, "org-1", "user-1", "key-1")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	for _, level := range Levels {
		if b := balanceOf(t, balances, level); b.Used != 0 || b.Remaining != 100 {
			t.Errorf("level %s new window used=%d remaining=%d, want 0/100 (refund must not top up new window)",
				level, b.Used, b.Remaining)
		}
	}
	// 旧窗口的用量应被回补到 0。
	err = svc.store.View(ctx, func(tx *Tx) error {
		usage, ok := tx.GetUsage(LevelOrg, "org-1", testEpoch.Unix())
		if !ok {
			t.Fatal("old window usage record missing")
		}
		if usage.Used != 0 {
			t.Errorf("old window used=%d, want 0 after refund", usage.Used)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestWindowLeftClosedRightOpen(t *testing.T) {
	svc, clk := setup(t)
	ctx := context.Background()

	// 窗口 [0,60)：t=59 与 t=0 同窗口，t=60 属于下一个窗口。
	clk.Advance(59 * time.Second)
	if _, err := svc.Consume(ctx, consumeReq("c1", 10)); err != nil {
		t.Fatalf("consume at t=59: %v", err)
	}
	clk.Advance(1 * time.Second) // t=60，进入下一窗口
	res, err := svc.Consume(ctx, consumeReq("c2", 10))
	if err != nil {
		t.Fatalf("consume at t=60: %v", err)
	}
	b := balanceOf(t, res.Balances, LevelOrg)
	if b.Used != 10 {
		t.Errorf("t=60 belongs to new window, used=%d want 10", b.Used)
	}
	if b.WindowStart != testEpoch.Unix()+60 {
		t.Errorf("window start=%d, want %d", b.WindowStart, testEpoch.Unix()+60)
	}
}

func TestSetConfigVersionCondition(t *testing.T) {
	svc, _ := setup(t)
	ctx := context.Background()

	// 用旧版本更新配置必须失败。
	_, err := svc.SetConfig(ctx, LevelOrg, "org-1", 200, 60, 99)
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict, got %v", err)
	}
	// 用当前版本更新成功。
	cfg, err := svc.SetConfig(ctx, LevelOrg, "org-1", 200, 60, 1)
	if err != nil {
		t.Fatalf("set config: %v", err)
	}
	if cfg.Version != 2 || cfg.Limit != 200 {
		t.Errorf("version=%d limit=%d, want 2/200", cfg.Version, cfg.Limit)
	}
}

func TestConsumeWithoutConfig(t *testing.T) {
	svc := NewService(NewMemoryStore())
	_, err := svc.Consume(context.Background(), consumeReq("c1", 1))
	if !errors.Is(err, ErrQuotaNotConfigured) {
		t.Fatalf("want ErrQuotaNotConfigured, got %v", err)
	}
}

func TestRefundUnknownConsume(t *testing.T) {
	svc, _ := setup(t)
	_, err := svc.Refund(context.Background(), RefundRequest{
		RequestID: "r1", ConsumeRequestID: "nope", Amount: 1,
	})
	if !errors.Is(err, ErrConsumeNotFound) {
		t.Fatalf("want ErrConsumeNotFound, got %v", err)
	}
}

func TestFileStorePersistence(t *testing.T) {
	path := t.TempDir() + "/store.json"
	clk := newClock(testEpoch)

	store, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	svc := NewService(store, WithClock(clk.Now))
	ctx := context.Background()
	if _, err := svc.SetConfig(ctx, LevelOrg, "org-1", 100, 60, 0); err != nil {
		t.Fatalf("set config: %v", err)
	}
	if _, err := svc.Consume(ctx, ConsumeRequest{
		RequestID: "c1", OrgID: "org-1", UserID: "u", KeyID: "k", Amount: 10,
	}); err == nil {
		t.Fatal("consume should fail: user/key levels not configured")
	}
	// 配齐三层后消费成功。
	for _, level := range []Level{LevelUser, LevelKey} {
		if _, err := svc.SetConfig(ctx, level, string(level)[0:1], 100, 60, 0); err != nil {
			t.Fatalf("set config: %v", err)
		}
	}
	req := ConsumeRequest{RequestID: "c1", OrgID: "org-1", UserID: "u", KeyID: "k", Amount: 10}
	if _, err := svc.Consume(ctx, req); err != nil {
		t.Fatalf("consume: %v", err)
	}

	// 重新打开文件，状态必须完整恢复（含幂等记录）。
	reopened, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	svc2 := NewService(reopened, WithClock(clk.Now))
	res, err := svc2.Consume(ctx, req)
	if err != nil {
		t.Fatalf("replay consume: %v", err)
	}
	if !res.Duplicate {
		t.Fatal("consume should be a duplicate after reload")
	}
	balances, err := svc2.Balances(ctx, "org-1", "u", "k")
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	if b := balanceOf(t, balances, LevelOrg); b.Used != 10 {
		t.Errorf("org used=%d after reload, want 10", b.Used)
	}
}

func TestFileStoreRollbackOnError(t *testing.T) {
	path := t.TempDir() + "/store.json"
	store, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// 事务中途失败，已做的修改必须回滚，且不落盘。
	err = store.Update(context.Background(), func(tx *Tx) error {
		if err := tx.PutConfig(QuotaConfig{Level: LevelOrg, SubjectID: "x", Limit: 1, WindowSeconds: 1}, 0); err != nil {
			return err
		}
		return errors.New("boom")
	})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("want boom, got %v", err)
	}
	_ = store.View(context.Background(), func(tx *Tx) error {
		if _, ok := tx.GetConfig(LevelOrg, "x"); ok {
			t.Error("config should have been rolled back")
		}
		return nil
	})
}
