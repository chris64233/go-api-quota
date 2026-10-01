package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/chris64233/go-api-quota/internal/quota"
)

// httpClock 是 httpapi 测试用的可控时钟。
type httpClock struct {
	mu  sync.Mutex
	now time.Time
}

func newHTTPClock() *httpClock {
	return &httpClock{now: time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)}
}

func (c *httpClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *httpClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return newTestServerWithOpts(t)
}

func newTestServerWithOpts(t *testing.T, opts ...quota.Option) *httptest.Server {
	t.Helper()
	svc := quota.NewService(quota.NewMemoryStore(), opts...)
	srv := httptest.NewServer(NewServer(svc))
	t.Cleanup(srv.Close)
	return srv
}

func doJSON(t *testing.T, method, url string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp, out
}

func configureAll(t *testing.T, base string) {
	t.Helper()
	for _, level := range []string{"org", "user", "key"} {
		resp, body := doJSON(t, http.MethodPut, base+"/v1/quotas", map[string]any{
			"level": level, "subject_id": level + "-1", "limit": 100, "window_seconds": 60,
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("configure %s: status=%d body=%v", level, resp.StatusCode, body)
		}
	}
}

func TestHTTPConsumeAndBalances(t *testing.T) {
	srv := newTestServer(t)
	configureAll(t, srv.URL)

	resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/consume", map[string]any{
		"request_id": "c1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 25,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("consume status=%d body=%v", resp.StatusCode, body)
	}

	resp, body = doJSON(t, http.MethodGet,
		srv.URL+"/v1/balances?org_id=org-1&user_id=user-1&key_id=key-1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("balances status=%d body=%v", resp.StatusCode, body)
	}
	balances, ok := body["balances"].([]any)
	if !ok || len(balances) != 3 {
		t.Fatalf("want 3 level balances, got %v", body)
	}
	for _, item := range balances {
		b := item.(map[string]any)
		if b["used"].(float64) != 25 || b["remaining"].(float64) != 75 {
			t.Errorf("level %s used=%v remaining=%v, want 25/75",
				b["level"], b["used"], b["remaining"])
		}
	}
}

func TestHTTPErrorCodesAreDistinguishable(t *testing.T) {
	srv := newTestServer(t)
	configureAll(t, srv.URL)

	// 额度不足 → 409 insufficient_quota
	resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/consume", map[string]any{
		"request_id": "c1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 101,
	})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "insufficient_quota" {
		t.Fatalf("insufficient: status=%d body=%v", resp.StatusCode, body)
	}

	// 先成功消费，再用同号异内容重放 → 409 idempotency_conflict
	doJSON(t, http.MethodPost, srv.URL+"/v1/consume", map[string]any{
		"request_id": "c2", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 10,
	})
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/consume", map[string]any{
		"request_id": "c2", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 11,
	})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "idempotency_conflict" {
		t.Fatalf("idempotency: status=%d body=%v", resp.StatusCode, body)
	}

	// 退还不存在的消费 → 404 consume_not_found
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/refund", map[string]any{
		"request_id": "r1", "consume_request_id": "nope", "amount": 1,
	})
	if resp.StatusCode != http.StatusNotFound || body["error"].(map[string]any)["code"] != "consume_not_found" {
		t.Fatalf("not found: status=%d body=%v", resp.StatusCode, body)
	}

	// 版本条件不满足 → 409 version_conflict
	resp, body = doJSON(t, http.MethodPut, srv.URL+"/v1/quotas", map[string]any{
		"level": "org", "subject_id": "org-1", "limit": 50, "window_seconds": 60, "expected_version": 99,
	})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "version_conflict" {
		t.Fatalf("version: status=%d body=%v", resp.StatusCode, body)
	}

	// 参数不合法 → 400 validation
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/consume", map[string]any{
		"request_id": "c3", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": -1,
	})
	if resp.StatusCode != http.StatusBadRequest || body["error"].(map[string]any)["code"] != "validation" {
		t.Fatalf("validation: status=%d body=%v", resp.StatusCode, body)
	}
}

func TestHTTPRefundFlow(t *testing.T) {
	srv := newTestServer(t)
	configureAll(t, srv.URL)

	doJSON(t, http.MethodPost, srv.URL+"/v1/consume", map[string]any{
		"request_id": "c1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 40,
	})
	resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/refund", map[string]any{
		"request_id": "r1", "consume_request_id": "c1", "amount": 15,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("refund status=%d body=%v", resp.StatusCode, body)
	}
	// 重复退还同号同内容 → 200 且 duplicate=true
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/refund", map[string]any{
		"request_id": "r1", "consume_request_id": "c1", "amount": 15,
	})
	if resp.StatusCode != http.StatusOK || body["duplicate"] != true {
		t.Fatalf("duplicate refund status=%d body=%v", resp.StatusCode, body)
	}
	// 超额退还 → 409 refund_exceeds
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/refund", map[string]any{
		"request_id": "r2", "consume_request_id": "c1", "amount": 26,
	})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "refund_exceeds" {
		t.Fatalf("exceeds: status=%d body=%v", resp.StatusCode, body)
	}
}

func TestHTTPReservationFlow(t *testing.T) {
	srv := newTestServer(t)
	configureAll(t, srv.URL)

	// 创建预占。
	resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/reservations", map[string]any{
		"request_id": "rsv1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1",
		"amount": 30, "ttl_seconds": 60,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("reserve status=%d body=%v", resp.StatusCode, body)
	}
	rsv := body["reservation"].(map[string]any)
	if rsv["state"] != "reserved" {
		t.Fatalf("state=%v, want reserved", rsv["state"])
	}
	// 余额视图体现 reserved 占用。
	for _, item := range body["balances"].([]any) {
		b := item.(map[string]any)
		if b["reserved"].(float64) != 30 || b["remaining"].(float64) != 70 {
			t.Errorf("level %v reserved=%v remaining=%v, want 30/70", b["level"], b["reserved"], b["remaining"])
		}
	}

	// 同号重放 → 200 duplicate=true，不重复预占。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/reservations", map[string]any{
		"request_id": "rsv1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1",
		"amount": 30, "ttl_seconds": 60,
	})
	if resp.StatusCode != http.StatusOK || body["duplicate"] != true {
		t.Fatalf("duplicate reserve status=%d body=%v", resp.StatusCode, body)
	}
	// 同号不同内容 → 409 idempotency_conflict。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/reservations", map[string]any{
		"request_id": "rsv1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 31,
	})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "idempotency_conflict" {
		t.Fatalf("idempotency: status=%d body=%v", resp.StatusCode, body)
	}

	// 查询预占状态。
	resp, body = doJSON(t, http.MethodGet, srv.URL+"/v1/reservations?request_id=rsv1", nil)
	if resp.StatusCode != http.StatusOK || body["reservation"].(map[string]any)["state"] != "reserved" {
		t.Fatalf("get reservation status=%d body=%v", resp.StatusCode, body)
	}

	// 确认 → reserved 转为正式消费。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/reservations/confirm", map[string]any{"request_id": "rsv1"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("confirm status=%d body=%v", resp.StatusCode, body)
	}
	if body["reservation"].(map[string]any)["state"] != "confirmed" {
		t.Fatalf("state=%v, want confirmed", body["reservation"])
	}
	if body["consume"].(map[string]any)["amount"].(float64) != 30 {
		t.Fatalf("consume amount=%v, want 30", body["consume"])
	}
	// 重复确认 → 200 duplicate=true。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/reservations/confirm", map[string]any{"request_id": "rsv1"})
	if resp.StatusCode != http.StatusOK || body["duplicate"] != true {
		t.Fatalf("duplicate confirm status=%d body=%v", resp.StatusCode, body)
	}
	// 已确认再取消 → 409 reservation_state_conflict。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/reservations/cancel", map[string]any{"request_id": "rsv1"})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "reservation_state_conflict" {
		t.Fatalf("cancel confirmed: status=%d body=%v", resp.StatusCode, body)
	}

	// 最终三层余额：used=30，reserved=0。
	resp, body = doJSON(t, http.MethodGet,
		srv.URL+"/v1/balances?org_id=org-1&user_id=user-1&key_id=key-1", nil)
	for _, item := range body["balances"].([]any) {
		b := item.(map[string]any)
		if b["used"].(float64) != 30 || b["reserved"].(float64) != 0 || b["remaining"].(float64) != 70 {
			t.Errorf("level %v used=%v reserved=%v remaining=%v, want 30/0/70",
				b["level"], b["used"], b["reserved"], b["remaining"])
		}
	}
}

func TestHTTPReservationCancelAndExpire(t *testing.T) {
	srv := newTestServer(t)
	configureAll(t, srv.URL)

	// 取消流程。
	doJSON(t, http.MethodPost, srv.URL+"/v1/reservations", map[string]any{
		"request_id": "rsv1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 20,
	})
	resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/reservations/cancel", map[string]any{"request_id": "rsv1"})
	if resp.StatusCode != http.StatusCreated || body["reservation"].(map[string]any)["state"] != "cancelled" {
		t.Fatalf("cancel status=%d body=%v", resp.StatusCode, body)
	}
	// 取消不存在的预占 → 404 reservation_not_found。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/reservations/confirm", map[string]any{"request_id": "ghost"})
	if resp.StatusCode != http.StatusNotFound || body["error"].(map[string]any)["code"] != "reservation_not_found" {
		t.Fatalf("not found: status=%d body=%v", resp.StatusCode, body)
	}
	// 三层额度已全部释放。
	resp, body = doJSON(t, http.MethodGet,
		srv.URL+"/v1/balances?org_id=org-1&user_id=user-1&key_id=key-1", nil)
	for _, item := range body["balances"].([]any) {
		b := item.(map[string]any)
		if b["reserved"].(float64) != 0 || b["remaining"].(float64) != 100 {
			t.Errorf("level %v reserved=%v remaining=%v, want 0/100", b["level"], b["reserved"], b["remaining"])
		}
	}

	// 预占不足整笔失败 → 409 insufficient_quota。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/reservations", map[string]any{
		"request_id": "rsv2", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 101,
	})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "insufficient_quota" {
		t.Fatalf("insufficient: status=%d body=%v", resp.StatusCode, body)
	}
}

func TestHTTPReservationExpirySweep(t *testing.T) {
	clk := newHTTPClock()
	srv := newTestServerWithOpts(t,
		quota.WithClock(clk.Now),
		quota.WithReservationTTL(time.Minute),
	)
	configureAll(t, srv.URL)

	// 默认 TTL 1 分钟的预占。
	resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/reservations", map[string]any{
		"request_id": "rsv1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 25,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("reserve status=%d body=%v", resp.StatusCode, body)
	}
	// 未到期：扫描结果为空。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/reservations/expire", nil)
	if resp.StatusCode != http.StatusOK || len(body["expired"].([]any)) != 0 {
		t.Fatalf("premature sweep status=%d body=%v", resp.StatusCode, body)
	}
	// 到期后扫描：rsv1 被回收。
	clk.Advance(61 * time.Second)
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/reservations/expire", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sweep status=%d body=%v", resp.StatusCode, body)
	}
	expired := body["expired"].([]any)
	if len(expired) != 1 || expired[0] != "rsv1" {
		t.Fatalf("expired=%v, want [rsv1]", expired)
	}
	// 查询确认状态为 expired，且额度已释放。
	resp, body = doJSON(t, http.MethodGet, srv.URL+"/v1/reservations?request_id=rsv1", nil)
	if resp.StatusCode != http.StatusOK || body["reservation"].(map[string]any)["state"] != "expired" {
		t.Fatalf("get expired status=%d body=%v", resp.StatusCode, body)
	}
	resp, body = doJSON(t, http.MethodGet,
		srv.URL+"/v1/balances?org_id=org-1&user_id=user-1&key_id=key-1", nil)
	for _, item := range body["balances"].([]any) {
		b := item.(map[string]any)
		if b["reserved"].(float64) != 0 || b["remaining"].(float64) != 100 {
			t.Errorf("level %v reserved=%v remaining=%v, want 0/100", b["level"], b["reserved"], b["remaining"])
		}
	}
}

func TestHTTPRebalanceFlow(t *testing.T) {
	srv := newTestServer(t)
	configureAll(t, srv.URL)

	// 先制造一笔预占与一笔消费，作为调整依据。
	doJSON(t, http.MethodPost, srv.URL+"/v1/reservations", map[string]any{
		"request_id": "rsv1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1",
		"amount": 10, "ttl_seconds": 600,
	})

	// 重平衡：组织 -20、用户 +20，密钥不变。
	resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/rebalances", map[string]any{
		"request_id": "rb1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1",
		"org_limit": 80, "user_limit": 120, "key_limit": 100,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("rebalance status=%d body=%v", resp.StatusCode, body)
	}
	rb := body["rebalance"].(map[string]any)
	levels := rb["levels"].([]any)
	if len(levels) != 3 {
		t.Fatalf("levels=%v, want 3", levels)
	}
	wantDelta := map[string]float64{"org": -20, "user": 20, "key": 0}
	wantLimit := map[string]float64{"org": 80, "user": 120, "key": 100}
	for _, item := range levels {
		entry := item.(map[string]any)
		level := entry["level"].(string)
		if entry["delta"].(float64) != wantDelta[level] {
			t.Errorf("level %s delta=%v, want %v", level, entry["delta"], wantDelta[level])
		}
		if entry["limit_before"].(float64) != 100 || entry["limit_after"].(float64) != wantLimit[level] {
			t.Errorf("level %s before/after=%v/%v, want 100/%v",
				level, entry["limit_before"], entry["limit_after"], wantLimit[level])
		}
		if entry["reserved"].(float64) != 10 {
			t.Errorf("level %s reserved basis=%v, want 10", level, entry["reserved"])
		}
		inFlight := entry["in_flight_reservations"].([]any)
		if len(inFlight) != 1 || inFlight[0] != "rsv1" {
			t.Errorf("level %s in_flight=%v, want [rsv1]", level, inFlight)
		}
	}
	// 余额反映新额度，used/reserved 不变，并可追溯到重平衡单。
	for _, item := range body["balances"].([]any) {
		b := item.(map[string]any)
		level := b["level"].(string)
		if b["limit"].(float64) != wantLimit[level] {
			t.Errorf("level %v limit=%v, want %v", level, b["limit"], wantLimit[level])
		}
		if b["reserved"].(float64) != 10 {
			t.Errorf("level %v reserved=%v, want 10", level, b["reserved"])
		}
		if b["last_rebalance_request_id"] != "rb1" {
			t.Errorf("level %v last_rebalance_request_id=%v, want rb1",
				level, b["last_rebalance_request_id"])
		}
	}

	// 同号同内容重放 → 200 duplicate=true。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/rebalances", map[string]any{
		"request_id": "rb1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1",
		"org_limit": 80, "user_limit": 120, "key_limit": 100,
	})
	if resp.StatusCode != http.StatusOK || body["duplicate"] != true {
		t.Fatalf("duplicate rebalance status=%d body=%v", resp.StatusCode, body)
	}

	// 同号不同目标 → 409 idempotency_conflict。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/rebalances", map[string]any{
		"request_id": "rb1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1",
		"org_limit": 79, "user_limit": 121, "key_limit": 100,
	})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "idempotency_conflict" {
		t.Fatalf("idempotency: status=%d body=%v", resp.StatusCode, body)
	}

	// 非零和 → 400 validation。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/rebalances", map[string]any{
		"request_id": "rb2", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1",
		"org_limit": 80, "user_limit": 110, "key_limit": 100,
	})
	if resp.StatusCode != http.StatusBadRequest || body["error"].(map[string]any)["code"] != "validation" {
		t.Fatalf("non-zero-sum: status=%d body=%v", resp.StatusCode, body)
	}

	// 目标低于已预占 → 409 insufficient_quota，整笔不落地。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/rebalances", map[string]any{
		"request_id": "rb3", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1",
		"org_limit": 5, "user_limit": 195, "key_limit": 100,
	})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "insufficient_quota" {
		t.Fatalf("insufficient: status=%d body=%v", resp.StatusCode, body)
	}

	// 查询不存在的重平衡单 → 404 rebalance_not_found。
	resp, body = doJSON(t, http.MethodGet, srv.URL+"/v1/rebalances?request_id=ghost", nil)
	if resp.StatusCode != http.StatusNotFound || body["error"].(map[string]any)["code"] != "rebalance_not_found" {
		t.Fatalf("not found: status=%d body=%v", resp.StatusCode, body)
	}
}

func TestHTTPRebalanceConflictAndHistory(t *testing.T) {
	clk := newHTTPClock()
	srv := newTestServerWithOpts(t, quota.WithClock(clk.Now))
	configureAll(t, srv.URL)

	rb1 := map[string]any{
		"request_id": "rb1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1",
		"org_limit": 80, "user_limit": 120, "key_limit": 100,
	}
	resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/rebalances", rb1)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("rb1 status=%d body=%v", resp.StatusCode, body)
	}

	// 窗口滚动后原单重放 → 409 rebalance_conflict。
	clk.Advance(61 * time.Second)
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/rebalances", rb1)
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "rebalance_conflict" {
		t.Fatalf("window rotation conflict: status=%d body=%v", resp.StatusCode, body)
	}

	// 再来一笔调整改变目标额度，原单重放同样冲突。
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/rebalances", map[string]any{
		"request_id": "rb2", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1",
		"org_limit": 70, "user_limit": 120, "key_limit": 110,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("rb2 status=%d body=%v", resp.StatusCode, body)
	}
	// rb2 发生在新窗口且无在途预占：in_flight_reservations 被 omitempty 省略，允许缺失。
	rb2Levels := body["rebalance"].(map[string]any)["levels"].([]any)
	if entry := rb2Levels[0].(map[string]any); entry["in_flight_reservations"] != nil {
		t.Errorf("rb2 should carry no in-flight reservations, got %v", entry["in_flight_reservations"])
	}

	// 重平衡历史：全部、按主体过滤、单笔查询。
	resp, body = doJSON(t, http.MethodGet, srv.URL+"/v1/rebalances", nil)
	if resp.StatusCode != http.StatusOK || len(body["rebalances"].([]any)) != 2 {
		t.Fatalf("history status=%d body=%v", resp.StatusCode, body)
	}
	resp, body = doJSON(t, http.MethodGet, srv.URL+"/v1/rebalances?org_id=org-1", nil)
	if resp.StatusCode != http.StatusOK || len(body["rebalances"].([]any)) != 2 {
		t.Fatalf("filtered history status=%d body=%v", resp.StatusCode, body)
	}
	resp, body = doJSON(t, http.MethodGet, srv.URL+"/v1/rebalances?request_id=rb2", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get one status=%d body=%v", resp.StatusCode, body)
	}
	if body["rebalance"].(map[string]any)["request_id"] != "rb2" {
		t.Fatalf("get one body=%v", body)
	}

	// 消费历史与预占历史接口。
	doJSON(t, http.MethodPost, srv.URL+"/v1/consume", map[string]any{
		"request_id": "c1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 5,
	})
	resp, body = doJSON(t, http.MethodGet, srv.URL+"/v1/consume-history?key_id=key-1", nil)
	if resp.StatusCode != http.StatusOK || len(body["consumes"].([]any)) != 1 {
		t.Fatalf("consume history status=%d body=%v", resp.StatusCode, body)
	}
	resp, body = doJSON(t, http.MethodGet, srv.URL+"/v1/reservation-history?org_id=org-1", nil)
	if resp.StatusCode != http.StatusOK || len(body["reservations"].([]any)) != 0 {
		t.Fatalf("reservation history status=%d body=%v", resp.StatusCode, body)
	}
}
