package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chris64233/go-api-quota/internal/quota"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	svc := quota.NewService(quota.NewMemoryStore())
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
