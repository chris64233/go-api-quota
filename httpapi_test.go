package apiquota

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	svc, _ := newTestService(t)
	return NewHandler(svc)
}

func do(t *testing.T, h http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

func TestHTTPAPI(t *testing.T) {
	h := newTestHandler(t)

	// 配置查询
	code, quota := do(t, h, "GET", "/v1/quotas/key/key1", "")
	if code != http.StatusOK || quota["entity_id"] != "key1" {
		t.Fatalf("get quota: code=%d body=%v", code, quota)
	}

	// 消费
	code, c := do(t, h, "POST", "/v1/consumptions", `{"requestId":"r1","keyId":"key1","amount":30}`)
	if code != http.StatusCreated {
		t.Fatalf("consume: code=%d body=%v", code, c)
	}
	consumptionID, _ := c["id"].(string)
	if consumptionID == "" {
		t.Fatalf("consumption id missing: %v", c)
	}

	// 三层余额查询
	code, b := do(t, h, "GET", "/v1/balances?keyId=key1", "")
	if code != http.StatusOK {
		t.Fatalf("balances: code=%d body=%v", code, b)
	}
	key := b["key"].(map[string]any)
	if key["used"].(float64) != 30 || key["remaining"].(float64) != 70 {
		t.Errorf("key balance = %v", key)
	}

	// 退还
	code, rf := do(t, h, "POST", "/v1/consumptions/"+consumptionID+"/refunds", `{"requestId":"rf1","amount":10}`)
	if code != http.StatusOK || rf["total_refunded"].(float64) != 10 {
		t.Fatalf("refund: code=%d body=%v", code, rf)
	}

	// 幂等冲突：同号异内容 → 409 + IDEMPOTENCY_CONFLICT
	code, e := do(t, h, "POST", "/v1/consumptions", `{"requestId":"r1","keyId":"key1","amount":31}`)
	if code != http.StatusConflict {
		t.Fatalf("idempotency conflict: code=%d body=%v", code, e)
	}
	if e["error"].(map[string]any)["code"] != string(CodeIdempotencyConflict) {
		t.Errorf("error body = %v", e)
	}

	// 额度不足 → 409 + QUOTA_EXHAUSTED
	code, e = do(t, h, "POST", "/v1/consumptions", `{"requestId":"r2","keyId":"key1","amount":999}`)
	if code != http.StatusConflict || e["error"].(map[string]any)["code"] != string(CodeQuotaExhausted) {
		t.Errorf("quota exhausted: code=%d body=%v", code, e)
	}

	// 未知密钥 → 404 + NOT_FOUND
	code, e = do(t, h, "POST", "/v1/consumptions", `{"requestId":"r3","keyId":"ghost","amount":1}`)
	if code != http.StatusNotFound || e["error"].(map[string]any)["code"] != string(CodeNotFound) {
		t.Errorf("not found: code=%d body=%v", code, e)
	}

	// 参数非法 → 400 + INVALID_ARGUMENT
	code, e = do(t, h, "POST", "/v1/consumptions", `{"requestId":"r4","keyId":"key1","amount":-5}`)
	if code != http.StatusBadRequest || e["error"].(map[string]any)["code"] != string(CodeInvalidArgument) {
		t.Errorf("invalid: code=%d body=%v", code, e)
	}
}

func TestHTTPConfigureQuotaViaAPI(t *testing.T) {
	svc := NewService(NewMemoryStore())
	h := NewHandler(svc)

	// 先配组织，再配用户、密钥
	code, _ := do(t, h, "PUT", "/v1/quotas/org/o1", `{"limit":100,"windowSeconds":3600}`)
	if code != http.StatusOK {
		t.Fatalf("put org quota: %d", code)
	}
	code, _ = do(t, h, "PUT", "/v1/quotas/user/u1", `{"parentId":"o1","limit":50,"windowSeconds":3600}`)
	if code != http.StatusOK {
		t.Fatalf("put user quota: %d", code)
	}
	// 父级不存在 → 404
	code, e := do(t, h, "PUT", "/v1/quotas/key/k1", `{"parentId":"nobody","limit":10,"windowSeconds":3600}`)
	if code != http.StatusNotFound || e["error"].(map[string]any)["code"] != string(CodeNotFound) {
		t.Errorf("orphan key quota: code=%d body=%v", code, e)
	}
}
