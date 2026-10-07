package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chris64233/go-api-quota/internal/quota"
)

// newSealTestServer 创建带可控时钟的测试服务并配置三层配额（limit=100，窗口 60s）。
func newSealTestServer(t *testing.T) (*httptest.Server, *httpClock, int64) {
	t.Helper()
	clk := newHTTPClock()
	srv := newTestServerWithOpts(t, quota.WithClock(clk.Now))
	configureAll(t, srv.URL)
	sec := clk.Now().Unix()
	return srv, clk, sec - sec%60
}

func TestHTTPSealWindow(t *testing.T) {
	srv, clk, ws := newSealTestServer(t)

	// 窗口未结束 → 409 window_not_ended
	resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/seals", map[string]any{
		"request_id": "s1", "level": "org", "subject_id": "org-1", "window_start": ws,
	})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "window_not_ended" {
		t.Fatalf("seal open window: status=%d body=%v", resp.StatusCode, body)
	}

	clk.Advance(61 * time.Second)
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/seals", map[string]any{
		"request_id": "s1", "level": "org", "subject_id": "org-1", "window_start": ws,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seal: status=%d body=%v", resp.StatusCode, body)
	}
	// 幂等重放 → 200 duplicate
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/seals", map[string]any{
		"request_id": "s1", "level": "org", "subject_id": "org-1", "window_start": ws,
	})
	if resp.StatusCode != http.StatusOK || body["duplicate"] != true {
		t.Fatalf("replay seal: status=%d body=%v", resp.StatusCode, body)
	}
	// 不同请求号重复封存 → 409 window_sealed
	resp, body = doJSON(t, http.MethodPost, srv.URL+"/v1/seals", map[string]any{
		"request_id": "s2", "level": "org", "subject_id": "org-1", "window_start": ws,
	})
	if resp.StatusCode != http.StatusConflict || body["error"].(map[string]any)["code"] != "window_sealed" {
		t.Fatalf("reseal: status=%d body=%v", resp.StatusCode, body)
	}

	resp, body = doJSON(t, http.MethodGet, srv.URL+"/v1/seals?level=org&subject_id=org-1", nil)
	if resp.StatusCode != http.StatusOK || len(body["seals"].([]any)) != 1 {
		t.Fatalf("list seals: status=%d body=%v", resp.StatusCode, body)
	}
}

func TestHTTPWindowReportAndLateSettlement(t *testing.T) {
	srv, clk, ws := newSealTestServer(t)

	doJSON(t, http.MethodPost, srv.URL+"/v1/consume", map[string]any{
		"request_id": "c1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 30,
	})
	doJSON(t, http.MethodPost, srv.URL+"/v1/reservations", map[string]any{
		"request_id": "rsv1", "org_id": "org-1", "user_id": "user-1", "key_id": "key-1", "amount": 20,
	})
	clk.Advance(61 * time.Second)
	for _, level := range []string{"org", "user", "key"} {
		resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/seals", map[string]any{
			"request_id": "s-" + level, "level": level, "subject_id": level + "-1", "window_start": ws,
		})
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("seal %s: status=%d body=%v", level, resp.StatusCode, body)
		}
	}

	// 迟到的确认仍按原请求号处理。
	resp, body := doJSON(t, http.MethodPost, srv.URL+"/v1/reservations/confirm", map[string]any{
		"request_id": "rsv1",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("late confirm: status=%d body=%v", resp.StatusCode, body)
	}

	resp, body = doJSON(t, http.MethodGet,
		srv.URL+"/v1/windows/report?level=org&subject_id=org-1&window_start="+
			fmtInt(ws), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("report: status=%d body=%v", resp.StatusCode, body)
	}
	if body["sealed"] != true || body["used"].(float64) != 50 || body["reserved"].(float64) != 0 {
		t.Fatalf("report body=%v", body)
	}
	if len(body["entries"].([]any)) != 2 {
		t.Fatalf("entries=%v", body["entries"])
	}
	snap := body["snapshot"].(map[string]any)
	if snap["limit"].(float64) != 100 || len(snap["open_reservations"].([]any)) != 1 {
		t.Fatalf("snapshot=%v", snap)
	}
}

func fmtInt(v int64) string {
	return fmt.Sprintf("%d", v)
}
