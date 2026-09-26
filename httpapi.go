package apiquota

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// NewHandler 返回配额服务的 HTTP API。
//
//	PUT  /v1/quotas/{level}/{entityID}      配置配额 {parentID, limit, windowSeconds}
//	GET  /v1/quotas/{level}/{entityID}      查询配额配置
//	POST /v1/consumptions                   消费 {requestId, keyId, amount}
//	POST /v1/consumptions/{id}/refunds      退还 {requestId, amount}
//	GET  /v1/balances?keyId=...             查询三层余额
func NewHandler(svc *Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/quotas/{level}/{entityID}", handlePutQuota(svc))
	mux.HandleFunc("GET /v1/quotas/{level}/{entityID}", handleGetQuota(svc))
	mux.HandleFunc("POST /v1/consumptions", handleConsume(svc))
	mux.HandleFunc("POST /v1/consumptions/{id}/refunds", handleRefund(svc))
	mux.HandleFunc("GET /v1/balances", handleBalances(svc))
	return mux
}

type quotaPayload struct {
	ParentID      string `json:"parentId"`
	Limit         int64  `json:"limit"`
	WindowSeconds int64  `json:"windowSeconds"`
}

func handlePutQuota(svc *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var p quotaPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeError(w, invalidArg("invalid JSON body"))
			return
		}
		cfg, err := svc.ConfigureQuota(r.Context(), QuotaConfig{
			Level:    Level(r.PathValue("level")),
			EntityID: r.PathValue("entityID"),
			ParentID: p.ParentID,
			Limit:    p.Limit,
			Window:   time.Duration(p.WindowSeconds) * time.Second,
		})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, cfg)
	}
}

func handleGetQuota(svc *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, err := svc.GetQuota(r.Context(), Level(r.PathValue("level")), r.PathValue("entityID"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, cfg)
	}
}

type consumePayload struct {
	RequestID string `json:"requestId"`
	KeyID     string `json:"keyId"`
	Amount    int64  `json:"amount"`
}

func handleConsume(svc *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var p consumePayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeError(w, invalidArg("invalid JSON body"))
			return
		}
		c, err := svc.Consume(r.Context(), ConsumeRequest{
			RequestID: p.RequestID,
			KeyID:     p.KeyID,
			Amount:    p.Amount,
		})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, c)
	}
}

type refundPayload struct {
	RequestID string `json:"requestId"`
	Amount    int64  `json:"amount"`
}

func handleRefund(svc *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var p refundPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeError(w, invalidArg("invalid JSON body"))
			return
		}
		res, err := svc.Refund(r.Context(), RefundRequest{
			RequestID:     p.RequestID,
			ConsumptionID: r.PathValue("id"),
			Amount:        p.Amount,
		})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	}
}

func handleBalances(svc *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, err := svc.Balances(r.Context(), r.URL.Query().Get("keyId"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, b)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError 将业务错误映射为 HTTP 状态码，错误体中保留可判别的 code。
func writeError(w http.ResponseWriter, err error) {
	var e *Error
	if !errors.As(err, &e) {
		e = &Error{Code: "INTERNAL", Message: err.Error()}
	}
	status := http.StatusInternalServerError
	switch e.Code {
	case CodeInvalidArgument:
		status = http.StatusBadRequest
	case CodeNotFound:
		status = http.StatusNotFound
	case CodeQuotaExhausted, CodeWindowExpired, CodeVersionConflict, CodeStateConflict, CodeIdempotencyConflict:
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]any{"error": e})
}
