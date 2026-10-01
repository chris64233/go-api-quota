// Package httpapi 提供配额服务的 REST 接口。
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/chris64233/go-api-quota/internal/quota"
)

// Server 把 quota.Service 暴露为 HTTP 接口。
type Server struct {
	svc *quota.Service
	mux *http.ServeMux
}

// NewServer 创建 HTTP 服务。
func NewServer(svc *quota.Service) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.mux.HandleFunc("PUT /v1/quotas", s.handlePutQuota)
	s.mux.HandleFunc("GET /v1/quotas", s.handleGetQuota)
	s.mux.HandleFunc("POST /v1/consume", s.handleConsume)
	s.mux.HandleFunc("POST /v1/refund", s.handleRefund)
	s.mux.HandleFunc("GET /v1/balances", s.handleBalances)
	s.mux.HandleFunc("POST /v1/reservations", s.handleReserve)
	s.mux.HandleFunc("POST /v1/reservations/confirm", s.handleConfirmReservation)
	s.mux.HandleFunc("POST /v1/reservations/cancel", s.handleCancelReservation)
	s.mux.HandleFunc("POST /v1/reservations/expire", s.handleExpireReservations)
	s.mux.HandleFunc("GET /v1/reservations", s.handleGetReservation)
	s.mux.HandleFunc("POST /v1/rebalances", s.handleRebalance)
	s.mux.HandleFunc("GET /v1/rebalances", s.handleGetRebalance)
	s.mux.HandleFunc("GET /v1/consume-history", s.handleConsumeHistory)
	s.mux.HandleFunc("GET /v1/reservation-history", s.handleReservationHistory)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

type errorBody struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// 错误码与 HTTP 状态映射，让额度不足、窗口、版本、状态、幂等冲突彼此可判别。
func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "internal"
	switch {
	case errors.Is(err, quota.ErrValidation):
		status, code = http.StatusBadRequest, "validation"
	case errors.Is(err, quota.ErrQuotaNotConfigured):
		status, code = http.StatusNotFound, "quota_not_configured"
	case errors.Is(err, quota.ErrConsumeNotFound):
		status, code = http.StatusNotFound, "consume_not_found"
	case errors.Is(err, quota.ErrReservationNotFound):
		status, code = http.StatusNotFound, "reservation_not_found"
	case errors.Is(err, quota.ErrRebalanceNotFound):
		status, code = http.StatusNotFound, "rebalance_not_found"
	case errors.Is(err, quota.ErrReservationState):
		status, code = http.StatusConflict, "reservation_state_conflict"
	case errors.Is(err, quota.ErrInsufficientQuota):
		status, code = http.StatusConflict, "insufficient_quota"
	case errors.Is(err, quota.ErrIdempotencyConflict):
		status, code = http.StatusConflict, "idempotency_conflict"
	case errors.Is(err, quota.ErrVersionConflict):
		status, code = http.StatusConflict, "version_conflict"
	case errors.Is(err, quota.ErrRefundWindowExpired):
		status, code = http.StatusConflict, "refund_window_expired"
	case errors.Is(err, quota.ErrRefundExceeds):
		status, code = http.StatusConflict, "refund_exceeds"
	case errors.Is(err, quota.ErrRebalanceConflict):
		status, code = http.StatusConflict, "rebalance_conflict"
	}
	writeJSON(w, status, errorBody{Error: apiError{Code: code, Message: err.Error()}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: apiError{
			Code: "bad_json", Message: err.Error(),
		}})
		return false
	}
	return true
}

type putQuotaRequest struct {
	Level           string `json:"level"`
	SubjectID       string `json:"subject_id"`
	Limit           int64  `json:"limit"`
	WindowSeconds   int64  `json:"window_seconds"`
	ExpectedVersion int64  `json:"expected_version"`
}

func (s *Server) handlePutQuota(w http.ResponseWriter, r *http.Request) {
	var req putQuotaRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	cfg, err := s.svc.SetConfig(r.Context(), quota.Level(req.Level), req.SubjectID,
		req.Limit, req.WindowSeconds, req.ExpectedVersion)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

func (s *Server) handleGetQuota(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	cfg, err := s.svc.GetConfig(r.Context(), quota.Level(q.Get("level")), q.Get("subject_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req quota.ConsumeRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	result, err := s.svc.Consume(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if result.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, result)
}

func (s *Server) handleRefund(w http.ResponseWriter, r *http.Request) {
	var req quota.RefundRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	result, err := s.svc.Refund(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if result.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, result)
}

func (s *Server) handleBalances(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	balances, err := s.svc.Balances(r.Context(), q.Get("org_id"), q.Get("user_id"), q.Get("key_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"balances": balances})
}

// reservationActionRequest 是确认 / 取消操作的请求体。
type reservationActionRequest struct {
	RequestID string `json:"request_id"`
}

func (s *Server) handleReserve(w http.ResponseWriter, r *http.Request) {
	var req quota.ReserveRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	result, err := s.svc.Reserve(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if result.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, result)
}

func (s *Server) handleConfirmReservation(w http.ResponseWriter, r *http.Request) {
	var req reservationActionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	result, err := s.svc.ConfirmReservation(r.Context(), req.RequestID)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if result.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, result)
}

func (s *Server) handleCancelReservation(w http.ResponseWriter, r *http.Request) {
	var req reservationActionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	result, err := s.svc.CancelReservation(r.Context(), req.RequestID)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if result.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, result)
}

func (s *Server) handleExpireReservations(w http.ResponseWriter, r *http.Request) {
	result, err := s.svc.ExpireDue(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetReservation(w http.ResponseWriter, r *http.Request) {
	rec, err := s.svc.GetReservation(r.Context(), r.URL.Query().Get("request_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"reservation": rec})
}

// rebalanceRequest 是重平衡接口的请求体。三层目标额度分别用
// org_limit/user_limit/key_limit 给出，服务端校验三者必须全部提供且零和。
type rebalanceRequest struct {
	RequestID string `json:"request_id"`
	OrgID     string `json:"org_id"`
	UserID    string `json:"user_id"`
	KeyID     string `json:"key_id"`
	OrgLimit  int64  `json:"org_limit"`
	UserLimit int64  `json:"user_limit"`
	KeyLimit  int64  `json:"key_limit"`
}

func (s *Server) handleRebalance(w http.ResponseWriter, r *http.Request) {
	var req rebalanceRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	result, err := s.svc.Rebalance(r.Context(), quota.RebalanceRequest{
		RequestID: req.RequestID,
		OrgID:     req.OrgID,
		UserID:    req.UserID,
		KeyID:     req.KeyID,
		TargetLimits: map[quota.Level]int64{
			quota.LevelOrg:  req.OrgLimit,
			quota.LevelUser: req.UserLimit,
			quota.LevelKey:  req.KeyLimit,
		},
	})
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if result.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, result)
}

// handleGetRebalance 带 request_id 时返回单笔重平衡单；
// 否则按 org_id/user_id/key_id 过滤返回重平衡历史（均可选，全空返回全部）。
func (s *Server) handleGetRebalance(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if requestID := q.Get("request_id"); requestID != "" {
		rec, err := s.svc.GetRebalance(r.Context(), requestID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"rebalance": rec})
		return
	}
	records, err := s.svc.RebalanceHistory(r.Context(), historyFilterFromQuery(r))
	if err != nil {
		writeError(w, err)
		return
	}
	if records == nil {
		records = []quota.RebalanceRecord{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"rebalances": records})
}

func historyFilterFromQuery(r *http.Request) quota.HistoryFilter {
	q := r.URL.Query()
	return quota.HistoryFilter{
		OrgID:  q.Get("org_id"),
		UserID: q.Get("user_id"),
		KeyID:  q.Get("key_id"),
	}
}

func (s *Server) handleConsumeHistory(w http.ResponseWriter, r *http.Request) {
	records, err := s.svc.ConsumeHistory(r.Context(), historyFilterFromQuery(r))
	if err != nil {
		writeError(w, err)
		return
	}
	if records == nil {
		records = []quota.ConsumeRecord{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"consumes": records})
}

func (s *Server) handleReservationHistory(w http.ResponseWriter, r *http.Request) {
	records, err := s.svc.ReservationHistory(r.Context(), historyFilterFromQuery(r))
	if err != nil {
		writeError(w, err)
		return
	}
	if records == nil {
		records = []quota.ReservationRecord{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"reservations": records})
}
