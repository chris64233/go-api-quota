// Package httpapi 提供配额服务的 REST 接口。
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
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
	s.mux.HandleFunc("POST /v1/seals", s.handleSealWindow)
	s.mux.HandleFunc("GET /v1/seals", s.handleListSeals)
	s.mux.HandleFunc("GET /v1/windows/report", s.handleWindowReport)
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
	case errors.Is(err, quota.ErrWindowSealed):
		status, code = http.StatusConflict, "window_sealed"
	case errors.Is(err, quota.ErrWindowNotEnded):
		status, code = http.StatusConflict, "window_not_ended"
	case errors.Is(err, quota.ErrWindowNotSealable):
		status, code = http.StatusConflict, "window_not_sealable"
	case errors.Is(err, quota.ErrSealNotFound):
		status, code = http.StatusNotFound, "seal_not_found"
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

func (s *Server) handleSealWindow(w http.ResponseWriter, r *http.Request) {
	var req quota.SealRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	result, err := s.svc.SealWindow(r.Context(), req)
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

func (s *Server) handleListSeals(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	seals, err := s.svc.ListSeals(r.Context(), quota.Level(q.Get("level")), q.Get("subject_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"seals": seals})
}

func (s *Server) handleWindowReport(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var windowStart int64
	if v := q.Get("window_start"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &windowStart); err != nil {
			writeError(w, fmt.Errorf("%w: invalid window_start %q", quota.ErrValidation, v))
			return
		}
	}
	report, err := s.svc.WindowReport(r.Context(), quota.Level(q.Get("level")), q.Get("subject_id"), windowStart)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}
