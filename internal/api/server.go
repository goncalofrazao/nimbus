package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/goncalofrazao/nimbus/internal/store"
)

// DefaultAddr is where the control plane listens unless overridden. It binds
// loopback only: the API is unauthenticated until the auth story (Epic C)
// lands, so exposing it beyond the host must be an explicit operator choice.
const DefaultAddr = "127.0.0.1:7440"

// Server is the control-plane HTTP API. It owns nothing itself — every
// mutation goes straight through the durable store, so the API is exactly as
// crash-safe as the store beneath it.
type Server struct {
	st  *store.Store
	log *slog.Logger
	mux *http.ServeMux
}

// NewServer returns the control-plane handler over st. A nil log discards.
func NewServer(st *store.Store, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Server{st: st, log: log, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("POST /v1/workloads", s.apply)
	s.mux.HandleFunc("GET /v1/workloads", s.state)
	s.mux.HandleFunc("GET /v1/workloads/{name}", s.get)
	s.mux.HandleFunc("POST /v1/workloads/{name}/scale", s.scale)
	s.mux.HandleFunc("DELETE /v1/workloads/{name}", s.delete)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintln(w, "ok")
}

// apply upserts a batch of workloads atomically. Validation failures are the
// client's fault (400); a store commit failure is ours (500).
func (s *Server) apply(w http.ResponseWriter, r *http.Request) {
	var req ApplyRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Workloads) == 0 {
		writeErr(w, http.StatusBadRequest, "no workloads in request")
		return
	}
	for i := range req.Workloads {
		if err := req.Workloads[i].Validate(); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	n, err := s.st.ApplyAll(req.Workloads)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if n > 0 {
		s.log.Info("applied", "changed", n, "generation", s.st.Generation())
	}
	writeJSON(w, http.StatusOK, ApplyResponse{Applied: n, Generation: s.st.Generation()})
}

func (s *Server) state(w http.ResponseWriter, _ *http.Request) {
	st := s.st.State()
	writeJSON(w, http.StatusOK, State{Generation: st.Generation, Workloads: st.Workloads})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	wl, ok := s.st.Get(name)
	if !ok {
		writeErr(w, http.StatusNotFound, fmt.Sprintf("no such workload %q", name))
		return
	}
	writeJSON(w, http.StatusOK, wl)
}

func (s *Server) scale(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req ScaleRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Replicas < 0 {
		writeErr(w, http.StatusBadRequest, "replicas must be non-negative")
		return
	}
	existed, err := s.st.Scale(name, req.Replicas)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !existed {
		writeErr(w, http.StatusNotFound, fmt.Sprintf("no such workload %q", name))
		return
	}
	s.log.Info("scaled", "workload", name, "replicas", req.Replicas, "generation", s.st.Generation())
	writeJSON(w, http.StatusOK, MutationResponse{Generation: s.st.Generation(), Existed: true})
}

// delete is idempotent: removing a workload that isn't there succeeds with
// Existed=false — the desired end state ("not present") already holds.
func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	existed, err := s.st.Delete(name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if existed {
		s.log.Info("deleted", "workload", name, "generation", s.st.Generation())
	}
	writeJSON(w, http.StatusOK, MutationResponse{Generation: s.st.Generation(), Existed: existed})
}

// decodeJSON strictly decodes one JSON value: unknown fields and trailing
// data are rejected, so a typo'd request fails loudly instead of half-applying.
func decodeJSON(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("bad request body: %w", err)
	}
	if dec.More() {
		return fmt.Errorf("bad request body: trailing data")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, Error{Error: msg})
}
