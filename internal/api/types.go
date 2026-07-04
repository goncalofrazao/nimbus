// Package api defines the JSON wire types shared by the control-plane server
// and its clients (the operator CLI and the node agent). Keeping them in one
// small package means the server and client can never drift out of sync.
package api

import "github.com/goncalofrazao/nimbus/internal/spec"

// State is the cluster's desired state plus the store generation it reflects.
// It is the body of GET /v1/workloads.
type State struct {
	Generation int64           `json:"generation"`
	Workloads  []spec.Workload `json:"workloads"`
}

// ApplyRequest is the body of POST /v1/workloads: the workloads to upsert.
// It is the same shape as a spec file, so `apply -spec f.json` forwards as-is.
type ApplyRequest struct {
	Workloads []spec.Workload `json:"workloads"`
}

// ApplyResponse reports how an apply changed the store.
type ApplyResponse struct {
	Applied    int   `json:"applied"` // workloads that differed from what was stored
	Generation int64 `json:"generation"`
}

// ScaleRequest sets a workload's replica count.
type ScaleRequest struct {
	Replicas int `json:"replicas"`
}

// MutationResponse is the result of a scale/delete.
type MutationResponse struct {
	Generation int64 `json:"generation"`
	Existed    bool  `json:"existed"`
}

// Error is the body of any non-2xx response.
type Error struct {
	Error string `json:"error"`
}
