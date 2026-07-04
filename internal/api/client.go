package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/goncalofrazao/nimbus/internal/spec"
)

// Client talks to the control plane. It is the only way the operator CLI and
// (later) the node agent reach the API, so response handling lives here once.
type Client struct {
	base string
	hc   *http.Client
}

// NewClient returns a client for the control plane at base, e.g.
// "http://127.0.0.1:7440".
func NewClient(base string) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		hc:   &http.Client{Timeout: 10 * time.Second},
	}
}

// Health checks the control plane is up.
func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/healthz", nil, nil)
}

// Apply upserts workloads into the cluster's desired state.
func (c *Client) Apply(ctx context.Context, ws []spec.Workload) (ApplyResponse, error) {
	var out ApplyResponse
	err := c.do(ctx, http.MethodPost, "/v1/workloads", ApplyRequest{Workloads: ws}, &out)
	return out, err
}

// State fetches the full desired state.
func (c *Client) State(ctx context.Context) (State, error) {
	var out State
	err := c.do(ctx, http.MethodGet, "/v1/workloads", nil, &out)
	return out, err
}

// Get fetches one workload by name.
func (c *Client) Get(ctx context.Context, name string) (spec.Workload, error) {
	var out spec.Workload
	err := c.do(ctx, http.MethodGet, "/v1/workloads/"+url.PathEscape(name), nil, &out)
	return out, err
}

// Scale sets a workload's replica count.
func (c *Client) Scale(ctx context.Context, name string, replicas int) (MutationResponse, error) {
	var out MutationResponse
	err := c.do(ctx, http.MethodPost, "/v1/workloads/"+url.PathEscape(name)+"/scale",
		ScaleRequest{Replicas: replicas}, &out)
	return out, err
}

// Delete removes a workload. Deleting a missing workload succeeds with
// Existed=false.
func (c *Client) Delete(ctx context.Context, name string) (MutationResponse, error) {
	var out MutationResponse
	err := c.do(ctx, http.MethodDelete, "/v1/workloads/"+url.PathEscape(name), nil, &out)
	return out, err
}

// do sends one JSON request and decodes the JSON response. Non-2xx responses
// become errors carrying the server's message, so CLI users see "no such
// workload \"web\"", not a status code.
func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("control plane unreachable at %s: %w", c.base, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var e Error
		if json.NewDecoder(resp.Body).Decode(&e) == nil && e.Error != "" {
			return fmt.Errorf("control plane: %s", e.Error)
		}
		return fmt.Errorf("control plane: %s %s returned HTTP %d", method, path, resp.StatusCode)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
