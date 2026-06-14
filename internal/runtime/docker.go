// Package runtime is Nimbus's container runtime client. It speaks the Docker
// Engine API directly over the host's Unix socket using nothing but the Go
// standard library — no Docker SDK, no third-party dependencies. The Engine
// API is a plain HTTP/JSON API; the only twist is that it is served over
// /var/run/docker.sock instead of a TCP port, so we give http.Transport a
// custom dialer and address the daemon with a throwaway host name.
//
// This is the lowest layer Nimbus actuates through today. On a real Linux
// node it can later be swapped for a containerd or raw runc/namespaces
// backend behind the same Runtime interface.
package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DefaultSocket is the Docker Engine's Unix socket on Linux and macOS.
const DefaultSocket = "/var/run/docker.sock"

// Label keys Nimbus stamps on every container it owns, so it can recover its
// world purely by querying the daemon — the daemon is the source of truth for
// what is actually running, never our memory.
const (
	LabelManaged  = "nimbus.managed"  // always "true" on our containers
	LabelWorkload = "nimbus.workload" // the workload name
	LabelReplica  = "nimbus.replica"  // replica ordinal within the workload
)

// Client talks to a Docker daemon over a Unix socket.
type Client struct {
	http *http.Client
	// base is a dummy URL scheme+host; the real endpoint is the socket. The
	// host is ignored because the transport always dials the socket.
	base string
}

// New returns a Client bound to the default Docker socket.
func New() *Client { return NewWithSocket(DefaultSocket) }

// NewWithSocket returns a Client bound to a specific Unix socket path.
func NewWithSocket(socket string) *Client {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
		// One daemon, one socket: keep a small warm pool, fail fast.
		MaxIdleConns:        8,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	return &Client{
		http: &http.Client{Transport: tr},
		base: "http://docker",
	}
}

// ContainerSpec is the minimal shape Nimbus needs to launch a container.
type ContainerSpec struct {
	Name   string
	Image  string
	Cmd    []string
	Env    []string
	Labels map[string]string
}

// Container is a flattened view of a container's runtime state, assembled
// from the daemon's list/inspect responses.
type Container struct {
	ID       string
	Name     string
	Image    string
	State    string // "running", "exited", "created", ...
	ExitCode int
	Labels   map[string]string
}

// Workload returns the nimbus workload this container belongs to, or "".
func (c Container) Workload() string { return c.Labels[LabelWorkload] }

// Running reports whether the container is currently executing.
func (c Container) Running() bool { return c.State == "running" }

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

// Ping verifies the daemon is reachable and responsive.
func (c *Client) Ping(ctx context.Context) error {
	resp, err := c.do(ctx, http.MethodGet, "/_ping", nil)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return c.apiError(resp)
	}
	return nil
}

// Pull fetches an image if it is not already present. The Engine streams
// newline-delimited JSON progress events; the pull is complete only once that
// stream is fully drained, so we read it to EOF before returning.
func (c *Client) Pull(ctx context.Context, image string) error {
	repo, tag := splitImage(image)
	q := url.Values{"fromImage": {repo}, "tag": {tag}}
	resp, err := c.do(ctx, http.MethodPost, "/images/create?"+q.Encode(), nil)
	if err != nil {
		return fmt.Errorf("pull %s: %w", image, err)
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("pull %s: %w", image, c.apiError(resp))
	}
	// Drain progress, surfacing any terminal error event the stream reports.
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var ev struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(sc.Bytes(), &ev) == nil && ev.Error != "" {
			return fmt.Errorf("pull %s: %s", image, ev.Error)
		}
	}
	return sc.Err()
}

// Create creates (but does not start) a container from the spec and returns
// its ID. Nimbus's ownership labels are merged in automatically.
func (c *Client) Create(ctx context.Context, spec ContainerSpec) (string, error) {
	labels := map[string]string{LabelManaged: "true"}
	for k, v := range spec.Labels {
		labels[k] = v
	}
	body := map[string]any{
		"Image":  spec.Image,
		"Cmd":    spec.Cmd,
		"Env":    spec.Env,
		"Labels": labels,
	}
	path := "/containers/create"
	if spec.Name != "" {
		path += "?" + url.Values{"name": {spec.Name}}.Encode()
	}
	resp, err := c.do(ctx, http.MethodPost, path, body)
	if err != nil {
		return "", fmt.Errorf("create %q: %w", spec.Name, err)
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("create %q: %w", spec.Name, c.apiError(resp))
	}
	var out struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("create %q: decode: %w", spec.Name, err)
	}
	return out.ID, nil
}

// Start starts a created container. Starting an already-running container is
// treated as success (the Engine returns 304), keeping the call idempotent.
func (c *Client) Start(ctx context.Context, id string) error {
	resp, err := c.do(ctx, http.MethodPost, "/containers/"+id+"/start", nil)
	if err != nil {
		return fmt.Errorf("start %s: %w", short(id), err)
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusNotModified:
		return nil
	default:
		return fmt.Errorf("start %s: %w", short(id), c.apiError(resp))
	}
}

// Stop stops a running container, giving it timeout to exit before the daemon
// kills it. Stopping an already-stopped container is success.
func (c *Client) Stop(ctx context.Context, id string, timeout time.Duration) error {
	q := url.Values{"t": {strconv.Itoa(int(timeout.Seconds()))}}
	resp, err := c.do(ctx, http.MethodPost, "/containers/"+id+"/stop?"+q.Encode(), nil)
	if err != nil {
		return fmt.Errorf("stop %s: %w", short(id), err)
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusNotModified:
		return nil
	default:
		return fmt.Errorf("stop %s: %w", short(id), c.apiError(resp))
	}
}

// Remove deletes a container. With force it is killed first if running.
// A missing container is treated as already-removed (success).
func (c *Client) Remove(ctx context.Context, id string, force bool) error {
	q := url.Values{"force": {strconv.FormatBool(force)}}
	resp, err := c.do(ctx, http.MethodDelete, "/containers/"+id+"?"+q.Encode(), nil)
	if err != nil {
		return fmt.Errorf("remove %s: %w", short(id), err)
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("remove %s: %w", short(id), c.apiError(resp))
	}
}

// List returns containers (including stopped ones) matching every label in
// the filter. Pass nil to list all Nimbus-managed containers.
func (c *Client) List(ctx context.Context, labels map[string]string) ([]Container, error) {
	want := map[string]string{LabelManaged: "true"}
	for k, v := range labels {
		want[k] = v
	}
	// Docker's label filter is a JSON object of "key=value" entries.
	pairs := make([]string, 0, len(want))
	for k, v := range want {
		pairs = append(pairs, k+"="+v)
	}
	filters, _ := json.Marshal(map[string][]string{"label": pairs})
	q := url.Values{"all": {"true"}, "filters": {string(filters)}}
	resp, err := c.do(ctx, http.MethodGet, "/containers/json?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list: %w", c.apiError(resp))
	}
	var raw []struct {
		ID     string            `json:"Id"`
		Names  []string          `json:"Names"`
		Image  string            `json:"Image"`
		State  string            `json:"State"`
		Labels map[string]string `json:"Labels"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("list: decode: %w", err)
	}
	out := make([]Container, 0, len(raw))
	for _, r := range raw {
		name := ""
		if len(r.Names) > 0 {
			name = strings.TrimPrefix(r.Names[0], "/")
		}
		out = append(out, Container{
			ID: r.ID, Name: name, Image: r.Image,
			State: r.State, Labels: r.Labels,
		})
	}
	return out, nil
}

// Inspect returns the full state of one container, including its exit code.
func (c *Client) Inspect(ctx context.Context, id string) (*Container, error) {
	resp, err := c.do(ctx, http.MethodGet, "/containers/"+id+"/json", nil)
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", short(id), err)
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("inspect %s: %w", short(id), c.apiError(resp))
	}
	var r struct {
		ID    string `json:"Id"`
		Name  string `json:"Name"`
		Image string `json:"Image"`
		State struct {
			Status   string `json:"Status"`
			ExitCode int    `json:"ExitCode"`
		} `json:"State"`
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("inspect %s: decode: %w", short(id), err)
	}
	return &Container{
		ID: r.ID, Name: strings.TrimPrefix(r.Name, "/"), Image: r.Image,
		State: r.State.Status, ExitCode: r.State.ExitCode, Labels: r.Config.Labels,
	}, nil
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

// do issues one request to the daemon, JSON-encoding body when non-nil.
func (c *Client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode body: %w", err)
		}
		rdr = strings.NewReader(string(b))
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.http.Do(req)
}

// apiError reads the daemon's JSON error body ({"message": "..."}) and turns
// it into a Go error tagged with the HTTP status.
func (c *Client) apiError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	var e struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(b, &e) == nil && e.Message != "" {
		return fmt.Errorf("docker %d: %s", resp.StatusCode, e.Message)
	}
	msg := strings.TrimSpace(string(b))
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return fmt.Errorf("docker %d: %s", resp.StatusCode, msg)
}

// drain reads and closes a response body so the connection can be reused.
func drain(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
	}
}

// splitImage splits "repo:tag" into its parts, defaulting the tag to "latest".
// A digest reference (repo@sha256:...) is passed through as the repo with an
// empty tag, which the Engine accepts.
func splitImage(image string) (repo, tag string) {
	if i := strings.IndexByte(image, '@'); i >= 0 {
		return image, ""
	}
	// A ':' is only a tag separator if it is in the final path segment (host
	// ports like registry:5000/img are not tags).
	if i := strings.LastIndexByte(image, ':'); i >= 0 && !strings.ContainsAny(image[i:], "/") {
		return image[:i], image[i+1:]
	}
	return image, "latest"
}

// short trims a container ID to the conventional 12-char display form.
func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
