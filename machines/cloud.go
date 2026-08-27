package machines

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// The machines layer's HTTP surface, spelled once.
//
// SOURCE OF TRUTH: hanzoai/cloud, apps/sandbox (the routes) and
// apps/sandbox/wire (the bodies). These constants and the json tags on
// ExecRequest/ExecResult/writeBody are a byte-for-byte copy of that contract,
// and a copy is a thing that can drift.
//
// It is a copy for a reason that is worth writing down rather than fixing
// wrong: `apps/sandbox/wire` is a package inside module github.com/hanzoai/cloud
// and is not in any published version of it — `go get github.com/hanzoai/cloud@latest`
// resolves v1.801.477, which does not contain the package. Even once it is
// published, importing one leaf drags cloud's entire module graph (measured:
// 100+ indirect requirements for a package whose own dependency list is empty).
//
// The real fix is upstream and is one line of go.mod: make `wire` its own
// module, which is exactly what its doc comment already asks for — it exists so
// that programs on different release cadences cannot disagree about the wire,
// and that property is only real for programs that can import it. Until then
// this file is the disagreement, contained in one place, with the tests below
// pinning every path and every field name so a drift is a failing test rather
// than a 404 in production.
const (
	pathMachines = "/v1/sandbox/boxes"
	subWrite     = "/fs/write"
	subExec      = "/proc/exec"

	hdrOrg = "X-Org-Id"
	hdrKey = "X-API-Key"
)

// ErrUnconfigured is a deployment with no machines layer. It is returned rather
// than defaulted around: a function runtime that cannot reach a machine must say
// so, not invent an answer. This is the same fail-closed posture the /v1/functions
// registry already takes when its executor is unset.
var ErrUnconfigured = errors.New("machines layer not configured: FN_MACHINES_UPSTREAM unset")

// Error carries a machines-layer refusal with its status, so the caller can
// keep the distinction that matters: a 503 (no warm machine) is a capacity
// problem the client may retry, a 4xx is not.
type Error struct {
	Op     string
	Status int
	Body   string
}

func (e *Error) Error() string {
	return fmt.Sprintf("machines %s: %d %s", e.Op, e.Status, e.Body)
}

// Retryable reports whether the machines layer said "not now" rather than "no".
func (e *Error) Retryable() bool {
	return e.Status == http.StatusServiceUnavailable || e.Status == http.StatusTooManyRequests
}

// Cloud is the ONE implementation of Machines: an HTTP client of the machines
// layer. It holds no state about any machine — the layer owns that — so it is
// safe to share across every in-flight invoke.
type Cloud struct {
	Upstream string       // e.g. http://cloud.hanzo.svc.cluster.local:8000
	Key      string       // service credential, KMS-sourced into the pod env
	HTTP     *http.Client // nil ⇒ DefaultClient with a bounded response-header wait
}

var _ Machines = (*Cloud)(nil)

// NewCloud trims the upstream once so every URL below is a plain concatenation.
func NewCloud(upstream, key string) *Cloud {
	return &Cloud{
		Upstream: strings.TrimRight(strings.TrimSpace(upstream), "/"),
		Key:      strings.TrimSpace(key),
		// The response-header timeout bounds a wedged machine without bounding
		// the BODY: a legitimate run streams for as long as its own timeout
		// allows, and that ceiling is enforced machine-side by TimeoutSec.
		HTTP: &http.Client{Transport: &http.Transport{ResponseHeaderTimeout: 60 * time.Second}},
	}
}

func (c *Cloud) client() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *Cloud) Claim(ctx context.Context, s Spec) (Lease, error) {
	if c.Upstream == "" {
		return Lease{}, ErrUnconfigured
	}
	body := map[string]any{"project": s.Project, "class": s.Class}
	if s.TTLSec > 0 {
		body["ttlSec"] = s.TTLSec
	}
	var out struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Host   string `json:"host"`
		Error  string `json:"error"`
	}
	if err := c.do(ctx, http.MethodPost, c.Upstream+pathMachines, s.Org, body, &out, "claim"); err != nil {
		return Lease{}, err
	}
	// A machine with no id is not a machine. The layer records a row even for a
	// claim it could not satisfy, so an empty id here means the pool answered
	// with a receipt rather than a machine, and running against it would be
	// running against nothing.
	if out.ID == "" {
		return Lease{}, &Error{Op: "claim", Status: http.StatusBadGateway, Body: "machines layer returned no machine id"}
	}
	return Lease{ID: out.ID, Org: s.Org, Status: out.Status}, nil
}

// writeBody is the machines layer's file-write shape. Content is a Go string,
// so any byte sequence Go can hold survives the JSON round trip; the layer's
// contentB64 field is for callers that must move invalid UTF-8, which a source
// file is not.
type writeBody struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

func (c *Cloud) Write(ctx context.Context, l Lease, path, content string) error {
	if c.Upstream == "" {
		return ErrUnconfigured
	}
	return c.do(ctx, http.MethodPost, c.machineURL(l, subWrite), l.Org, writeBody{Path: path, Content: content}, nil, "write")
}

func (c *Cloud) Exec(ctx context.Context, l Lease, r ExecRequest) (ExecResult, error) {
	if c.Upstream == "" {
		return ExecResult{}, ErrUnconfigured
	}
	var out ExecResult
	if err := c.do(ctx, http.MethodPost, c.machineURL(l, subExec), l.Org, r, &out, "exec"); err != nil {
		return ExecResult{}, err
	}
	return out, nil
}

func (c *Cloud) Release(ctx context.Context, l Lease) error {
	if c.Upstream == "" {
		return ErrUnconfigured
	}
	return c.do(ctx, http.MethodDelete, c.machineURL(l, ""), l.Org, nil, nil, "release")
}

// machineURL escapes the id because it reaches this process from a body the
// machines layer wrote, not from a constant. Path-escaping it costs nothing and
// keeps a hostile id from addressing a different machine.
func (c *Cloud) machineURL(l Lease, sub string) string {
	return c.Upstream + pathMachines + "/" + url.PathEscape(l.ID) + sub
}

// do is the single request path: one place that sets the credential, one place
// that reads a status, one place that decodes. Every verb above is a call to it.
func (c *Cloud) do(ctx context.Context, method, target, org string, in, out any, op string) error {
	var rdr io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("machines %s: encode: %w", op, err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, rdr)
	if err != nil {
		return fmt.Errorf("machines %s: %w", op, err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// The tenant travels as a header the machines layer already reads. fn never
	// derives it, defaults it, or falls back to a "system" org — an invoke with
	// no org is refused upstream, which is the correct place for that refusal.
	if org != "" {
		req.Header.Set(hdrOrg, org)
	}
	if c.Key != "" {
		req.Header.Set(hdrKey, c.Key)
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return fmt.Errorf("machines %s: %w", op, err)
	}
	defer func() { _ = resp.Body.Close() }()
	// 1 MiB is generous for a control-plane answer and is the cap that keeps a
	// misconfigured upstream from being an OOM. Program OUTPUT does not come
	// through here — it comes through Exec's result, capped the same way.
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &Error{Op: op, Status: resp.StatusCode, Body: strings.TrimSpace(string(raw))}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return &Error{Op: op, Status: resp.StatusCode, Body: "undecodable response: " + err.Error()}
	}
	return nil
}
