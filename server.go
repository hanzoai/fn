package fn

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/hanzoai/fn/machines"
)

// maxBody caps a request. A function's source and its input arrive in it, and
// four megabytes is far past anything an interpreted single-file function needs
// while staying far below a memory problem.
const maxBody = 4 << 20

// OrgHeader is where the tenant comes from, and there is no fallback.
//
// fn does not authenticate anyone. It is an in-cluster executor: identity is
// terminated by IAM at the api.hanzo.ai edge, the /v1/functions registry
// upstream of here decides whose function this is, and fn is told the answer.
// Re-deriving it would be building a second auth path, and defaulting it would
// be worse — every unattributed invoke would silently land in one org's
// machines. So a missing header is a refusal, in the open, naming the caller's
// bug.
//
// The deployment consequence is a NetworkPolicy, not a credential: fn must be
// reachable only from cloud. It has no public address and must never get one.
const OrgHeader = "X-Org-Id"

// Server is the HTTP surface. Two routes do the work and they are the same
// work: build a Request, hand it to the Runner, shape the answer for whoever
// asked.
type Server struct {
	R   *Runner
	Log *slog.Logger
}

func NewServer(r *Runner, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{R: r, Log: log}
}

// Handler is every route fn serves.
//
// NOT here, deliberately: /v1/upload, /v1/download/{id} and /v1/files/{sid}.
// Those are the LibreChat contract's session-file siblings, and serving them
// means holding a session's files after the machine that made them is gone —
// which is a store, and a store is state, and state is how this becomes a
// control plane again. They belong to the long-lived machine (boxd), which
// still has the files. See the README.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/functions/{name}/invoke", s.invoke)
	mux.HandleFunc("POST /v1/exec", s.librechat)
	mux.HandleFunc("GET /healthz", s.health)
	return mux
}

// invokeBody is fn's own request shape. `code` is required because fn holds no
// registry: the /v1/functions registry owns the name-to-source mapping, and a
// second copy of it here would be a second answer to "what does this function
// contain" living on the far side of a network boundary.
type invokeBody struct {
	Runtime    string `json:"runtime"`
	Code       string `json:"code"`
	Input      string `json:"input"`
	TimeoutSec int    `json:"timeoutSec"`
}

func (s *Server) invoke(w http.ResponseWriter, r *http.Request) {
	var body invokeBody
	if err := decode(r, &body); err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	org := strings.TrimSpace(r.Header.Get(OrgHeader))
	if org == "" {
		fail(w, http.StatusBadRequest, OrgHeader+" required")
		return
	}
	res, err := s.run(r, Request{
		Org:        org,
		Name:       r.PathValue("name"),
		Runtime:    body.Runtime,
		Code:       body.Code,
		Input:      body.Input,
		TimeoutSec: body.TimeoutSec,
	})
	if err != nil {
		code, msg := statusFor(err)
		fail(w, code, msg)
		return
	}
	write(w, http.StatusOK, res)
}

// execBody is the code-interpreter contract's request, fixed by a client we do
// not own (@librechat/agents CodeExecutor). `lang` is what the /v1/functions
// registry's executor client sends, `args` carries the caller's input, and both
// spellings of every runtime resolve through the same table.
type execBody struct {
	Lang string   `json:"lang"`
	Code string   `json:"code"`
	Args []string `json:"args"`
}

// execView is that contract's response. `files` is always empty and that is not
// a stub — fn releases the machine at the end of the invoke, so there is no
// filesystem left to enumerate. A run that must leave artifacts behind is a
// long-lived machine, not a function.
type execView struct {
	SessionID string   `json:"session_id"`
	Stdout    string   `json:"stdout"`
	Stderr    string   `json:"stderr"`
	Files     []string `json:"files"`
	ExitCode  int      `json:"exitCode"`
}

// librechat serves the path the deployed /v1/functions registry already calls.
//
// This route is the reason fn works against production without a cloud release:
// the registry's executor client POSTs {lang, code, args} to
// CODE_EXEC_UPSTREAM + /v1/exec and reads stdout/stderr back. Point that env var
// at fn and the 503 that surface has returned since it was written becomes a
// real answer. Nothing upstream has to change EXCEPT the one thing named at
// OrgHeader — the registry does not currently forward X-Org-Id, and without a
// tenant there is no machine to claim.
func (s *Server) librechat(w http.ResponseWriter, r *http.Request) {
	var body execBody
	if err := decode(r, &body); err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	org := strings.TrimSpace(r.Header.Get(OrgHeader))
	if org == "" {
		fail(w, http.StatusBadRequest, OrgHeader+" required: fn claims machines per tenant and will not default one")
		return
	}
	var input string
	if len(body.Args) > 0 {
		input = strings.Join(body.Args, "\n")
	}
	res, err := s.run(r, Request{Org: org, Name: "exec", Runtime: body.Lang, Code: body.Code, Input: input})
	if err != nil {
		code, msg := statusFor(err)
		fail(w, code, msg)
		return
	}
	write(w, http.StatusOK, execView{
		SessionID: res.MachineID,
		Stdout:    res.Stdout,
		Stderr:    res.Stderr,
		Files:     []string{},
		ExitCode:  res.ExitCode,
	})
}

// run bounds the invoke and records what happened. The deadline is the
// program's own ceiling plus the same slack the lease gets, so a wedged machine
// cannot hold this request open past the point where the lease reaps it anyway.
//
// The log line names the org, the function, the machine and the outcome. It
// does NOT name the code or the input: those are the tenant's, and a runtime
// that logs the programs it runs is a runtime that leaks them.
func (s *Server) run(r *http.Request, req Request) (Result, error) {
	ctx, cancel := contextWithBudget(r, clamp(req.TimeoutSec))
	defer cancel()
	start := time.Now()
	res, err := s.R.Invoke(ctx, req)
	s.Log.Info("invoke",
		"org", req.Org, "fn", req.Name, "runtime", req.Runtime,
		"machine", res.MachineID, "exit", res.ExitCode,
		"ms", time.Since(start).Milliseconds(), "err", err)
	return res, err
}

func contextWithBudget(r *http.Request, timeoutSec int) (ctx context.Context, cancel context.CancelFunc) {
	return context.WithTimeout(r.Context(), time.Duration(timeoutSec)*time.Second+leaseSlack)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	// Configured-ness is answered by asking the machines client, not by a
	// separate copy of the env var: one place decides what "configured" means.
	ok := true
	if c, isCloud := s.R.M.(*machines.Cloud); isCloud && c.Upstream == "" {
		ok = false
	}
	code := http.StatusOK
	if !ok {
		code = http.StatusServiceUnavailable
	}
	write(w, code, map[string]any{"ok": ok, "runtimes": Runtimes()})
}

// statusFor maps a failure to what the caller should do about it.
//
// The line that matters: a program that exits non-zero is NOT an error here. It
// never reaches this function, because a failed program is a successful
// invocation — 200 with a non-zero exitCode. Only the machine failing gets a 5xx.
func statusFor(err error) (int, string) {
	switch {
	case errors.Is(err, ErrNoRuntime):
		return http.StatusBadRequest, err.Error()
	case errors.Is(err, machines.ErrUnconfigured):
		return http.StatusServiceUnavailable, err.Error()
	}
	var me *machines.Error
	if errors.As(err, &me) && me.Retryable() {
		// No warm machine. The pool is undersized or every machine is bound —
		// a capacity fact the caller may retry, and one an operator can act on.
		return http.StatusServiceUnavailable, err.Error()
	}
	// Everything else is the machines layer refusing or unreachable. It is not
	// the caller's request that is wrong, so it is not a 4xx.
	return http.StatusBadGateway, err.Error()
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		// DisallowUnknownFields is on because a typo'd field that silently does
		// nothing is how a caller ships a function with the wrong timeout and
		// finds out in production. The cost is that a new field upstream is a
		// visible 400 here rather than a silent drop, which is the trade we want.
		return errors.New("invalid request body: " + err.Error())
	}
	return nil
}

func write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, msg string) {
	write(w, code, map[string]any{"status": code, "error": msg})
}
