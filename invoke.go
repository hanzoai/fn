// Package fn runs a function on a machine.
//
// A function invoke is a machine with a seconds-long lease. That is not an
// analogy, it is the implementation: claim a warm machine, write the source
// onto it, run it, give the machine back. Four calls, one file, no state.
//
// Everything Fission needed a control plane for is either somewhere else or
// gone. The pool is the machines layer's. The registry, the billing and the
// authentication are the /v1/functions registry's, upstream of here. The
// builder is deleted. What remains is this file, and it should stay small
// enough that "has fn become Fission again" is answerable by reading it.
package fn

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hanzoai/fn/machines"
)

// The invoke clamp. Both numbers are the /v1/functions registry's, restated
// here because fn is reachable without it: an unset timeout is 30s and no
// request may exceed 15 minutes. A function that needs longer is not a function
// — it is a job, and hanzoai/tasks already runs those.
const (
	DefaultTimeoutSec = 30
	MaxTimeoutSec     = 900

	// leaseSlack is how much longer the machine may live than the program is
	// allowed to run. It is the backstop for a Release that never happens —
	// this process being OOM-killed between claim and release is the case no
	// amount of deferring covers, and without a TTL that machine is leaked out
	// of the pool permanently.
	leaseSlack = 60 * time.Second

	// releaseGrace bounds the give-back. It is deliberately generous: failing
	// to release costs a machine until its TTL expires, so it is worth waiting.
	releaseGrace = 30 * time.Second
)

// ErrNoRuntime is an unknown runtime, refused BEFORE a machine is claimed. A
// claim that is going to be thrown away one line later is a machine taken from
// the pool for nothing.
var ErrNoRuntime = errors.New("unknown runtime")

// Request is one invocation.
type Request struct {
	Org        string // tenant; the machines layer refuses an empty one
	Name       string // the function's name — a label and a filename, not a lookup key
	Runtime    string // python | node | deno | bash, or the py/js/ts aliases
	Code       string // the source, in full. fn holds no registry and never resolves Name to code.
	Input      string // handed to the program on stdin
	TimeoutSec int    // clamped to [1, MaxTimeoutSec]; 0 means DefaultTimeoutSec
}

// Result is one finished invocation. The machine's ExecResult is embedded
// rather than copied field by field, so "what a run returns" has one definition.
type Result struct {
	machines.ExecResult
	MachineID string `json:"machineId"`
	Runtime   string `json:"runtime"`
}

// Runner is the whole service. Its only dependency is the machines layer, which
// is what makes "fn has no pool of its own" a fact about the type rather than a
// claim in a comment: there is nowhere to put one.
type Runner struct {
	M   machines.Machines
	Log *slog.Logger
}

func NewRunner(m machines.Machines, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.Default()
	}
	return &Runner{M: m, Log: log}
}

// Invoke is the product: claim, write, exec, release.
//
// The machine is released on EVERY path out of here after a successful claim,
// including a client that hung up mid-run — hence the deferred release on a
// context detached from the caller's. A cancelled invoke that skips the release
// leaves a bound pod in the pool and the pool shrinks by one, silently, per
// cancelled request. That is the failure that empties a warm pool overnight.
func (r *Runner) Invoke(ctx context.Context, req Request) (Result, error) {
	rt, ok := LookupRuntime(req.Runtime)
	if !ok {
		return Result{}, fmt.Errorf("%w %q: use one of %s", ErrNoRuntime, req.Runtime, strings.Join(Runtimes(), ", "))
	}
	if strings.TrimSpace(req.Code) == "" {
		return Result{}, errors.New("code is empty")
	}
	timeout := clamp(req.TimeoutSec)

	lease, err := r.M.Claim(ctx, machines.Spec{
		Org: req.Org,
		// ALWAYS exec. The machines layer exempts this class from its
		// one-live-machine-per-project rule, because a class with a project
		// volume is exclusive per project — asking for `dev` here would make
		// the second concurrent invoke of a function a 409 instead of an
		// answer. The lease length is the only thing that distinguishes a
		// function from a coding session, and this is where it is chosen.
		Class:   machines.ClassExec,
		Project: req.Name,
		TTLSec:  int((time.Duration(timeout)*time.Second + leaseSlack).Seconds()),
	})
	if err != nil {
		return Result{}, err
	}
	defer func() {
		// context.WithoutCancel: the caller's context is very often already
		// done by the time we get here — that is precisely the case that leaks.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseGrace)
		defer cancel()
		if err := r.M.Release(rctx, lease); err != nil {
			// Logged, not returned. The invoke's result is the caller's answer
			// and a give-back failure does not change it; the lease TTL is what
			// actually recovers the machine.
			r.Log.Error("release failed; machine held until its lease expires",
				"machine", lease.ID, "org", req.Org, "fn", req.Name, "err", err)
		}
	}()

	file := sourceName(req.Name, rt.Ext)
	// The write path is machine-project-relative and leading-slashed, the argv
	// path is bare: the filesystem surface and the process surface address the
	// same file the way each of them spells it.
	if err := r.M.Write(ctx, lease, "/"+file, req.Code); err != nil {
		return Result{MachineID: lease.ID, Runtime: rt.Name}, err
	}

	res, err := r.M.Exec(ctx, lease, machines.ExecRequest{
		Argv:       rt.Argv(file),
		Stdin:      req.Input,
		TimeoutSec: timeout,
	})
	return Result{ExecResult: res, MachineID: lease.ID, Runtime: rt.Name}, err
}

func clamp(sec int) int {
	switch {
	case sec <= 0:
		return DefaultTimeoutSec
	case sec > MaxTimeoutSec:
		return MaxTimeoutSec
	default:
		return sec
	}
}

// sourceName is the file the code is written to. The function's name is kept in
// it because an operator reading a machine's filesystem should be able to tell
// what ran, and the random suffix is because two invocations must never be able
// to address the same file even if they somehow share a machine.
//
// Every byte of the caller's name that is not [a-z0-9_-] is dropped rather than
// escaped. This string becomes a path AND an argv element, so "sanitise" here
// means "cannot be anything but a bare filename" — no dots, so no traversal and
// no second extension; no leading dash, so it cannot be read as a flag.
func sourceName(name, ext string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(name) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_', c == '-':
			b.WriteRune(c)
		}
	}
	safe := strings.TrimLeft(b.String(), "-")
	if len(safe) > 48 {
		safe = safe[:48]
	}
	if safe == "" {
		safe = "fn"
	}
	return safe + "-" + newID() + ext
}

func newID() string {
	var b [6]byte
	// crypto/rand.Read cannot fail on any supported platform; since Go 1.24 it
	// panics rather than returning an error, so there is no branch to take here.
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
