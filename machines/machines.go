// Package machines is fn's view of the machines layer: claim a machine, put a
// file on it, run something, give it back.
//
// It is a CLIENT and only a client. There is no pool in this package, no
// scheduler, no queue, no image builder, and adding one is the failure mode
// this whole repo exists to avoid — a function runtime that grows its own pool
// manager has re-implemented Fission's poolmgr, which is the thing we deleted.
//
// The warm pool already exists, once, in the machines layer: it lists idle pods
// by label and binds one with a label patch. fn asks that layer for a machine
// exactly the way an agent session or a code-exec call does. Same object, same
// pool, different lease length — that is the entire distinction between the
// three products built on it.
//
// The interface is four verbs because a function invoke needs exactly four
// things. If a fifth appears, check that it is not a lifetime concern in
// disguise; those belong to the machines layer, not here.
package machines

import "context"

// Class is the LIFETIME of a machine, not the workload that runs on it.
//
// The machines layer treats these differently in one respect that matters here:
// `exec` is exempt from the one-live-machine-per-project rule, because it has no
// project volume to contend over. A function invoke is therefore always `exec`,
// and two invocations of the same function can run at once. `dev` and `desktop`
// carry a volume and are exclusive per project — a function must never ask for
// one, or the second concurrent invoke gets a 409 instead of an answer.
const (
	ClassExec    = "exec"    // seconds, no volume, concurrent — a function invoke
	ClassDev     = "dev"     // hours, project volume, exclusive — a coding session
	ClassDesktop = "desktop" // same as dev with a display
)

// Spec is what fn asks the machines layer for. It is deliberately small: fn
// does not choose an image, a node, a size or a placement. Those are the
// machines layer's decisions and fn has no business having an opinion.
type Spec struct {
	Org     string // tenant. Machines are per-org and fn never crosses that line.
	Class   string // one of the Class* constants above
	Project string // the machine's label — for a function, its name
	TTLSec  int    // a floor under leaks: the machines layer reaps past this
}

// Lease is a claimed machine. It is what Release needs and nothing more; the
// address is the machines layer's business, since every call goes back through
// it rather than direct to the machine.
type Lease struct {
	ID     string
	Org    string
	Status string
}

// ExecRequest runs one command. Argv only — never a shell string.
//
// The shell form exists in the machines layer's own contract because a coding
// agent's steps are shell lines. A function invoke has no such excuse: the argv
// is built from the runtime table, and passing user-supplied text through a
// shell here would make `; rm -rf /` a feature of the runtime rather than of the
// submitted program. The submitted program is already free to do that inside the
// sandbox; it must not be able to do it to the argv that starts it.
type ExecRequest struct {
	Argv       []string          `json:"argv"`
	Cwd        string            `json:"cwd,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Stdin      string            `json:"stdin,omitempty"`
	TimeoutSec int               `json:"timeoutSec,omitempty"`
}

// ExecResult is one finished command.
//
// A non-zero ExitCode is a SUCCESSFUL call carrying a failed program. "Your
// function threw" and "the machine is broken" are different facts, and a caller
// that cannot tell them apart retries the wrong one — so the first is a 200 with
// a non-zero code and the second is a transport error.
type ExecResult struct {
	ExitCode   int    `json:"exitCode"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	DurationMs int64  `json:"durationMs"`
	TimedOut   bool   `json:"timedOut,omitempty"`
}

// Machines is the whole dependency. fn is written against this and nothing
// else, which is what keeps the invoke path testable without a cluster and what
// keeps a second pool from ever being tempting: there is no seam here for one.
type Machines interface {
	Claim(ctx context.Context, s Spec) (Lease, error)
	Write(ctx context.Context, l Lease, path, content string) error
	Exec(ctx context.Context, l Lease, r ExecRequest) (ExecResult, error)
	Release(ctx context.Context, l Lease) error
}
