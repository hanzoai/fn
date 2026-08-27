package fn

import (
	"sort"
	"strings"
)

// Runtime is how a source file becomes an argv. That is the whole of it.
//
// There is no build step here and there will not be one. Fission had a builder
// — source to package to deployment, an image per function, a registry to hold
// them — and it is the single largest thing we dropped when we deleted it. What
// replaces it is this table: an interpreter that is already in the machine image
// reads a file we just wrote. That is why the runtimes are all interpreted, and
// why adding Go or Rust here is not a small change but a decision to build a
// builder. Do not make it in this file.
type Runtime struct {
	Name string   // canonical name
	Ext  string   // source extension, with the dot
	argv []string // the interpreter invocation; the source basename is appended
}

// Argv is the command that runs `file`. The file is passed as the LAST
// argument and never interpolated into a string, so a name cannot become a
// flag's value or a second command.
func (r Runtime) Argv(file string) []string {
	out := make([]string, 0, len(r.argv)+1)
	out = append(out, r.argv...)
	return append(out, file)
}

// runtimes is the closed set. Keys are every name a caller may use, and the
// short aliases are not a convenience — they are the contract. The
// /v1/functions registry stores `python`/`node`/`deno` and its executor client
// translates them to `py`/`js`/`ts` on the wire, so a table that knew only one
// of the two spellings would answer half the traffic with "unknown runtime".
var runtimes = map[string]Runtime{
	"python": {Name: "python", Ext: ".py", argv: []string{"python3"}},
	"py":     {Name: "python", Ext: ".py", argv: []string{"python3"}},
	"node":   {Name: "node", Ext: ".js", argv: []string{"node"}},
	"js":     {Name: "node", Ext: ".js", argv: []string{"node"}},
	// Deno is given the network and the filesystem because the sandbox is the
	// containment boundary, not Deno's permission prompts. A second, weaker
	// boundary inside the first one only produces functions that fail for
	// reasons the caller cannot see.
	"deno": {Name: "deno", Ext: ".ts", argv: []string{"deno", "run", "-A"}},
	"ts":   {Name: "deno", Ext: ".ts", argv: []string{"deno", "run", "-A"}},
	"bash": {Name: "bash", Ext: ".sh", argv: []string{"bash"}},
	"sh":   {Name: "bash", Ext: ".sh", argv: []string{"bash"}},
}

// LookupRuntime resolves a caller's spelling. Case and surrounding space are
// forgiven because they come from JSON written by hand; nothing else is.
func LookupRuntime(name string) (Runtime, bool) {
	r, ok := runtimes[strings.ToLower(strings.TrimSpace(name))]
	return r, ok
}

// Runtimes lists the canonical names, sorted, for the error a caller gets when
// they name one that does not exist. A refusal that does not say what WOULD have
// worked makes the caller guess.
func Runtimes() []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range runtimes {
		if !seen[r.Name] {
			seen[r.Name] = true
			out = append(out, r.Name)
		}
	}
	sort.Strings(out)
	return out
}
