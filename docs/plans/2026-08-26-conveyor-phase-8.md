# Phase 8 — Proto and codegen

**Goal:** define the agent wire contract once, in a schema, and generate Go for
it — so phases 9–10 argue about behaviour instead of JSON field names.

*Done when:* `make proto` regenerates committed code from `proto/conveyor/v1`,
a compatibility test fails if the wire format changes silently, and `buf lint`
passes in CI.

---

## How to read this plan

Same deal as phase 7: tests and configs are given verbatim; implementation is
requirements and hints. Where more than one answer is defensible, the options
are argued, not asserted.

---

## Global constraints

1. **The `.proto` file is the contract.** Generated `.pb.go` files are build
   artifacts that happen to be committed. Every review question about a field
   is answered by editing the schema, never the generated file.
2. **Field numbers are forever.** Renaming a field is free; changing its number
   is a breaking change; deleting one without `reserved` invites reuse bugs.
   This is the phase's core lesson — see Task 3's test.
3. **Generated code is not `internal/`.** Both binaries will import it, and
   `internal/` blocks external imports only — but the *agent* binary does not
   exist yet, and keeping generated code outside `internal/` marks it as
   shared-by-construction. It lives under `gen/`.
4. **Toolchain pinned in `go.mod`, not on the wiki.** Go 1.24+ `tool`
   directives make `go tool buf` work for everyone who can build the repo.
5. No hand-written serialization anywhere. If a test wants specific bytes, it
   encodes through the generated API.

---

## Part 0 — The primer

### 0.1 Why a schema at all

You could ship JSON over the gRPC stream. What you would lose:

- **Types across languages.** `payload` is `bytes` on the wire, `[]byte` in Go,
  `Uint8Array` elsewhere — declared once, checked at compile time.
- **Evolution rules.** Protobuf's numbering scheme is what lets phase 12 add a
  field without redeploying every agent. Ad-hoc JSON drifts silently.
- **A place for the contract to live.** The `.proto` becomes the document the
  agent team reads first.

What you pay: a codegen step, two plugins, and generated files in review diffs.

### 0.2 `oneof`: a tagged union that enforces itself

An agent connection carries several *kinds* of message down one stream. Two
encodings:

```protobuf
// A: flat fields, "exactly one is set" enforced by humans
message AgentToServer {
  Heartbeat heartbeat = 1;
  Progress progress = 2;
  Result result = 3;
}

// B: oneof — exactly-one is enforced by the type system
message AgentToServer {
  oneof payload {
    Heartbeat heartbeat = 1;
    Progress progress = 5;
    Result result = 6;
  }
}
```

B wins, and not for style. In B, generated Go gives you
`msg.GetPayload().(type)` — a switch with a nil-safe default for unknown
variants, which is also your forward-compatibility story when a newer server
sends a variant your older agent doesn't know. In A, "which field is live?" is
a runtime convention; every handler re-invents it wrong.

Note the numbers above are **not sequential**. That is deliberate: leave gaps
where future variants might slot in logically. Numbers cost nothing; running
out of adjacency costs history.

### 0.3 Field number rules (the whole compatibility story)

| Change | Compatible? |
|---|---|
| Add a field with a fresh number | yes |
| Rename a field | yes |
| Delete a field | only with `reserved` on its number |
| Reuse a deleted field's number | **never** — old clients see garbage types |
| Change a field's number | **breaking** — it *is* a different field |

`reserved 4, 7 to 9; reserved "old_name";` documents the graveyard right where
the next editor will look.

### 0.4 buf in three files

- `buf.yaml` — module config + lint/breaking rules for `buf lint` / `buf breaking`.
- `buf.gen.yaml` — which plugins run and where output goes.
- Plugins themselves — `protoc-gen-go`, `protoc-gen-go-grpc`; installed via
  `tool` directives, invoked by buf from `go tool`.

`buf breaking --against '.git#branch=main,subdir=proto'` compares the current schema
against mainline and fails CI on incompatible edits. This turns rule-table 0.3
from documentation into enforcement.

### 0.5 `go tool` directives

```bash
go get -tool github.com/bufbuild/buf/cmd/buf@v1.50.0
go get -tool google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.7
go get -tool google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
```

Each adds a `tool` directive to `go.mod`; afterwards `go tool buf`,
`go tool protoc-gen-go` resolve to those exact versions on any machine that
can build the module. No README step, no mise entry, no "works on my machine".

(Versions above: take whatever latest the registry offers when you run this —
then commit what you got. Pinning matters; the specific pins don't.)

---

## Task 1 — Toolchain

**Goal:** `make proto` regenerates everything from scratch on a clean clone.

### Steps

1. `go get -tool` the three binaries above.
2. Create `proto/conveyor/v1/conveyor.proto` (Task 2 fills the messages).
3. Create `buf.yaml` and `buf.gen.yaml`:

```yaml
# buf.yaml
version: v2
modules:
  - path: proto
lint:
  use: [STANDARD]
breaking:
  use: [FILE]
```

```yaml
# buf.gen.yaml
version: v2
plugins:
  - local: ["go", "tool", "protoc-gen-go"]
    out: gen/conveyor/v1
    opt: paths=source_relative
  - local: ["go", "tool", "protoc-gen-go-grpc"]
    out: gen/conveyor/v1
    opt: paths=source_relative
```

4. Wire the Makefile:

```makefile
proto: ## regenerate gRPC code from the schemas
	go tool buf format -w
	go tool buf lint
	go tool buf generate
```

5. Commit the generated `.pb.go` files. (Not vendoring them is defensible for
   libraries; for an application, committing keeps `go build` hermetic and
   reviews diffable.)

### Done when

`rm -rf gen && make proto && go build ./...` succeeds, and a fresh clone never
needs a global protobuf install.

---

## Task 2 — The schema

**Goal:** the whole agent protocol in one reviewed file.

### Steps

1. Write `proto/conveyor/v1/conveyor.proto`.
2. `make proto`, read the generated code once — particularly how `oneof`
   became an interface plus wrapper structs, and what `nil` payload means.
3. Commit.

### The shape

```protobuf
syntax = "proto3";

package conveyor.v1;

option go_package = "github.com/mddemin/conveyor/gen/conveyor/v1;conveyorv1";

// ServerToAgent is one frame pushed to an agent's open stream.
message ServerToAgent {
  oneof payload {
    // Assignment hands the agent a job it now holds a server-side lease on.
    Assignment assignment = 1;

    // CancelJob asks the agent to stop a job it is running. Best effort:
    // the lease expiring is the real deadline.
    CancelJob cancel_job = 3;
  }
}

message Assignment {
  string job_id = 1;
  string type = 2;     // selects the executor agent-side
  bytes payload = 3;   // opaque to the queue, meaningful to the executor
}

message CancelJob {
  string job_id = 1;
}

// AgentToServer is one frame an agent sends up its open stream.
message AgentToServer {
  oneof payload {
    Heartbeat heartbeat = 1;   // liveness only; carries nothing yet
    Progress progress = 5;     // advisory, never persisted in phase 8
    Result result = 6;         // terminal: success iff error is empty
  }
}

message Heartbeat {}

message Progress {
  string job_id = 1;
  string message = 2;
}

message Result {
  string job_id = 1;
  string error = 2;            // empty means success
}
```

### Decisions worth writing down in the PR description

- **Envelope-plus-oneof, not one RPC per direction per kind.** One long-lived
  bidirectional stream (`Assign`, arriving in phase 9) multiplexes everything;
  reconnect logic exists once, ordering is guaranteed, and the server can push
  cancellations without the agent polling.
- **Field-number gaps (no 2 in either oneof)** — room for the next variant
  without renumbering neighbours.
- **`Result.error` as `string`, not a nested error message.** Errors cross the
  boundary as text because the retry policy already treats causes as opaque;
  typed errors would imply typed handling we deliberately do at the edge only.
- **No service block yet.** Phase 9 adds `rpc Assign(stream AgentToServer)
  returns (stream ServerToAgent);`. Adding an RPC is compatible; shipping the
  messages first lets phase 9's server tests compile against real types.

### Done when

`buf lint` passes, `gen/conveyor/v1/` contains the two generated files, and
`go build ./...` accepts them.

---

## Task 3 — The compatibility test

**Goal:** prove the wire format is frozen, mechanically.

### The idea

Golden bytes: encode representative messages once, commit the encoding, assert
every future regeneration produces byte-identical output. Any change that
alters the wire — renumbered fields, changed types, deleted-and-reused slots —
fails here with a diff you can read.

### Tests

```go
package conveyorv1_test

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/mddemin/conveyor/gen/conveyor/v1"
	"google.golang.org/protobuf/proto"
)

// representative covers every oneof branch and every scalar shape the
// protocol uses. Extend it whenever the schema grows a variant.
func representative() []*pb.ServerToAgent /* and AgentToServer; see below */ {
	return nil // two functions in the real file, one per direction
}

// TestWireFormatIsFrozen regenerates golden files with:
//
//	OVERWRITE_GOLDEN=1 go test ./internal/rpc/...
//
// and otherwise asserts the committed bytes still match. If this test fails,
// either the change is breaking (fix the schema) or it is intentional
// (regenerate, review the hex diff, commit both).
func TestWireFormatIsFrozen(t *testing.T) {
	for _, tc := range fixtures() {
		got, err := proto.Marshal(tc.msg)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		path := filepath.Join("testdata", tc.name+".hex")
		if os.Getenv("OVERWRITE_GOLDEN") != "" {
			os.MkdirAll("testdata", 0o755)
			os.WriteFile(path, []byte(hex.Dump(got)), 0o644)
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v (run OVERWRITE_GOLDEN=1 once)", tc.name, err)
		}
		var buf bytes.Buffer
		hex.DumpWriter(got, &buf)
		if !bytes.Equal(buf.Bytes(), want) {
			t.Errorf("%s wire format drifted:\n--- committed\n+++ current\n%s\n%s",
				tc.name, want, buf.Bytes())
		}
	}
}

// TestUnknownVariantIsForwardCompatible proves the forward-compat claim from
// the primer: decoding a frame containing an unknown oneof case yields a
// non-nil message whose GetPayload() is nil, and no error.
func TestUnknownVariantIsForwardCompatible(t *testing.T) {
	// Hand-build a frame whose field number is not in the schema (e.g. 99),
	// decode it as AgentToServer, assert err == nil, msg != nil,
	// msg.GetPayload() == nil.
}
```

Where the fixtures live: `internal/rpc/` gets created in this phase as the
future home of the gRPC server/client (phase 9 fills it). Schema tests belong
next to the code that will consume the messages, not inside `gen/`, which is
owned by the generator.

### Done when

- Deleting a field's `reserved` line and reusing its number makes
  `TestWireFormatIsFrozen` fail with a readable hex diff.
- Adding a brand-new variant makes only the golden files change (after explicit
  regeneration), proving additive changes are cheap.
- `TestUnknownVariantIsForwardCompatible` passes without any schema edit.

---

## Task 4 — Breaking-change enforcement (small, do not skip)

Add to CI, after staticcheck:

```yaml
      - name: proto breaking check
        run: go tool buf breaking proto --against '.git#branch=main,subdir=proto'
```

and to the Makefile:

```makefile
proto-breaking: ## fail if the schema broke wire compatibility vs main
	go tool buf breaking proto --against '.git#branch=main,subdir=proto'
```

First PR that tries to renumber a field now cannot merge. That is the phase's
lesson wearing a seatbelt.

---

## Phase review checklist

```bash
gofmt -l .
go vet ./...
staticcheck -checks=all ./...
rm -rf gen && make proto && git diff --stat   # regeneration is reproducible
go test -race ./...
go tool buf lint
go tool buf breaking proto --against '.git#branch=main,subdir=proto'
```

By eye:

- `gen/` contains only files named in `buf.gen.yaml`'s outputs.
- No `internal/` import appears in `gen/**`.
- The `.proto` comments explain *semantics* (who sends what, when), not syntax.
- `google.golang.org/grpc` may appear in `go.mod` after generation pulls in
  grpc's registration code — sanctioned dependency, spec §1.

---

## What phase 9 inherits

Real message types for both pump directions, a frozen wire format enforced by
golden files and CI, and an empty `internal/rpc` waiting for the two-pump
server. Phase 9 adds the service method and the agent registry; the schema
work is already behind it.
