package rpc

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/mddemin/conveyor/gen/conveyor/v1"
	"google.golang.org/protobuf/proto"
)

func fixtures() []struct {
	name string
	msg  proto.Message
} {
	return []struct {
		name string
		msg  proto.Message
	}{
		{
			name: "server_to_agent_assignment",
			msg: &pb.ServerToAgent{
				Payload: &pb.ServerToAgent_Assignment{
					Assignment: &pb.Assignment{
						JobId:   "j-1",
						Type:    "resize",
						Payload: []byte{0x01, 0x02},
						Attempt: 1,
					},
				},
			},
		},
		{
			name: "server_to_agent_cancel_job",
			msg: &pb.ServerToAgent{
				Payload: &pb.ServerToAgent_CancelJob{
					CancelJob: &pb.CancelJob{JobId: "j-2"},
				},
			},
		},
		{
			name: "agent_to_server_heartbeat",
			msg: &pb.AgentToServer{
				Payload: &pb.AgentToServer_Heartbeat{
					Heartbeat: &pb.Heartbeat{},
				},
			},
		},
		{
			name: "agent_to_server_progress",
			msg: &pb.AgentToServer{
				Payload: &pb.AgentToServer_Progress{
					Progress: &pb.Progress{JobId: "j-3", Message: "halfway"},
				},
			},
		},
		{
			name: "agent_to_server_result_success",
			msg: &pb.AgentToServer{
				Payload: &pb.AgentToServer_Result{
					Result: &pb.Result{JobId: "j-4", Error: "", Attempt: 0},
				},
			},
		},
		{
			name: "agent_to_server_result_failure",
			msg: &pb.AgentToServer{
				Payload: &pb.AgentToServer_Result{
					Result: &pb.Result{JobId: "j-5", Error: "oops", Attempt: 5},
				},
			},
		},
		{
			name: "agent_to_server_result_permanent",
			msg: &pb.AgentToServer{
				Payload: &pb.AgentToServer_Result{
					Result: &pb.Result{JobId: "j-6", Error: "malformed payload", Attempt: 0, Permanent: true},
				},
			},
		},
		{
			name: "agent_to_server_register",
			msg: &pb.AgentToServer{
				Payload: &pb.AgentToServer_Register{
					Register: &pb.Register{AgentId: "a-1", Capabilities: []string{"rewrite", "log"}},
				},
			},
		},
	}
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
		gotHex := hex.Dump(got)
		if os.Getenv("OVERWRITE_GOLDEN") != "" {
			if err := os.MkdirAll("testdata", 0o755); err != nil {
				t.Fatalf("mkdir testdata: %v", err)
			}
			if err := os.WriteFile(path, []byte(gotHex), 0o644); err != nil {
				t.Fatalf("write golden: %v", err)
			}
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v (run OVERWRITE_GOLDEN=1 once)", tc.name, err)
		}
		if string(want) != gotHex {
			t.Errorf("%s wire format drifted:\n--- committed\n+++ current\n%s\n%s",
				tc.name, want, gotHex)
		}
	}
}

// TestUnknownVariantIsForwardCompatible proves the forward-compat claim from
// the primer: decoding a frame containing an unknown oneof case yields a
// non-nil message whose GetPayload() is nil, and no error.
func TestUnknownVariantIsForwardCompatible(t *testing.T) {
	// field 99, wire type 2 (length-delimited) => tag (99<<3|2)=794 => varint 0x9a 0x06
	raw := []byte{0x9a, 0x06, 0x01, 'x'}
	var msg pb.AgentToServer
	if err := proto.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("unmarshal unknown variant: %v", err)
	}
	if msg.GetPayload() != nil {
		t.Fatalf("expected nil payload for unknown variant, got %T", msg.GetPayload())
	}
}
