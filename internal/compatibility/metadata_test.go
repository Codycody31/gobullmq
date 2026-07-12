package compatibility

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsMalformedOrIncompleteManifest(t *testing.T) {
	valid, err := json.Marshal(validMetadata())
	if err != nil {
		t.Fatal(err)
	}
	var unknown map[string]any
	if err := json.Unmarshal(valid, &unknown); err != nil {
		t.Fatal(err)
	}
	unknown["unexpected"] = true
	withUnknown, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		raw  []byte
		want string
	}{
		{name: "malformed", raw: []byte(`{"schemaVersion":`), want: "decode manifest"},
		{name: "incomplete", raw: []byte(`{}`), want: "schemaVersion"},
		{name: "unknown field", raw: withUnknown, want: "unknown field"},
		{name: "trailing value", raw: append(valid, []byte(` {}`)...), want: "trailing JSON value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bullmq.json")
			if err := os.WriteFile(path, tt.raw, 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func validMetadata() Metadata {
	var metadata Metadata
	metadata.SchemaVersion = 1
	metadata.ReleaseLine = "v1.1"
	metadata.Branch = "release/v1.1-bullmq-v4.12.2"
	metadata.BullMQ.Version = "4.12.2"
	metadata.BullMQ.Tag = "v4.12.2"
	metadata.BullMQ.Commit = "a01bb0b0345509cde6c74843323de6b67729f310"
	metadata.Go.Minimum = "1.20"
	metadata.Go.CI = "1.26.5"
	metadata.Node.Version = "22.23.1"
	metadata.Redis.Version = "7.4"
	metadata.Redis.Mode = []string{"standalone", "cluster"}
	return metadata
}

func TestMetadataValidate(t *testing.T) {
	metadata := validMetadata()
	if err := metadata.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	tests := []struct {
		name string
		edit func(*Metadata)
		want string
	}{
		{name: "schema", edit: func(m *Metadata) { m.SchemaVersion = 2 }, want: "schemaVersion"},
		{name: "release line", edit: func(m *Metadata) { m.ReleaseLine = "v1.1.0" }, want: "releaseLine"},
		{name: "BullMQ tag", edit: func(m *Metadata) { m.BullMQ.Tag = "v4.12.3" }, want: "does not match"},
		{name: "commit", edit: func(m *Metadata) { m.BullMQ.Commit = "abc" }, want: "40-character"},
		{name: "modes", edit: func(m *Metadata) { m.Redis.Mode = []string{"cluster"} }, want: "redis.mode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := metadata
			tt.edit(&candidate)
			err := candidate.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestMetadataValidateBranch(t *testing.T) {
	metadata := validMetadata()
	for _, branch := range []string{
		"release/v1.2-bullmq-v4.12.2",
		"release/v1.1-bullmq-v4.13.0",
		"release/bullmq-v4.12.2",
	} {
		if err := metadata.ValidateBranch(branch); err == nil {
			t.Fatalf("ValidateBranch(%q) succeeded", branch)
		}
	}
	if err := metadata.CheckCurrentBranch("agent/compatibility"); err != nil {
		t.Fatalf("development branch: %v", err)
	}
}

func TestMetadataCheckNodeLock(t *testing.T) {
	root := t.TempDir()
	nodeDir := filepath.Join(root, "integration", "node")
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	packageFile := `{"dependencies":{"bullmq":"4.12.2","ioredis":"5.3.2"}}`
	if err := os.WriteFile(filepath.Join(nodeDir, "package.json"), []byte(packageFile), 0o644); err != nil {
		t.Fatal(err)
	}
	lock := `{"packages":{"node_modules/bullmq":{"version":"4.12.2"},"node_modules/ioredis":{"version":"5.3.2"}}}`
	if err := os.WriteFile(filepath.Join(nodeDir, "package-lock.json"), []byte(lock), 0o644); err != nil {
		t.Fatal(err)
	}
	metadata := validMetadata()
	if err := metadata.CheckNodeLock(root); err != nil {
		t.Fatalf("CheckNodeLock: %v", err)
	}
	metadata.BullMQ.Version = "4.12.3"
	if err := metadata.CheckNodeLock(root); err == nil {
		t.Fatal("CheckNodeLock succeeded with a mismatched BullMQ version")
	}
}
