package compatibility

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+(?:\.[0-9]+)?$`)
	commitPattern  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	branchPattern  = regexp.MustCompile(`^release/v([0-9]+\.[0-9]+)-bullmq-v([0-9]+\.[0-9]+\.[0-9]+)$`)
)

// Metadata is the branch-local BullMQ compatibility contract.
type Metadata struct {
	SchemaVersion int    `json:"schemaVersion"`
	ReleaseLine   string `json:"releaseLine"`
	Branch        string `json:"branch"`
	BullMQ        struct {
		Version string `json:"version"`
		Tag     string `json:"tag"`
		Commit  string `json:"commit"`
	} `json:"bullmq"`
	Go struct {
		Minimum string `json:"minimum"`
		CI      string `json:"ci"`
	} `json:"go"`
	Node struct {
		Version string `json:"version"`
	} `json:"node"`
	Redis struct {
		Version string   `json:"version"`
		Mode    []string `json:"mode"`
	} `json:"redis"`
}

// Load reads and validates a compatibility manifest.
func Load(path string) (Metadata, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Metadata{}, fmt.Errorf("read manifest: %w", err)
	}

	var metadata Metadata
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return Metadata{}, fmt.Errorf("decode manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Metadata{}, fmt.Errorf("decode manifest: trailing JSON value")
		}
		return Metadata{}, fmt.Errorf("decode manifest trailing data: %w", err)
	}
	if err := metadata.Validate(); err != nil {
		return Metadata{}, err
	}
	return metadata, nil
}

// Validate checks the closed manifest schema and cross-field invariants.
func (metadata Metadata) Validate() error {
	if metadata.SchemaVersion != 1 {
		return fmt.Errorf("schemaVersion = %d, want 1", metadata.SchemaVersion)
	}
	if !strings.HasPrefix(metadata.ReleaseLine, "v") || !versionPattern.MatchString(strings.TrimPrefix(metadata.ReleaseLine, "v")) || strings.Count(metadata.ReleaseLine, ".") != 1 {
		return fmt.Errorf("releaseLine %q must be v<major>.<minor>", metadata.ReleaseLine)
	}
	if !versionPattern.MatchString(metadata.BullMQ.Version) || strings.Count(metadata.BullMQ.Version, ".") != 2 {
		return fmt.Errorf("bullmq.version %q must be <major>.<minor>.<patch>", metadata.BullMQ.Version)
	}
	if metadata.BullMQ.Tag != "v"+metadata.BullMQ.Version {
		return fmt.Errorf("bullmq.tag %q does not match version %q", metadata.BullMQ.Tag, metadata.BullMQ.Version)
	}
	if !commitPattern.MatchString(metadata.BullMQ.Commit) {
		return fmt.Errorf("bullmq.commit %q must be a 40-character lowercase SHA", metadata.BullMQ.Commit)
	}
	if err := metadata.ValidateBranch(metadata.Branch); err != nil {
		return err
	}
	for field, value := range map[string]string{
		"go.minimum":    metadata.Go.Minimum,
		"go.ci":         metadata.Go.CI,
		"node.version":  metadata.Node.Version,
		"redis.version": metadata.Redis.Version,
	} {
		if !versionPattern.MatchString(value) {
			return fmt.Errorf("%s %q is not a numeric version", field, value)
		}
	}
	if len(metadata.Redis.Mode) != 2 || metadata.Redis.Mode[0] != "standalone" || metadata.Redis.Mode[1] != "cluster" {
		return fmt.Errorf("redis.mode = %q, want [standalone cluster]", metadata.Redis.Mode)
	}
	return nil
}

// ValidateBranch verifies a release branch against both version axes.
func (metadata Metadata) ValidateBranch(branch string) error {
	matches := branchPattern.FindStringSubmatch(branch)
	if matches == nil {
		return fmt.Errorf("branch %q must match release/v<go-major>.<go-minor>-bullmq-v<major>.<minor>.<patch>", branch)
	}
	if "v"+matches[1] != metadata.ReleaseLine {
		return fmt.Errorf("branch Go line v%s does not match manifest %s", matches[1], metadata.ReleaseLine)
	}
	if matches[2] != metadata.BullMQ.Version {
		return fmt.Errorf("branch BullMQ version %s does not match manifest %s", matches[2], metadata.BullMQ.Version)
	}
	return nil
}

// CheckCurrentBranch validates release branches and ignores development branches.
func (metadata Metadata) CheckCurrentBranch(branch string) error {
	if !strings.HasPrefix(branch, "release/") {
		return nil
	}
	return metadata.ValidateBranch(branch)
}

// CheckDocuments ensures the human-readable compatibility surfaces stay aligned.
func (metadata Metadata) CheckDocuments(root string) error {
	checks := map[string][]string{
		"README.md": {
			metadata.BullMQ.Version,
			metadata.BullMQ.Commit,
			metadata.ReleaseLine,
		},
		"doc.go": {
			metadata.BullMQ.Version,
			metadata.BullMQ.Commit,
			metadata.ReleaseLine,
			"COMPATIBILITY.md",
		},
		"COMPATIBILITY.md": {
			metadata.BullMQ.Version,
			metadata.BullMQ.Commit,
			metadata.ReleaseLine + ".x",
			metadata.Branch,
		},
	}
	for path, values := range checks {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		for _, value := range values {
			if !bytes.Contains(raw, []byte(value)) {
				return fmt.Errorf("%s does not contain compatibility value %q", path, value)
			}
		}
	}
	return nil
}

// CheckNodeLock verifies the exact Node interoperability dependency versions.
func (metadata Metadata) CheckNodeLock(root string) error {
	wants := map[string]string{
		"bullmq":  metadata.BullMQ.Version,
		"ioredis": "5.3.2",
	}
	packagePath := filepath.Join(root, "integration", "node", "package.json")
	packageRaw, err := os.ReadFile(packagePath)
	if err != nil {
		return fmt.Errorf("read node package file: %w", err)
	}
	var packageFile struct {
		Dependencies map[string]string `json:"dependencies"`
	}
	if err := json.Unmarshal(packageRaw, &packageFile); err != nil {
		return fmt.Errorf("decode node package file: %w", err)
	}
	for name, want := range wants {
		if got := packageFile.Dependencies[name]; got != want {
			return fmt.Errorf("node dependency %s pin = %q, want %q", name, got, want)
		}
	}

	path := filepath.Join(root, "integration", "node", "package-lock.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read node lockfile: %w", err)
	}
	var lock struct {
		Packages map[string]struct {
			Version string `json:"version"`
		} `json:"packages"`
	}
	if err := json.Unmarshal(raw, &lock); err != nil {
		return fmt.Errorf("decode node lockfile: %w", err)
	}
	for name, want := range wants {
		lockName := "node_modules/" + name
		got, ok := lock.Packages[lockName]
		if !ok {
			return fmt.Errorf("node lockfile is missing %s", lockName)
		}
		if got.Version != want {
			return fmt.Errorf("node lockfile %s version = %s, want %s", lockName, got.Version, want)
		}
	}
	return nil
}

// CheckUpstream verifies the upstream ref and every vendored Lua source byte.
func (metadata Metadata) CheckUpstream(root, upstream string) error {
	head, err := gitOutput(upstream, "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("resolve upstream HEAD: %w", err)
	}
	if head != metadata.BullMQ.Commit {
		return fmt.Errorf("upstream HEAD = %s, want %s", head, metadata.BullMQ.Commit)
	}
	tagCommit, err := gitOutput(upstream, "rev-list", "-n", "1", metadata.BullMQ.Tag)
	if err != nil {
		return fmt.Errorf("resolve upstream tag %s: %w", metadata.BullMQ.Tag, err)
	}
	if tagCommit != metadata.BullMQ.Commit {
		return fmt.Errorf("upstream tag %s resolves to %s, want %s", metadata.BullMQ.Tag, tagCommit, metadata.BullMQ.Commit)
	}

	patterns := []struct {
		local    string
		upstream string
	}{
		{filepath.Join(root, "internal", "lua", "*.lua"), filepath.Join(upstream, "src", "commands")},
		{filepath.Join(root, "internal", "lua", "includes", "*.lua"), filepath.Join(upstream, "src", "commands", "includes")},
	}
	for _, pattern := range patterns {
		paths, err := filepath.Glob(pattern.local)
		if err != nil {
			return fmt.Errorf("list vendored Lua files: %w", err)
		}
		if len(paths) == 0 {
			return fmt.Errorf("no vendored Lua files matched %s", pattern.local)
		}
		for _, localPath := range paths {
			upstreamPath := filepath.Join(pattern.upstream, filepath.Base(localPath))
			localRaw, err := os.ReadFile(localPath)
			if err != nil {
				return fmt.Errorf("read %s: %w", localPath, err)
			}
			upstreamRaw, err := os.ReadFile(upstreamPath)
			if err != nil {
				return fmt.Errorf("upstream script missing for %s: %w", filepath.Base(localPath), err)
			}
			if !bytes.Equal(localRaw, upstreamRaw) {
				return fmt.Errorf("vendored Lua file %s differs from BullMQ %s", localPath, metadata.BullMQ.Tag)
			}
		}
	}
	return nil
}

func gitOutput(dir string, args ...string) (string, error) {
	command := exec.Command("git", append([]string{"-C", dir}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %s", strings.Join(args, " "), strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}
