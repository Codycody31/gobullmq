//go:build ignore

package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"go.codycody31.dev/gobullmq/internal/compatibility"
)

func main() {
	var (
		root      = flag.String("root", ".", "repository root")
		branch    = flag.String("branch", "", "branch to validate; defaults to the current Git branch")
		upstream  = flag.String("upstream-dir", "", "checked-out BullMQ repository to compare; defaults to a temporary clone")
		generated = flag.Bool("generated", true, "regenerate Lua wrappers and require a clean diff")
	)
	flag.Parse()

	absRoot, err := filepath.Abs(*root)
	check(err)
	metadata, err := compatibility.Load(filepath.Join(absRoot, "compatibility", "bullmq.json"))
	check(err)

	currentBranch := *branch
	if currentBranch == "" {
		currentBranch, err = commandOutput(absRoot, "git", "branch", "--show-current")
		check(err)
	}
	check(metadata.CheckCurrentBranch(currentBranch))
	absUpstream := *upstream
	if absUpstream == "" {
		absUpstream, err = os.MkdirTemp("", "gobullmq-bullmq-")
		check(err)
		defer os.RemoveAll(absUpstream)
		_, err = commandOutput(absRoot, "git", "clone", "--depth", "1", "--branch", metadata.BullMQ.Tag, "https://github.com/taskforcesh/bullmq.git", absUpstream)
		check(err)
	} else {
		absUpstream, err = filepath.Abs(absUpstream)
		check(err)
	}
	check(metadata.CheckUpstream(absRoot, absUpstream))
	check(metadata.CheckNodeLock(absRoot))
	check(metadata.CheckDocuments(absRoot))
	if *generated {
		checkGeneratedSet(absRoot)
		output, err := commandOutput(absRoot, "git", "diff", "--exit-code", "HEAD", "--", "internal/lua/*_lua.go")
		if err != nil {
			check(fmt.Errorf("generated Lua wrappers have uncommitted changes before regeneration:\n%s", output))
		}
		_, err = commandOutput(absRoot, "go", "run", "./scripts/generate_lua.go")
		check(err)
		output, err = commandOutput(absRoot, "git", "diff", "--exit-code", "HEAD", "--", "internal/lua/*_lua.go")
		if err != nil {
			check(fmt.Errorf("generated Lua wrappers are stale:\n%s", output))
		}
	}
	fmt.Printf("compatibility: BullMQ %s (%s), Go line %s\n", metadata.BullMQ.Version, metadata.BullMQ.Commit, metadata.ReleaseLine)
}

func checkGeneratedSet(root string) {
	sources, err := filepath.Glob(filepath.Join(root, "internal", "lua", "*.lua"))
	check(err)
	wrappers, err := filepath.Glob(filepath.Join(root, "internal", "lua", "*_lua.go"))
	check(err)

	expected := make(map[string]bool, len(sources))
	for _, source := range sources {
		name := strings.Split(filepath.Base(source), "-")[0] + "_lua.go"
		expected[name] = true
	}
	actual := make(map[string]bool, len(wrappers))
	for _, wrapper := range wrappers {
		actual[filepath.Base(wrapper)] = true
	}
	for name := range expected {
		if !actual[name] {
			check(fmt.Errorf("generated Lua wrapper %s is missing", name))
		}
	}
	for name := range actual {
		if !expected[name] {
			check(fmt.Errorf("generated Lua wrapper %s has no vendored script", name))
		}
	}
}

func commandOutput(dir, name string, args ...string) (string, error) {
	command := exec.Command(name, args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		return strings.TrimSpace(string(output)), fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(output)), nil
}

func check(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "compatibility: %v\n", err)
	os.Exit(1)
}
