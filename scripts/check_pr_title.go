//go:build ignore

package main

import (
	"flag"
	"fmt"
	"os"

	"go.codycody31.dev/gobullmq/internal/compatibility"
)

func main() {
	title := flag.String("title", os.Getenv("PR_TITLE"), "pull request title")
	base := flag.String("base", os.Getenv("PR_BASE"), "pull request base branch")
	flag.Parse()
	if err := compatibility.CheckPRTitle(*title, *base); err != nil {
		fmt.Fprintf(os.Stderr, "pr-title: %v\n", err)
		os.Exit(1)
	}
}
