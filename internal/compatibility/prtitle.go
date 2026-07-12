package compatibility

import (
	"fmt"
	"regexp"
	"strings"
)

var conventionalTitlePattern = regexp.MustCompile(`^(feat|fix|perf|refactor|docs|test|build|ci|chore|revert)(\([[:alnum:]_.@/-]+\))?(!)?: .+`)

// CheckPRTitle validates a squash title and the restrictions for fixed lines.
func CheckPRTitle(title, baseBranch string) error {
	matches := conventionalTitlePattern.FindStringSubmatch(title)
	if matches == nil {
		return fmt.Errorf("PR title %q must use Conventional Commit syntax", title)
	}
	if strings.HasPrefix(baseBranch, "release/") {
		if matches[1] == "feat" {
			return fmt.Errorf("fixed release branch %s rejects feature PR %q", baseBranch, title)
		}
		if matches[3] == "!" {
			return fmt.Errorf("fixed release branch %s rejects breaking PR %q", baseBranch, title)
		}
	}
	return nil
}
