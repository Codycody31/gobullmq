package compatibility

import (
	"strings"
	"testing"
)

func TestCheckPRTitle(t *testing.T) {
	tests := []struct {
		name  string
		title string
		base  string
		want  string
	}{
		{name: "patch on fixed line", title: "fix(worker): renew locks", base: "release/v1.1-bullmq-v4.12.2"},
		{name: "maintenance on fixed line", title: "chore(release): release v1.1.0", base: "release/v1.1-bullmq-v4.12.2"},
		{name: "feature on main", title: "feat(queue): add getter", base: "main"},
		{name: "feature on fixed line", title: "feat(queue): add getter", base: "release/v1.1-bullmq-v4.12.2", want: "rejects feature"},
		{name: "breaking on fixed line", title: "fix(worker)!: change callback", base: "release/v1.1-bullmq-v4.12.2", want: "rejects breaking"},
		{name: "invalid", title: "update worker", base: "main", want: "Conventional Commit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckPRTitle(tt.title, tt.base)
			if tt.want == "" && err != nil {
				t.Fatalf("CheckPRTitle: %v", err)
			}
			if tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)) {
				t.Fatalf("CheckPRTitle error = %v, want %q", err, tt.want)
			}
		})
	}
}
