package demo

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aminkbi/taskforge/internal/broker"
)

func TestHandlerAppendFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "demo.log")
	handler := Handler{Logger: slog.Default()}

	err := handler.HandleTask(context.Background(), broker.TaskMessage{
		ID:      "demo-task-1",
		Name:    TaskAppendFile,
		Payload: []byte(`{"path":"` + path + `","line":"hello scheduler"}`),
	})
	if err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	text := string(content)
	if !strings.Contains(text, "task_id=demo-task-1") {
		t.Fatalf("content = %q, want task_id", text)
	}
	if !strings.Contains(text, "line=hello scheduler") {
		t.Fatalf("content = %q, want line", text)
	}
}
