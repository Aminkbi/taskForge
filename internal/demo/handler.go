package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

const TaskAppendFile = "demo.append_file"

type AppendFilePayload struct {
	Path string `json:"path"`
	Line string `json:"line"`
}

type Handler struct {
	Logger *slog.Logger
}

func (h Handler) HandleTask(_ context.Context, msg broker.TaskMessage) error {
	switch msg.Name {
	case TaskAppendFile:
		return h.appendFile(msg)
	default:
		return fmt.Errorf("demo handler: unsupported task %q", msg.Name)
	}
}

func (h Handler) appendFile(msg broker.TaskMessage) error {
	var payload AppendFilePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("decode append_file payload: %w", err)
	}
	if payload.Path == "" {
		return fmt.Errorf("append_file payload path is required")
	}

	file, err := os.OpenFile(payload.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open demo output file: %w", err)
	}
	defer file.Close()

	line := fmt.Sprintf(
		"%s task_id=%s task_name=%s line=%s\n",
		time.Now().UTC().Format(time.RFC3339Nano),
		msg.ID,
		msg.Name,
		payload.Line,
	)
	if _, err := file.WriteString(line); err != nil {
		return fmt.Errorf("write demo output file: %w", err)
	}

	if h.Logger != nil {
		h.Logger.Info("demo task appended line", "path", payload.Path, "task_id", msg.ID, "line", payload.Line)
	}
	return nil
}
