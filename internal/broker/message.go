package broker

import "time"

type TaskMessage struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	Queue             string            `json:"queue"`
	Payload           []byte            `json:"payload"`
	Headers           map[string]string `json:"headers,omitempty"`
	Attempt           int               `json:"attempt"`
	MaxAttempts       int               `json:"max_attempts"`
	VisibilityTimeout time.Duration     `json:"visibility_timeout"`
	ETA               *time.Time        `json:"eta,omitempty"`
	Timeout           *time.Duration    `json:"timeout,omitempty"`
	IdempotencyKey    string            `json:"idempotency_key,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
}

type Lease struct {
	Token      string
	Queue      string
	TaskID     string
	ConsumerID string
	ExpiresAt  time.Time
}
