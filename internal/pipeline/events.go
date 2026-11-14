// Package pipeline implements the Redis Streams event-driven document
// pipeline: typed event schemas, atomic state management via Lua scripts,
// and the Splitter/Extractor worker daemons.
package pipeline

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/google/uuid"
)

// Processing status values stored in doc-extractor:state:{request_id}.
const (
	StatusProcessing = "PROCESSING"
	StatusCompleted  = "COMPLETED"
	StatusFailed     = "FAILED"
)

// Status values carried by the terminal output event.
const (
	OutputStatusSuccess = "SUCCESS"
	OutputStatusFailed  = "FAILED"
)

// FailedOffsetNone marks a FAILED output event raised by the Splitter,
// before any part ever existed.
const FailedOffsetNone = -1

// MaxParts bounds how many parts a single request may fan out into, so a
// pathological split can't produce an unbounded number of stream events.
const MaxParts = 1000

// InputEvent is the payload of stream:doc-extractor:input.
type InputEvent struct {
	RequestID      string `json:"request_id"`
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id"`
	FilePath       string `json:"file_path"`
}

// ParseInputEvent decodes the "data" field of an input stream message.
func ParseInputEvent(data string) (InputEvent, error) {
	var ev InputEvent
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		return InputEvent{}, fmt.Errorf("decode input event: %w", err)
	}
	return ev, nil
}

// Validate checks that every identity field is a well-formed UUID and that
// file_path is an absolute http(s) URL.
func (e InputEvent) Validate() error {
	if err := validateUUID("request_id", e.RequestID); err != nil {
		return err
	}
	if err := validateUUID("conversation_id", e.ConversationID); err != nil {
		return err
	}
	if err := validateUUID("message_id", e.MessageID); err != nil {
		return err
	}
	return validateHTTPURL("file_path", e.FilePath)
}

// SplittedEvent is the payload of stream:doc-extractor:splitted: one part
// out of `total` belonging to a request.
type SplittedEvent struct {
	RequestID      string `json:"request_id"`
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id"`
	Offset         int    `json:"offset"`
	Total          int    `json:"total"`
	FilePath       string `json:"file_path"`
}

// ParseSplittedEvent decodes the "data" field of a splitted stream message.
func ParseSplittedEvent(data string) (SplittedEvent, error) {
	var ev SplittedEvent
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		return SplittedEvent{}, fmt.Errorf("decode splitted event: %w", err)
	}
	return ev, nil
}

// Validate checks identity fields, file_path, and that offset/total are
// well-formed (0 <= offset < total <= MaxParts).
func (e SplittedEvent) Validate() error {
	if err := validateUUID("request_id", e.RequestID); err != nil {
		return err
	}
	if err := validateUUID("conversation_id", e.ConversationID); err != nil {
		return err
	}
	if err := validateUUID("message_id", e.MessageID); err != nil {
		return err
	}
	if err := validateHTTPURL("file_path", e.FilePath); err != nil {
		return err
	}
	if e.Total < 1 || e.Total > MaxParts {
		return fmt.Errorf("total out of range: %d", e.Total)
	}
	if e.Offset < 0 || e.Offset >= e.Total {
		return fmt.Errorf("offset out of range: %d (total %d)", e.Offset, e.Total)
	}
	return nil
}

// OutputEvent is the payload of stream:doc-extractor:output, documenting
// the schema any downstream consumer should expect. The pipeline itself
// never marshals this struct: both terminal events are built and published
// by the Lua scripts in internal/pipeline/lua, inside the same atomic
// commit as the state transition that triggers them.
type OutputEvent struct {
	RequestID      string   `json:"request_id"`
	ConversationID string   `json:"conversation_id"`
	MessageID      string   `json:"message_id"`
	Status         string   `json:"status"`
	Total          int      `json:"total,omitempty"`
	MarkdownURLs   []string `json:"markdown_urls,omitempty"`
	CompletedAt    int64    `json:"completed_at,omitempty"`
	FailedOffset   int      `json:"failed_offset,omitempty"`
	Error          string   `json:"error,omitempty"`
	FailedAt       int64    `json:"failed_at,omitempty"`
}

func validateUUID(field, value string) error {
	if _, err := uuid.Parse(value); err != nil {
		return fmt.Errorf("invalid %s: %w", field, err)
	}
	return nil
}

func validateHTTPURL(field, value string) error {
	u, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("invalid %s: %w", field, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid %s: scheme must be http or https", field)
	}
	if u.Host == "" {
		return fmt.Errorf("invalid %s: missing host", field)
	}
	return nil
}
