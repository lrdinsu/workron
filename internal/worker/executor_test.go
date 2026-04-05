package worker

import (
	"context"
	"testing"
)

func TestExecutor_Success(t *testing.T) {
	e := NewExecutor()

	err := e.Execute(context.Background(), "echo hello", nil)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestExecutor_EmptyCommand(t *testing.T) {
	e := NewExecutor()

	err := e.Execute(context.Background(), "", nil)
	if err == nil {
		t.Errorf("expected an error for empty command, got nil")
	}
}

func TestExecutor_InvalidCommand(t *testing.T) {
	e := NewExecutor()

	err := e.Execute(context.Background(), "thiscommanddoesnotexist", nil)
	if err == nil {
		t.Errorf("expected an error for invalid command, got nil")
	}
}

func TestExecutor_FailedCommand(t *testing.T) {
	e := NewExecutor()

	err := e.Execute(context.Background(), "sh -c 'exit 1'", nil)
	if err == nil {
		t.Errorf("expected an error for failed command, got nil")
	}
}
