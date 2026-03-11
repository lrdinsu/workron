package worker

import "testing"

func TestExecutor_Success(t *testing.T) {
	e := NewExecutor()

	err := e.Execute("echo hello")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestExecutor_EmptyCommand(t *testing.T) {
	e := NewExecutor()

	err := e.Execute("")
	if err == nil {
		t.Errorf("expected an error for empty command, got nil")
	}
}

func TestExecutor_InvalidCommand(t *testing.T) {
	e := NewExecutor()

	err := e.Execute("thiscommanddoesnotexist")
	if err == nil {
		t.Errorf("expected an error for invalid command, got nil")
	}
}

func TestExecutor_FailedCommand(t *testing.T) {
	e := NewExecutor()

	err := e.Execute("sh -c 'exit 1'")
	if err == nil {
		t.Errorf("expected an error for failed command, got nil")
	}
}
