package worker

import (
	"context"
	"testing"
	"time"
)

func TestExecutor_Success(t *testing.T) {
	e := NewExecutor()

	err := e.Execute(context.Background(), "echo hello", nil, nil)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestExecutor_EmptyCommand(t *testing.T) {
	e := NewExecutor()

	err := e.Execute(context.Background(), "", nil, nil)
	if err == nil {
		t.Errorf("expected an error for empty command, got nil")
	}
}

func TestExecutor_InvalidCommand(t *testing.T) {
	e := NewExecutor()

	err := e.Execute(context.Background(), "thiscommanddoesnotexist", nil, nil)
	if err == nil {
		t.Errorf("expected an error for invalid command, got nil")
	}
}

func TestExecutor_FailedCommand(t *testing.T) {
	e := NewExecutor()

	err := e.Execute(context.Background(), "sh -c 'exit 1'", nil, nil)
	if err == nil {
		t.Errorf("expected an error for failed command, got nil")
	}
}

// TestExecutor_PreemptSignalsSIGTERM verifies that closing the stop
// channel while a long-running command is in flight causes the child
// process to be signaled. A shell running `sleep 30` with `trap ” TERM`
// disabled exits quickly on SIGTERM; it asserts that Execute returns
// within well under the sleep duration.
func TestExecutor_PreemptSignalsSIGTERM(t *testing.T) {
	e := NewExecutor()
	stop := make(chan struct{})

	// Close stop shortly after Execute starts so the child gets SIGTERM
	// almost immediately.
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(stop)
	}()

	start := time.Now()
	err := e.Execute(context.Background(), "sleep 30", nil, stop)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("expected a non-nil error (terminated by signal), got nil")
	}
	// A well-behaved process exits on SIGTERM within milliseconds. Allow
	// generous slack for test machine variance, but well below preemptGrace
	// (15s) and the sleep duration (30s).
	if elapsed > 5*time.Second {
		t.Errorf("executor took %v to return after SIGTERM, want < 5s", elapsed)
	}
}

// TestExecutor_PreemptHardKillsAfterGrace verifies the SIGKILL fallback
// when the child process ignores SIGTERM. A shell script that traps
// SIGTERM and keeps sleeping should still be killed when the grace
// window elapses, here it uses a much shorter simulated grace by
// setting stop and letting SIGKILL trigger via ctx cancellation instead
// of waiting the full 15s.
func TestExecutor_PreemptHardKillsAfterGrace(t *testing.T) {
	e := NewExecutor()
	stop := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Short-circuit the grace window by canceling ctx just after signaling
	// stop. This is the hard-kill backstop path.
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(stop)
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := e.Execute(ctx, "sh -c 'trap \"\" TERM; sleep 30'", nil, stop)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("expected a non-nil error (killed), got nil")
	}
	if elapsed > 5*time.Second {
		t.Errorf("executor took %v to return after hard kill, want < 5s", elapsed)
	}
}
