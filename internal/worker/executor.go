package worker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// preemptGrace is how long a preempted process has to exit cleanly
// after receiving SIGTERM before the executor escalates to SIGKILL.
const preemptGrace = 15 * time.Second

// Executor is responsible for running shell commands.
type Executor struct{}

// NewExecutor creates a new Executor instance.
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute runs a shell command string and returns an error if it fails.
//
// Context: canceling ctx hard-kills the child process via the default
// exec.CommandContext behavior (SIGKILL).
//
// Preempt: if the stop channel closes while the command is running,
// the executor sends SIGTERM to the child, waits up to preemptGrace
// for clean exit, then escalates to SIGKILL. This is the graceful
// counterpart to context cancellation: SIGTERM lets the job save a
// checkpoint before shutting down. A nil stop channel disables preempt
// signaling (reads on a nil channel block forever, so the goroutine
// just sits on the done branch).
//
// If env is non-nil, its key=value pairs are appended to the process
// environment.
func (e *Executor) Execute(ctx context.Context, command string, env map[string]string, stop <-chan struct{}) error {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return fmt.Errorf("empty command")
	}

	// parts[0] is the program, parts[1:] are the arguments.
	cmd := exec.CommandContext(ctx, parts[0], parts[1:]...)

	if len(env) > 0 {
		cmd.Env = os.Environ()
		for k, v := range env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}

	// Capture combined stdout + stderr so error messages still include
	// the process output (matches the previous CombinedOutput behavior).
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("command %q start: %w", command, err)
	}

	// Watch for preempt while the process is running. done is closed
	// when Wait returns so the goroutine exits cleanly in the happy path.
	done := make(chan struct{})
	go func() {
		select {
		case <-stop:
			// Graceful stop: SIGTERM, wait up to preemptGrace, then SIGKILL.
			// cmd.Process is non-nil because Start returned ok.
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-time.After(preemptGrace):
				_ = cmd.Process.Kill()
			case <-done:
			}
		case <-done:
		}
	}()

	err := cmd.Wait()
	close(done)
	if err != nil {
		return fmt.Errorf("command %q failed: %w\noutput: %s", command, err, out.String())
	}
	return nil
}
