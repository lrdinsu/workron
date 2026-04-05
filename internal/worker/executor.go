package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Executor is responsible for running shell commands
type Executor struct{}

// NewExecutor creates a new Executor instance
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute runs a shell command string and return an error if it fails.
// The context enables cancellation (e.g. for preemption).
// If env is non-nil, its key=value pairs are appended to the process environment.
func (e *Executor) Execute(ctx context.Context, command string, env map[string]string) error {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return fmt.Errorf("empty command")
	}

	// parts[0] is the program, parts[1:] are the arguments
	cmd := exec.CommandContext(ctx, parts[0], parts[1:]...)

	if len(env) > 0 {
		cmd.Env = os.Environ()
		for k, v := range env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}

	// CombinedOutput runs the command and captures stdout + stderr together
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("command %q failed: %w\noutput: %s", command, err, string(output))
	}

	return nil
}
