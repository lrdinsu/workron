package worker

import (
	"fmt"
	"os/exec"
	"strings"
)

// Executor is responsible for running shell commands
type Executor struct{}

// NewExecutor creates a new Executor instance
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute runs a shell command string and return an error if it fails
// It captures stdout and stderr so failure reasons are visible in logs
func (e *Executor) Execute(command string) error {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return fmt.Errorf("empty command")
	}

	// parts[0] is the program, parts[1:] are the arguments
	cmd := exec.Command(parts[0], parts[1:]...)

	// CombinedOutput runs the command and captures stdout + stderr together
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("command %q failed: %w\noutput: %s", command, err, string(output))
	}

	return nil

}
