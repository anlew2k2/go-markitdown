package services

import (
	"os/exec"
	"strings"
)

// RunCommand splits args on whitespace and executes the resulting command
// directly, returning its combined stdout/stderr output.
func RunCommand(args string) (string, error) {
	fields := strings.Fields(args)
	if len(fields) == 0 {
		return "", nil
	}

	cmd := exec.Command(fields[0], fields[1:]...)
	output, err := cmd.CombinedOutput()
	return string(output), err
}
