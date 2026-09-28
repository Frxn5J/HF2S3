//go:build !windows

package main

import (
	"os"
	"syscall"
)

// reexec replaces this process with a fresh copy of itself. The PID does not
// change, so a container whose main process is the gateway keeps running.
func reexec() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	return syscall.Exec(exe, os.Args, os.Environ())
}
