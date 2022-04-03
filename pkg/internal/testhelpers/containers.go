// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

type CloseAll struct{ toClose []func() }

func (c *CloseAll) Append(closer func()) {
	c.toClose = append(c.toClose, closer)
}

func (c CloseAll) Close() error {
	for i := len(c.toClose) - 1; i >= 0; i-- {
		c.toClose[i]()
	}
	return nil
}

type stdoutLogConsumer struct{ service string }

func (s stdoutLogConsumer) Accept(l testcontainers.Log) {
	if l.LogType == testcontainers.StderrLog {
		fmt.Print(l.LogType, " ", "service ", s.service, string(l.Content))
	} else {
		fmt.Print("service ", s.service, string(l.Content))
	}
}

func PrintContainerLogs(container testcontainers.Container) {
	logs, err := container.Logs(context.TODO())
	if err != nil {
		fmt.Println("Error fetching logs: ", err)
		return
	}
	defer logs.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(logs)
	if err != nil {
		fmt.Println("Error reading from logs: ", err)
		return
	}

	logStr := buf.String()
	fmt.Printf("Error starting container, Logs: \n %s", logStr)
}

func StopContainer(ctx context.Context, container testcontainers.Container, printLogs bool, t testing.TB) {
	if !printLogs && t != nil && t.Failed() {
		PrintContainerLogs(container)
	}
	if printLogs {
		err := container.StopLogProducer()
		if err != nil {
			fmt.Fprintln(os.Stderr, "couldn't stop log producer", err)
		}
	}

	err := container.Terminate(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "couldn't terminate container", err)
	}
}

// TempDir returns a temp directory for tests
func TempDir(name string) (string, error) {
	tmpDir := ""

	if runtime.GOOS == "darwin" {
		// Docker on Mac lacks access to default os tmp dir - "/var/folders/random_number"
		// so switch to cross-user tmp dir
		tmpDir = "/tmp"
	}
	return ioutil.TempDir(tmpDir, name)
}
