// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"

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

	logSlice := make([]string, 0)
	h := make([]byte, 8)
	for {
		_, err := logs.Read(h)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from logs: ", err)
			return
		}

		count := binary.BigEndian.Uint32(h[4:])
		if count == 0 {
			continue
		}
		b := make([]byte, count)
		_, err = logs.Read(b)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from logs: ", err)
			return
		}
		logSlice = append(logSlice, string(b))
	}

	fmt.Printf("DB container logs (last 100 lines): \n")
	n := len(logSlice) - 100 /* get last 100 lines */
	if n < 0 {
		n = 0
	}
	fmt.Println(logSlice[n:])
}

func StopContainer(ctx context.Context, container testcontainers.Container, printLogs bool, t Result) {
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
