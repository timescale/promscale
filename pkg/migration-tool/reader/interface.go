package reader

import "time"

const defaultReadTimeout = time.Minute * 5

type Reader interface {
	// Run runs the reader and starts fetching the samples from the storage.
	Run(chan<- error)
	// SigStop signals a stop signal to the reader. This is mainly used to introduce halts
	// while doing integration tests.
	SigStop()
}
