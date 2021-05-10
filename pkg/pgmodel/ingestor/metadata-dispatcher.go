package ingestor

import "github.com/timescale/promscale/pkg/pgmodel/model"

type metadataDispatcher struct {
	toCopiers chan<- metadataRequest
}

func newMetadataDispatcher(toCopiers chan<- metadataRequest) *metadataDispatcher {
	return &metadataDispatcher{toCopiers}
}

// CompleteMetricCreation to implement the Dispatcher interface.
func (m *metadataDispatcher) CompleteMetricCreation() error { return nil }

// Close to implement the Dispatcher interface.
func (m *metadataDispatcher) Close() {
	close(m.toCopiers)
}

func (m *metadataDispatcher) Insert(data interface{}) (uint64, error) {
	rows, ok := data.([]model.Metadata)
	if !ok {
		panic("invalid data received")
	}
	errCh := make(chan error, 1)
	request := metadataRequest{
		batch: rows,
		errCh: errCh,
	}
	m.toCopiers <- request

	err, ok := <-errCh
	if ok {
		return 0, err
	}
	return uint64(len(rows)), nil
}
