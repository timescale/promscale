package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/atomic"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
)

const (
	DefaultReadTimeout     = time.Minute * 5
	ResponseDataSizeLimit  = 1024 * 1024 * 25 // 25 Mega-bytes.
	MaxTimeRangeDeltaLimit = time.Minute * 32
)

type RemoteRead struct {
	url                    string
	client                 remote.ReadClient
	plan                   *plan.Plan
	sigBlockWrite          chan struct{}
	sigBlockRead           chan struct{} // To the writer.
	currentFetchersRunning atomic.Uint32
}

// New creates a new RemoteRead. It creates a ReadClient that is imported from Prometheus remote storage.
// RemoteRead takes help of plan to understand how to create fetchers.
func New(readStorageUrl string, p *plan.Plan, sigRead, sigWrite chan struct{}) (*RemoteRead, error) {
	parsedUrl, err := url.Parse(readStorageUrl)
	if err != nil {
		return nil, fmt.Errorf("url-parse: %w", err)
	}
	rc, err := remote.NewReadClient(fmt.Sprintf("reader-%d", 1), &remote.ClientConfig{
		URL:     &config.URL{URL: parsedUrl},
		Timeout: model.Duration(DefaultReadTimeout),
	})
	if err != nil {
		return nil, fmt.Errorf("creating remote-client: %w", err)
	}
	read := &RemoteRead{
		url:           readStorageUrl,
		plan:          p,
		client:        rc,
		sigBlockRead:  sigRead,
		sigBlockWrite: sigWrite,
	}
	read.currentFetchersRunning.Store(0)
	return read, nil
}

// Run runs the remote read and starts fetching the samples from the read storage.
func (rr *RemoteRead) Run(wg *sync.WaitGroup, readerUp *atomic.Bool) error {
	readerUp.Store(true)
	fmt.Println("reader is up")
	defer func() {
		close(rr.sigBlockRead)
		readerUp.Store(false)
		fmt.Println("reading closed!")
		wg.Done()
	}()
	timeRangeMinutesDelta := time.Minute.Milliseconds()
	count := 0
	for i := rr.plan.Mint; i <= rr.plan.Maxt; {
		count++
		fmt.Println(count, i, i <= rr.plan.Maxt, "sending with delta", timeRangeMinutesDelta/(60*1000))
		mint := i
		maxt := mint + timeRangeMinutesDelta
		fetch := rr.createFetch(rr.url, mint, maxt)
		result, err := fetch.start(context.Background(), []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "", ".*")})
		if err != nil {
			return fmt.Errorf("remote-read run: %w", err)
		}
		if len(result.Timeseries) == 0 {
			if timeRangeMinutesDelta < MaxTimeRangeDeltaLimit.Milliseconds() {
				timeRangeMinutesDelta *= 2
			}
			i += timeRangeMinutesDelta + 1
			continue
		}
		blockRef := rr.plan.CreateBlock(mint, maxt)
		blockRef.Timeseries = result.Timeseries
		if len(result.Timeseries) > 0 {
			fmt.Println(result.Timeseries[0].Labels)
			fmt.Println(result.Timeseries[0].Samples)
		}
		fmt.Println("sending read signal")
		rr.sigBlockRead <- struct{}{}
		<-rr.sigBlockWrite
		fmt.Println("received write signal")
		qResultSize := result.Size() * 4
		if qResultSize < ResponseDataSizeLimit && timeRangeMinutesDelta >= MaxTimeRangeDeltaLimit.Milliseconds() {
			i += timeRangeMinutesDelta + 1
			continue
		} else if qResultSize < ResponseDataSizeLimit {
			timeRangeMinutesDelta *= 2
			i += timeRangeMinutesDelta + 1
			continue
		}
		timeRangeMinutesDelta = time.Minute.Milliseconds()
	}
	fmt.Println("reader is down")
	return nil
}

type SeriesResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}

// series returns the series that are present in the remote read storage.
func (rr *RemoteRead) series(mint, maxt int64) ([]map[string]string, error) {
	var client http.Client
	seriesURL := fmt.Sprintf(`%s/api/v1/series?match[]={__name__=~=".*"}&start=%d&end=%d`, rr.url, mint, maxt)
	response, err := client.Get(seriesURL)
	if err != nil {
		return nil, fmt.Errorf("fetching series: %w", err)
	}
	bstream, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("reading series response: %w", err)
	}
	var resp SeriesResponse
	if err = json.Unmarshal(bstream, &resp); err != nil {
		return nil, fmt.Errorf("unmarshalling series response: %w", err)
	}
	return resp.Data, nil
}

type fetch struct {
	url        string
	mint, maxt int64
	clientCopy remote.ReadClient // Maintain a copy of remote client.
}

func (rr *RemoteRead) createFetch(url string, mint, maxt int64) *fetch {
	f := &fetch{
		url:        url,
		mint:       mint,
		maxt:       maxt,
		clientCopy: rr.client,
	}
	rr.currentFetchersRunning.Add(1)
	return f
}

// start starts fetching the samples from remote read storage as client based on the matchers.
func (f *fetch) start(context context.Context, matchers []*labels.Matcher) (*prompb.QueryResult, error) {
	ms, err := toLabelMatchers(matchers)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	readRequest := &prompb.Query{
		StartTimestampMs: f.mint,
		EndTimestampMs:   f.maxt,
		Matchers:         ms,
	}
	result, err := f.clientCopy.Read(context, readRequest)
	if err != nil {
		return nil, fmt.Errorf("executing client-read: %w", err)
	}
	return result, nil
}

func toLabelMatchers(matchers []*labels.Matcher) ([]*prompb.LabelMatcher, error) {
	pbMatchers := make([]*prompb.LabelMatcher, 0, len(matchers))
	for _, m := range matchers {
		var mType prompb.LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			mType = prompb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			mType = prompb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			mType = prompb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			mType = prompb.LabelMatcher_NRE
		default:
			return nil, errors.New("invalid matcher type")
		}
		pbMatchers = append(pbMatchers, &prompb.LabelMatcher{
			Type:  mType,
			Name:  m.Name,
			Value: m.Value,
		})
	}
	return pbMatchers, nil
}
