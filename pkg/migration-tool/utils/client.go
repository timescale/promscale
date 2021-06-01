// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package utils

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/pkg/errors"
	"github.com/prometheus/common/config"
	configutil "github.com/prometheus/common/config"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/prompb"
	"github.com/schollz/progressbar/v3"
	"github.com/timescale/promscale/pkg/log"
)

var userAgent = fmt.Sprintf("Prometheus/%s", version.Version)

const (
	maxErrMsgLen = 512
	// Read defines the type of client. It is used to fetch the auth from authStore.
	Read = iota
	// Write defines the type of client. It is used to fetch the auth from authStore.
	Write
	// Retry makes the client to retry in case of a timeout or error.
	Retry
	// Skip makes the client to skip the current slab.
	Skip
	// Abort aborts the client from any further retries and exits the migration process.
	// This can be applied after a timeout or error.
	Abort
)

type ClientRuntime struct {
	URL          string
	Timeout      time.Duration
	OnTimeout    uint8
	OnTimeoutStr string
	OnErr        uint8
	OnErrStr     string
	MaxRetry     int
	// Delay defines the time we should wait before a retry. This can happen
	// either in a timeout or after an error.
	Delay time.Duration
}

func SlabEnums(typ string, skipApplicable bool) (uint8, error) {
	switch typ {
	case "retry":
		return Retry, nil
	case "skip":
		if !skipApplicable {
			return 0, fmt.Errorf("invalid slab enums: 'skip' is not applicable")
		}
		return Skip, nil
	case "abort":
		return Abort, nil
	default:
		return 0, fmt.Errorf("invalid slab enums: expected from ['retry', 'skip', 'abort']")
	}
}

type Client struct {
	remoteName string
	url        *config.URL
	cHTTP      *http.Client
	cRuntime   ClientRuntime
}

// Runtime returns the client runtime.
func (c *Client) Runtime() ClientRuntime {
	return c.cRuntime
}

// NewClient creates a new read or write client. The `clientType` should be either `read` or `write`. The client type
// is used to get the auth from the auth store. If the `clientType` is other than the ones specified, then auth may not work.
func NewClient(remoteName string, cRuntime ClientRuntime, httpConfig configutil.HTTPClientConfig) (*Client, error) {
	parsedUrl, err := url.Parse(cRuntime.URL)
	if err != nil {
		return nil, fmt.Errorf("parsing-%s-url: %w", remoteName, err)
	}
	httpClient, err := configutil.NewClientFromConfig(httpConfig, fmt.Sprintf("%s_client", remoteName), configutil.WithHTTP2Disabled())
	if err != nil {
		return nil, err
	}

	t := httpClient.Transport
	httpClient.Transport = &nethttp.Transport{
		RoundTripper: t,
	}

	return &Client{
		remoteName: remoteName,
		url:        &config.URL{URL: parsedUrl},
		cHTTP:      httpClient,
		cRuntime:   cRuntime,
	}, nil
}

func shouldRetry(r uint8, cr ClientRuntime, retries int, message string) (retry bool) {
	// Skip is not applicable in read process, since it is meaningless and implementing it makes the code in Read() untidy.
	switch r {
	case Retry:
		if cr.MaxRetry != 0 {
			// If MaxRetry is 0, we are expected to retry forever.
			if retries > cr.MaxRetry {
				log.Info("msg", "exceeded retrying limit in "+message+". Erring out.")
				return false
			}
		}
		message += "."
		log.Info("msg", fmt.Sprintf("%s Retrying again after delay", message), "delay", cr.Delay)
		time.Sleep(cr.Delay)
		return true
	case Abort:
	}
	return false
}

func (c *Client) Read(ctx context.Context, query *prompb.Query, desc string) (result *prompb.QueryResult, numBytesCompressed int, numBytesUncompressed int, err error) {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			query,
		},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, -1, -1, errors.Wrapf(err, "unable to marshal read request")
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(compressed))
	if err != nil {
		return nil, -1, -1, errors.Wrap(err, "unable to create request")
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Add("Accept-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	numAttempts := -1

retry:
	numAttempts++
	ctx, cancel := context.WithTimeout(ctx, c.cRuntime.Timeout)
	defer cancel()

	httpReq = httpReq.WithContext(ctx)

	httpResp, err := c.cHTTP.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			if shouldRetry(c.cRuntime.OnTimeout, c.cRuntime, numAttempts, "read request timeout") {
				goto retry
			}
			return nil, 0, 0, fmt.Errorf("error sending request: %w", err)
		}
		err = fmt.Errorf("error sending request: %w", err)
		if shouldRetry(c.cRuntime.OnErr, c.cRuntime, numAttempts, fmt.Sprintf("error:\n%s\n", err.Error())) {
			goto retry
		}
		return nil, 0, 0, err
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, httpResp.Body)
		_ = httpResp.Body.Close()
	}()

	var reader bytes.Buffer
	if desc != "" {
		bar := progressbar.DefaultBytes(
			httpResp.ContentLength,
			desc,
		)
		_, _ = io.Copy(io.MultiWriter(&reader, bar), httpResp.Body)
	} else {
		// This case belongs to when the client is used to fetch the progress metric from progress-metric url.
		_, _ = io.Copy(&reader, httpResp.Body)
	}
	compressed, err = ioutil.ReadAll(bytes.NewReader(reader.Bytes()))
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("error reading response. HTTP status code: %s", httpResp.Status))
		if shouldRetry(c.cRuntime.OnErr, c.cRuntime, numAttempts, fmt.Sprintf("error:\n%s\n", err.Error())) {
			goto retry
		}
		return nil, -1, -1, err
	}

	if httpResp.StatusCode/100 != 2 {
		if shouldRetry(c.cRuntime.OnErr, c.cRuntime, numAttempts, fmt.Sprintf("received status code: %d", httpResp.StatusCode)) {
			goto retry
		}
		return nil, -1, -1, errors.Errorf("remote server %s returned HTTP status %s: %s", c.url.String(), httpResp.Status, strings.TrimSpace(string(compressed)))
	}

	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		// Snappy errors also happen in case of deadline or context cancel since the read was terminated in middle of the process.
		// Lets treat them as timeout case.
		if shouldRetry(c.cRuntime.OnTimeout, c.cRuntime, numAttempts, "invalid snappy: This can be either due to partial read or an actual context timeout. "+
			"Treating this as read-request timeout") {
			goto retry
		}
		return nil, -1, -1, errors.Wrap(err, "error reading response")
	}

	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	if err != nil {
		err = errors.Wrap(err, "unable to unmarshal response body")
		if shouldRetry(c.cRuntime.OnErr, c.cRuntime, numAttempts, fmt.Sprintf("error:\n%s\n", err.Error())) {
			goto retry
		}
		return nil, -1, -1, err
	}

	if len(resp.Results) != len(req.Queries) {
		err = errors.Errorf("responses: want %d, got %d", len(req.Queries), len(resp.Results))
		if shouldRetry(c.cRuntime.OnErr, c.cRuntime, numAttempts, fmt.Sprintf("error:\n%s\n", err.Error())) {
			goto retry
		}
		return nil, -1, -1, err
	}

	return resp.Results[0], len(compressed), len(uncompressed), nil
}

// PrompbResponse is a type that contains promb-result and information pertaining to it.
type PrompbResponse struct {
	ID                   int
	Result               *prompb.QueryResult
	NumBytesCompressed   int
	NumBytesUncompressed int
}

var prompbResponsePool = sync.Pool{New: func() interface{} { return new(PrompbResponse) }}

func PutPrompbResponse(p *PrompbResponse) {
	p.ID = 0
	p.NumBytesUncompressed = 0
	p.NumBytesCompressed = 0
	p.Result.Timeseries = p.Result.Timeseries[:0]
	prompbResponsePool.Put(p)
}

// ReadConcurrent calls the Read and responds on the channels.
func (c *Client) ReadConcurrent(ctx context.Context, query *prompb.Query, shardID int, desc string, responseChan chan<- interface{}) {
	result, numBytesCompressed, numBytesUncompressed, err := c.Read(ctx, query, desc)
	if err != nil {
		responseChan <- fmt.Errorf("read-channels: %w", err)
		return
	}
	pr := prompbResponsePool.Get().(*PrompbResponse)
	pr.ID = shardID
	pr.Result = result
	pr.NumBytesUncompressed = numBytesUncompressed
	pr.NumBytesCompressed = numBytesCompressed
	responseChan <- pr
}

// RecoverableError is an error for which the send samples can retry with a backoff.
type RecoverableError struct {
	error
}

// Store sends a batch of samples to the HTTP endpoint, the request is the proto marshalled
// and encoded bytes from codec.go.
func (c *Client) Store(ctx context.Context, req []byte) error {
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(req))
	if err != nil {
		// Errors from NewRequest are from unparsable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	ctx, cancel := context.WithTimeout(ctx, c.cRuntime.Timeout)
	defer cancel()

	httpReq = httpReq.WithContext(ctx)

	httpResp, err := c.cHTTP.Do(httpReq)
	if err != nil {
		// Errors from Client.Do are from (for example) network errors, so are
		// recoverable.
		return RecoverableError{err}
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, httpResp.Body)
		_ = httpResp.Body.Close()
	}()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = errors.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return RecoverableError{err}
	}
	return err
}
