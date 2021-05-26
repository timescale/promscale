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

func SlabEnums(typ string) (uint8, error) {
	switch typ {
	case "retry":
		return Retry, nil
	case "skip":
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

func shouldRetry(r uint8, cr ClientRuntime, retries int, message string) (retry bool, throwErr bool) {
	switch r {
	case Retry:
		if cr.MaxRetry != 0 {
			// If MaxRetry is 0, we are expected to retry forever.
			if retries > cr.MaxRetry {
				log.Info("msg", "exceeded retrying limit in "+message+". Erring out.")
				return false, true
			}
		}
		log.Info("msg", fmt.Sprintf("%s Retrying again after delay", message), "delay", cr.Delay)
		return true, false
	case Skip:
		log.Info("msg", fmt.Sprintf("%s Skipping current slab", message))
		return false, false
	case Abort:
	}
	return false, true
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

	var retries int
retry:
	ctx, cancel := context.WithTimeout(ctx, c.cRuntime.Timeout)
	defer cancel()

	httpReq = httpReq.WithContext(ctx)

	httpResp, err := c.cHTTP.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			attemptRetry, throwErr := shouldRetry(c.cRuntime.OnTimeout, c.cRuntime, retries, "read request timeout.")
			if attemptRetry {
				time.Sleep(c.cRuntime.Delay)
				retries++
				goto retry
			}
			if throwErr {
				return nil, 0, 0, fmt.Errorf("error sending request: %w", err)
			}
			return nil, 0, 0, nil
		}
		attemptRetry, throwErr := shouldRetry(c.cRuntime.OnErr, c.cRuntime, retries, fmt.Sprintf("error:\n%s\n", err.Error()))
		if attemptRetry {
			time.Sleep(c.cRuntime.Delay)
			retries++
			goto retry
		}
		if throwErr {
			return nil, 0, 0, fmt.Errorf("error sending request: %w", err)
		}
		return nil, 0, 0, nil
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
		return nil, -1, -1, errors.Wrap(err, fmt.Sprintf("error reading response. HTTP status code: %s", httpResp.Status))
	}

	if httpResp.StatusCode/100 != 2 {
		return nil, -1, -1, errors.Errorf("remote server %s returned HTTP status %s: %s", c.url.String(), httpResp.Status, strings.TrimSpace(string(compressed)))
	}

	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, -1, -1, errors.Wrap(err, "error reading response")
	}

	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	if err != nil {
		return nil, -1, -1, errors.Wrap(err, "unable to unmarshal response body")
	}

	if len(resp.Results) != len(req.Queries) {
		return nil, -1, -1, errors.Errorf("responses: want %d, got %d", len(req.Queries), len(resp.Results))
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

// ReadConcurrent calls the Read and responds on the channels.
func (c *Client) ReadConcurrent(ctx context.Context, query *prompb.Query, shardID int, desc string, responseChan chan<- interface{}) {
	result, numBytesCompressed, numBytesUncompressed, err := c.Read(ctx, query, desc)
	if err != nil {
		responseChan <- fmt.Errorf("read-channels: %w", err)
		return
	}
	responseChan <- &PrompbResponse{
		ID:                   shardID,
		Result:               result,
		NumBytesCompressed:   numBytesCompressed,
		NumBytesUncompressed: numBytesUncompressed,
	}
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
