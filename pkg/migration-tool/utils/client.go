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
	promConfig "github.com/prometheus/common/config"
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
)

type (
	RecoveryAction uint8
	eventType      uint8
)

const (
	// Retry makes the client to retry in case of a timeout or error.
	Retry RecoveryAction = iota
	// Skip makes the client to skip the current slab.
	Skip
	// Abort aborts the client from any further retries and exits the migration process.
	// This can be applied after a timeout or error.
	Abort
)

const (
	onTimeout eventType = iota
	onError
)

type ClientConfig struct {
	URL          string
	Timeout      time.Duration
	OnTimeout    RecoveryAction
	OnTimeoutStr string
	OnErr        RecoveryAction
	OnErrStr     string
	MaxRetry     int
	// Delay defines the time we should wait before a retry. This can happen
	// either in a timeout or after an error.
	Delay time.Duration
}

func (c ClientConfig) shouldRetry(on eventType, retries int, message string) (retry bool) {
	// Skip is not applicable in read process, since it is meaningless and implementing it makes the code in Read() untidy.
	makeDecision := func(r RecoveryAction) bool {
		switch r {
		case Retry:
			if c.MaxRetry != 0 {
				// If MaxRetry is 0, we are expected to retry forever.
				if retries > c.MaxRetry {
					log.Info("msg", "exceeded retrying limit in "+message+". Erring out.")
					return false
				}
			}
			message += "."
			log.Info("msg", fmt.Sprintf("%s Retrying again after delay", message), "delay", c.Delay)
			time.Sleep(c.Delay)
			return true
		case Abort:
		}
		return false
	}
	switch on {
	case onTimeout:
		return makeDecision(RecoveryAction(c.OnTimeout))
	case onError:
		return makeDecision(RecoveryAction(c.OnErr))
	default:
		panic("invalid 'on'. Should be 'error' or 'timeout'")
	}
}

func RecoveryActionEnum(typ string) (RecoveryAction, error) {
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

func ParseClientInfo(conf *ClientConfig) error {
	wrapErr := func(typ string, err error) error {
		return fmt.Errorf("enum conversion '%s': %w", typ, err)
	}
	enum, err := RecoveryActionEnum(conf.OnTimeoutStr)
	if err != nil {
		return wrapErr("on-timeout", err)
	}
	conf.OnTimeout = enum

	enum, err = RecoveryActionEnum(conf.OnErrStr)
	if err != nil {
		return wrapErr("on-error", err)
	}
	conf.OnErr = enum
	return nil
}

type Client struct {
	remoteName   string
	url          *promConfig.URL
	cHTTP        *http.Client
	clientConfig ClientConfig
}

// Config returns the client runtime.
func (c *Client) Config() ClientConfig {
	return c.clientConfig
}

// NewClient creates a new read or write client. The `clientType` should be either `read` or `write`. The client type
// is used to get the auth from the auth store. If the `clientType` is other than the ones specified, then auth may not work.
func NewClient(remoteName string, clientConfig ClientConfig, httpConfig promConfig.HTTPClientConfig) (*Client, error) {
	parsedUrl, err := url.Parse(clientConfig.URL)
	if err != nil {
		return nil, fmt.Errorf("parsing-%s-url: %w", remoteName, err)
	}
	httpClient, err := promConfig.NewClientFromConfig(httpConfig, fmt.Sprintf("%s_client", remoteName), promConfig.WithHTTP2Disabled())
	if err != nil {
		return nil, err
	}

	t := httpClient.Transport
	httpClient.Transport = &nethttp.Transport{
		RoundTripper: t,
	}

	return &Client{
		remoteName:   remoteName,
		url:          &promConfig.URL{URL: parsedUrl},
		cHTTP:        httpClient,
		clientConfig: clientConfig,
	}, nil
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

	numAttempts := -1
retry:
	numAttempts++
	if numAttempts > 1 {
		// Retrying case. Reset the context so that timeout context works.
		// If we do not reset the context, retrying with the timeout will not work
		// since the context parent has already been cancelled.
		ctx.Done()
		//
		// Note: We use httpReq.WithContext(ctx) which means if a timeout occurs and we are asked
		// to retry, the context in the httpReq has already been cancelled. Hence, we should create
		// the new context for retry with a new httpReq otherwise we are reusing the already cancelled
		// httpReq.
		ctx = context.Background()
	}

	// Recompress the data on retry, otherwise the bytes.NewReader will make it invalid,
	// leading to corrupt snappy data output on remote-read server.
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

	ctx, cancel := context.WithTimeout(ctx, c.clientConfig.Timeout)
	defer cancel()

	httpReq = httpReq.WithContext(ctx)

	httpResp, err := c.cHTTP.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			if c.clientConfig.shouldRetry(onTimeout, numAttempts, "read request timeout") {
				goto retry
			}
			return nil, 0, 0, fmt.Errorf("error sending request: %w", err)
		}
		err = fmt.Errorf("error sending request: %w", err)
		if c.clientConfig.shouldRetry(onError, numAttempts, fmt.Sprintf("error:\n%s\n", err.Error())) {
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
		if c.clientConfig.shouldRetry(onError, numAttempts, fmt.Sprintf("error:\n%s\n", err.Error())) {
			goto retry
		}
		return nil, -1, -1, err
	}

	if httpResp.StatusCode/100 != 2 {
		if c.clientConfig.shouldRetry(onError, numAttempts, fmt.Sprintf("received status code: %d", httpResp.StatusCode)) {
			goto retry
		}
		return nil, -1, -1, errors.Errorf("remote server %s returned HTTP status %s: %s", c.url.String(), httpResp.Status, strings.TrimSpace(string(compressed)))
	}

	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		if c.clientConfig.shouldRetry(onError, numAttempts, fmt.Sprintf("error:\n%s\n", err.Error())) {
			goto retry
		}
		return nil, -1, -1, errors.Wrap(err, "error reading response")
	}

	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	if err != nil {
		err = errors.Wrap(err, "unable to unmarshal response body")
		if c.clientConfig.shouldRetry(onError, numAttempts, fmt.Sprintf("error:\n%s\n", err.Error())) {
			goto retry
		}
		return nil, -1, -1, err
	}

	if len(resp.Results) != len(req.Queries) {
		err = errors.Errorf("responses: want %d, got %d", len(req.Queries), len(resp.Results))
		if c.clientConfig.shouldRetry(onError, numAttempts, fmt.Sprintf("error:\n%s\n", err.Error())) {
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
	ctx, cancel := context.WithTimeout(ctx, c.clientConfig.Timeout)
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
