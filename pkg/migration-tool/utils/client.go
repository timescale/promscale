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
	"github.com/prometheus/common/config"
	configutil "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/prompb"
	"github.com/schollz/progressbar/v3"
)

var userAgent = fmt.Sprintf("Prometheus/%s", version.Version)

const (
	maxErrMsgLen = 512
	// Read defines the type of client. It is used to fetch the auth from authStore.
	Read = iota
	// Write defines the type of client. It is used to fetch the auth from authStore.
	Write
)

type Client struct {
	remoteName string
	url        *config.URL
	Client     *http.Client
	timeout    time.Duration
}

// clientConfig configures a client.
type clientConfig struct {
	URL              *configutil.URL
	Timeout          model.Duration
	HTTPClientConfig configutil.HTTPClientConfig
}

// NewClient creates a new read or write client. The `clientType` should be either `read` or `write`. The client type
// is used to get the auth from the auth store. If the `clientType` is other than the ones specified, then auth may not work.
func NewClient(remoteName, urlString string, clientType uint, timeout model.Duration) (*Client, error) {
	parsedUrl, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("parsing-%d-url: %w", clientType, err)
	}
	conf := &clientConfig{
		URL:     &config.URL{URL: parsedUrl},
		Timeout: timeout,
	}
	clientConfig, ok := authStore.Load(clientType)
	if ok {
		conf.HTTPClientConfig = clientConfig.(configutil.HTTPClientConfig)
	}
	httpClient, err := configutil.NewClientFromConfig(conf.HTTPClientConfig, fmt.Sprintf("remote_storage_%d_client", clientType), false, false)
	if err != nil {
		return nil, err
	}

	t := httpClient.Transport
	httpClient.Transport = &nethttp.Transport{
		RoundTripper: t,
	}

	return &Client{
		remoteName: remoteName,
		url:        conf.URL,
		Client:     httpClient,
		timeout:    time.Duration(conf.Timeout),
	}, nil
}

func (c *Client) Read(ctx context.Context, query *prompb.Query, desc string) (result *prompb.QueryResult, err error) {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			query,
		},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal read request: %w", err)
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Add("Accept-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpReq = httpReq.WithContext(ctx)

	httpResp, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
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
		return nil, fmt.Errorf("error reading response. HTTP status code: %s: %w", httpResp.Status, err)
	}

	if httpResp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("remote server %s returned HTTP status %s: %s", c.url.String(), httpResp.Status, strings.TrimSpace(string(compressed)))
	}

	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal response body: %w", err)
	}

	if len(resp.Results) != len(req.Queries) {
		return nil, fmt.Errorf("responses: want %d, got %d", len(req.Queries), len(resp.Results))
	}

	return resp.Results[0], nil
}

// PrompbResponse is a type that contains promb-result and information pertaining to it.
type PrompbResponse struct {
	ID                   int
	Result               *prompb.QueryResult
	NumBytesCompressed   int
	NumBytesUncompressed int
}

// ReadChannels calls the Read and responds on the channels.
func (c *Client) ReadChannels(ctx context.Context, query *prompb.Query, shardID int, desc string, responseChan chan<- interface{}) {
	result, err := c.Read(ctx, query, desc)
	if err != nil {
		responseChan <- fmt.Errorf("read-channels: %w", err)
		return
	}
	responseChan <- &PrompbResponse{
		ID:     shardID,
		Result: result,
	}
}

// RecoverableError is an error for which the send samples can retry with a backoff.
type RecoverableError struct {
	error
}

// Store sends a batch of samples to the HTTP endpoint, the request is the proto marshalled
// and encoded bytes from codec.go.
func (c *Client) Store(ctx context.Context, req *[]byte) error {
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(*req))
	if err != nil {
		// Errors from NewRequest are from unparsable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpReq = httpReq.WithContext(ctx)

	httpResp, err := c.Client.Do(httpReq)
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
		err = fmt.Errorf("server returned HTTP status %s: %s: %w", httpResp.Status, line, err)
	}
	if httpResp.StatusCode/100 == 5 {
		return RecoverableError{err}
	}
	return err
}
