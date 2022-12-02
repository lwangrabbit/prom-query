package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"golang.org/x/net/context/ctxhttp"

	"github.com/lwangrabbit/prom-query/pkg/value"
)

const maxErrMsgLen = 256

// Client allows reading and writing from/to a remote HTTP endpoint.
type Client struct {
	index   int // Used to differentiate clients in metrics.
	url     *config_util.URL
	client  *http.Client
	timeout time.Duration
}

// ClientConfig configures a Client.
type ClientConfig struct {
	URL              *config_util.URL
	Timeout          model.Duration
	HTTPClientConfig config_util.HTTPClientConfig
}

// NewClient creates a new Client.
func NewClient(index int, conf *ClientConfig) (*Client, error) {
	httpClient, err := config_util.NewClientFromConfig(conf.HTTPClientConfig, "read")
	if err != nil {
		return nil, err
	}

	return &Client{
		index:   index,
		url:     conf.URL,
		client:  httpClient,
		timeout: time.Duration(conf.Timeout),
	}, nil
}

type recoverableError struct {
	error
}

// Name identifies the client.
func (c Client) Name() string {
	return fmt.Sprintf("%d:%s", c.index, c.url)
}

func (c *Client) instantQueryUrl(query string, ts int64) string {
	return fmt.Sprintf("%v/api/v1/query?query=%v&time=%v", c.url.String(), query, ts)
}

func (c *Client) rangeQueryUrl(query string, startTs, endTs int64, step int) string {
	return fmt.Sprintf("%v/api/v1/query_range?query=%v&start=%v&end=%v&step=%v", c.url.String(), query, startTs, endTs, step)
}

// QueryInstant execute instant query to a remote endpoint.
func (c *Client) QueryInstant(ctx context.Context, query string, ts int64) (*InstantQueryResult, error) {
	httpReq, err := http.NewRequest("GET", c.instantQueryUrl(query, ts), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %v", err)
	}
	httpReq.Header.Set("X-Prometheus-Instant-Query-Version", "0.1.0")

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpResp, err := ctxhttp.Do(ctx, c.client, httpReq)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("server returned HTTP status %s", httpResp.Status)
	}

	raw, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	var rsp InstantQueryResult
	err = json.Unmarshal(raw, &rsp)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal response body: %v", err)
	}
	return &rsp, nil
}

type InstantQueryResult struct {
	Data   *InstantQueryData `json:"data"`
	Status string            `json:"status"`
}
type InstantQueryData struct {
	ResultType value.ValueType `json:"resultType"`
	Result     *value.Vector   `json:"result"`
}

// QueryRange execute range query to a remote endpoint.
func (c *Client) QueryRange(ctx context.Context, query string, startTs, endTs int64, step int) (*RangeQueryResult, error) {
	httpReq, err := http.NewRequest("GET", c.rangeQueryUrl(query, startTs, endTs, step), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %v", err)
	}
	httpReq.Header.Set("X-Prometheus-Instant-Query-Version", "0.1.0")

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpResp, err := ctxhttp.Do(ctx, c.client, httpReq)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("server returned HTTP status %s", httpResp.Status)
	}

	raw, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	var rsp RangeQueryResult
	err = json.Unmarshal(raw, &rsp)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal response body: %v", err)
	}
	return &rsp, nil
}

type RangeQueryResult struct {
	Data   *RangeQueryData `json:"data"`
	Status string          `json:"status"`
}
type RangeQueryData struct {
	ResultType value.ValueType `json:"resultType"`
	Result     *value.Matrix   `json:"result"`
}
