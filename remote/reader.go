package remote

import (
	"context"

	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
)

type Reader struct {
	queryables []Queryable
}

type ReadConfig struct {
	URL     *config_util.URL
	Timeout model.Duration
	Name    string
}

func NewReader(configs []*ReadConfig) (*Reader, error) {
	queryables := make([]Queryable, 0, len(configs))
	for i, conf := range configs {
		c, err := NewClient(i, &ClientConfig{
			URL:              conf.URL,
			Timeout:          conf.Timeout,
			HTTPClientConfig: config_util.HTTPClientConfig{},
		})
		if err != nil {
			return nil, err
		}
		q := QueryableClient(c)
		queryables = append(queryables, q)
	}
	return &Reader{queryables: queryables}, nil
}

func (s *Reader) Querier(ctx context.Context) (Querier, error) {
	querables := s.queryables
	queriers := make([]Querier, 0, len(querables))
	for _, queryable := range querables {
		q, err := queryable.Querier(ctx)
		if err != nil {
			return nil, err
		}
		queriers = append(queriers, q)
	}
	return NewMergeQuerier(queriers), nil
}

func (s *Reader) Close() error {
	return nil
}

func (s *Reader) StartTime() (int64, error) {
	return int64(model.Latest), nil
}

func QueryableClient(c *Client) Queryable {
	return QueryableFunc(func(ctx context.Context) (Querier, error) {
		return &querier{
			ctx:    ctx,
			client: c,
		}, nil
	})
}

// querier is an adapter to make a Client usable as a remote.Querier.
type querier struct {
	ctx    context.Context
	client *Client
}

// Select implements remote.Querier and uses the given matchers to read series
// sets from the Client.
func (q *querier) Select(p *SelectParams) (SeriesSet, error) {
	if p.Step == 0 {
		res, err := q.client.QueryInstant(q.ctx, p.Query, p.Start)
		if err != nil {
			return nil, err
		}
		return FromInstantQueryResult(res), nil
	}

	res, err := q.client.QueryRange(q.ctx, p.Query, p.Start, p.End, int(p.Step))
	if err != nil {
		return nil, err
	}
	return FromRangeQueryResult(res), nil
}

// LabelValues implements remote.Querier and is a noop.
func (q *querier) LabelValues(name string) ([]string, error) {
	// TODO implement?
	return nil, nil
}

// Close implements remote.Querier and is a noop.
func (q *querier) Close() error {
	return nil
}
