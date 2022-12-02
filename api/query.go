package api

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/lwangrabbit/prom-query/promql"

	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"

	"github.com/lwangrabbit/prom-query/pkg/value"
	"github.com/lwangrabbit/prom-query/remote"
)

var (
	queryEngine  *promql.Engine
	remoteReader *remote.Reader
)

const (
	DefaultQueryMaxConcurrency = 20
	DefaultQueryMaxSamples     = 50000000
	DefaultQueryTimeout        = 2 * time.Minute
)

type ReadConfig struct {
	URL     string
	Timeout time.Duration
}

func Init(configs []*ReadConfig) error {
	engineOpts := promql.EngineOpts{
		MaxConcurrent: DefaultQueryMaxConcurrency,
		MaxSamples:    DefaultQueryMaxSamples,
		Timeout:       DefaultQueryTimeout,
	}
	queryEngine = promql.NewEngine(engineOpts)

	var err error
	var rConfs = make([]*remote.ReadConfig, 0, len(configs))
	for _, conf := range configs {
		u, err := url.Parse(conf.URL)
		if err != nil {
			return err
		}
		rconf := &remote.ReadConfig{
			URL:     &config_util.URL{URL: u},
			Timeout: model.Duration(conf.Timeout),
			Name:    fmt.Sprintf("promql-read-%v", conf.URL),
		}
		rConfs = append(rConfs, rconf)
	}
	remoteReader, err = remote.NewReader(rConfs)
	if err != nil {
		return err
	}
	return nil
}

func Query(query string) (*QueryResult, error) {
	ts := time.Now().Unix()
	qry := queryEngine.NewQuery(remoteReader, query, ts, ts, 0)
	ctx, cancal := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	defer cancal()
	res := qry.Exec(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	return &QueryResult{
		Data: &QueryData{
			ResultType: res.Value.Type(),
			Result:     res.Value,
		},
		Status: "success",
	}, nil
}

func QueryRange(query string, startTs, endTs int64, step int) (*QueryResult, error) {
	qry := queryEngine.NewQuery(remoteReader, query, startTs, endTs, step)
	ctx, cancal := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	defer cancal()
	res := qry.Exec(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	return &QueryResult{
		Data: &QueryData{
			ResultType: res.Value.Type(),
			Result:     res.Value,
		},
		Status: "success",
	}, nil
}

type QueryResult struct {
	Data   *QueryData `json:"data"`
	Status string     `json:"status"`
}
type QueryData struct {
	ResultType value.ValueType `json:"resultType"`
	Result     value.Value     `json:"result"`
}
