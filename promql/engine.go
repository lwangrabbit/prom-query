package promql

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lwangrabbit/prom-query/pkg/gate"
	"github.com/lwangrabbit/prom-query/pkg/value"
	"github.com/lwangrabbit/prom-query/remote"
)

type (
	// ErrQueryTimeout is returned if a query timed out during processing.
	ErrQueryTimeout string
	// ErrQueryCanceled is returned if a query was canceled during processing.
	ErrQueryCanceled string
	// ErrTooManySamples is returned if a query would woud load more than the maximum allowed samples into memory.
	ErrTooManySamples string
	// ErrStorage is returned if an error was encountered in the storage layer
	// during query handling.
	ErrStorage error
)

func (e ErrQueryTimeout) Error() string {
	return fmt.Sprintf("query timed out in %s", string(e))
}
func (e ErrQueryCanceled) Error() string {
	return fmt.Sprintf("query was canceled in %s", string(e))
}
func (e ErrTooManySamples) Error() string {
	return fmt.Sprintf("query processing would load too many samples into memory in %s", string(e))
}

// query implements the Query interface.
type query struct {
	// Underlying data provider.
	queryable remote.Queryable
	// The original query string.
	params *remote.SelectParams
	// Result matrix for reuse.
	matrix value.Matrix
	// Cancellation function for the query.
	cancel func()

	// The engine against which the query is executed.
	ng *Engine
}

// Cancel implements the Query interface.
func (q *query) Cancel() {
	if q.cancel != nil {
		q.cancel()
	}
}

// Close implements the Query interface.
func (q *query) Close() {
	for _, s := range q.matrix {
		putPointSlice(s.Points)
	}
}

// Exec implements the Query interface.
func (q *query) Exec(ctx context.Context) *value.Result {
	res, err := q.ng.exec(ctx, q)
	return &value.Result{Err: err, Value: res}
}

// contextDone returns an error if the context was canceled or timed out.
func contextDone(ctx context.Context, env string) error {
	select {
	case <-ctx.Done():
		return contextErr(ctx.Err(), env)
	default:
		return nil
	}
}

func contextErr(err error, env string) error {
	switch err {
	case context.Canceled:
		return ErrQueryCanceled(env)
	case context.DeadlineExceeded:
		return ErrQueryTimeout(env)
	default:
		return err
	}
}

type Engine struct {
	timeout            time.Duration
	gate               *gate.Gate
	maxSamplesPerQuery int
}

type EngineOpts struct {
	MaxConcurrent int
	MaxSamples    int
	Timeout       time.Duration
}

func NewEngine(opts EngineOpts) *Engine {
	return &Engine{
		gate:               gate.New(opts.MaxConcurrent),
		timeout:            opts.Timeout,
		maxSamplesPerQuery: opts.MaxSamples,
	}
}

func (ng *Engine) NewQuery(q remote.Queryable, qs string, startTs, endTs int64, step int) *query {
	qry := &query{
		queryable: q,
		params: &remote.SelectParams{
			Query: qs,
			Start: startTs,
			End:   endTs,
			Step:  int64(step),
		},
		ng: ng,
	}
	return qry
}

// exec excutes the query.
func (ng *Engine) exec(ctx context.Context, q *query) (value.Value, error) {
	if err := ng.gate.Start(ctx); err != nil {
		return nil, contextErr(err, "query queue")
	}
	defer ng.gate.Done()

	series, err := ng.populateSeries(ctx, q.queryable, q.params)
	if err != nil {
		return nil, err
	}

	if q.params.Start == q.params.End && q.params.Step == 0 {
		start := q.params.Start
		evaluator := &evaluator{
			startTimestamp: start,
			endTimestamp:   start,
			interval:       1,
			ctx:            ctx,
			maxSamples:     ng.maxSamplesPerQuery,
		}
		val := evaluator.eval(series)
		mat, ok := val.(value.Matrix)
		if !ok {
			panic(fmt.Errorf("promql.Engine.exec: invalid expression type %q", val.Type()))
		}
		q.matrix = mat
		// Convert matrix with one value per series into vector.
		vector := make(value.Vector, len(mat))
		for i, s := range mat {
			// Point might have a different timestamp, force it to the evaluation
			// timestamp as that is when we ran the evaluation.
			vector[i] = value.Sample{Metric: s.Metric, Point: value.Point{V: s.Points[0].V, T: start}}
		}
		return vector, nil
	}

	// Range evaluation
	evaluator := &evaluator{
		startTimestamp: q.params.Start,
		endTimestamp:   q.params.End,
		interval:       q.params.Step,
		ctx:            ctx,
		maxSamples:     ng.maxSamplesPerQuery,
	}
	val := evaluator.eval(series)
	if err != nil {
		return nil, err
	}
	mat, ok := val.(value.Matrix)
	if !ok {
		panic(fmt.Errorf("promql.Engine.exec: invalid expression type %q", val.Type()))
	}
	q.matrix = mat
	if err := contextDone(ctx, "expression evaluation"); err != nil {
		return nil, err
	}
	sort.Sort(mat)
	return mat, nil
}

func (ng *Engine) populateSeries(ctx context.Context, q remote.Queryable, params *remote.SelectParams) ([]remote.Series, error) {
	queries, err := q.Querier(ctx)
	if err != nil {
		return nil, err
	}
	set, err := queries.Select(params)
	if err != nil {
		return nil, err
	}
	ret, err := expandSeriesSet(ctx, set)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func expandSeriesSet(ctx context.Context, it remote.SeriesSet) (res []remote.Series, err error) {
	for it.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		res = append(res, it.At())
	}
	return res, it.Err()
}

var pointPool = sync.Pool{}

func getPointSlice(sz int) []value.Point {
	p := pointPool.Get()
	if p != nil {
		return p.([]value.Point)
	}
	return make([]value.Point, 0, sz)
}

func putPointSlice(p []value.Point) {
	pointPool.Put(p[:0])
}

type evaluator struct {
	ctx context.Context

	startTimestamp int64
	endTimestamp   int64
	interval       int64

	maxSamples     int
	currentSamples int
}

func (ev *evaluator) eval(series []remote.Series) value.Value {
	numSteps := int((ev.endTimestamp-ev.startTimestamp)/ev.interval) + 1
	mat := make(value.Matrix, 0, len(series))
	it := remote.NewBuffer(durationSeconds(LookbackDelta))
	for _, s := range series {
		it.Reset(s.Iterator())
		ss := value.Series{
			Metric: s.Labels(),
			Points: getPointSlice(numSteps),
		}

		for ts := ev.startTimestamp; ts <= ev.endTimestamp; ts += ev.interval {
			_, v, ok := ev.vectorSelectorSingle(it, ts)
			if ok {
				if ev.currentSamples < ev.maxSamples {
					ss.Points = append(ss.Points, value.Point{V: v, T: ts})
					ev.currentSamples++
				} else {
					ev.error(ErrTooManySamples("query execution"))
				}
			}
		}

		if len(ss.Points) > 0 {
			mat = append(mat, ss)
		}
	}
	return mat
}

// vectorSelectorSingle evaluates a instant vector for the iterator of one time series.
func (ev *evaluator) vectorSelectorSingle(it *remote.BufferedSeriesIterator, ts int64) (int64, float64, bool) {
	refTime := ts
	var t int64
	var v float64

	ok := it.Seek(refTime)
	if !ok {
		if it.Err() != nil {
			ev.error(it.Err())
		}
	}

	if ok {
		t, v = it.Values()
	}

	if !ok || t > refTime {
		t, v, ok = it.PeekBack(1)
		if !ok || t < refTime-durationSeconds(LookbackDelta) {
			return 0, 0, false
		}
	}
	if value.IsStaleNaN(v) {
		return 0, 0, false
	}
	return t, v, true
}

// errorf causes a panic with the input formatted into an error.
func (ev *evaluator) errorf(format string, args ...interface{}) {
	ev.error(fmt.Errorf(format, args...))
}

// error causes a panic with the given error.
func (ev *evaluator) error(err error) {
	panic(err)
}

func durationSeconds(d time.Duration) int64 {
	return int64(d / (time.Second / time.Nanosecond))
}

const (
	DefaultLookbackDelta = 5 * time.Minute
)

// LookbackDelta determines the time since the last sample after which a time
// series is considered stale.
var LookbackDelta = DefaultLookbackDelta
