// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"errors"

	"github.com/lwangrabbit/prom-query/pkg/labels"
)

// The errors exposed.
var (
	ErrNotFound                    = errors.New("not found")
	ErrOutOfOrderSample            = errors.New("out of order sample")
	ErrDuplicateSampleForTimestamp = errors.New("duplicate sample for timestamp")
	ErrOutOfBounds                 = errors.New("out of bounds")
)

// A Queryable handles queries against a remote.
type Queryable interface {
	// Querier returns a new Querier on the remote.
	Querier(ctx context.Context) (Querier, error)
}

// Querier provides reading access to time series data.
type Querier interface {
	// Select returns a set of series that matches the given label matchers.
	Select(*SelectParams) (SeriesSet, error)

	// LabelValues returns all potential values for a label name.
	LabelValues(name string) ([]string, error)

	// Close releases the resources of the Querier.
	Close() error
}

// SelectParams specifies parameters passed to data selections.
type SelectParams struct {
	Query string // Query string
	Start int64  // Start time for this select.
	End   int64  // End time for this select.
	Step  int64  // Query step size.
}

// QueryableFunc is an adapter to allow the use of ordinary functions as
// Queryables. It follows the idea of http.HandlerFunc.
type QueryableFunc func(ctx context.Context) (Querier, error)

// Querier calls f() with the given parameters.
func (f QueryableFunc) Querier(ctx context.Context) (Querier, error) {
	return f(ctx)
}

// SeriesSet contains a set of series.
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

// Series represents a single time series.
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	Labels() labels.Labels

	// Iterator returns a new iterator of the data of the series.
	Iterator() SeriesIterator
}

// SeriesIterator iterates over the data of a time series.
type SeriesIterator interface {
	// Seek advances the iterator forward to the value at or after
	// the given timestamp.
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	At() (t int64, v float64)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}
