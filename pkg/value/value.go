// Copyright 2016 The Prometheus Authors
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

package value

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/lwangrabbit/prom-query/pkg/labels"
)

const (
	// NormalNaN is a quiet NaN. This is also math.NaN().
	NormalNaN uint64 = 0x7ff8000000000001

	// StaleNaN is a signalling NaN, due to the MSB of the mantissa being 0.
	// This value is chosen with many leading 0s, so we have scope to store more
	// complicated values in the future. It is 2 rather than 1 to make
	// it easier to distinguish from the NormalNaN by a human when debugging.
	StaleNaN uint64 = 0x7ff0000000000002
)

// IsStaleNaN returns true when the provided NaN value is a stale marker.
func IsStaleNaN(v float64) bool {
	return math.Float64bits(v) == StaleNaN
}

// Value is a generic interface for values resulting from a query evaluation.
type Value interface {
	Type() ValueType
	String() string
}

func (Matrix) Type() ValueType { return ValueTypeMatrix }
func (Vector) Type() ValueType { return ValueTypeVector }
func (Scalar) Type() ValueType { return ValueTypeScalar }
func (String) Type() ValueType { return ValueTypeString }

// ValueType describes a type of a value.
type ValueType string

// The valid value types.
const (
	ValueTypeNone   = "none"
	ValueTypeVector = "vector"
	ValueTypeScalar = "scalar"
	ValueTypeMatrix = "matrix"
	ValueTypeString = "string"
)

// String represents a string value.
type String struct {
	T int64
	V string
}

func (s String) String() string {
	return s.V
}

func (s String) MarshalJSON() ([]byte, error) {
	return json.Marshal([...]interface{}{float64(s.T) / 1000, s.V})
}

// Scalar is a data point that's explicitly not associated with a metric.
type Scalar struct {
	T int64
	V float64
}

func (s Scalar) String() string {
	v := strconv.FormatFloat(s.V, 'f', -1, 64)
	return fmt.Sprintf("scalar: %v @[%v]", v, s.T)
}

func (s Scalar) MarshalJSON() ([]byte, error) {
	v := strconv.FormatFloat(s.V, 'f', -1, 64)
	return json.Marshal([...]interface{}{float64(s.T) / 1000, v})
}

// Series is a stream of data points belonging to a metric.
type Series struct {
	Metric labels.Labels `json:"metric"`
	Points []Point       `json:"values"`
}

func (s Series) String() string {
	vals := make([]string, len(s.Points))
	for i, v := range s.Points {
		vals[i] = v.String()
	}
	return fmt.Sprintf("%s =>\n%s", s.Metric, strings.Join(vals, "\n"))
}

// Point represents a single data point for a given timestamp.
type Point struct {
	T int64
	V float64
}

func (p Point) String() string {
	v := strconv.FormatFloat(p.V, 'f', -1, 64)
	return fmt.Sprintf("%v @[%v]", v, p.T)
}

// MarshalJSON implements json.Marshaler.
func (p Point) MarshalJSON() ([]byte, error) {
	v := strconv.FormatFloat(p.V, 'f', -1, 64)
	return json.Marshal([...]interface{}{float64(p.T), v})
}

func (p *Point) UnmarshalJSON(b []byte) error {
	var v []interface{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	if len(v) < 2 {
		return errors.New("point unmarshal err: len<2")
	}
	ts, ok := v[0].(float64)
	if !ok {
		return errors.New("point unmarshal err: ts format err")
	}
	vs, ok := v[1].(string)
	if !ok {
		return errors.New("point unmsarshal err: value format err")
	}
	vf, err := strconv.ParseFloat(vs, 64)
	if err != nil {
		return errors.New("point unmarshal err: float format err")
	}
	p.T = int64(ts)
	p.V = vf
	return nil
}

// Sample is a single sample belonging to a metric.
type Sample struct {
	Point

	Metric labels.Labels
}

func (s Sample) String() string {
	return fmt.Sprintf("%s => %s", s.Metric, s.Point)
}

func (s Sample) MarshalJSON() ([]byte, error) {
	v := struct {
		M labels.Labels `json:"metric"`
		V Point         `json:"value"`
	}{
		M: s.Metric,
		V: s.Point,
	}
	return json.Marshal(v)
}

func (s *Sample) UnmarshalJSON(b []byte) error {
	v := struct {
		M labels.Labels `json:"metric"`
		V Point         `json:"value"`
	}{}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return err
	}
	s.Metric = v.M
	s.Point = v.V
	return nil
}

// Vector is basically only an alias for model.Samples, but the
// contract is that in a Vector, all Samples have the same timestamp.
type Vector []Sample

func (vec Vector) String() string {
	entries := make([]string, len(vec))
	for i, s := range vec {
		entries[i] = s.String()
	}
	return strings.Join(entries, "\n")
}

func (v *Vector) UnmarshalJSON(b []byte) error {
	var data []Sample
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&data); err != nil {
		return err
	}
	*v = data
	return nil
}

// ContainsSameLabelset checks if a vector has samples with the same labelset
// Such a behaviour is semantically undefined
// https://github.com/prometheus/prometheus/issues/4562
func (vec Vector) ContainsSameLabelset() bool {
	l := make(map[uint64]struct{}, len(vec))
	for _, s := range vec {
		hash := s.Metric.Hash()
		if _, ok := l[hash]; ok {
			return true
		}
		l[hash] = struct{}{}
	}
	return false
}

// Matrix is a slice of Seriess that implements sort.Interface and
// has a String method.
type Matrix []Series

func (m Matrix) String() string {
	// TODO(fabxc): sort, or can we rely on order from the querier?
	strs := make([]string, len(m))

	for i, ss := range m {
		strs[i] = ss.String()
	}

	return strings.Join(strs, "\n")
}

// TotalSamples returns the total number of samples in the series within a matrix.
func (m Matrix) TotalSamples() int {
	numSamples := 0
	for _, series := range m {
		numSamples += len(series.Points)
	}
	return numSamples
}

func (m Matrix) Len() int           { return len(m) }
func (m Matrix) Less(i, j int) bool { return labels.Compare(m[i].Metric, m[j].Metric) < 0 }
func (m Matrix) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

// ContainsSameLabelset checks if a matrix has samples with the same labelset
// Such a behaviour is semantically undefined
// https://github.com/prometheus/prometheus/issues/4562
func (m Matrix) ContainsSameLabelset() bool {
	l := make(map[uint64]struct{}, len(m))
	for _, ss := range m {
		hash := ss.Metric.Hash()
		if _, ok := l[hash]; ok {
			return true
		}
		l[hash] = struct{}{}
	}
	return false
}

// Result holds the resulting value of an execution or an error
// if any occurred.
type Result struct {
	Err   error
	Value Value
}

// Vector returns a Vector if the result value is one. An error is returned if
// the result was an error or the result value is not a Vector.
func (r *Result) Vector() (Vector, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	v, ok := r.Value.(Vector)
	if !ok {
		return nil, fmt.Errorf("query result is not a Vector")
	}
	return v, nil
}

// Matrix returns a Matrix. An error is returned if
// the result was an error or the result value is not a Matrix.
func (r *Result) Matrix() (Matrix, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	v, ok := r.Value.(Matrix)
	if !ok {
		return nil, fmt.Errorf("query result is not a range Vector")
	}
	return v, nil
}

// Scalar returns a Scalar value. An error is returned if
// the result was an error or the result value is not a Scalar.
func (r *Result) Scalar() (Scalar, error) {
	if r.Err != nil {
		return Scalar{}, r.Err
	}
	v, ok := r.Value.(Scalar)
	if !ok {
		return Scalar{}, fmt.Errorf("query result is not a Scalar")
	}
	return v, nil
}

func (r *Result) String() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	if r.Value == nil {
		return ""
	}
	return r.Value.String()
}
