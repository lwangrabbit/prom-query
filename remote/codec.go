package remote

import (
	"fmt"
	"sort"

	"github.com/prometheus/common/model"

	"github.com/lwangrabbit/prom-query/pkg/labels"
	"github.com/lwangrabbit/prom-query/prompb"
)

// FromInstantQueryResult unpack a QueryResult proto.
func FromInstantQueryResult(res *InstantQueryResult) SeriesSet {
	if res.Status != "success" {
		return nil
	}
	if res.Data == nil || res.Data.Result == nil {
		return nil
	}
	v := res.Data.Result
	series := make([]Series, 0, len(*v))
	for _, s := range *v {
		labels := s.Metric
		if err := validateLabelsAndMetricName(labels); err != nil {
			return errSeriesSet{err: err}
		}
		sample := prompb.Sample{
			Value:     s.V,
			Timestamp: s.T,
		}
		series = append(series, &concreteSeries{
			labels:  labels,
			samples: []prompb.Sample{sample},
		})
	}
	sort.Sort(byLabel(series))
	return &concreteSeriesSet{
		series: series,
	}
}

// FromRangeQueryResult unpack a QueryResult proto.
func FromRangeQueryResult(res *RangeQueryResult) SeriesSet {
	if res.Status != "success" {
		return nil
	}
	if res.Data == nil || res.Data.Result == nil {
		return nil
	}
	v := res.Data.Result
	series := make([]Series, 0, len(*v))
	for _, s := range *v {
		labels := s.Metric
		if err := validateLabelsAndMetricName(labels); err != nil {
			return errSeriesSet{err: err}
		}
		samples := make([]prompb.Sample, len(s.Points))
		for _, pt := range s.Points {
			samples = append(samples, prompb.Sample{
				Value:     pt.V,
				Timestamp: pt.T,
			})
		}
		series = append(series, &concreteSeries{
			labels:  labels,
			samples: samples,
		})
	}
	sort.Sort(byLabel(series))
	return &concreteSeriesSet{
		series: series,
	}
}

type byLabel []Series

func (a byLabel) Len() int           { return len(a) }
func (a byLabel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byLabel) Less(i, j int) bool { return labels.Compare(a[i].Labels(), a[j].Labels()) < 0 }

func labelProtosToLabels(labelPairs []*prompb.Label) labels.Labels {
	result := make(labels.Labels, 0, len(labelPairs))
	for _, l := range labelPairs {
		result = append(result, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	sort.Sort(result)
	return result
}

// validateLabelsAndMetricName validates the label names/values and metric names returned from remote read.
func validateLabelsAndMetricName(ls labels.Labels) error {
	for _, l := range ls {
		if l.Name == labels.MetricName && !model.IsValidMetricName(model.LabelValue(l.Value)) {
			return fmt.Errorf("Invalid metric name: %v", l.Value)
		}
		if !model.LabelName(l.Name).IsValid() {
			return fmt.Errorf("Invalid label name: %v", l.Name)
		}
		if !model.LabelValue(l.Value).IsValid() {
			return fmt.Errorf("Invalid label value: %v", l.Value)
		}
	}
	return nil
}

// errSeriesSet implements remote.SeriesSet, just returning an error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool {
	return false
}

func (errSeriesSet) At() Series {
	return nil
}

func (e errSeriesSet) Err() error {
	return e.err
}

// concreteSeriesSet implements remote.SeriesSet.
type concreteSeriesSet struct {
	cur    int
	series []Series
}

func (c *concreteSeriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *concreteSeriesSet) At() Series {
	return c.series[c.cur-1]
}

func (c *concreteSeriesSet) Err() error {
	return nil
}

// concreteSeries implements remote.Series.
type concreteSeries struct {
	labels  labels.Labels
	samples []prompb.Sample
}

func (c *concreteSeries) Labels() labels.Labels {
	return labels.New(c.labels...)
}

func (c *concreteSeries) Iterator() SeriesIterator {
	return newConcreteSeriersIterator(c)
}

// concreteSeriesIterator implements remote.SeriesIterator.
type concreteSeriesIterator struct {
	cur    int
	series *concreteSeries
}

func newConcreteSeriersIterator(series *concreteSeries) SeriesIterator {
	return &concreteSeriesIterator{
		cur:    -1,
		series: series,
	}
}

// Seek implements remote.SeriesIterator.
func (c *concreteSeriesIterator) Seek(t int64) bool {
	c.cur = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].Timestamp >= t
	})
	return c.cur < len(c.series.samples)
}

// At implements remote.SeriesIterator.
func (c *concreteSeriesIterator) At() (t int64, v float64) {
	s := c.series.samples[c.cur]
	return s.Timestamp, s.Value
}

// Next implements remote.SeriesIterator.
func (c *concreteSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.samples)
}

// Err implements remote.SeriesIterator.
func (c *concreteSeriesIterator) Err() error {
	return nil
}
