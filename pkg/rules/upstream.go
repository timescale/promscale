// This file contains code copied from
// https://github.com/prometheus/prometheus/blob/344b272b3eea8348f0b1653eae3afd43833c861d/rules/recording.go#L1

package rules

import (
	"context"
	"fmt"
	"net/http"
	"time"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/notifier"
	prometheus_promql "github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/util/strutil"

	promscale_promql "github.com/timescale/promscale/pkg/promql"
)

// engineQueryFunc returns a new query function that executes instant queries against
// the given engine.
// It converts scalar into vector results.
// Note: This function is copied from the link given in the starting of the file and modified
// to adapt to Promscale's PromQL engine.
func engineQueryFunc(engine *promscale_promql.Engine, q promscale_promql.Queryable) rules.QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (prometheus_promql.Vector, error) {
		q, err := engine.NewInstantQuery(q, nil, qs, t)
		if err != nil {
			return nil, err
		}
		res := q.Exec(ctx)
		if res.Err != nil {
			return nil, res.Err
		}
		switch v := res.Value.(type) {
		case promscale_promql.Vector:
			return yoloVector(&v), nil
		case promscale_promql.Scalar:
			return prometheus_promql.Vector{prometheus_promql.Sample{
				Point:  prometheus_promql.Point(v),
				Metric: labels.Labels{},
			}}, nil
		default:
			return nil, fmt.Errorf("rule result is not a vector or scalar")
		}
	}
}

// Note: This is a compile-time test to make sure that sizes of both vectors are the same.
// If the upstream vector changes, then this will block compilation. This protects the yoloVector
// function from breaking with unexpected changes in Prometheus mod version.
// We also have a unit test for this that compares the equality of these two structs.
var _ = [1]bool{}[unsafe.Sizeof(promscale_promql.Vector{})-unsafe.Sizeof(prometheus_promql.Vector{})] // #nosec

// My guess is this should be way faster than looping through individual samples
// and converting into Prometheus Vector type. This lets us convert the type with
// very less processing.
func yoloVector(v *promscale_promql.Vector) prometheus_promql.Vector {
	return *(*prometheus_promql.Vector)(unsafe.Pointer(v)) // #nosec
}

type sender interface {
	Send(alerts ...*notifier.Alert)
}

func sendAlerts(s sender, externalURL string) rules.NotifyFunc {
	return func(ctx context.Context, expr string, alerts ...*rules.Alert) {
		var res []*notifier.Alert

		for _, alert := range alerts {
			a := &notifier.Alert{
				StartsAt:     alert.FiredAt,
				Labels:       alert.Labels,
				Annotations:  alert.Annotations,
				GeneratorURL: externalURL + strutil.TableLinkForExpression(expr),
			}
			a.EndsAt = alert.ResolvedAt
			if alert.ResolvedAt.IsZero() {
				a.EndsAt = alert.ValidUntil
			}
			res = append(res, a)
		}

		if len(alerts) > 0 {
			s.Send(res...)
		}
	}
}

func do(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req.WithContext(ctx))
}
