// This file contains code copied from
// https://github.com/prometheus/prometheus/blob/3fc7d1168718c87e0534f80bfc42a832802f5c46/rules/manager.go#L186

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
		q, err := engine.NewInstantQuery(q, qs, t)
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

// My guess is this should be way faster than looping through individual samples
// and converting into Prometheus Vector type. This lets us convert the type with
// very less processing.
func yoloVector(v *promscale_promql.Vector) prometheus_promql.Vector {
	return *(*prometheus_promql.Vector)(unsafe.Pointer(v))
}

type sender interface {
	Send(alerts ...*notifier.Alert)
}

// sendAlerts implements the rules.NotifyFunc for a Notifier.
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
			if !alert.ResolvedAt.IsZero() {
				a.EndsAt = alert.ResolvedAt
			} else {
				a.EndsAt = alert.ValidUntil
			}
			res = append(res, a)
		}

		if len(alerts) > 0 {
			fmt.Println("sending alerts")
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
