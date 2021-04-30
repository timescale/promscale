package text

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/timescale/promscale/pkg/prompb"
)

var timeProvider = time.Now

// ParseRequest parses an incoming HTTP request as a Prometheus text format.
func ParseRequest(r *http.Request, wr *prompb.WriteRequest) error {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("error reading request body: %w", err)
	}

	var (
		p       = textparse.New(b, r.Header.Get("Content-Type"))
		defTime = int64(model.TimeFromUnixNano(timeProvider().UnixNano()))
		et      textparse.Entry
		ll      []prompb.Label
	)

	for {
		if et, err = p.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error parsing text entries: %w", err)
		}

		switch et {
		case textparse.EntryType,
			textparse.EntryHelp,
			textparse.EntryUnit,
			textparse.EntryComment:
			continue
		default:
		}

		t := defTime
		_, tp, v := p.Series()
		if tp != nil {
			t = *tp
		}

		var lset labels.Labels
		_ = p.Metric(&lset)

		ll = make([]prompb.Label, 0, len(lset))

		for i := range lset {
			ll = append(ll, prompb.Label{
				Name:  lset[i].Name,
				Value: lset[i].Value,
			})
		}

		wr.Timeseries = append(wr.Timeseries, prompb.TimeSeries{
			Labels: ll,
			Samples: []prompb.Sample{
				{
					Timestamp: t,
					Value:     v,
				},
			},
		})
	}

	return nil
}
