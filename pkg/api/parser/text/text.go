package text

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/util"
)

var timeProvider = time.Now

// ParseRequest parses an incoming HTTP request as a Prometheus text format.
func ParseRequest(r *http.Request, wr *prompb.WriteRequest) error {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("error reading request body: %w", err)
	}

	var (
		et      textparse.Entry
		defTime = int64(model.TimeFromUnixNano(timeProvider().UnixNano()))
	)

	p, err := textparse.New(b, r.Header.Get("Content-Type"))
	if err != nil {
		return fmt.Errorf("parsing contents from request body: %w", err)
	}

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

		ll := util.LabelToPrompbLabels(lset)

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
